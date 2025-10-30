import os
import json
import time
from datetime import datetime, timezone
from itertools import tee

from azure.identity import DefaultAzureCredential, ClientSecretCredential
from azure.ai.documentintelligence import DocumentIntelligenceClient
from azure.ai.documentintelligence.models import AnalyzeDocumentRequest
from azure.storage.blob import BlobServiceClient
from azure.core.credentials import AzureKeyCredential

PROGRESS_FILE = "progress_processed.txt"

# ------------------------------------------------------------
# helpers to serialize DI polygons / bounding regions
# ------------------------------------------------------------
def serialize(obj):
    # If object is BoundingRegion-like
    if hasattr(obj, "page_number") and hasattr(obj, "polygon"):
        return {
            "page_number": obj.page_number,
            "polygon": serialize(obj.polygon),
        }
    # If object is list of points
    elif isinstance(obj, list) and all(hasattr(p, "x") and hasattr(p, "y") for p in obj):
        # just return raw points; json.dumps will fail if you leave Point objects,
        # so convert to dicts
        return [{"x": p.x, "y": p.y} for p in obj]
    # If it's a JSON-native type, return as-is
    elif isinstance(obj, (dict, list, str, int, float, bool, type(None))):
        return obj
    else:
        raise TypeError(f"Object of type '{type(obj).__name__}' is not JSON serializable")


# ------------------------------------------------------------
# your extraction helpers (kept as-is, just formatting)
# ------------------------------------------------------------
def content_field(value):
    result_content = {
        "content": value["content"],
        "confidence": value["confidence"],
    }
    for region in value.bounding_regions:
        result_content["polygon"] = region["polygon"]
    return result_content


def extract_object_array(field, dict_data):
    result_list = []
    for data in dict_data:
        holder = {field: data}
        holder_dict = extract_object(holder)
        for _, holder_value in holder_dict.items():
            result_list.append(holder_value)
    return result_list


def extract_object(dict_data):
    result_dict_data = {}
    for field, attributes in dict_data.items():
        dict_data_type = attributes["type"]
        attribute_check_list = attributes.keys()

        if dict_data_type == "object":
            if "content" in attribute_check_list or "valueObject" in attribute_check_list:
                inner_object = attributes["valueObject"]
                result_dict_data[field] = extract_object(inner_object)

        elif dict_data_type == "array":
            if "content" in attribute_check_list or "valueArray" in attribute_check_list:
                inner_object = attributes["valueArray"]
                result_dict_data[field] = {"item": extract_object_array(field, inner_object)}

        else:
            if "content" in attribute_check_list:
                result_dict_data[field] = content_field(attributes)

    return result_dict_data


# ------------------------------------------------------------
# progress file helpers
# ------------------------------------------------------------
def read_processed_from_progress(path: str = PROGRESS_FILE) -> set[str]:
    """
    Read progress_processed.txt and return a set of PDF/blob names that
    were successfully processed (status == 'OK').

    Expected line format:
        mode::BLOB_NAME\tpages\ttimestamp\tSTATUS
    """
    done = set()
    if not os.path.exists(path):
        return done

    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            parts = line.split("\t")
            if len(parts) < 4:
                continue
            left = parts[0]  # e.g. "aad::pdf/myfile.pdf"
            status = parts[3]
            if status != "OK":
                continue
            mode_and_name = left.split("::", 1)
            if len(mode_and_name) != 2:
                continue
            blob_name = mode_and_name[1]
            done.add(blob_name)
    return done


def log_progress(mode: str, blob_name: str, pages: int, status: str, path: str = PROGRESS_FILE):
    ts = datetime.now(timezone.utc).isoformat()
    line = f"{mode}::{blob_name}\t{pages}\t{ts}\t{status}\n"
    with open(path, "a", encoding="utf-8") as f:
        f.write(line)


# ------------------------------------------------------------
# config / clients
# ------------------------------------------------------------
def load_connections_config() -> dict:
    """
    Load ../connections.json relative to this file (since your script
    lives in python/ and connections.json is one level up).
    """
    this_dir = os.path.dirname(os.path.abspath(__file__))
    root_dir = os.path.dirname(this_dir)
    config_path = os.path.join(root_dir, "connections.json")
    with open(config_path, "r", encoding="utf-8") as f:
        return json.load(f)


def build_di_client_from_config(cfg: dict) -> tuple[DocumentIntelligenceClient, str]:
    """
    Build a Document Intelligence client.
    Priority:
    1. If tenant/client/secret are present -> AAD (mode 'aad')
    2. elif DI-Key present -> key (mode 'azure-key')
    3. else -> DefaultAzureCredential (mode 'aad')
    """
    endpoint = cfg.get("DI-Endpoint") or cfg.get("DI_Endpoint")
    if not endpoint:
        raise ValueError("connections.json missing DI-Endpoint / DI_Endpoint")

    tenant_id = cfg.get("tenant_id")
    client_id = cfg.get("client_id")
    client_secret = cfg.get("client_secret")

    if tenant_id and client_id and client_secret:
        credential = ClientSecretCredential(tenant_id, client_id, client_secret)
        mode = "aad"
    elif "DI-Key" in cfg:
        credential = AzureKeyCredential(cfg["DI-Key"])
        mode = "azure-key"
    else:
        credential = DefaultAzureCredential(exclude_interactive_browser_credential=False)
        mode = "aad"

    client = DocumentIntelligenceClient(endpoint=endpoint.rstrip("/"), credential=credential)
    return client, mode


def build_blob_client_from_config(cfg: dict, container_name: str):
    account_url = (
        cfg.get("SA-endpoint")
        or cfg.get("storage_account_url")
        or cfg.get("SA_endpoint")
    )
    if not account_url:
        raise ValueError("connections.json missing storage account url (SA-endpoint).")

    cred = DefaultAzureCredential()
    blob_service = BlobServiceClient(account_url=account_url, credential=cred)
    return blob_service.get_container_client(container_name)


# ------------------------------------------------------------
# azure poller builder
# ------------------------------------------------------------
def poller_list_create(
    document_analysis_client: DocumentIntelligenceClient,
    container_client,
    list_of_blob,
    processed_blob_set: set[str],
    mode: str,
    extraction_model_id: str,
):
    pollers: list = []
    targets: list[str] = []

    start_time = datetime.now()
    for blob_name in list_of_blob:
        # we only care about pdfs
        if not blob_name.endswith(".pdf"):
            continue

        # skip if processed (from progress file)
        if blob_name in processed_blob_set:
            continue

        blob_client = container_client.get_blob_client(blob_name)
        blob_url = blob_client.url

        # we don't know page count yet -> 0
        log_progress(mode, blob_name, 0, "START")

        try:
            poller = document_analysis_client.begin_analyze_document(
                extraction_model_id,
                AnalyzeDocumentRequest(url_source=blob_url),
            )
            pollers.append(poller)

            # e.g. pdf/myfile.pdf -> myfile.json
            if "/" in blob_name:
                output_name = blob_name.split("/", 1)[1][:-4] + ".json"
            else:
                output_name = blob_name[:-4] + ".json"

            targets.append((blob_name, output_name))
            print(".", end="", flush=True)
        except Exception as e:
            # record failure to start
            log_progress(mode, blob_name, 0, f"ERROR:{type(e).__name__}")
            print(f"\nencountered error -- skipping: {blob_name} ({e})")
            continue

    secs = (datetime.now() - start_time).total_seconds()
    print(f"\nPL TIME: {secs} secs.", flush=True)
    return pollers, targets


# ------------------------------------------------------------
# write result (local + optional azure)
# ------------------------------------------------------------
def print_analyzed_contents(
    result,
    file_name: str,
    output_azure: bool = False,
    output_dir=None,
    container_client=None,
    output_container_name: str | None = None,
):
    """
    Write analyzed_data to local json/<container>/file_name
    and optionally upload to Azure Blob.
    """
    # ---- turn DI result into your dict structure ----
    # result is an AnalyzeResult object, not a dict
    analyzed_data: dict = {}
    docs = getattr(result, "documents", None)
    if docs:
        for doc in docs:
            if not getattr(doc, "fields", None):
                continue
            # doc.fields is a dict-like of FieldValue objects
            # your extract_object() expects dict in DI REST shape
            # so we convert-ish by using doc.fields._to_generated()
            generated_fields = doc.fields  # type: ignore
            doc_data_analyzed = extract_object(generated_fields)
            for field, value in doc_data_analyzed.items():
                analyzed_data[field] = value

    # 1. local path
    if output_dir is None:
        target_directory = os.path.join(os.getcwd(), "json", output_container_name or "default")
    else:
        target_directory = output_dir
    os.makedirs(target_directory, exist_ok=True)
    output_file = os.path.join(target_directory, file_name)

    # 2. to JSON string
    json_data = json.dumps(analyzed_data, default=serialize, indent=4)

    # 3. write local
    with open(output_file, "w", encoding="utf-8") as f:
        f.write(json_data)

    # 4. optional upload
    if output_azure:
        if container_client is None:
            raise ValueError("output_azure=True but container_client is None")

        with open(output_file, "rb") as data:
            container_client.upload_blob(name=file_name, data=data, overwrite=True)


# ------------------------------------------------------------
# local mode
# ------------------------------------------------------------
def analyze_local_documents(input_dir: str, output_dir: str, extraction_model_id: str):
    """
    Run Document Intelligence locally against PDFs in input_dir.
    Outputs JSONs into output_dir.
    """
    cfg = load_connections_config()
    di_client, mode = build_di_client_from_config(cfg)

    os.makedirs(output_dir, exist_ok=True)

    processed = read_processed_from_progress()

    for fname in sorted(os.listdir(input_dir)):
        if not fname.lower().endswith(".pdf"):
            continue

        pdf_path = os.path.join(input_dir, fname)

        # progress file uses just the filename in local mode
        if fname in processed:
            continue

        log_progress(mode, fname, 0, "START")
        try:
            with open(pdf_path, "rb") as f:
                poller = di_client.begin_analyze_document(
                    extraction_model_id, f, content_type="application/pdf"
                )
                result = poller.result()

            json_name = os.path.splitext(fname)[0] + ".json"

            print_analyzed_contents(
                result,
                json_name,
                output_azure=False,
                output_dir=output_dir,
            )

            log_progress(mode, fname, 0, "OK")
            print(f"[OK] {fname} -> {os.path.join(output_dir, json_name)}")
        except Exception as e:
            log_progress(mode, fname, 0, f"ERROR:{type(e).__name__}")
            print(f"[ERR] {fname}: {e}")


# ------------------------------------------------------------
# azure mode
# ------------------------------------------------------------
def analyze_azure_documents(container_name: str, extraction_model_id: str):
    cfg = load_connections_config()

    # 1) clients
    di_client, mode = build_di_client_from_config(cfg)
    container_client = build_blob_client_from_config(cfg, container_name)

    # 2) output container (create once if uploading results)
    # use a CLEAN name, not "/update_jsons"
    output_container_name = cfg.get("output_container_name") or "update_jsons"
    account_url = (
        cfg.get("SA-endpoint")
        or cfg.get("storage_account_url")
        or cfg.get("SA_endpoint")
    )
    output_container_client = BlobServiceClient(
        account_url=account_url,
        credential=DefaultAzureCredential()
    ).get_container_client(output_container_name)

    # 3) already done set
    processed_blob_set = read_processed_from_progress()

    # 4) build pollers
    blob_list = container_client.list_blob_names()
    poller_list, targets = poller_list_create(
        di_client,
        container_client,
        blob_list,
        processed_blob_set,
        mode,
        extraction_model_id,
    )

    # 5) run pollers + write/upload results
    for poller, (orig_blob_name, json_name) in zip(poller_list, targets):
        try:
            result = poller.result()
            print_analyzed_contents(
                result,
                file_name=json_name,
                output_azure=True,
                container_client=output_container_client,
                output_container_name=output_container_name,
            )
            log_progress(mode, orig_blob_name, 0, "OK")
        except Exception as e:
            log_progress(mode, orig_blob_name, 0, f"ERROR:{type(e).__name__}")


# ------------------------------------------------------------
# entrypoint
# ------------------------------------------------------------
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="WCVI DI Schema Extractor")
    parser.add_argument("--mode", choices=["local", "azure"], default="local",
                        help="Run locally on PDFs or on Azure blobs (default: local)")
    parser.add_argument("--input_dir", default="./pdf/test",
                        help="Local input directory for PDF files (used in local mode)")
    parser.add_argument("--output_dir", default="./json/update_jsons",
                        help="Local output directory for JSON files (used in local mode)")
    parser.add_argument("--container", default=None,
                        help="Azure container name for PDFs (used in azure mode)")
    parser.add_argument("--model", default="prebuilt-layout",
                        help="Model ID for DI extraction (default: prebuilt-layout)")
    args = parser.parse_args()

    # we just need it for both modes; keeps the "file one level up" check honest
    _ = load_connections_config()

    match args.mode:
        case "local":
            print(f"Running LOCAL mode\nInput: {args.input_dir}\nOutput: {args.output_dir}")
            analyze_local_documents(
                input_dir=args.input_dir,
                output_dir=args.output_dir,
                extraction_model_id=args.model,
            )
        case "azure":
            print("Running AZURE mode")
            container_name = args.container or "raw_pdf"
            analyze_azure_documents(
                container_name=container_name,
                extraction_model_id=args.model,
            )
