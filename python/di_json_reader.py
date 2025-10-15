#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
AAD-only Azure Document Intelligence reader with persistent progress tracking.

Features
--------
- Two modes:
  1) local: read PDFs from a local folder (default: ./tempdata)
  2) azure: list & download PDFs from a blob container using AAD (no keys/SAS)

- Any DI model ID (e.g., 'prebuilt-read', 'prebuilt-document', or custom)

- Progress tracking:
  * Stored in a tab-separated text file (default: ./progress_processed.txt)
  * Format per successful file: "<mode>::<id>\t<PAGES>\t<ISO8601>\tOK"
  * 'id' is filename (local) or "container/blobname" (azure)
  * Without --reset, already-processed files are skipped
  * With --reset, the progress file is removed and everything is re-run

- PDF-only enforcement (skips non-PDF silently)

- Page-count check from a single DI run (no re-run); zero pages -> treated as error

- Output:
  * Writes JSON to ./json/<output-container>/<basename>.json
  * Optional: re-uploads JSON to Azure Blob with AAD (no keys/SAS)

Config (connections.json)
-------------------------
{
  "DI-Key": "YOUR_DI_KEY",
  "DI-Endpoint": "https://<your-di>.cognitiveservices.azure.com/",
  "SA-endpoint": "https://<yourstorage>.blob.core.windows.net/"
}
"""

from __future__ import annotations

import argparse
import json
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Set, Tuple

# Azure DI SDK (v4)
from azure.ai.documentintelligence import DocumentIntelligenceClient
from azure.core.credentials import AzureKeyCredential

# Azure Storage (AAD)
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, ContainerClient, BlobClient

# -----------------------------------------------------------------------------
# Configuration Models
# -----------------------------------------------------------------------------

@dataclass
class ConnectionsConfig:
    di_endpoint: str
    di_key: str
    storage_account_url: str

@dataclass
class RunOptions:
    mode: str                          # "local" or "azure"
    model_id: str                      # DI model ID
    local_directory: str               # local folder
    container_name: str                # azure: source container
    max_pdfs: int | None               # azure: cap number of PDFs
    upload_output: bool                # upload JSON back to blob
    output_container: str              # azure: target container for outputs
    tps_limit: int                     # azure: result collection throttle
    progress_file: str                 # path to progress file (txt/tsv)
    reset: bool                        # if True, delete progress and re-run

# -----------------------------------------------------------------------------
# JSON Serialization Helper
# -----------------------------------------------------------------------------

def serialize(obj: Any) -> Any:
    """Fallback JSON serializer (polygons etc.)."""
    if isinstance(obj, list) and obj and hasattr(obj[0], "x") and hasattr(obj[0], "y"):
        return [[pt.x, pt.y] for pt in obj]
    if isinstance(obj, (dict, list, str, int, float, bool, type(None))):
        return obj
    raise TypeError(f"Object of type '{type(obj).__name__}' is not JSON serializable")

# -----------------------------------------------------------------------------
# Your Extractor (unchanged API, richer content payload)
# -----------------------------------------------------------------------------

def content_field(node: Dict[str, Any]) -> Dict[str, Any]:
    """Return 'content' + 'confidence' (+ polygon/spans if present) from a typed node."""
    payload: Dict[str, Any] = {
        "content": node.get("content", ""),
        "confidence": node.get("confidence", None),
    }
    if "polygon" in node:
        payload["polygon"] = node["polygon"]
    if "spans" in node:
        payload["spans"] = node["spans"]
    return payload

def extract_object_array(field_name: str, array_nodes: List[Dict[str, Any]]) -> List[Any]:
    """Walk a typed array and flatten via your extract_object logic."""
    output: List[Any] = []
    for element in array_nodes:
        holder = {field_name: element}
        extracted = extract_object(holder)
        for _, value in extracted.items():
            output.append(value)
    return output

def extract_object(typed_dict: Dict[str, Any]) -> Dict[str, Any]:
    """
    Walk a typed tree shaped like:
      { someField: { type: "...", content?: "...", valueObject?: {...}, valueArray?: [...] }, ... }
    """
    simplified: Dict[str, Any] = {}
    for field_name, attributes in typed_dict.items():
        node_type = attributes["type"]
        keys_present = attributes.keys()

        if node_type == "object":
            if "content" in keys_present or "valueObject" in keys_present:
                inner = attributes["valueObject"]
                simplified[field_name] = extract_object(inner)

        elif node_type == "array":
            if "content" in keys_present or "valueArray" in keys_present:
                inner = attributes["valueArray"]
                simplified[field_name] = extract_object_array(field_name, inner)

        else:
            if "content" in keys_present:
                simplified[field_name] = content_field(attributes)

    return simplified

# -----------------------------------------------------------------------------
# DI Result → Typed Tree Adapter (supports prebuilt-read and custom models)
# -----------------------------------------------------------------------------

def _polygon_to_pairs(polygon: Any) -> List[List[float]]:
    """Convert flat DI polygon [x1,y1,x2,y2,...] to [[x,y], ...]."""
    if not polygon:
        return []
    if isinstance(polygon, list) and polygon and isinstance(polygon[0], list):
        return polygon
    if isinstance(polygon, list):
        pairs: List[List[float]] = []
        it = iter(polygon)
        for x in it:
            y = next(it, None)
            if y is None:
                break
            pairs.append([x, y])
        return pairs
    return []

def _spans_to_list(spans: Any) -> List[Dict[str, int]]:
    """Normalize DI spans -> list of {offset, length} dicts."""
    if not spans:
        return []
    if isinstance(spans, dict):
        spans = [spans]
    out: List[Dict[str, int]] = []
    for s in spans:
        offset = getattr(s, "offset", None) if hasattr(s, "offset") else s.get("offset")
        length = getattr(s, "length", None) if hasattr(s, "length") else s.get("length")
        out.append({"offset": offset, "length": length})
    return out

def _typed_scalar(content: Any = None,
                  confidence: float | None = None,
                  polygon: List[List[float]] | None = None,
                  spans: List[Dict[str, int]] | None = None) -> Dict[str, Any]:
    node: Dict[str, Any] = {"type": "string", "content": content}
    if confidence is not None:
        node["confidence"] = confidence
    if polygon is not None:
        node["polygon"] = polygon
    if spans is not None:
        node["spans"] = spans
    return node

def _typed_number(value: Any,
                  confidence: float | None = None,
                  polygon: List[List[float]] | None = None,
                  spans: List[Dict[str, int]] | None = None) -> Dict[str, Any]:
    node: Dict[str, Any] = {"type": "number", "content": value}
    if confidence is not None:
        node["confidence"] = confidence
    if polygon is not None:
        node["polygon"] = polygon
    if spans is not None:
        node["spans"] = spans
    return node

def _typed_array(items: List[Dict[str, Any]]) -> Dict[str, Any]:
    return {"type": "array", "valueArray": items}

def _typed_object(obj: Dict[str, Any]) -> Dict[str, Any]:
    return {"type": "object", "valueObject": obj}

def _map_document_field(field_obj: Any) -> Dict[str, Any]:
    """Map a DI DocumentField (custom model) to typed schema recursively."""
    try:
        field_type = getattr(field_obj, "type", None) or getattr(field_obj, "value_type", None) or "string"
        content = getattr(field_obj, "content", None)
        confidence = getattr(field_obj, "confidence", None)
        spans = _spans_to_list(getattr(field_obj, "spans", None) or getattr(field_obj, "span", None))

        if field_type in (
            "string", "date", "time", "phoneNumber", "countryRegion", "currency",
            "integer", "number", "selectionMark", "address"
        ):
            typed_value = None
            for attr in (
                "value_string", "value_date", "value_time", "value_phone_number",
                "value_country_region", "value_currency", "value_integer", "value_number",
                "value_selection_mark", "value_address"
            ):
                if hasattr(field_obj, attr) and getattr(field_obj, attr) is not None:
                    typed_value = getattr(field_obj, attr)
                    break
            if isinstance(typed_value, (int, float)):
                return _typed_number(typed_value, confidence=confidence, spans=spans)
            return _typed_scalar(content if content is not None else typed_value,
                                 confidence=confidence, spans=spans)

        if field_type == "array":
            items: List[Dict[str, Any]] = []
            arr = getattr(field_obj, "value_array", None)
            if arr:
                for element in arr:
                    items.append(_typed_object({"item": _map_document_field(element)}))
            return _typed_array(items)

        if field_type == "object":
            obj_map: Dict[str, Any] = {}
            value_obj = getattr(field_obj, "value_object", None) or {}
            for key, val in value_obj.items():
                obj_map[key] = _map_document_field(val)
            return _typed_object(obj_map)

        return _typed_scalar(content, confidence=confidence, spans=spans)

    except Exception:
        return _typed_scalar(getattr(field_obj, "content", None),
                             confidence=getattr(field_obj, "confidence", None))

def analyze_result_to_typed_tree(result: Any) -> Tuple[Dict[str, Any], int]:
    """
    Convert a DI AnalyzeResult to your typed schema.
    Returns (typed_root, page_count).
    """
    # Custom model first
    try:
        documents = getattr(result, "documents", None)
        if documents and len(documents) > 0 and getattr(documents[0], "fields", None):
            root_fields: Dict[str, Any] = {}
            for key, field_obj in documents[0].fields.items():
                root_fields[key] = _map_document_field(field_obj)
            # Page count best-effort: result.pages may exist even for custom
            page_count = len(getattr(result, "pages", []) or [])
            return _typed_object(root_fields), page_count
    except Exception:
        pass

    # Prebuilt-read shape
    doc_content = getattr(result, "content", "") or ""
    typed_pages: List[Dict[str, Any]] = []

    pages = getattr(result, "pages", []) or []
    for page in pages:
        page_number = getattr(page, "page_number", None) or getattr(page, "page", None)
        page_conf = getattr(page, "confidence", None)
        page_poly = _polygon_to_pairs(getattr(page, "polygon", None))
        page_spans = _spans_to_list(getattr(page, "spans", None) or getattr(page, "span", None))
        page_content = getattr(page, "content", "") or ""

        typed_lines: List[Dict[str, Any]] = []
        for line in getattr(page, "lines", []) or []:
            line_content = getattr(line, "content", "") or ""
            line_conf = getattr(line, "confidence", None)
            line_poly = _polygon_to_pairs(getattr(line, "polygon", None))
            line_spans = _spans_to_list(getattr(line, "spans", None) or getattr(line, "span", None))

            typed_words: List[Dict[str, Any]] = []
            for word in getattr(line, "words", []) or []:
                word_content = getattr(word, "content", "") or ""
                word_conf = getattr(word, "confidence", None)
                word_poly = _polygon_to_pairs(getattr(word, "polygon", None))
                word_spans = _spans_to_list(getattr(word, "spans", None) or getattr(word, "span", None))
                typed_words.append(_typed_object({
                    "content": _typed_scalar(word_content, confidence=word_conf, polygon=word_poly, spans=word_spans)
                }))

            typed_lines.append(_typed_object({
                "content": _typed_scalar(line_content, confidence=line_conf, polygon=line_poly, spans=line_spans),
                "words": _typed_array(typed_words)
            }))

        typed_pages.append(_typed_object({
            "pageNumber": _typed_number(page_number if page_number is not None else ""),
            "content": _typed_scalar(page_content, confidence=page_conf, polygon=page_poly, spans=page_spans),
            "lines": _typed_array(typed_lines)
        }))

    typed_root = _typed_object({
        "content": _typed_scalar(doc_content),
        "pages": _typed_array(typed_pages)
    })
    page_count = len(pages)
    return typed_root, page_count

# -----------------------------------------------------------------------------
# Output + Progress Writers
# -----------------------------------------------------------------------------

def write_json_locally(payload: Dict[str, Any], output_dir: str, filename: str) -> str:
    """Write JSON payload to a file in output_dir. Returns full path."""
    os.makedirs(output_dir, exist_ok=True)
    full_path = os.path.join(output_dir, filename)
    with open(full_path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2, default=serialize)
    print(f"[OK] Wrote: {full_path}")
    return full_path

def upload_file_to_blob_via_aad(storage_account_url: str,
                                container_name: str,
                                local_path: str) -> None:
    """Upload a local file to an Azure Blob container via AAD."""
    credential = DefaultAzureCredential()
    blob_service = BlobServiceClient(account_url=storage_account_url, credential=credential)
    container_client: ContainerClient = blob_service.get_container_client(container_name)
    try:
        container_client.create_container()  # idempotent
    except Exception:
        pass
    blob_name = os.path.basename(local_path)
    with open(local_path, "rb") as data:
        container_client.upload_blob(name=blob_name, data=data, overwrite=True)
    print(f"[OK] Uploaded to blob: {container_name}/{blob_name}")

def load_progress(progress_path: str) -> Set[str]:
    """
    Read the progress file and return the set of keys already processed successfully.
    Each line format: "<mode>::<id>\t<PAGES>\t<ISO8601>\tOK"
    We return only the "<mode>::<id>" tokens with status=OK.
    """
    processed: Set[str] = set()
    if not os.path.exists(progress_path):
        return processed
    with open(progress_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            parts = line.split("\t")
            if len(parts) < 4:
                continue
            key_token, pages, iso_ts, status = parts[:4]
            if status.upper() == "OK":
                processed.add(key_token)
    return processed

def append_progress(progress_path: str, key_token: str, pages: int) -> None:
    """
    Append a success record to the progress file.
    Format: "<mode>::<id>\t<PAGES>\t<ISO8601>\tOK"
    """
    ts = datetime.now(timezone.utc).isoformat()
    line = f"{key_token}\t{pages}\t{ts}\tOK\n"
    with open(progress_path, "a", encoding="utf-8") as f:
        f.write(line)

# -----------------------------------------------------------------------------
# Analysis Pipelines
# -----------------------------------------------------------------------------

def save_analyzed_result(result: Any,
                         output_basename: str,
                         output_root: str,
                         upload_output: bool,
                         storage_account_url: str,
                         output_container: str) -> Tuple[str, int]:
    """
    Convert DI result to your schema, write JSON locally, optionally upload.
    Returns (local_output_path, page_count).
    """
    typed_root, page_count = analyze_result_to_typed_tree(result)
    if page_count <= 0:
        raise ValueError("Document appears to have zero pages per AnalyzeResult.")

    extracted = extract_object(typed_root["valueObject"])
    local_dir = os.path.join(os.getcwd(), "json", output_container)
    local_path = write_json_locally(extracted, local_dir, f"{output_basename}.json")

    if upload_output:
        upload_file_to_blob_via_aad(storage_account_url, output_container, local_path)

    return local_path, page_count

def analyze_local_pdfs(di_client: DocumentIntelligenceClient,
                       model_id: str,
                       local_directory: str,
                       output_container: str,
                       upload_output: bool,
                       storage_account_url: str,
                       progress_path: str,
                       processed_keys: Set[str]) -> None:
    """
    Analyze local PDFs, resuming based on progress file. Only *.pdf are processed.
    """
    start_time = datetime.now()
    print(f"[INFO] Local dir: {local_directory} | Model: {model_id}")

    if not os.path.isdir(local_directory):
        raise FileNotFoundError(f"Local directory not found: {local_directory}")

    filenames = sorted(os.listdir(local_directory))
    processed_count = 0
    skipped_count = 0
    error_count = 0

    for filename in filenames:
        if not filename.lower().endswith(".pdf"):
            continue

        key_token = f"local::{filename}"
        if key_token in processed_keys:
            skipped_count += 1
            continue

        file_path = os.path.join(local_directory, filename)
        try:
            with open(file_path, "rb") as file_stream:
                poller = di_client.begin_analyze_document(model_id, file_stream)
                result = poller.result()

            base = os.path.splitext(filename.replace(" ", ""))[0]
            _, page_count = save_analyzed_result(
                result=result,
                output_basename=f"local_{base}",
                output_root=os.path.join("json", output_container),
                upload_output=upload_output,
                storage_account_url=storage_account_url,
                output_container=output_container,
            )
            append_progress(progress_path, key_token, page_count)
            processed_count += 1
            print(f"[OK] {filename} (pages={page_count})")
        except Exception as exc:
            error_count += 1
            print(f"[ERR] {filename}: {exc}")

    elapsed = (datetime.now() - start_time).total_seconds()
    print(f"[OK] Local: processed={processed_count}, skipped={skipped_count}, errors={error_count} in {elapsed:.2f}s")

def analyze_azure_pdfs(di_client: DocumentIntelligenceClient,
                       storage_account_url: str,
                       container_name: str,
                       model_id: str,
                       max_pdfs: int | None,
                       upload_output: bool,
                       output_container: str,
                       tps_limit: int,
                       progress_path: str,
                       processed_keys: Set[str]) -> None:
    """
    Analyze PDFs from Azure Blob using AAD, resume based on progress file.
    """
    print(f"[INFO] Container: {container_name} | Model: {model_id}")

    credential = DefaultAzureCredential()
    blob_service = BlobServiceClient(account_url=storage_account_url, credential=credential)
    container_client: ContainerClient = blob_service.get_container_client(container_name)

    submitted: List[Tuple[Any, str]] = []
    queued = 0

    # Queue submissions by downloading PDF bytes (no SAS/public needed)
    for blob_props in container_client.list_blobs():
        blob_name = blob_props.name
        if not blob_name.lower().endswith(".pdf"):
            continue

        key_token = f"azure::{container_name}/{blob_name}"
        if key_token in processed_keys:
            continue

        if max_pdfs is not None and queued >= max_pdfs:
            break

        blob_client: BlobClient = container_client.get_blob_client(blob_name)
        pdf_bytes = blob_client.download_blob().readall()
        poller = di_client.begin_analyze_document(model_id, pdf_bytes)
        submitted.append((poller, blob_name))
        queued += 1
        print(".", end="", flush=True)

    print(f" queued {queued}")
    if queued == 0:
        print("[INFO] Nothing to process (all done or filtered).")
        return

    # Collect results with a simple TPS throttle
    processed_count = 0
    error_count = 0
    start_collect = datetime.now()
    tps_bucket = 0

    for poller, blob_name in submitted:
        if tps_bucket >= tps_limit:
            time.sleep(1)
            tps_bucket = 0

        key_token = f"azure::{container_name}/{blob_name}"
        try:
            result = poller.result()
            base = os.path.splitext(os.path.basename(blob_name))[0]
            _, page_count = save_analyzed_result(
                result=result,
                output_basename=base,
                output_root=os.path.join("json", output_container),
                upload_output=upload_output,
                storage_account_url=storage_account_url,
                output_container=output_container,
            )
            append_progress(progress_path, key_token, page_count)
            processed_count += 1
            tps_bucket += 1
            print(f"[OK] {blob_name} (pages={page_count})")
        except Exception as exc:
            error_count += 1
            print(f"[ERR] {blob_name}: {exc}")

    elapsed = (datetime.now() - start_collect).total_seconds()
    print(f"[OK] Azure: processed={processed_count}, errors={error_count} in {elapsed:.2f}s")

# -----------------------------------------------------------------------------
# CLI, Config, Entrypoint
# -----------------------------------------------------------------------------

def load_connections_config() -> ConnectionsConfig:
    """
    Load `connections.json` from CWD and return settings.
    Expects:
      - DI-Key
      - DI-Endpoint (or legacy DI_Endpoint)
      - SA-endpoint (HTTPS account URL)
    """
    config_path = os.path.join(os.getcwd(), "connections.json")
    with open(config_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    di_endpoint = data.get("DI-Endpoint") or data.get("DI_Endpoint")
    if not di_endpoint:
        raise ValueError("connections.json missing 'DI-Endpoint' (or legacy 'DI_Endpoint').")

    di_key = data.get("DI-Key")
    if not di_key:
        raise ValueError("connections.json missing 'DI-Key'.")

    storage_account_url = data.get("SA-endpoint") or data.get("SA-enpoint")
    if not storage_account_url:
        raise ValueError("connections.json missing 'SA-endpoint' (HTTPS account URL).")

    return ConnectionsConfig(
        di_endpoint=di_endpoint.rstrip("/"),
        di_key=di_key,
        storage_account_url=storage_account_url.rstrip("/"),
    )

def parse_args() -> RunOptions:
    """Parse CLI flags."""
    parser = argparse.ArgumentParser(
        description="Analyze PDFs with Azure Document Intelligence using AAD for Storage access, with progress tracking."
    )
    parser.add_argument("--mode", choices=["local", "azure"], default="local",
                        help="Where to read PDFs from.")
    parser.add_argument("--model-id", required=True,
                        help="Document Intelligence model ID (e.g., 'prebuilt-read' or your custom model ID).")
    parser.add_argument("--local-directory", default="./tempdata",
                        help="Local folder for PDFs when --mode local.")
    parser.add_argument("--container", default="raw_pdf",
                        help="Azure Blob container to read from when --mode azure.")
    parser.add_argument("--max-pdfs", type=int, default=None,
                        help="Max number of PDFs to process from the container.")
    parser.add_argument("--upload-output", action="store_true",
                        help="Upload JSON results back to storage via AAD.")
    parser.add_argument("--output-container", default="results-json",
                        help="Container to upload JSON outputs into (when --upload-output).")
    parser.add_argument("--tps-limit", type=int, default=8,
                        help="Simple TPS throttle (results collection) in azure mode.")
    parser.add_argument("--progress-file", default="./progress_processed.txt",
                        help="Path to the progress tracking file.")
    parser.add_argument("--reset", action="store_true",
                        help="Clear the progress file and re-run everything.")

    args = parser.parse_args()
    return RunOptions(
        mode=args.mode,
        model_id=args.model_id,
        local_directory=args.local_directory,
        container_name=args.container,
        max_pdfs=args.max_pdfs,
        upload_output=args.upload_output,
        output_container=args.output_container,
        tps_limit=args.tps_limit,
        progress_file=args.progress_file,
        reset=args.reset,
    )

def main() -> None:
    """Entrypoint: config, progress, and pipelines."""
    options = parse_args()
    connections = load_connections_config()

    # Reset progress file if requested
    if options.reset and os.path.exists(options.progress_file):
        os.remove(options.progress_file)
        print(f"[INFO] Progress file reset: {options.progress_file}")

    processed_keys = load_progress(options.progress_file)

    # DI client (service key for DI)
    di_client = DocumentIntelligenceClient(
        endpoint=connections.di_endpoint,
        credential=AzureKeyCredential(connections.di_key),
    )

    if options.mode == "local":
        analyze_local_pdfs(
            di_client=di_client,
            model_id=options.model_id,
            local_directory=options.local_directory,
            output_container=options.output_container,
            upload_output=options.upload_output,
            storage_account_url=connections.storage_account_url,
            progress_path=options.progress_file,
            processed_keys=processed_keys,
        )
    else:
        analyze_azure_pdfs(
            di_client=di_client,
            storage_account_url=connections.storage_account_url,
            container_name=options.container_name,
            model_id=options.model_id,
            max_pdfs=options.max_pdfs,
            upload_output=options.upload_output,
            output_container=options.output_container,
            tps_limit=options.tps_limit,
            progress_path=options.progress_file,
            processed_keys=processed_keys,
        )

if __name__ == "__main__":
    main()


# Deps:
# pip install azure-ai-documentintelligence azure-identity azure-storage-blob

# # Local mode (resume based on progress file)
# python di_json_reader.py --mode local --model-id prebuilt-read

# # Azure mode
# python di_json_reader.py --mode azure --model-id prebuilt-read --container raw_pdf

# # Force re-run everything (clear progress)
# python di_json_reader.py --mode azure --model-id prebuilt-read --container raw_pdf --reset

# # Custom progress file
# python di_json_reader.py --mode local --model-id prebuilt-read --progress-file ./my_progress.txt

# python python/test.py --mode azure --model-id fia-roving-aerial-1 --container fia --prefix aerial/pdf --output-container fia/aerial --output-prefix update_jsons --uplo
# ad-output --max-worker 16 --tps-limit 8 --retries 3 --reset