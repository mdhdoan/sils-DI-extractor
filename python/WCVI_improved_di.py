# wcvi_di_runner.py
# Run like:
#   python .\python\WCVI_improved_di.py --mode azure --model wcvi-sil-4 --container wcvi --input_dir wcvi-sil-2023/pdf --output_dir wcvi-sil-2023/update_json
from __future__ import annotations

import argparse, os, json, time, math, threading
from datetime import datetime, timezone
from typing import Any, Dict, List, Set, Tuple

# Azure DI
from azure.ai.documentintelligence import DocumentIntelligenceClient
from azure.core.credentials import AzureKeyCredential
# Azure Storage (AAD)
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, ContainerClient
from azure.core.exceptions import ServiceRequestError, ServiceResponseError, HttpResponseError

# ------------- Defaults (kept from your old CLI) -------------
DEFAULT_INPUT_DIR  = "./pdf/test"
DEFAULT_OUTPUT_DIR = "./json/update_jsons"
PROGRESS_FILE = "progress_processed.txt"

# ------------- Helpers: progress -------------
def read_processed_from_progress(path: str = PROGRESS_FILE) -> Set[str]:
    """
    Returns set of ids marked OK in TSV: "<mode>::<id>\t<PAGES>\t<ISO8601>\tOK"
    """
    done: Set[str] = set()
    if not os.path.exists(path): return done
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line: continue
            parts = line.split("\t")
            if len(parts) < 4: continue
            status = parts[3].upper()
            if status != "OK": continue
            done.add(parts[0])  # e.g., "azure::wcvi/wcvi-sil-2023/pdf/abc.pdf" or "local::abc.pdf"
    return done

def log_progress(mode: str, id_token: str, pages: int, status: str, path: str = PROGRESS_FILE):
    ts = datetime.now(timezone.utc).isoformat()
    with open(path, "a", encoding="utf-8") as f:
        f.write(f"{mode}::{id_token}\t{pages}\t{ts}\t{status}\n")

# ------------- Helpers: config / clients -------------
def load_connections_config() -> dict:
    """
    ./connections.json:
      {
        "DI-Endpoint": "https://<di>.cognitiveservices.azure.com/",
        "DI-Key": "...",                # OR omit to use AAD
        "SA-endpoint": "https://<acct>.blob.core.windows.net"
      }
    """
    with open(os.path.join(os.getcwd(), "connections.json"), "r", encoding="utf-8") as f:
        data = json.load(f)
    return data

def build_di_client(cfg: dict) -> DocumentIntelligenceClient:
    endpoint = (cfg.get("DI-Endpoint") or cfg.get("DI_Endpoint") or "").rstrip("/")
    key = cfg.get("DI-Key")
    if key:
        return DocumentIntelligenceClient(endpoint=endpoint, credential=AzureKeyCredential(key))
    # AAD fallback if no key
    return DocumentIntelligenceClient(endpoint=endpoint, credential=DefaultAzureCredential())

def build_container_client(cfg: dict, container: str) -> ContainerClient:
    sa = (cfg.get("SA-endpoint") or cfg.get("SA_endpoint") or "").rstrip("/")
    cred = DefaultAzureCredential()
    return BlobServiceClient(account_url=sa, credential=cred).get_container_client(container)

# ------------- Sanitizers / path mappers -------------
def sanitize_blob_name(name: str) -> str:
    s = (name or "").replace("\\", "/").lstrip("./")
    while "//" in s: s = s.replace("//", "/")
    if s.endswith("/"): raise ValueError(f"Blob name may not end with '/': {s}")
    for ch in s:
        if ord(ch) < 32: raise ValueError(f"Invalid control character in blob name: {repr(ch)}")
    return s

def make_output_blob_key(orig_pdf_key: str, input_prefix: str, output_dir_prefix: str) -> str:
    """
    Map 'wcvi-sil-2023/pdf/ABC.pdf' + (input_prefix='wcvi-sil-2023/pdf')
        → 'wcvi-sil-2023/update_json/ABC.json'  (if output_dir_prefix is 'wcvi-sil-2023/update_json')
    If output_dir_prefix is a plain leaf like 'update_json', we place at that leaf.
    """
    orig_pdf_key = sanitize_blob_name(orig_pdf_key)
    input_prefix = sanitize_blob_name(input_prefix) if input_prefix else ""
    output_dir_prefix = sanitize_blob_name(output_dir_prefix) if output_dir_prefix else "update_json"

    base = os.path.splitext(os.path.basename(orig_pdf_key))[0] + ".json"
    # If output_dir_prefix looks like 'wcvi-sil-2023/update_json', just join
    return sanitize_blob_name(f"{output_dir_prefix}/{base}")

# ------------- Serialize & extraction (minimal) -------------
def serialize(obj: Any) -> Any:
    if isinstance(obj, list) and obj and hasattr(obj[0], "x") and hasattr(obj[0], "y"):
        return [[p.x, p.y] for p in obj]
    if isinstance(obj, (dict, list, str, int, float, bool, type(None))):
        return obj
    raise TypeError(f"Not JSON serializable: {type(obj).__name__}")

def content_field(node: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {"content": node.get("content", ""), "confidence": node.get("confidence")}
    if "polygon" in node: out["polygon"] = node["polygon"]
    return out

def extract_object_array(field_name: str, arr: List[Dict[str, Any]]) -> List[Any]:
    out: List[Any] = []
    for el in arr:
        holder = {field_name: el}
        for _, v in extract_object(holder).items():
            out.append(v)
    return out

def extract_object(typed_dict: Dict[str, Any]) -> Dict[str, Any]:
    simplified: Dict[str, Any] = {}
    for field, attrs in typed_dict.items():
        node_type = attrs.get("type")
        if node_type == "object":
            inner = attrs.get("valueObject")
            if isinstance(inner, dict): simplified[field] = extract_object(inner)
        elif node_type == "array":
            inner = attrs.get("valueArray")
            if isinstance(inner, list): simplified[field] = extract_object_array(field, inner)
        else:
            if "content" in attrs: simplified[field] = content_field(attrs)
    return simplified

def analyze_result_to_typed_tree(result: Any) -> Tuple[Dict[str, Any], int]:
    docs = getattr(result, "documents", None)
    if docs and len(docs) > 0 and getattr(docs[0], "fields", None):
        root_fields: Dict[str, Any] = {}
        doc0 = docs[0]
        for key, field_obj in doc0.fields.items():
            # minimal scalar mapping
            content = getattr(field_obj, "content", None)
            conf = getattr(field_obj, "confidence", None)
            poly = None
            brs = getattr(field_obj, "bounding_regions", None) or []
            if brs:
                poly = [[p.x, p.y] for p in brs[0].polygon] if hasattr(brs[0].polygon[0], "x") else brs[0].polygon
            node = {"type": "string", "content": content}
            if conf is not None: node["confidence"] = conf
            if poly is not None: node["polygon"] = node.get("polygon", poly)
            root_fields[key] = node
        pages = getattr(result, "pages", []) or []
        return {"type": "object", "valueObject": root_fields}, len(pages)
    # prebuilt fallback
    pages = getattr(result, "pages", []) or []
    root = {"type": "object", "valueObject": {"content": {"type": "string", "content": getattr(result, "content", "")}}}
    return root, len(pages)

# ------------- Output -------------
def write_json_locally(payload: Dict[str, Any], output_dir: str, filename: str) -> str:
    os.makedirs(output_dir, exist_ok=True)
    path = os.path.join(output_dir, filename)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2, default=serialize)
    print(f"[OK] wrote: {path}")
    return path

def upload_json_via_aad(storage_account_url: str, container: str, local_path: str, blob_name: str, overwrite: bool=True) -> None:
    cred = DefaultAzureCredential()
    svc = BlobServiceClient(account_url=storage_account_url, credential=cred)
    cc = svc.get_container_client(container)
    try: cc.create_container()
    except Exception: pass
    with open(local_path, "rb") as data:
        cc.upload_blob(name=blob_name, data=data, overwrite=overwrite)
    print(f"[OK] uploaded: {container}/{blob_name}")

# ------------- Azure rate limiter (simple) -------------
class Rate:
    def __init__(self, tps:int=8):
        self.tps=max(1,tps); self.win=1.0; self.lock=threading.Lock(); self.bucket=[]
    def _prune(self, now): 
        cutoff=now-self.win
        while self.bucket and self.bucket[0]<cutoff: self.bucket.pop(0)
    def acquire(self):
        while True:
            with self.lock:
                now=time.time(); self._prune(now)
                if len(self.bucket)<self.tps:
                    self.bucket.append(now); return
                sleep_for=(self.bucket[0]+self.win)-now
            if sleep_for>0: time.sleep(sleep_for)

def is_transient(exc: Exception) -> bool:
    if isinstance(exc, (ServiceRequestError, ServiceResponseError)): return True
    if isinstance(exc, HttpResponseError):
        code=getattr(exc,"status_code",None) or getattr(exc,"status",None)
        return code in (429,500,502,503,504)
    return False

# ------------- Pipelines -------------
def save_result_to_outputs(result: Any,
                           output_basename: str,
                           local_output_dir: str,
                           do_upload: bool,
                           storage_account_url: str,
                           output_container: str,
                           output_blob_name: str,
                           overwrite: bool) -> Tuple[str,int]:
    typed_root, pages = analyze_result_to_typed_tree(result)
    if pages<=0: raise ValueError("AnalyzeResult has zero pages.")
    extracted = extract_object(typed_root["valueObject"])
    local_path = write_json_locally(extracted, local_output_dir, f"{output_basename}.json")
    if do_upload:
        upload_json_via_aad(storage_account_url, output_container, local_path, output_blob_name, overwrite=overwrite)
    return local_path, pages

def run_local(di: DocumentIntelligenceClient,
              model_id: str,
              input_dir: str,
              output_dir: str,
              progress_path: str) -> None:
    if not os.path.isdir(input_dir): raise FileNotFoundError(f"Input dir not found: {input_dir}")
    processed = read_processed_from_progress(progress_path)
    files = sorted([f for f in os.listdir(input_dir) if f.lower().endswith(".pdf")])
    new = [f for f in files if f"local::{f}" not in processed]
    if not new: print("[INFO] All PDFs already processed."); return

    ok=err=0; start=time.time()
    for fname in new:
        token=f"local::{fname}"
        pdf_path=os.path.join(input_dir,fname)
        try:
            with open(pdf_path,"rb") as fh:
                poller=di.begin_analyze_document(model_id, fh)
                result=poller.result()
            base=os.path.splitext(fname)[0]
            _, pages = save_result_to_outputs(
                result,
                output_basename=base,
                local_output_dir=output_dir,
                do_upload=False,
                storage_account_url="",  # unused
                output_container="",
                output_blob_name="",
                overwrite=True
            )
            log_progress("local", fname, pages, "OK", progress_path)
            ok+=1
        except Exception as e:
            log_progress("local", fname, 0, f"ERR:{e}", progress_path); err+=1
            print(f"[ERR] {fname}: {e}")
    elapsed=time.time()-start
    print(f"[DONE] local processed={ok}, errors={err}, elapsed={elapsed:.2f}s")

def run_azure(di: DocumentIntelligenceClient,
              cfg: dict,
              container: str,
              input_prefix: str,
              output_dir_prefix: str,
              model_id: str,
              tps:int,
              progress_path:str,
              overwrite: bool) -> None:
    sa = (cfg.get("SA-endpoint") or cfg.get("SA_endpoint") or "").rstrip("/")
    cc = build_container_client(cfg, container)

    input_prefix = sanitize_blob_name(input_prefix) if input_prefix else ""
    output_dir_prefix = sanitize_blob_name(output_dir_prefix) if output_dir_prefix else "update_json"

    # enumerate
    processed = read_processed_from_progress(progress_path)
    candidates=[]
    for b in cc.list_blobs(name_starts_with=input_prefix or None):
        if not b.name.lower().endswith(".pdf"): continue
        token=f"azure::{container}/{b.name}"
        if token in processed: continue
        candidates.append(b.name)

    if not candidates:
        print("[INFO] Nothing to process."); return

    rate=Rate(tps); ok=err=0; start=time.time()
    out_local_dir = DEFAULT_OUTPUT_DIR if output_dir_prefix=="" else os.path.join(".", output_dir_prefix.replace("/", os.sep))
    os.makedirs(out_local_dir, exist_ok=True)

    for name in candidates:
        token=f"azure::{container}/{name}"
        try:
            # download PDF bytes
            pdf_bytes = cc.get_blob_client(name).download_blob().readall()
            # rate-limit+submit
            rate.acquire()
            poller = di.begin_analyze_document(model_id, pdf_bytes)
            result = poller.result()

            json_base  = os.path.splitext(os.path.basename(name))[0]
            json_blob  = make_output_blob_key(name, input_prefix, output_dir_prefix)  # where to upload in container
            local_path, pages = save_result_to_outputs(
                result,
                output_basename=json_base,
                local_output_dir=out_local_dir,                   # local mirror of output dir
                do_upload=True,
                storage_account_url=sa,
                output_container=container,
                output_blob_name=json_blob,
                overwrite=overwrite
            )
            log_progress("azure", f"{container}/{name}", pages, "OK", progress_path)
            print(f"[OK] {name} (pages={pages}) → {json_blob}")
            ok+=1
        except Exception as e:
            log_progress("azure", f"{container}/{name}", 0, f"ERR:{e}", progress_path)
            print(f"[ERR] {name}: {e}")
            if not is_transient(e):
                pass
    elapsed=time.time()-start
    print(f"[DONE] azure processed={ok}, errors={err}, elapsed={elapsed:.2f}s")

# ------------- CLI -------------
def parse_args():
    p = argparse.ArgumentParser("WCVI DI runner (local/azure) with progress + prefix mapping")
    p.add_argument("--mode", choices=["local","azure"], default="local")
    # keep your legacy flag names:
    p.add_argument("--model", dest="model_id", required=True, help="DI model id (e.g., wcvi-sil-4)")
    p.add_argument("--container", default="wcvi", help="Azure Blob container (azure mode)")
    p.add_argument("--input_dir", default=DEFAULT_INPUT_DIR,
                   help="local: folder of PDFs | azure: input prefix, e.g. 'wcvi-sil-2023/pdf'")
    p.add_argument("--output_dir", default=DEFAULT_OUTPUT_DIR,
                   help="local: folder for JSON | azure: output prefix in container, e.g. 'wcvi-sil-2023/update_json'")
    p.add_argument("--tps", type=int, default=8, help="simple TPS limiter (azure)")
    p.add_argument("--overwrite", action="store_true", help="overwrite JSON blobs in azure")
    p.add_argument("--no-overwrite", dest="overwrite", action="store_false")
    p.set_defaults(overwrite=True)
    p.add_argument("--reset", action="store_true", help="delete progress TSV before run")
    p.add_argument("--progress_file", default=PROGRESS_FILE)
    return p.parse_args()

def main():
    args = parse_args()
    cfg = load_connections_config()
    if args.reset and os.path.exists(args.progress_file):
        os.remove(args.progress_file); print(f"[INFO] progress reset: {args.progress_file}")

    di = build_di_client(cfg)

    if args.mode == "local":
        # local: input_dir is filesystem, output_dir is filesystem
        run_local(
            di=di,
            model_id=args.model_id,
            input_dir=args.input_dir,
            output_dir=args.output_dir,
            progress_path=args.progress_file
        )
    else:
        # azure: input_dir is blob prefix, output_dir is output prefix inside same container
        run_azure(
            di=di,
            cfg=cfg,
            container=args.container,
            input_prefix=args.input_dir,
            output_dir_prefix=args.output_dir,
            model_id=args.model_id,
            tps=args.tps,
            progress_path=args.progress_file,
            overwrite=args.overwrite
        )

if __name__ == "__main__":
    main()

# Local:
# python ./python/WCVI_improved_di.py --mode azure --model wcvi-sil-4 --container wcvi --input_dir wcvi-sil-2023/pdf --output_dir wcvi-sil-2023/update_json


# Azure: 
# python ./python/WCVI_improved_di.py --mode local --model wcvi-sil-4 --input_dir wcvi-sil-2023/pdf --output_dir wcvi-sil-2023/update_json

#RESET:
#python ./python/WCVI_improved_di.py --mode azure --model wcvi-sil-4 --container wcvi --input_dir wcvi-sil-2023/pdf --output_dir wcvi-sil-2023/update_json --reset

# No overwrite:
# python ./python/WCVI_improved_di.py --mode azure --model wcvi-sil-4 --container wcvi --input_dir wcvi-sil-2023/pdf --output_dir wcvi-sil-2023/update_json --no-overwrite
