#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse, os, sys, json, time, base64, mimetypes
from datetime import datetime, timezone
from urllib.parse import urlparse, urljoin, quote
import requests
from requests.exceptions import RequestException, SSLError, Timeout

# AAD
try:
    from azure.identity import DefaultAzureCredential
except Exception:
    DefaultAzureCredential = None

OK, ERR = "[OK]", "[ERR]"

# =============================================================================
# Basic utilities
# =============================================================================

def die(msg, code=1):
    print(f"{ERR} {msg}")
    sys.exit(code)

def ensure_dir(path):
    os.makedirs(path, exist_ok=True)

def rfc1123_now():
    return datetime.now(timezone.utc).strftime("%a, %d %b %Y %H:%M:%S GMT")

def validate_url(name, value):
    try:
        u = urlparse(value)
    except Exception:
        die(f"{name} is not a valid URL: {value}")
    if u.scheme not in ("https", "http") or not u.netloc:
        die(f"{name} must be an absolute URL (https://...), got: {value}")
    return value.rstrip("/")

def get_required(d: dict, k: str) -> str:
    v = str(d.get(k, "")).strip()
    if not v:
        die(f"Missing or empty '{k}' in connections.json")
    return v

def get_required_any(d: dict, keys, label: str) -> str:
    for k in keys:
        v = str(d.get(k, "")).strip()
        if v:
            if k != label:
                print(f"{OK} Using '{k}' for {label} (back-compat).")
            return v
    die(f"Missing or empty {' / '.join(keys)} in connections.json (expect '{label}')")

# =============================================================================
# Identity: acquire token + label principal type
# =============================================================================

def _b64url_decode(s: str) -> bytes:
    s += "=" * (-len(s) % 4)
    return base64.urlsafe_b64decode(s)

def _classify_identity_from_claims_and_env(claims: dict) -> str:
    import os
    upn   = claims.get("upn") or claims.get("preferred_username")
    env_client_id  = os.getenv("AZURE_CLIENT_ID")
    env_client_sec = os.getenv("AZURE_CLIENT_SECRET")
    env_client_cert= os.getenv("AZURE_CLIENT_CERTIFICATE_PATH")
    if upn:
        return "User (Azure AD account)"
    if env_client_sec or env_client_cert:
        return "Service Principal (client credentials)"
    if env_client_id and not (env_client_sec or env_client_cert):
        return "Managed Identity (user-assigned)"
    return "Managed Identity (system-assigned)"

def get_aad_token_for_storage():
    if DefaultAzureCredential is None:
        die("azure-identity not installed. Install with: pip install azure-identity")
    try:
        cred = DefaultAzureCredential()
        token = cred.get_token("https://storage.azure.com/.default")
        # print labeled identity
        parts = token.token.split(".")
        payload = json.loads(_b64url_decode(parts[1]))
        label = _classify_identity_from_claims_and_env(payload)
        oid   = payload.get("oid") or payload.get("sub")
        appid = payload.get("appid")
        upn   = payload.get("upn") or payload.get("preferred_username")
        tid   = payload.get("tid")
        print(f"{OK} AAD token acquired -> {label} | oid={oid} | appid={appid} | upn={upn} | tenant={tid}")
        return token.token
    except Exception as e:
        die(f"Failed to acquire AAD token: {e}")

# =============================================================================
# Storage (AAD) list/download
# =============================================================================

def list_blobs_aad(sa_endpoint: str, container: str, bearer: str, prefix: str = None, timeout=15):
    """
    Returns a list of blob names in a container.
    Requires data-plane permissions (e.g., Storage Blob Data Reader/Contributor).
    """
    base = f"{sa_endpoint}/{container}"
    params = {"restype": "container", "comp": "list", "maxresults": "5000"}
    if prefix:
        params["prefix"] = prefix
    marker = None
    names = []
    while True:
        q = "&".join([f"{k}={quote(v)}" for k, v in params.items() if v])
        if marker:
            q += f"&marker={quote(marker)}"
        url = f"{base}?{q}"
        headers = {"Authorization": f"Bearer {bearer}", "x-ms-version": "2021-10-04", "x-ms-date": rfc1123_now()}
        r = requests.get(url, headers=headers, timeout=timeout)
        if r.status_code != 200:
            raise RuntimeError(f"List blobs failed {r.status_code}: {r.text[:200]}")
        # Quick XML parsing to collect names
        txt = r.text
        names += [seg.split("</Name>")[0] for seg in txt.split("<Name>")[1:]]
        # NextMarker
        marker = ""
        if "<NextMarker>" in txt:
            marker = txt.split("<NextMarker>")[1].split("</NextMarker>")[0]
        if not marker:
            break
    return names

def download_blob_aad(sa_endpoint: str, container: str, blob_name: str, bearer: str, timeout=60) -> bytes:
    url = f"{sa_endpoint}/{container}/{quote(blob_name)}"
    headers = {"Authorization": f"Bearer {bearer}", "x-ms-version": "2021-10-04", "x-ms-date": rfc1123_now()}
    r = requests.get(url, headers=headers, timeout=timeout, stream=True)
    if r.status_code == 200:
        return r.content
    elif r.status_code == 404:
        raise FileNotFoundError(f"Blob not found: {container}/{blob_name}")
    elif r.status_code == 403:
        raise PermissionError(f"Forbidden downloading {container}/{blob_name} (check RBAC).")
    else:
        raise RuntimeError(f"Download failed {r.status_code}: {r.text[:200]}")

# =============================================================================
# Document Intelligence (prebuilt-read via REST)
# =============================================================================

def di_list_models(di_endpoint, di_key, timeout_sec=10):
    url = urljoin(di_endpoint + "/", "documentintelligence/documentModels")
    params = {"api-version": "2024-11-30"}
    headers = {"Ocp-Apim-Subscription-Key": di_key, "Accept": "application/json"}
    try:
        resp = requests.get(url, headers=headers, params=params, timeout=timeout_sec)
        if resp.status_code == 200:
            print(f"{OK} DI reachable; List Models = 200")
            return True
        print(f"{ERR} DI List Models unexpected {resp.status_code}: {resp.text[:200]}")
        return False
    except Exception as e:
        print(f"{ERR} DI List Models error: {e}")
        return False

def di_analyze_bytes(di_endpoint, di_key, data: bytes, filename: str):
    mime, _ = mimetypes.guess_type(filename)
    if not mime:
        mime = "application/pdf" if filename.lower().endswith(".pdf") else "application/octet-stream"
    url = urljoin(di_endpoint + "/", "documentintelligence/documentModels/prebuilt-read:analyze")
    params = {"api-version": "2024-11-30"}
    headers = {"Ocp-Apim-Subscription-Key": di_key, "Content-Type": mime, "Accept": "application/json"}

    try:
        resp = requests.post(url, headers=headers, params=params, data=data, timeout=120)
        if resp.status_code == 200:
            j = resp.json()
            return _pack_di_results(j, filename)
        elif resp.status_code == 202:
            op_loc = resp.headers.get("operation-location") or resp.headers.get("Operation-Location")
            if not op_loc:
                raise RuntimeError("202 without operation-location")
            j = _poll_di(op_loc, di_key)
            return _pack_di_results(j, filename)
        else:
            raise RuntimeError(f"Analyze unexpected {resp.status_code}: {resp.text[:200]}")
    except Exception as e:
        raise RuntimeError(f"DI analyze error on {filename}: {e}")

def _poll_di(operation_url, di_key, timeout_sec=180, interval=2):
    headers = {"Ocp-Apim-Subscription-Key": di_key, "Accept": "application/json"}
    end = time.time() + timeout_sec
    while time.time() < end:
        r = requests.get(operation_url, headers=headers, timeout=20)
        if r.status_code in (200, 201):
            j = r.json()
            status = (j.get("status") or j.get("statusCode") or "").lower()
            if status.startswith("succeed") or status in ("succeeded", "completed"):
                return j
            if status.startswith("fail"):
                raise RuntimeError(f"DI analyze failed: {str(j)[:400]}")
        time.sleep(interval)
    raise TimeoutError("Timed out waiting for DI analyze result")

# =============================================================================
# Your extractor + adapter (now with confidence & coordinates)
# =============================================================================

def content_field(attributes):
    # returns the content string value
    return attributes.get("content", "")

def extract_object_array(field, dict_data):
    result_list = []
    for data in dict_data:
        holder = {field: data}
        holder_dict = extract_object(holder)
        for holder_field, holder_value in holder_dict.items():
            result_list.append(holder_value)
    return result_list

def extract_object(dict_data):
    result_dict_data = {}
    for field, attributes in dict_data.items():
        dict_data_type = attributes['type']
        attribute_check_list = attributes.keys()

        if dict_data_type == 'object':
            if 'content' in attribute_check_list or 'valueObject' in attribute_check_list:
                inner_object = attributes['valueObject']
                result_dict_data[field] = extract_object(inner_object)

        elif dict_data_type == 'array':
            if 'content' in attribute_check_list or 'valueArray' in attribute_check_list:
                inner_object = attributes['valueArray']
                result_dict_data[field] = extract_object_array(field, inner_object)

        else:
            if 'content' in attribute_check_list:
                result_dict_data[field] = content_field(attributes)
    return result_dict_data

def _polygon_to_pairs(poly):
    """
    DI polygons are usually flat lists: [x1,y1,x2,y2,...].
    Convert to list of [x,y] pairs. If already pairs, return as-is.
    """
    if not poly:
        return []
    if isinstance(poly, list) and poly and isinstance(poly[0], list):
        return poly
    if isinstance(poly, list) and all(isinstance(n, (int, float)) for n in poly):
        pairs = []
        it = iter(poly)
        for x in it:
            y = next(it, None)
            if y is None:
                break
            pairs.append([x, y])
        return pairs
    return []

def di_to_typed_tree_with_details(di_json: dict) -> dict:
    """
    Build a typed tree that preserves:
      - content (string)
      - confidence (number) where available
      - coordinates (polygon as list of [x,y] pairs) where available
      - spans (offsets) where available
      - pages -> lines -> words
    """
    # Consolidated document content
    doc_content = ""
    if isinstance(di_json, dict) and isinstance(di_json.get("content"), str):
        doc_content = di_json["content"]

    # Pages list (handles both top-level 'pages' and 'analyzeResult.pages')
    pages = []
    if isinstance(di_json.get("pages"), list):
        pages = di_json["pages"]
    elif isinstance(di_json.get("analyzeResult", {}).get("pages"), list):
        pages = di_json["analyzeResult"]["pages"]

    pages_typed = []
    for p in pages:
        page_num = p.get("pageNumber") or p.get("page")
        page_conf = p.get("confidence", None)  # not always present
        page_poly = _polygon_to_pairs(p.get("polygon"))
        page_content = p.get("content", "")

        # Lines
        lines_typed = []
        if isinstance(p.get("lines"), list):
            for ln in p["lines"]:
                if not isinstance(ln, dict):
                    continue
                ln_content = ln.get("content", "")
                ln_conf    = ln.get("confidence", None)
                # Prefer line polygon; else try first boundingRegion.polygon
                ln_poly    = _polygon_to_pairs(ln.get("polygon"))
                if not ln_poly:
                    brs = ln.get("boundingRegions") or []
                    if brs and isinstance(brs, list) and isinstance(brs[0], dict):
                        ln_poly = _polygon_to_pairs(brs[0].get("polygon"))

                # Words inside the line (optional)
                words_typed = []
                if isinstance(ln.get("words"), list):
                    for w in ln["words"]:
                        if not isinstance(w, dict):
                            continue
                        w_content = w.get("content", "")
                        w_conf    = w.get("confidence", None)
                        w_poly    = _polygon_to_pairs(w.get("polygon"))
                        if not w_poly:
                            brs = w.get("boundingRegions") or []
                            if brs and isinstance(brs, list) and isinstance(brs[0], dict):
                                w_poly = _polygon_to_pairs(brs[0].get("polygon"))
                        w_span = w.get("span") or w.get("spans")  # spans may be list or single
                        if isinstance(w_span, dict):
                            w_span = [w_span]
                        words_typed.append({
                            "type": "object",
                            "valueObject": {
                                "content":   {"type": "string", "content": w_content},
                                "confidence":{"type": "number", "content": w_conf if w_conf is not None else ""},
                                "polygon":   {"type": "array",  "valueArray": [{"type":"array","valueArray":[{"type":"number","content":xy} for xy in pt]} for pt in w_poly]},
                                "spans":     {"type": "array",  "valueArray": [{"type":"object","valueObject":{
                                    "offset": {"type":"number","content": s.get("offset","")},
                                    "length": {"type":"number","content": s.get("length","")}
                                }} for s in (w_span or [])]}
                            }
                        })

                ln_span = ln.get("span") or ln.get("spans")
                if isinstance(ln_span, dict):
                    ln_span = [ln_span]

                lines_typed.append({
                    "type": "object",
                    "valueObject": {
                        "content":   {"type": "string", "content": ln_content},
                        "confidence":{"type": "number", "content": ln_conf if ln_conf is not None else ""},
                        "polygon":   {"type": "array",  "valueArray": [{"type":"array","valueArray":[{"type":"number","content":xy} for xy in pt]} for pt in ln_poly]},
                        "spans":     {"type": "array",  "valueArray": [{"type":"object","valueObject":{
                            "offset": {"type":"number","content": s.get("offset","")},
                            "length": {"type":"number","content": s.get("length","")}
                        }} for s in (ln_span or [])]},
                        "words":     {"type": "array",  "valueArray": words_typed}
                    }
                })

        # words directly on page (fallback for older shapes)
        elif isinstance(p.get("words"), list):
            # synthesize a single line from words
            words = []
            for w in p["words"]:
                if not isinstance(w, dict): 
                    continue
                w_content = w.get("content","")
                w_conf    = w.get("confidence", None)
                w_poly    = _polygon_to_pairs(w.get("polygon"))
                if not w_poly:
                    brs = w.get("boundingRegions") or []
                    if brs and isinstance(brs, list) and isinstance(brs[0], dict):
                        w_poly = _polygon_to_pairs(brs[0].get("polygon"))
                words.append({
                    "type":"object",
                    "valueObject":{
                        "content":   {"type":"string","content":w_content},
                        "confidence":{"type":"number","content": w_conf if w_conf is not None else ""},
                        "polygon":   {"type":"array","valueArray":[{"type":"array","valueArray":[{"type":"number","content":xy} for xy in pt]} for pt in w_poly]},
                        "spans":     {"type":"array","valueArray":[]}
                    }
                })
            lines_typed.append({
                "type":"object",
                "valueObject":{
                    "content":{"type":"string","content":" ".join([w['valueObject']['content']['content'] for w in words])},
                    "confidence":{"type":"number","content":""},
                    "polygon":{"type":"array","valueArray":[]},
                    "spans":{"type":"array","valueArray":[]},
                    "words":{"type":"array","valueArray":words}
                }
            })

        page_span = p.get("span") or p.get("spans")
        if isinstance(page_span, dict):
            page_span = [page_span]

        pages_typed.append({
            "type": "object",
            "valueObject": {
                "pageNumber": {"type": "number", "content": str(page_num or "")},
                "confidence": {"type": "number", "content": page_conf if page_conf is not None else ""},
                "content":    {"type": "string", "content": page_content},
                "polygon":    {"type": "array",  "valueArray": [{"type":"array","valueArray":[{"type":"number","content":xy} for xy in pt]} for pt in _polygon_to_pairs(page_poly)]},
                "spans":      {"type": "array",  "valueArray": [{"type":"object","valueObject":{
                    "offset": {"type":"number","content": s.get("offset","")},
                    "length": {"type":"number","content": s.get("length","")}
                }} for s in (page_span or [])]},
                "lines":      {"type": "array",  "valueArray": lines_typed},
            },
        })

    typed = {
        "document": {
            "type": "object",
            "valueObject": {
                "content": {"type": "string", "content": doc_content},
                "pages":   {"type": "array",  "valueArray": pages_typed},
            },
        }
    }
    return typed

def _extract_text_snippet(j: dict) -> str:
    # Prefer consolidated "content"
    if isinstance(j, dict):
        if isinstance(j.get("content"), str) and j["content"].strip():
            return j["content"]
        if isinstance(j.get("value"), list):
            chunks = [v.get("content","") for v in j["value"] if isinstance(v, dict)]
            return "\n".join([c for c in chunks if c])
        ar = j.get("analyzeResult", {})
        pages = ar.get("pages", [])
        if isinstance(pages, list):
            return "\n".join([p.get("content","") for p in pages if isinstance(p, dict)])
    return ""

def _pack_di_results(j: dict, filename: str):
    # 1) plain text
    text = _extract_text_snippet(j)
    # 2) run user's extractor over a typed tree WITH details
    typed = di_to_typed_tree_with_details(j)
    extracted = extract_object(typed)
    return {"text": text, "extracted": extracted}

# =============================================================================
# Pipelines: local and online
# =============================================================================

def process_local(di_endpoint, di_key, out_dir):
    src_dir = os.path.join("pdf", "test")
    if not os.path.isdir(src_dir):
        die(f"Local source folder not found: {src_dir}")
    ensure_dir(out_dir)
    files = [f for f in sorted(os.listdir(src_dir)) if f.lower().endswith(".pdf")]
    if not files:
        print(f"{ERR} No PDFs in {src_dir}")
        return
    total, ok = 0, 0
    for name in files:
        total += 1
        path = os.path.join(src_dir, name)
        try:
            with open(path, "rb") as f:
                result = di_analyze_bytes(di_endpoint, di_key, f.read(), name)
            base = os.path.splitext(name)[0]
            out_txt  = os.path.join(out_dir, f"{base}.txt")
            out_json = os.path.join(out_dir, f"{base}.json")
            with open(out_txt, "w", encoding="utf-8") as w:
                w.write(result["text"])
            with open(out_json, "w", encoding="utf-8") as wj:
                json.dump(result["extracted"], wj, ensure_ascii=False, indent=2)
            print(f"{OK} Wrote {out_txt} and {out_json}")
            ok += 1
        except Exception as e:
            print(f"{ERR} {name}: {e}")
    print(f"{OK} Local done: {ok}/{total} succeeded")

def process_online(di_endpoint, di_key, sa_endpoint, container, bearer, out_dir, prefix=None, max_files=None):
    ensure_dir(out_dir)
    try:
        names = list_blobs_aad(sa_endpoint, container, bearer, prefix=prefix)
    except Exception as e:
        die(f"List blobs error: {e}")

    pdfs = [n for n in names if n.lower().endswith(".pdf")]
    if max_files:
        pdfs = pdfs[:max_files]
    if not pdfs:
        print(f"{ERR} No PDFs found in container '{container}' with prefix '{prefix or ''}'")
        return

    total, ok = 0, 0
    for blob in pdfs:
        total += 1
        try:
            data = download_blob_aad(sa_endpoint, container, blob, bearer)
            result = di_analyze_bytes(di_endpoint, di_key, data, os.path.basename(blob))
            base = os.path.splitext(os.path.basename(blob))[0]
            out_txt  = os.path.join(out_dir, f"{base}.txt")
            out_json = os.path.join(out_dir, f"{base}.json")
            with open(out_txt, "w", encoding="utf-8") as w:
                w.write(result["text"])
            with open(out_json, "w", encoding="utf-8") as wj:
                json.dump(result["extracted"], wj, ensure_ascii=False, indent=2)
            print(f"{OK} {container}/{blob} -> {out_txt}, {out_json}")
            ok += 1
        except Exception as e:
            print(f"{ERR} {container}/{blob}: {e}")
    print(f"{OK} Online done: {ok}/{total} succeeded")

# =============================================================================
# Main
# =============================================================================

def main():
    ap = argparse.ArgumentParser(description="Read PDFs using Azure DI (prebuilt-read). Local folder or Azure Blob (AAD).")
    ap.add_argument("-f", "--file", default="connections.json", help="Path to connections.json")
    ap.add_argument("--mode", choices=["local", "online"], default="local", help="Source: local ./pdf/test or Azure Blob")
    ap.add_argument("--container", default="raw_pdf", help="Blob container name for online mode")
    ap.add_argument("--prefix", default=None, help="Optional prefix in container (online mode)")
    ap.add_argument("--max", type=int, default=None, help="Limit number of PDFs (online mode)")
    ap.add_argument("--out", default="out", help="Output folder for extracted text")
    args = ap.parse_args()

    # Load config
    try:
        with open(args.file, "r", encoding="utf-8") as f:
            cfg = json.load(f)
    except FileNotFoundError:
        die(f"File not found: {args.file}")
    except json.JSONDecodeError as e:
        die(f"Invalid JSON: {e}")

    di_key = get_required(cfg, "DI-Key")
    di_endpoint = validate_url("DI-Endpoint", get_required_any(cfg, ["DI-Endpoint", "DI_Endpoint"], "DI-Endpoint"))
    sa_endpoint = validate_url("SA-endpoint", get_required(cfg, "SA-endpoint"))

    # Smoke test DI
    di_list_models(di_endpoint, di_key)

    if args.mode == "local":
        ensure_dir(args.out)
        process_local(di_endpoint, di_key, args.out)
    else:
        bearer = get_aad_token_for_storage()
        ensure_dir(args.out)
        process_online(di_endpoint, di_key, sa_endpoint, args.container, bearer, args.out, prefix=args.prefix, max_files=args.max)

if __name__ == "__main__":
    main()
