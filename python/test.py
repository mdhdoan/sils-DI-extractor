#!/usr/bin/env python3
import argparse, json, sys, socket, time, mimetypes, os
from urllib.parse import urlparse, urljoin, quote, urlencode
import requests
from requests.exceptions import RequestException, SSLError, Timeout
import base64
import hmac, hashlib
from datetime import datetime

OK = "[OK]"
ERR = "[ERR]"

def die(msg, code=1):
    print(f"{ERR} {msg}")
    sys.exit(code)

def validate_required(d, k):
    v = str(d.get(k, "")).strip()
    if not v:
        die(f"Missing or empty '{k}' in connections.json")
    return v

def validate_url(name, value):
    try:
        u = urlparse(value)
    except Exception:
        die(f"{name} is not a valid URL: {value}")
    if u.scheme not in ("https", "http") or not u.netloc:
        die(f"{name} must be an absolute URL (https://...), got: {value}")
    return value.rstrip("/")

def dns_check(hostname):
    try:
        ip = socket.gethostbyname(hostname)
        print(f"{OK} DNS resolved {hostname} -> {ip}")
        return True
    except socket.gaierror as e:
        print(f"{ERR} DNS failed for {hostname}: {e}")
        return False

def storage_https_reachability(sa_endpoint, timeout_sec=8):
    url = sa_endpoint + "/"
    try:
        resp = requests.get(url, timeout=timeout_sec)
        code = resp.status_code
        if code in (200, 400, 401, 403):
            ver = resp.headers.get("x-ms-version")
            errcode = resp.headers.get("x-ms-error-code")
            note = []
            if ver: note.append(f"x-ms-version={ver}")
            if errcode: note.append(f"x-ms-error-code={errcode}")
            print(f"{OK} Storage HTTPS reachable ({code})" + (f"; {', '.join(note)}" if note else ""))
            return True
        print(f"{ERR} Storage endpoint responded with unexpected status {code}")
        return False
    except (Timeout, SSLError) as e:
        print(f"{ERR} Storage HTTPS timeout/SSL error: {e}")
        return False
    except RequestException as e:
        print(f"{ERR} Storage HTTPS request error: {e}")
        return False

# ---------- Document Intelligence (REST) ----------
def di_list_models(di_endpoint, di_key, timeout_sec=10):
    url = urljoin(di_endpoint + "/", "documentintelligence/documentModels")
    params = {"api-version": "2024-11-30"}
    headers = {
        "Ocp-Apim-Subscription-Key": di_key,
        "Accept": "application/json"
    }
    try:
        resp = requests.get(url, headers=headers, params=params, timeout=timeout_sec)
        if resp.status_code == 200:
            print(f"{OK} Document Intelligence reachable; List Models = 200")
            return True
        elif resp.status_code in (401, 403):
            print(f"{ERR} Document Intelligence auth failed ({resp.status_code})")
            return False
        else:
            print(f"{ERR} Document Intelligence unexpected status {resp.status_code}: {resp.text[:200]}")
            return False
    except (Timeout, SSLError) as e:
        print(f"{ERR} Document Intelligence timeout/SSL error: {e}")
        return False
    except RequestException as e:
        print(f"{ERR} Document Intelligence request error: {e}")
        return False

def _di_poll(operation_url, di_key, timeout_sec=60, interval=2):
    headers = {"Ocp-Apim-Subscription-Key": di_key, "Accept": "application/json"}
    end = time.time() + timeout_sec
    while time.time() < end:
        r = requests.get(operation_url, headers=headers, timeout=10)
        if r.status_code in (200, 201):
            j = r.json()
            status = j.get("status") or j.get("statusCode")
            if status in ("succeeded", "Succeeded", "succeed", "completed"):
                return j
            if status in ("failed", "Failed"):
                raise RuntimeError(f"DI analyze failed: {j}")
        time.sleep(interval)
    raise TimeoutError("Timed out waiting for analyze result")

def di_analyze_file(di_endpoint, di_key, path):
    if not os.path.exists(path):
        print(f"{ERR} DI test file not found: {path}")
        return False
    mime, _ = mimetypes.guess_type(path)
    if not mime:
        # sensible fallbacks
        if path.lower().endswith(".pdf"): mime = "application/pdf"
        elif path.lower().endswith(".txt"): mime = "text/plain"
        else: mime = "application/octet-stream"

    url = urljoin(di_endpoint + "/", "documentintelligence/documentModels/prebuilt-read:analyze")
    params = {"api-version": "2024-11-30"}
    headers = {
        "Ocp-Apim-Subscription-Key": di_key,
        "Content-Type": mime,
        "Accept": "application/json"
    }
    try:
        with open(path, "rb") as f:
            resp = requests.post(url, headers=headers, params=params, data=f, timeout=60)
        # Newer API often returns 200 with inline result; legacy style returns 202 + operation-location
        if resp.status_code == 200:
            j = resp.json()
            return _summarize_read_result(j, os.path.basename(path))
        elif resp.status_code == 202:
            op_loc = resp.headers.get("operation-location") or resp.headers.get("Operation-Location")
            if not op_loc:
                print(f"{ERR} DI returned 202 but no operation-location header")
                return False
            j = _di_poll(op_loc, di_key)
            return _summarize_read_result(j, os.path.basename(path))
        elif resp.status_code in (401, 403):
            print(f"{ERR} DI analyze auth failed ({resp.status_code}) on {path}")
            return False
        else:
            print(f"{ERR} DI analyze unexpected {resp.status_code} on {path}: {resp.text[:200]}")
            return False
    except (Timeout, SSLError) as e:
        print(f"{ERR} DI analyze timeout/SSL error on {path}: {e}")
        return False
    except RequestException as e:
        print(f"{ERR} DI analyze request error on {path}: {e}")
        return False

def _summarize_read_result(j, label):
    # Try to extract plain text from common shapes (value/pages/paragraphs/content)
    text = ""
    if isinstance(j, dict):
        if "content" in j and isinstance(j["content"], str):
            text = j["content"]
        elif "pages" in j and isinstance(j["pages"], list):
            # concatenate lines or content
            lines = []
            for p in j["pages"]:
                segs = p.get("words") or p.get("lines") or []
                if isinstance(segs, list):
                    # join words/lines into text-ish
                    if segs and isinstance(segs[0], dict) and "content" in segs[0]:
                        lines.append(" ".join(s.get("content","") for s in segs))
                # fallback: page content
                if p.get("content"): lines.append(p["content"])
            text = "\n".join(lines) if lines else text
        elif "value" in j and isinstance(j["value"], list):
            # some responses wrap in 'value'
            chunks = []
            for v in j["value"]:
                if isinstance(v, dict) and "content" in v:
                    chunks.append(v["content"])
            text = "\n".join(chunks)
    snippet = (text[:300] + "…") if text and len(text) > 300 else (text or "")
    pages = None
    if isinstance(j, dict):
        if isinstance(j.get("pages"), list):
            pages = len(j["pages"])
        elif "analyzeResult" in j and isinstance(j["analyzeResult"], dict) and "pages" in j["analyzeResult"]:
            pages = len(j["analyzeResult"]["pages"])
    print(f"{OK} DI prebuilt-read on '{label}': pages={pages if pages is not None else 'n/a'}, snippet={repr(snippet)}")
    return True

# ---------- Storage Upload (SAS, connection string, or account key) ----------
def _parse_conn_string(cs: str):
    parts = dict([tuple(p.split("=", 1)) for p in cs.split(";") if "=" in p])
    return parts

def _build_blob_url(sa_endpoint, container, blob, sas=None):
    base = sa_endpoint.rstrip("/")
    return f"{base}/{container}/{quote(blob)}{(sas or '')}"

def _ensure_container_with_sas(sa_endpoint, container, sas):
    # HEAD first
    u = f"{sa_endpoint}/{container}{sas}&restype=container" if "?" in sas else f"{sa_endpoint}/{container}?restype=container{sas}"
    r = requests.head(u, timeout=10)
    if r.status_code == 200:
        return True
    if r.status_code == 404:
        # Try create
        u = f"{sa_endpoint}/{container}{sas}&restype=container" if "?" in sas else f"{sa_endpoint}/{container}?restype=container{sas}"
        r = requests.put(u, timeout=10)
        return r.status_code in (201, 202) or r.status_code == 409
    # 403 means SAS lacks container perms
    return r.status_code == 200

def _build_auth_header_shared_key(verb, url, headers, account_name, account_key_b64, content_length=0):
    # Minimal Shared Key construction for blob PUT (BlockBlob)
    u = urlparse(url)
    path = u.path
    query = dict([kv.split("=",1) for kv in u.query.split("&") if "=" in kv]) if u.query else {}
    # Canonicalized headers
    xms = {k.lower(): v for k, v in headers.items() if k.lower().startswith("x-ms-")}
    canon_headers = "".join(f"{k}:{xms[k]}\n" for k in sorted(xms))
    # Canonicalized resource
    canon_resource = f"/{account_name}{path}"
    if query:
        canon_resource += "\n" + "\n".join(f"{k}:{','.join(sorted(v.split(',')))}" for k,v in sorted(query.items()))
    # StringToSign
    string_to_sign = (
        f"{verb}\n"  # VERB
        f"\n"        # Content-Encoding
        f"\n"        # Content-Language
        f"{content_length}\n"  # Content-Length
        f"\n"        # Content-MD5
        f"{headers.get('Content-Type','')}\n"
        f"\n"        # Date
        f"\n"        # If-Modified-Since
        f"\n"        # If-Match
        f"\n"        # If-None-Match
        f"\n"        # If-Unmodified-Since
        f"\n"        # Range
        f"{canon_headers}{canon_resource}"
    )
    key = base64.b64decode(account_key_b64)
    sig = base64.b64encode(hmac.new(key, string_to_sign.encode("utf-8"), hashlib.sha256).digest()).decode()
    return f"SharedKey {account_name}:{sig}"

def storage_upload_blob(sa_endpoint, cfg, container, local_path, blob_name, keep=False):
    if not os.path.exists(local_path):
        print(f"{ERR} Upload test file not found: {local_path}")
        return False

    data = open(local_path, "rb").read()
    ct, _ = mimetypes.guess_type(local_path)
    if not ct: ct = "application/octet-stream"

    # Prefer SAS if provided
    sas = cfg.get("SA_SAS", "").strip()
    if sas and not sas.startswith("?"):
        sas = "?" + sas

    conn_str = cfg.get("SA_ConnectionString", "").strip()
    acct = cfg.get("SA_AccountName", "").strip()
    keyb64 = cfg.get("SA_AccountKey", "").strip()

    # Build URL base
    if sas:
        # Ensure container exists if SAS has container perms (ok if it doesn't)
        try:
            _ensure_container_with_sas(sa_endpoint, container, sas)
        except Exception:
            pass
        put_url = _build_blob_url(sa_endpoint, container, blob_name, sas)
        headers = {
            "x-ms-blob-type": "BlockBlob",
            "x-ms-version": "2021-10-04",
            "Content-Type": ct,
        }
        r = requests.put(put_url, headers=headers, data=data, timeout=30)
        if r.status_code in (201, 202):
            print(f"{OK} Blob uploaded via SAS: {container}/{blob_name}")
            if not keep:
                d = requests.delete(put_url, timeout=15)
                if d.status_code in (202, 204):
                    print(f"{OK} Cleanup: deleted blob {container}/{blob_name}")
            return True
        else:
            print(f"{ERR} SAS upload failed ({r.status_code}): {r.text[:200]}")
            return False

    # Connection string or account key path (Shared Key)
    if conn_str:
        parts = _parse_conn_string(conn_str)
        account_name = parts.get("AccountName")
        account_key_b64 = parts.get("AccountKey")
        if not account_name or not account_key_b64:
            print(f"{ERR} Connection string missing AccountName/AccountKey")
            return False
    elif acct and keyb64:
        account_name, account_key_b64 = acct, keyb64
    else:
        print(f"{ERR} No Storage auth provided. Add 'SA_SAS' or 'SA_ConnectionString' or 'SA_AccountName'+'SA_AccountKey' to connections.json.")
        return False

    # Create container if needed (best-effort)
    cont_url = f"{sa_endpoint}/{container}?restype=container"
    ts = datetime.utcnow().strftime("%a, %d %b %Y %H:%M:%S GMT")
    h = {"x-ms-version":"2021-10-04", "x-ms-date": ts}
    auth = _build_auth_header_shared_key("PUT", cont_url, h, account_name, account_key_b64)
    hc = {**h, "Authorization": auth}
    cr = requests.put(cont_url, headers=hc, timeout=10)
    # 201=created, 202=accepted, 409=exists, 403=forbidden (okay if already exists or insufficient perms—upload might still work)

    # Upload blob
    put_url = f"{sa_endpoint}/{container}/{quote(blob_name)}"
    ts = datetime.utcnow().strftime("%a, %d %b %Y %H:%M:%S GMT")
    headers = {
        "x-ms-blob-type": "BlockBlob",
        "x-ms-version": "2021-10-04",
        "x-ms-date": ts,
        "Content-Type": ct,
    }
    auth = _build_auth_header_shared_key("PUT", put_url, headers, account_name, account_key_b64, content_length=len(data))
    headers["Authorization"] = auth
    r = requests.put(put_url, headers=headers, data=data, timeout=30)
    if r.status_code in (201, 202):
        print(f"{OK} Blob uploaded: {container}/{blob_name}")
        if not keep:
            # delete
            ts = datetime.utcnow().strftime("%a, %d %b %Y %H:%M:%S GMT")
            dh = {"x-ms-version": "2021-10-04", "x-ms-date": ts}
            dauth = _build_auth_header_shared_key("DELETE", put_url, dh, account_name, account_key_b64)
            dh["Authorization"] = dauth
            d = requests.delete(put_url, headers=dh, timeout=15)
            if d.status_code in (202, 204):
                print(f"{OK} Cleanup: deleted blob {container}/{blob_name}")
        return True
    print(f"{ERR} Upload failed ({r.status_code}): {r.text[:200]}")
    return False

def main():
    ap = argparse.ArgumentParser(description="Connectivity/auth checks + optional DI read + Blob upload test.")
    ap.add_argument("-f", "--file", default="connections.json", help="Path to connections.json")
    ap.add_argument("--di-pdf", default="PDF test read.pdf", help="Path to PDF for DI test")
    ap.add_argument("--di-txt", default="Dont_Open_This.txt", help="Path to TXT for DI test")
    ap.add_argument("--container", default="healthcheck", help="Blob container to use for upload test")
    ap.add_argument("--keep", action="store_true", help="Keep uploaded blob (no cleanup)")
    args = ap.parse_args()

    # Load config
    try:
        with open(args.file, "r", encoding="utf-8") as f:
            cfg = json.load(f)
    except FileNotFoundError:
        die(f"File not found: {args.file}")
    except json.JSONDecodeError as e:
        die(f"Invalid JSON in {args.file}: {e}")

    di_key = validate_required(cfg, "DI-Key")
    di_endpoint = validate_url("DI_Endpoint", validate_required(cfg, "DI_Endpoint"))
    sa_endpoint = validate_url("SA-endpoint", validate_required(cfg, "SA-endpoint"))

    print("=== Validating endpoints ===")
    di_host = urlparse(di_endpoint).hostname
    sa_host = urlparse(sa_endpoint).hostname
    di_dns_ok = dns_check(di_host) if di_host else False
    sa_dns_ok = dns_check(sa_host) if sa_host else False

    print("\n=== Checking Azure Document Intelligence (List Models) ===")
    di_ok = di_dns_ok and di_list_models(di_endpoint, di_key)

    print("\n=== Checking Storage Account HTTPS reachability ===")
    sa_ok = sa_dns_ok and storage_https_reachability(sa_endpoint)

    # --- New tests ---
    print("\n=== DI prebuilt-read test on provided files ===")
    di_pdf_ok = di_analyze_file(di_endpoint, di_key, args.di_pdf)
    di_txt_ok = di_analyze_file(di_endpoint, di_key, args.di_txt)

    print("\n=== Storage upload test (creates a temp blob, then deletes unless --keep) ===")
    upload_ok = storage_upload_blob(sa_endpoint, cfg, args.container, "Dont_Open_This.txt", "Dont_Open_This.txt", keep=args.keep)

    print("\n=== Summary ===")
    print(f"DI Reachability     : {'PASS' if di_ok else 'FAIL'}")
    print(f"Storage Reachability: {'PASS' if sa_ok else 'FAIL'}")
    print(f"DI Read (PDF)       : {'PASS' if di_pdf_ok else 'FAIL'}")
    print(f"DI Read (TXT)       : {'PASS' if di_txt_ok else 'FAIL'}")
    print(f"Blob Upload         : {'PASS' if upload_ok else 'FAIL'}")

    # Exit code
    passes = sum([di_ok, sa_ok, di_pdf_ok, di_txt_ok, upload_ok])
    if passes == 5:
        sys.exit(0)
    elif passes == 0:
        sys.exit(1)
    else:
        sys.exit(2)

if __name__ == "__main__":
    main()
