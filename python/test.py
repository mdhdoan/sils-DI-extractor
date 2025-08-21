#!/usr/bin/env python3
import argparse, json, sys, socket, time, mimetypes, os
from urllib.parse import urlparse, urljoin, quote
import requests
from requests.exceptions import RequestException, SSLError, Timeout
from datetime import datetime, timezone

# NEW: Azure AD
try:
    from azure.identity import DefaultAzureCredential
except Exception:
    DefaultAzureCredential = None

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
    headers = {"Ocp-Apim-Subscription-Key": di_key, "Accept": "application/json"}
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
            if status and status.lower().startswith("succeed"):
                return j
            if status and status.lower().startswith("fail"):
                raise RuntimeError(f"DI analyze failed: {j}")
        time.sleep(interval)
    raise TimeoutError("Timed out waiting for analyze result")

def di_analyze_file(di_endpoint, di_key, path):
    if not os.path.exists(path):
        print(f"{ERR} DI test file not found: {path}")
        return False
    mime, _ = mimetypes.guess_type(path)
    if not mime:
        if path.lower().endswith(".pdf"): mime = "application/pdf"
        elif path.lower().endswith(".txt"): mime = "text/plain"
        else: mime = "application/octet-stream"

    url = urljoin(di_endpoint + "/", "documentintelligence/documentModels/prebuilt-read:analyze")
    params = {"api-version": "2024-11-30"}
    headers = {"Ocp-Apim-Subscription-Key": di_key, "Content-Type": mime, "Accept": "application/json"}
    try:
        with open(path, "rb") as f:
            resp = requests.post(url, headers=headers, params=params, data=f, timeout=60)
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
    text = ""
    if isinstance(j, dict):
        if isinstance(j.get("content"), str):
            text = j["content"]
        elif isinstance(j.get("value"), list):
            text = "\n".join([v.get("content","") for v in j["value"] if isinstance(v, dict)])
    snippet = (text[:300] + "…") if text and len(text) > 300 else (text or "")
    pages = None
    if isinstance(j, dict):
        if isinstance(j.get("pages"), list):
            pages = len(j["pages"])
        elif isinstance(j.get("analyzeResult", {}).get("pages", None), list):
            pages = len(j["analyzeResult"]["pages"])
    print(f"{OK} DI prebuilt-read on '{label}': pages={pages if pages is not None else 'n/a'}, snippet={repr(snippet)}")
    return True

# ---------- Storage Upload ----------
def _rfc1123_now():
    return datetime.now(timezone.utc).strftime("%a, %d %b %Y %H:%M:%S GMT")

def _aad_get_token():
    if DefaultAzureCredential is None:
        print(f"{ERR} azure-identity not installed. Install with: pip install azure-identity")
        return None
    try:
        cred = DefaultAzureCredential()
        token = cred.get_token("https://storage.azure.com/.default")
        return token.token
    except Exception as e:
        print(f"{ERR} Failed to acquire AAD token: {e}")
        return None

def _ensure_container_aad(sa_endpoint, container, bearer):
    # PUT container (idempotent); requires Data Contributor or higher
    url = f"{sa_endpoint}/{container}?restype=container"
    headers = {
        "Authorization": f"Bearer {bearer}",
        "x-ms-version": "2021-10-04",
        "x-ms-date": _rfc1123_now(),
    }
    r = requests.put(url, headers=headers, timeout=10)
    return r.status_code in (201, 202, 409) or r.status_code == 200

def _upload_blob_aad(sa_endpoint, container, local_path, blob_name, keep, bearer):
    with open(local_path, "rb") as f:
        data = f.read()
    ct, _ = mimetypes.guess_type(local_path)
    if not ct: ct = "application/octet-stream"

    put_url = f"{sa_endpoint}/{container}/{quote(blob_name)}"
    headers = {
        "Authorization": f"Bearer {bearer}",
        "x-ms-version": "2021-10-04",
        "x-ms-date": _rfc1123_now(),
        "x-ms-blob-type": "BlockBlob",
        "Content-Type": ct,
    }
    r = requests.put(put_url, headers=headers, data=data, timeout=30)
    if r.status_code in (201, 202):
        print(f"{OK} Blob uploaded via AAD: {container}/{blob_name}")
        if not keep:
            dh = {
                "Authorization": f"Bearer {bearer}",
                "x-ms-version": "2021-10-04",
                "x-ms-date": _rfc1123_now(),
            }
            d = requests.delete(put_url, headers=dh, timeout=15)
            if d.status_code in (202, 204):
                print(f"{OK} Cleanup: deleted blob {container}/{blob_name}")
        return True
    elif r.status_code == 403:
        print(f"{ERR} AAD upload forbidden (403). Ensure your principal has 'Storage Blob Data Contributor' on the account or container.")
        return False
    else:
        print(f"{ERR} AAD upload failed ({r.status_code}): {r.text[:200]}")
        return False

def storage_upload_blob(sa_endpoint, cfg, container, local_path, blob_name, keep=False):
    # Prefer SAS if present (note: account SAS won’t work when key-based auth is disabled; user-delegation SAS will)
    sas = cfg.get("SA_SAS", "").strip()
    if sas:
        if not sas.startswith("?"): sas = "?" + sas
        # Best-effort container ensure
        try:
            u = f"{sa_endpoint}/{container}{sas}&restype=container" if "?" in sas else f"{sa_endpoint}/{container}?restype=container{sas}"
            r = requests.put(u, timeout=10)
        except Exception:
            pass
        put_url = f"{sa_endpoint}/{container}/{quote(blob_name)}{sas}"
        with open(local_path, "rb") as f:
            data = f.read()
        ct, _ = mimetypes.guess_type(local_path)
        if not ct: ct = "application/octet-stream"
        headers = {"x-ms-blob-type": "BlockBlob", "x-ms-version": "2021-10-04", "Content-Type": ct}
        r = requests.put(put_url, headers=headers, data=data, timeout=30)
        if r.status_code in (201, 202):
            print(f"{OK} Blob uploaded via SAS: {container}/{blob_name}")
            if not keep:
                d = requests.delete(put_url, timeout=15)
                if d.status_code in (202, 204):
                    print(f"{OK} Cleanup: deleted blob {container}/{blob_name}")
            return True
        if r.status_code == 403 and "KeyBasedAuthenticationNotPermitted" in r.text:
            print(f"{ERR} Account-level SAS blocked by policy (KeyBasedAuthenticationNotPermitted). Falling back to AAD.")
        else:
            print(f"{ERR} SAS upload failed ({r.status_code}): {r.text[:200]}")
            # If generic 403, we’ll still try AAD next
        # continue into AAD fallback

    # Shared Key path is intentionally NOT attempted when the account blocks key-based auth.
    # We go straight to AAD.
    bearer = _aad_get_token()
    if not bearer:
        print(f"{ERR} Cannot proceed with AAD upload (no token).")
        return False

    # Ensure container (idempotent)
    _ensure_container_aad(sa_endpoint, container, bearer)

    # Upload via AAD
    return _upload_blob_aad(sa_endpoint, container, local_path, blob_name, keep, bearer)

def main():
    ap = argparse.ArgumentParser(description="Connectivity/auth checks + optional DI read + Blob upload test.")
    ap.add_argument("-f", "--file", default="connections.json", help="Path to connections.json")
    ap.add_argument("--di-pdf", default="PDF test read.pdf", help="Path to PDF for DI test")
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
    di_endpoint = validate_url("DI-Endpoint", validate_required(cfg, "DI-Endpoint"))
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

    print("\n=== DI prebuilt-read test on provided files ===")
    di_pdf_ok = di_analyze_file(di_endpoint, di_key, args.di_pdf)
    di_txt_ok = di_analyze_file(di_endpoint, di_key, args.di_txt)

    print("\n=== Storage upload test (AAD-aware; creates temp blob; deletes unless --keep) ===")
    upload_ok = storage_upload_blob(sa_endpoint, cfg, args.container, "Dont_Open_This.txt", "Dont_Open_This.txt", keep=args.keep)

    print("\n=== Summary ===")
    print(f"DI Reachability     : {'PASS' if di_ok else 'FAIL'}")
    print(f"Storage Reachability: {'PASS' if sa_ok else 'FAIL'}")
    print(f"DI Read (PDF)       : {'PASS' if di_pdf_ok else 'FAIL'}")
    print(f"DI Read (TXT)       : {'PASS' if di_txt_ok else 'FAIL'}")
    print(f"Blob Upload         : {'PASS' if upload_ok else 'FAIL'}")

    passes = sum([di_ok, sa_ok, di_pdf_ok, di_txt_ok, upload_ok])
    if passes == 5:
        sys.exit(0)
    elif passes == 0:
        sys.exit(1)
    else:
        sys.exit(2)

if __name__ == "__main__":
    main()
