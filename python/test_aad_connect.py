#!/usr/bin/env python3
import argparse, json, sys, socket, time, mimetypes, os, base64
from urllib.parse import urlparse, urljoin, quote
from datetime import datetime, timezone
import requests
from requests.exceptions import RequestException, SSLError, Timeout

# AAD
try:
    from azure.identity import DefaultAzureCredential
except Exception:
    DefaultAzureCredential = None

OK = "[OK]"
ERR = "[ERR]"

# ---------- Utils ----------
def die(msg, code=1):
    print(f"{ERR} {msg}")
    sys.exit(code)

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
                print(f"{OK} Using '{k}' for {label} (backward-compat).")
            return v
    die(f"Missing or empty {' / '.join(keys)} in connections.json (expect '{label}')")

def dns_check(hostname: str):
    try:
        ip = socket.gethostbyname(hostname)
        print(f"{OK} DNS resolved {hostname} -> {ip}")
        return True
    except socket.gaierror as e:
        print(f"{ERR} DNS failed for {hostname}: {e}")
        return False

def rfc1123_now():
    return datetime.now(timezone.utc).strftime("%a, %d %b %Y %H:%M:%S GMT")

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

# ---------- AAD token + identity print ----------
def b64url_decode(s: str) -> bytes:
    s += "=" * (-len(s) % 4)
    return base64.urlsafe_b64decode(s)

def print_token_identity(jwt_token: str):
    try:
        parts = jwt_token.split(".")
        payload = json.loads(b64url_decode(parts[1]))
        oid = payload.get("oid") or payload.get("appid")
        upn = payload.get("upn") or payload.get("preferred_username")
        tid = payload.get("tid")
        sub = payload.get("sub")
        azp = payload.get("azp")
        print(f"{OK} AAD token acquired: oid/appid={oid}, upn={upn}, tid={tid}, sub={sub}, azp={azp}")
    except Exception as e:
        print(f"{ERR} Could not decode AAD token claims: {e}")

def get_aad_token():
    if DefaultAzureCredential is None:
        die("azure-identity not installed. Install with: pip install azure-identity")
    try:
        cred = DefaultAzureCredential()
        token = cred.get_token("https://storage.azure.com/.default")
        print_token_identity(token.token)
        return token.token
    except Exception as e:
        die(f"Failed to acquire AAD token: {e}")

# ---------- Document Intelligence (REST, key header only) ----------
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
        print(f"{ERR} DI timeout/SSL error: {e}")
        return False
    except RequestException as e:
        print(f"{ERR} DI request error: {e}")
        return False

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
            return summarize_read_result(resp.json(), os.path.basename(path))
        elif resp.status_code == 202:
            op_loc = resp.headers.get("operation-location") or resp.headers.get("Operation-Location")
            if not op_loc:
                print(f"{ERR} DI returned 202 but no operation-location header")
                return False
            return poll_di(op_loc, di_key, os.path.basename(path))
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

def poll_di(operation_url, di_key, label, timeout_sec=60, interval=2):
    headers = {"Ocp-Apim-Subscription-Key": di_key, "Accept": "application/json"}
    end = time.time() + timeout_sec
    while time.time() < end:
        r = requests.get(operation_url, headers=headers, timeout=10)
        if r.status_code in (200, 201):
            j = r.json()
            status = (j.get("status") or j.get("statusCode") or "").lower()
            if status.startswith("succeed") or status in ("succeeded", "completed"):
                return summarize_read_result(j, label)
            if status.startswith("fail"):
                print(f"{ERR} DI analyze failed: {str(j)[:200]}")
                return False
        time.sleep(interval)
    print(f"{ERR} Timed out waiting for DI analyze result for {label}")
    return False

def summarize_read_result(j, label):
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

# ---------- Storage (AAD only) ----------
def ensure_container_aad(sa_endpoint, container, bearer):
    url = f"{sa_endpoint}/{container}?restype=container"
    headers = {
        "Authorization": f"Bearer {bearer}",
        "x-ms-version": "2021-10-04",
        "x-ms-date": rfc1123_now(),
    }
    r = requests.put(url, headers=headers, timeout=10)
    return r.status_code in (201, 202, 409, 200)

def upload_blob_aad(sa_endpoint, container, local_path, blob_name, bearer):
    if not os.path.exists(local_path):
        print(f"{ERR} Upload test file not found: {local_path}")
        return False
    with open(local_path, "rb") as f:
        data = f.read()
    ct, _ = mimetypes.guess_type(local_path)
    if not ct: ct = "application/octet-stream"
    put_url = f"{sa_endpoint}/{container}/{quote(blob_name)}"
    headers = {
        "Authorization": f"Bearer {bearer}",
        "x-ms-version": "2021-10-04",
        "x-ms-date": rfc1123_now(),
        "x-ms-blob-type": "BlockBlob",
        "Content-Type": ct,
    }
    r = requests.put(put_url, headers=headers, data=data, timeout=30)
    if r.status_code in (201, 202):
        print(f"{OK} Blob uploaded via AAD: {container}/{blob_name}")
        return True
    elif r.status_code == 403:
        print(f"{ERR} AAD upload forbidden (403). Ensure this principal has 'Storage Blob Data Contributor' on the account or container.")
        return False
    else:
        print(f"{ERR} AAD upload failed ({r.status_code}): {r.text[:200]}")
        return False

def delete_blob_aad(sa_endpoint, container, blob_name, bearer):
    del_url = f"{sa_endpoint}/{container}/{quote(blob_name)}"
    headers = {
        "Authorization": f"Bearer {bearer}",
        "x-ms-version": "2021-10-04",
        "x-ms-date": rfc1123_now(),
    }
    r = requests.delete(del_url, headers=headers, timeout=15)
    return r.status_code in (202, 204)

# ---------- Main ----------
def main():
    ap = argparse.ArgumentParser(description="AAD-only connectivity + DI read + Blob upload test.")
    ap.add_argument("-f", "--file", default="connections.json", help="Path to connections.json")
    ap.add_argument("--di-pdf", default="PDF test read.pdf", help="Path to PDF for DI test")
    ap.add_argument("--di-txt", default="Dont_Open_This.txt", help="Path to TXT for DI test")
    ap.add_argument("--container", default="healthcheck", help="Blob container to use")
    ap.add_argument("--keep", action="store_true", help="Keep uploaded blob (no cleanup)")
    ap.add_argument("--no-create", action="store_true", help="Do not attempt container creation (PUT container)")
    args = ap.parse_args()

    # Load config
    try:
        with open(args.file, "r", encoding="utf-8") as f:
            cfg = json.load(f)
    except FileNotFoundError:
        die(f"File not found: {args.file}")
    except json.JSONDecodeError as e:
        die(f"Invalid JSON in {args.file}: {e}")

    di_key = get_required(cfg, "DI-Key")
    di_endpoint = validate_url("DI-Endpoint", get_required_any(cfg, ["DI-Endpoint", "DI_Endpoint"], "DI-Endpoint"))
    sa_endpoint = validate_url("SA-endpoint", get_required(cfg, "SA-endpoint"))

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

    print("\n=== Storage upload test (AAD only) ===")
    bearer = get_aad_token()
    created = False
    if not args.no_create:
        created = ensure_container_aad(sa_endpoint, args.container, bearer)
        if created:
            print(f"{OK} Container ensure attempted: {args.container}")
    upload_ok = upload_blob_aad(sa_endpoint, args.container, args.di_txt, "Dont_Open_This.txt", bearer)
    if upload_ok and not args.keep:
        if delete_blob_aad(sa_endpoint, args.container, "Dont_Open_This.txt", bearer):
            print(f"{OK} Cleanup: deleted blob {args.container}/Dont_Open_This.txt")

    print("\n=== Summary ===")
    print(f"DI Reachability     : {'PASS' if di_ok else 'FAIL'}")
    print(f"Storage Reachability: {'PASS' if sa_ok else 'FAIL'}")
    print(f"DI Read (PDF)       : {'PASS' if di_pdf_ok else 'FAIL'}")
    print(f"Blob Upload (AAD)   : {'PASS' if upload_ok else 'FAIL'}")

    passes = sum([di_ok, sa_ok, di_pdf_ok, upload_ok])
    if passes == 4:
        sys.exit(0)
    elif passes == 0:
        sys.exit(1)
    else:
        sys.exit(2)

if __name__ == "__main__":
    main()
