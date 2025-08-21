#!/usr/bin/env python3
import argparse
import json
import sys
import socket
from urllib.parse import urlparse

import requests
from requests.exceptions import RequestException, SSLError, Timeout

try:
    from azure.ai.documentintelligence import DocumentIntelligenceClient
    from azure.core.credentials import AzureKeyCredential
    from azure.core.exceptions import HttpResponseError
except Exception as e:
    # We handle missing SDKs gracefully; tell the user how to install.
    DocumentIntelligenceClient = None
    AzureKeyCredential = None
    HttpResponseError = Exception


OK = "[OK]"
ERR = "[ERR]"


def die(msg: str, code: int = 1):
    print(f"{ERR} {msg}")
    sys.exit(code)


def validate_required(data: dict, key: str) -> str:
    if key not in data or not str(data[key]).strip():
        die(f"Missing or empty '{key}' in connections.json")
    return str(data[key]).strip()


def validate_url(name: str, value: str) -> str:
    try:
        u = urlparse(value)
    except Exception:
        die(f"{name} is not a valid URL: {value}")
    if u.scheme not in ("https", "http") or not u.netloc:
        die(f"{name} must be an absolute URL (https://...), got: {value}")
    return value


def dns_check(hostname: str):
    try:
        ip = socket.gethostbyname(hostname)
        print(f"{OK} DNS resolved {hostname} -> {ip}")
        return True
    except socket.gaierror as e:
        print(f"{ERR} DNS failed for {hostname}: {e}")
        return False


def storage_https_reachability(sa_endpoint: str, timeout_sec: int = 8) -> bool:
    """
    We only check reachability. A 200/401/403 indicates the endpoint is alive.
    No data is listed or modified.
    """
    url = sa_endpoint.rstrip("/") + "/"
    try:
        resp = requests.get(url, timeout=timeout_sec)
        code = resp.status_code
        if code in (200, 401, 403):
            # Helpful header if present
            ver = resp.headers.get("x-ms-version")
            errcode = resp.headers.get("x-ms-error-code")
            note = f"x-ms-version={ver}" if ver else "no x-ms-version"
            if errcode:
                note += f", x-ms-error-code={errcode}"
            print(f"{OK} Storage HTTPS reachable ({code}); {note}")
            return True
        print(f"{ERR} Storage endpoint responded with unexpected status {code}")
        return False
    except (Timeout, SSLError) as e:
        print(f"{ERR} Storage HTTPS timeout/SSL error: {e}")
        return False
    except RequestException as e:
        print(f"{ERR} Storage HTTPS request error: {e}")
        return False


def di_check(di_endpoint: str, di_key: str) -> bool:
    if DocumentIntelligenceClient is None or AzureKeyCredential is None:
        print(f"{ERR} Azure SDK not installed. Install with:\n"
              "    pip install azure-ai-documentintelligence azure-core requests\n")
        return False
    try:
        client = DocumentIntelligenceClient(
            endpoint=di_endpoint,
            credential=AzureKeyCredential(di_key),
        )
        info = client.get_info()  # Non-destructive capability call
        # `info` can be a dict-like object; pick some friendly fields if present
        api_version = getattr(info, "api_version", None) or getattr(info, "version", None) or info.get("apiVersion", None) if isinstance(info, dict) else None
        print(f"{OK} Document Intelligence reachable; get_info() succeeded"
              + (f" (api_version={api_version})" if api_version else ""))
        return True
    except HttpResponseError as e:
        # Authentication or endpoint issues will surface here
        print(f"{ERR} Document Intelligence API error: {e}")
        return False
    except Exception as e:
        print(f"{ERR} Document Intelligence unexpected error: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(description="Check connectivity to Azure resources using connections.json.")
    parser.add_argument("-f", "--file", default="connections.json", help="Path to connections.json (default: connections.json)")
    args = parser.parse_args()

    # Load and validate the json
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

    print("\n=== Checking Azure Document Intelligence ===")
    di_ok = di_dns_ok and di_check(di_endpoint, di_key)

    print("\n=== Checking Storage Account HTTPS reachability ===")
    sa_ok = sa_dns_ok and storage_https_reachability(sa_endpoint)

    print("\n=== Summary ===")
    print(f"Document Intelligence: {'PASS' if di_ok else 'FAIL'}")
    print(f"Storage Endpoint    : {'PASS' if sa_ok else 'FAIL'}")

    # Exit codes: 0 = all good, 2 = partial, 1 = total fail
    if di_ok and sa_ok:
        sys.exit(0)
    elif di_ok or sa_ok:
        sys.exit(2)
    else:
        sys.exit(1)


if __name__ == "__main__":
    main()
