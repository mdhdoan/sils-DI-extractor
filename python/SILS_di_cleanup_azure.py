#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Azure BC16 cleaner/validator (nested) + polygon + numeric checks

- Uses connections.json (same as your test.py)
- Auth via AAD (DefaultAzureCredential)
- Source:
    container: test_container
    prefix:    test_directory/upload_jsons/
- Target (output):
    container: test_container
    prefix:    test_directory/validated_jsons/
- For each JSON blob:
    - download
    - clean (content, zero-like, polygons)
    - validate (groups, precipitation, numeric, percent trio, SK_Count_data_2)
    - upload cleaned JSON
- At the end:
    - upload a single report.json summarizing issues per file
"""

import os
import sys
import json
import time
from datetime import datetime, timezone
from urllib.parse import urlparse, quote

import requests

OK, ERR = "[OK]", "[ERR]"

# ---------------------------------------------------------------------
# CONFIG (adjust if you want)
# ---------------------------------------------------------------------
SOURCE_CONTAINER = "test_container"
SOURCE_PREFIX = "test_directory/upload_jsons/"
TARGET_PREFIX = "test_directory/validated_jsons/"
REPORT_BLOB_NAME = TARGET_PREFIX + "report.json"

# ---------------------------------------------------------------------
# Azure Identity
# ---------------------------------------------------------------------
try:
    from azure.identity import DefaultAzureCredential
except Exception:
    DefaultAzureCredential = None


def die(msg, code=1):
    print(f"{ERR} {msg}")
    sys.exit(code)


def rfc1123_now():
    return datetime.now(timezone.utc).strftime("%a, %d %b %Y %H:%M:%S GMT")


def get_aad_token_for_storage():
    if DefaultAzureCredential is None:
        die("azure-identity not installed. Run: pip install azure-identity")
    cred = DefaultAzureCredential()
    token = cred.get_token("https://storage.azure.com/.default")
    print(f"{OK} AAD token acquired for storage")
    return token.token

# ---------------------------------------------------------------------
# connections.json helpers (same style as your previous scripts)
# ---------------------------------------------------------------------
def get_required(d: dict, k: str) -> str:
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
        die(f"{name} must be an absolute URL, got: {value}")
    return value.rstrip("/")

# ---------------------------------------------------------------------
# Blob helpers (list / download / upload)
# ---------------------------------------------------------------------
def list_blobs_aad(sa_endpoint: str, container: str, bearer: str, prefix: str = None, timeout=15):
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
        headers = {
            "Authorization": f"Bearer {bearer}",
            "x-ms-version": "2021-10-04",
            "x-ms-date": rfc1123_now(),
        }
        r = requests.get(url, headers=headers, timeout=timeout)
        if r.status_code != 200:
            raise RuntimeError(f"List blobs failed {r.status_code}: {r.text[:200]}")
        txt = r.text
        names += [seg.split("</Name>")[0] for seg in txt.split("<Name>")[1:]]
        marker = ""
        if "<NextMarker>" in txt:
            marker = txt.split("<NextMarker>")[1].split("</NextMarker>")[0]
        if not marker:
            break
    return names


def download_blob_aad(sa_endpoint: str, container: str, blob_name: str, bearer: str, timeout=60) -> bytes:
    url = f"{sa_endpoint}/{container}/{quote(blob_name)}"
    headers = {
        "Authorization": f"Bearer {bearer}",
        "x-ms-version": "2021-10-04",
        "x-ms-date": rfc1123_now(),
    }
    r = requests.get(url, headers=headers, timeout=timeout, stream=True)
    if r.status_code == 200:
        return r.content
    elif r.status_code == 404:
        raise FileNotFoundError(f"Blob not found: {blob_name}")
    else:
        raise RuntimeError(f"Download failed {r.status_code}: {r.text[:200]}")


def upload_blob_aad(sa_endpoint: str, container: str, blob_name: str, bearer: str, data: bytes, timeout=60):
    """
    Simple PUT blob (BlockBlob) via REST with AAD.
    Overwrites if exists.
    """
    url = f"{sa_endpoint}/{container}/{quote(blob_name)}"
    headers = {
        "Authorization": f"Bearer {bearer}",
        "x-ms-version": "2021-10-04",
        "x-ms-date": rfc1123_now(),
        "x-ms-blob-type": "BlockBlob",
        "Content-Length": str(len(data)),
        "Content-Type": "application/json; charset=utf-8",
    }
    r = requests.put(url, headers=headers, data=data, timeout=timeout)
    if r.status_code not in (201, 202):
        raise RuntimeError(f"Upload failed {r.status_code}: {r.text[:200]}")
    print(f"{OK} Uploaded {blob_name}")

# ---------------------------------------------------------------------
# Cleaning / validation logic (same as local version)
# ---------------------------------------------------------------------
survey_list = ['Survey Type: Ground','Survey Type: Aerial']
water_level_list = ['Water Level: % Bankfill: <25%',
                    'Water Level: % Bankfill: 25-50%',
                    'Water Level: % Bankfill: 50-75%',
                    'Water Level: % Bankfill: 75-100%',
                    'Water Level: % Bankfill: +100%']
weather_brightness_list = ['Weather: Brightness: Full','Weather: Brightness: Bright',
                    'Weather: Brightness: Medium','Weather: Brightness: Dark']
weather_cloudy_list = ['Weather: %Cloudy: 0%','Weather: %Cloudy: 25%',
                    'Weather: %Cloudy: 50%','Weather: %Cloudy: 75%','Weather: %Cloudy: 100%']
precipitation_type_list = ['Precipitation: Type: Rain','Precipitation: Type: Snow','Precipitation: Type: None']
precipitation_intensity_list = ['Precipitation: Intensity: Light','Precipitation: Intensity: Medium','Precipitation: Intensity: Heavy']
fish_visibility_list = ['Water Conditions: Fish Visibility: Low',
                    'Water Conditions: Fish Visibility: Medium','Water Conditions: Fish Visibility: High']
water_clarity_list = ['Water Conditions: Water Clarity: 0-0.25m','Water Conditions: Water Clarity: 0.25-0.5m',
                    'Water Conditions: Water Clarity: 0.5-1.0m','Water Conditions: Water Clarity: 1-3m',
                    'Water Conditions: Water Clarity: 3m to bottom']

NUMERIC_FIELDS = [
    "Live Count (incl. live tags):",
    "Live Count 2 (incl. live tags):",
    "% O.E. (Observer Efficiency):",
    "% Holding / Migrating:",
    "% Spawning:",
    "% Spawned Out:",
    "Dead:",
    "Dead 2:",
    "Other species:",
    "Other species 2:",
]

PERCENT_TRIO = ["% Holding / Migrating:", "% Spawning:", "% Spawned Out:"]

def clean_selected_flag(v: str) -> str:
    if not isinstance(v, str):
        return v
    val = v.strip()
    low = val.lower()
    if low == ":selected:":
        return "selected"
    if low == ":unselected:":
        return "unselected"
    if "selected" in low and "unselected" not in low:
        return "selected"
    if "unselected" in low:
        return "unselected"
    return val

def is_comment_field(field_name: str) -> bool:
    return "comment" in field_name.lower()

def normalize_zero_like(v: str) -> str:
    if not isinstance(v, str):
        return v
    raw = v.strip()
    if raw in ("Ø", "ø", "O", "o"):
        return "0"
    return v

def convert_polygon(poly):
    if not isinstance(poly, list):
        return poly
    if not poly:
        return poly
    if isinstance(poly[0], list) and len(poly[0]) == 2:
        return poly
    if all(isinstance(n, (int, float)) for n in poly):
        pairs = []
        it = iter(poly)
        for x in it:
            y = next(it, None)
            if y is None:
                break
            pairs.append([x, y])
        return pairs
    return poly

def clean_node(node, current_key_path=""):
    if isinstance(node, dict) and "content" in node:
        is_comment = "comment" in current_key_path.lower()
        content_val = clean_selected_flag(node["content"])
        if not is_comment:
            content_val = normalize_zero_like(content_val)
        new_node = {}
        for k, v in node.items():
            if k == "content":
                new_node[k] = content_val
            elif k == "polygon":
                new_node[k] = convert_polygon(v)
            else:
                child_path = f"{current_key_path}.{k}" if current_key_path else k
                new_node[k] = clean_node(v, child_path)
        return new_node

    if isinstance(node, dict):
        new_obj = {}
        for k, v in node.items():
            child_path = f"{current_key_path}.{k}" if current_key_path else k
            if k == "polygon":
                new_obj[k] = convert_polygon(v)
            else:
                new_obj[k] = clean_node(v, child_path)
        return new_obj

    if isinstance(node, list):
        return [clean_node(item, current_key_path) for item in node]

    return node

def get_content_from_cleaned(data: dict, field_name: str):
    v = data.get(field_name)
    if isinstance(v, dict) and "content" in v:
        return v["content"]
    return None

def parse_number_from_content(val):
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return float(val)
    if isinstance(val, str):
        txt = val.strip()
        if txt == "":
            return None
        try:
            return float(txt)
        except ValueError:
            return None
    return None

def at_least_one_selected(cleaned_data: dict, field_list: list) -> bool:
    for fname in field_list:
        val = get_content_from_cleaned(cleaned_data, fname)
        val = clean_selected_flag(val) if val is not None else val
        if val == "selected":
            return True
    return False

def validate_precipitation_rule(cleaned_data: dict):
    none_val = get_content_from_cleaned(cleaned_data, "Precipitation: Type: None")
    none_val = clean_selected_flag(none_val) if none_val is not None else none_val
    if none_val == "unselected":
        if not at_least_one_selected(cleaned_data, precipitation_intensity_list):
            return {"precipitation_type_list": "Precipitation: Type: None is unselected but no precipitation intensity selected."}
    return None

def validate_selection_groups(cleaned_data: dict):
    field_errors = {}
    groups = [
        ("survey_list", survey_list),
        ("water_level_list", water_level_list),
        ("weather_brightness_list", weather_brightness_list),
        ("weather_cloudy_list", weather_cloudy_list),
        ("precipitation_type_list", precipitation_type_list),
        ("fish_visibility_list", fish_visibility_list),
        ("water_clarity_list", water_clarity_list),
    ]
    for name, fields in groups:
        if not at_least_one_selected(cleaned_data, fields):
            field_errors[name] = f"Group '{name}' has no selected values."
    msg = validate_precipitation_rule(cleaned_data)
    if msg:
        field_errors.update(msg)
    return field_errors

def validate_numeric_fields(cleaned_data: dict):
    errs = {}
    for field_name in NUMERIC_FIELDS:
        val = get_content_from_cleaned(cleaned_data, field_name)
        if val is None or val == "":
            continue
        num = parse_number_from_content(val)
        if num is None:
            errs[field_name] = f"Field '{field_name}' should be numeric but got '{val}'."
    return errs

def validate_percentage_trio(cleaned_data: dict):
    vals = []
    for f in PERCENT_TRIO:
        v = get_content_from_cleaned(cleaned_data, f)
        num = parse_number_from_content(v)
        vals.append(num)
    if all(v is None for v in vals):
        return {}
    total = sum(v if v is not None else 0 for v in vals)
    if abs(total - 100) <= 0.5 or abs(total - 1) <= 0.01:
        return {}
    return {"percent_trio": f"Percent trio ({', '.join(PERCENT_TRIO)}) should sum to 100 or 1, got {total}"}

def validate_sk_count_data_2(cleaned_data: dict):
    errs = {}
    sk2 = cleaned_data.get("SK_Count_data_2")
    if not isinstance(sk2, list) or len(sk2) == 0:
        return errs
    first = sk2[0]
    if not isinstance(first, dict):
        return errs

    def getnum(d, key_options):
        for k in key_options:
            v = d.get(k)
            if isinstance(v, dict) and "content" in v:
                num = parse_number_from_content(v["content"])
                if num is not None:
                    return num
        return None

    female = getnum(first, ["Female", "Females", "female", "females"])
    nr = getnum(first, ["NR", "Nr", "nr"])
    p0 = getnum(first, ["0%", "0 %", "Zero", "zero"])
    p50 = getnum(first, ["50%", "50 %"])
    p100 = getnum(first, ["100%", "100 %"])

    if female is None:
        return errs
    parts = [nr, p0, p50, p100]
    if all(p is None for p in parts):
        return errs
    expected = sum(p for p in parts if p is not None)
    if abs(female - expected) > 0.01:
        errs["SK_Count_data_2"] = f"Female ({female}) != NR+0%+50%+100% ({expected})"
    return errs

# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------
def main():
    import argparse
    ap = argparse.ArgumentParser(description="Azure BC16 cleaner/validator")
    ap.add_argument("-f", "--file",     default="connections.json", help="connections.json path")
    ap.add_argument("--container",      default=SOURCE_CONTAINER,   help="Source/target container")
    ap.add_argument("--src-prefix",     default=SOURCE_PREFIX,      help="Prefix to read from")
    ap.add_argument("--dst-prefix",     default=TARGET_PREFIX,      help="Prefix to write cleaned files to")
    ap.add_argument("--report-blob",    default=REPORT_BLOB_NAME,   help="Where to upload the summary report")
    ap.add_argument("--report-blob",    default="report.json",      help="Blob name (path) to write report.json to")

    args = ap.parse_args()

    # 1) load config
    try:
        with open(args.file, "r", encoding="utf-8") as rf:
            cfg = json.load(rf)
    except Exception as e:
        die(f"Failed to load connections.json: {e}")

    sa_endpoint = validate_url("SA-endpoint", get_required(cfg, "SA-endpoint"))

    # 2) auth
    bearer = get_aad_token_for_storage()

    # 3) list source blobs
    try:
        blobs = list_blobs_aad(sa_endpoint, args.container, bearer, prefix=args.src_prefix)
    except Exception as e:
        die(f"Failed to list blobs: {e}")

    json_blobs = [b for b in blobs if b.lower().endswith(".json")]
    print(f"{OK} Found {len(json_blobs)} JSON blobs in {args.container}/{args.src_prefix}")

    report_data = {}

    # 4) process each
    for blob_name in json_blobs:
        print(f"[..] Processing {blob_name}")
        try:
            raw_bytes = download_blob_aad(sa_endpoint, args.container, blob_name, bearer)
            data = json.loads(raw_bytes.decode("utf-8"))
        except Exception as e:
            print(f"{ERR} {blob_name} -> cannot download/parse: {e}")
            report_data[blob_name] = {"file_error": str(e)}
            continue

        # clean
        cleaned = clean_node(data)

        # validations
        errors = {}
        errors.update(validate_selection_groups(cleaned))
        errors.update(validate_numeric_fields(cleaned))
        errors.update(validate_percentage_trio(cleaned))
        errors.update(validate_sk_count_data_2(cleaned))

        # upload cleaned json
        base_name = os.path.basename(blob_name)
        dst_blob_name = f"{args.dst_prefix.rstrip('/')}/{base_name}"
        cleaned_bytes = json.dumps(cleaned, ensure_ascii=False, indent=2).encode("utf-8")
        try:
            upload_blob_aad(sa_endpoint, args.container, dst_blob_name, bearer, cleaned_bytes)
        except Exception as e:
            print(f"{ERR} Failed to upload cleaned {base_name}: {e}")
            # still record errors
        report_data[blob_name] = errors
        if errors:
            print(f"{ERR} {blob_name} -> {len(errors)} issues")
        else:
            print(f"{OK} {blob_name} -> passes all checks")

        # 5) upload report.json
    from datetime import datetime, timezone

    # Add a generation timestamp (UTC ISO 8601)
    timestamp = datetime.now(timezone.utc).isoformat(timespec="seconds")
    report_data["_generated_at"] = timestamp

    report_bytes = json.dumps(report_data, ensure_ascii=False, indent=2).encode("utf-8")

    # normalize the report blob name to avoid trailing slash weirdness
    report_blob_name = args.report_blob.strip("/")
    if not report_blob_name.lower().endswith(".json"):
        # if user passed only a folder-like path, append report.json
        report_blob_name = report_blob_name + "/report.json"

    try:
        upload_blob_aad(sa_endpoint, args.container, report_blob_name, bearer, report_bytes)
        print(f"{OK} Uploaded report to {report_blob_name}")
    except Exception as e:
        print(f"{ERR} Failed to upload report.json to {report_blob_name}: {e}")

    # OPTIONAL: also drop a flat copy at container root for easy access
    try:
        upload_blob_aad(sa_endpoint, args.container, "report.json", bearer, report_bytes)
        print(f"{OK} Uploaded flat report.json to container root")
    except Exception as e:
        print(f"{ERR} Failed to upload root report.json: {e}")



if __name__ == "__main__":
    main()
