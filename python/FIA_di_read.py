#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Azure Document Intelligence (DI) reader with progress tracking.

- Modes:
  * local  : read PDFs from a folder (default: ./tempdata)
  * azure  : read PDFs from a blob container via AAD (no keys/SAS)

- Any DI model ID (e.g., 'prebuilt-read', 'prebuilt-document', or custom)

- Progress:
  * TSV file (default: ./progress_processed.txt)
  * Line format: "<mode>::<id>\t<PAGES>\t<ISO8601>\tOK"
  * '--reset' deletes progress and re-runs everything

- Output:
  * ./json/<output-container>/<basename>.json
  * JSON contains polygons but NO spans (per request)

Requires:
  pip install azure-ai-documentintelligence azure-identity azure-storage-blob
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

import threading
import math
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from azure.core.exceptions import ServiceRequestError, ServiceResponseError, HttpResponseError

# =============================================================================
# Configuration constants
# =============================================================================

# --- Normalization tables ---
O_SLASH_ALIASES = {"Ø", "ø"}
SELECTION_TOKENS = {":selected:": "selected", ":unselected:": "unselected"}

# --- Checkbox groups (exact field names) ---
water_level_list = [
    "Water Level: % Bankfill: <25%",
    "Water Level: % Bankfill: 25-50%",
    "Water Level: % Bankfill: 50-75%",
    "Water Level: % Bankfill: 75-100%",
    "Water Level: % Bankfill: +100%",
]
weather_brightness_list = [
    "Weather: Brightness: Full",
    "Weather: Brightness: Bright",
    "Weather: Brightness: Medium",
    "Weather: Brightness: Dark",
]
weather_cloudy_list = [
    "Weather: %Cloudy: 0%",
    "Weather: %Cloudy: 25%",
    "Weather: %Cloudy: 50%",
    "Weather: %Cloudy: 75%",
    "Weather: %Cloudy: 100%",
]
precipitation_type_list = [
    "Precipitation: Type: Rain",
    "Precipitation: Type: Snow",
    "Precipitation: Type: None",
]
precipitation_intensity_list = [
    "Precipitation: Intensity: Light",
    "Precipitation: Intensity: Medium",
    "Precipitation: Intensity: Heavy",
]
fish_visibility_list = [
    "Water Conditions: Fish Visibility: Low",
    "Water Conditions: Fish Visibility: Medium",
    "Water Conditions: Fish Visibility: High",
]
water_clarity_list = [
    "Water Conditions: Water Clarity: 0-0.25m",
    "Water Conditions: Water Clarity: 0.25-0.5m",
    "Water Conditions: Water Clarity: 0.5-1.0m",
    "Water Conditions: Water Clarity: 1-3m",
    "Water Conditions: Water Clarity: 3m to bottom",
]


@dataclass
class ConnectionsConfig:
    """Connection settings loaded from connections.json."""
    di_endpoint: str
    di_key: str
    storage_account_url: str


class AdaptiveRateLimiter:
    """
    Token-bucket limiter with adaptive backoff on 429/503.
    - target_tps: base tokens per second
    - backoff: decreases effective_tps on throttle, gradually recovers
    """
    def __init__(self, target_tps: int, window: float = 1.0,
                 min_tps: int = 1, recovery_half_life_s: float = 15.0) -> None:
        self.target_tps = max(target_tps, min_tps)
        self.window = window
        self.min_tps = max(1.0, float(min_tps))
        self._lock = threading.Lock()
        self._last_check = time.perf_counter()
        self._tokens = self.target_tps * self.window
        self._effective_tps = float(self.target_tps)
        self._last_penalty_ts = time.perf_counter()
        self._half_life = recovery_half_life_s

    def _recover(self, now: float) -> None:
        """Gradually recover effective_tps back toward target_tps over time."""
        elapsed = now - self._last_penalty_ts
        if elapsed <= 0:
            return
        factor = 0.5 ** (elapsed / self._half_life)
        new_tps = self.target_tps - (self.target_tps - self._effective_tps) * factor
        self._effective_tps = max(self.min_tps, min(self.target_tps, new_tps))

    def acquire(self) -> None:
        """
        Acquire a token, sleeping if necessary so that calls don't exceed
        the current effective_tps.
        """
        while True:
            with self._lock:
                now = time.perf_counter()
                # Refill tokens
                elapsed = now - self._last_check
                if elapsed > 0:
                    self._recover(now)
                    rate = self._effective_tps * self.window
                    self._tokens = min(rate, self._tokens + elapsed * self._effective_tps)
                    self._last_check = now

                if self._tokens >= 1:
                    self._tokens -= 1
                    return

                # Need to wait
                needed = 1 - self._tokens
                wait_time = needed / max(self._effective_tps, 1e-6)
            time.sleep(wait_time)

    def penalize(self) -> None:
        """Reduce effective_tps sharply after a throttle (429/503) response."""
        with self._lock:
            self._effective_tps = max(self.min_tps, self._effective_tps / 2.0)
            self._last_penalty_ts = time.perf_counter()


# =============================================================================
# Extractor (flat schema: content, confidence, polygon)
# =============================================================================

def serialize(obj: Any) -> Any:
    """
    Fallback JSON serializer.
    Supports polygon point-objects (having .x/.y) by converting to [x, y] pairs.
    """
    if isinstance(obj, list) and obj and hasattr(obj[0], "x") and hasattr(obj[0], "y"):
        return [[pt.x, pt.y] for pt in obj]
    if isinstance(obj, (dict, list, str, int, float, bool, type(None))):
        return obj
    raise TypeError(f"Object of type '{type(obj).__name__}' is not JSON serializable")


def content_field(node: Dict[str, Any]) -> Dict[str, Any]:
    """
    Reduce a typed scalar/object field to a small payload.
    Keeps: content, confidence, polygon. Drops: spans.
    """
    out: Dict[str, Any] = {
        "content": node.get("content", ""),
        "confidence": node.get("confidence", None),
    }
    if "polygon" in node:
        out["polygon"] = node["polygon"]
    return out


def extract_object_array(field_name: str, array_nodes: List[Dict[str, Any]]) -> List[Any]:
    """
    Flatten a typed array by reusing extract_object on each element.
    Example typed shape:
      [{"type":"object","valueObject":{...}}, ...]
    """
    out: List[Any] = []
    for element in array_nodes:
        holder = {field_name: element}
        extracted = extract_object(holder)
        # each holder has a single key
        for _, v in extracted.items():
            out.append(v)
    return out


def extract_object(typed_dict: Dict[str, Any]) -> Dict[str, Any]:
    """
    Walk a typed tree:
      { field: { type: "...", content?, valueObject?, valueArray? }, ... }
    Build a simplified dict of the same structure with content/confidence/polygon only.
    """
    simplified: Dict[str, Any] = {}
    for field, attrs in typed_dict.items():
        node_type = attrs["type"]
        if node_type == "object":
            inner = attrs.get("valueObject")
            if isinstance(inner, dict):
                simplified[field] = {"item": extract_object(inner)}
        elif node_type == "array":
            array_nodes = attrs.get("valueArray", [])
            if array_nodes and isinstance(array_nodes[0], dict) and array_nodes[0].get("type") == "object":
                simplified[field] = extract_object_array(field, array_nodes)
            else:
                simplified[field] = [content_field(x) for x in array_nodes]
        else:
            simplified[field] = content_field(attrs)
    return simplified


# =============================================================================
# Content patchers (normalize checkboxes, Ø->0 where appropriate, etc.)
# =============================================================================

def _normalize_content_value(val):
    """
    1) Ø/ø -> "0"
    2) :selected:/ :unselected: -> strip colons
    3) If only "selected" or "unselected" present, standardize casing.
    """
    if val is None:
        return val
    if isinstance(val, str):
        v = val.strip()
        if v in O_SLASH_ALIASES:
            return "0"
        if v in SELECTION_TOKENS:
            return SELECTION_TOKENS[v]
        lower = v.lower()
        if lower == "selected":
            return "selected"
        if lower == "unselected":
            return "unselected"
    return val


def _walk_and_patch_content_inplace(node, warnings):
    """
    Recursively walk the typed tree and apply normalization rules.

    A 'leaf' looks like: {"content": <str>, "confidence": <float>, "polygon": ...}

    Rules (simplified):
      - normalize Ø/ø -> "0"
      - normalize checkbox values to "selected"/"unselected" if they look like that
    """
    if isinstance(node, dict):
        # If this dict looks like a leaf with 'content', patch it
        if "content" in node:
            node["content"] = _normalize_content_value(node["content"])
        for v in node.values():
            _walk_and_patch_content_inplace(v, warnings)
    elif isinstance(node, list):
        for item in node:
            _walk_and_patch_content_inplace(item, warnings)


def _collect_group_refs(root_obj: dict, names: list[str]) -> list[Tuple[str, dict]]:
    """
    Find all dicts with 'content' whose parent key is one of `names`.

    Returns a list of (field_name, node_dict) pairs, where node_dict is the
    leaf containing 'content'.
    """
    out: list[Tuple[str, dict]] = []

    def _walk(current, current_name=None):
        if isinstance(current, dict):
            if "content" in current and current_name in names:
                out.append((current_name, current))
            for k, v in current.items():
                _walk(v, k)
        elif isinstance(current, list):
            for v in current:
                _walk(v, current_name)

    _walk(root_obj, None)
    return out


def _enforce_single_selection_in_group(root_obj: dict, names: list[str], warnings: list[str]):
    """
    For a group of checkbox fields, ensure at most one is 'selected'.
    - If multiple selected: keep the one with highest confidence.
    - Others are forced to 'unselected' and a warning is recorded.
    """
    refs = _collect_group_refs(root_obj, names)
    selected = [(name, node) for (name, node) in refs if node.get("content") == "selected"]
    if len(selected) <= 1:
        return

    def conf(n: dict) -> float:
        return float(n.get("confidence") or 0.0)

    selected.sort(key=lambda x: conf(x[1]), reverse=True)
    keep_name, keep_node = selected[0]
    for drop_name, drop_node in selected[1:]:
        drop_node["content"] = "unselected"
        warnings.append(
            f"Multiple selected in group {names}; keeping {keep_name}, unselecting {drop_name}"
        )


def apply_content_patchers_inplace(root_obj: dict) -> List[str]:
    """
    Apply all content normalization rules to the typed tree in-place.

    - Normalizes zero-like glyphs (Ø/ø) to "0"
    - Normalizes checkbox tokens to "selected"/"unselected"
    - Enforces single-selection within specific checkbox groups
    """
    warnings: List[str] = []
    _walk_and_patch_content_inplace(root_obj, warnings)

    # Enforce single-selection groups
    for group in [
        water_level_list,
        weather_brightness_list,
        weather_cloudy_list,
        precipitation_type_list,
        precipitation_intensity_list,
        fish_visibility_list,
        water_clarity_list,
    ]:
        _enforce_single_selection_in_group(root_obj, group, warnings)

    return warnings


# =============================================================================
# Typed tree construction (from DI result)
# =============================================================================

def _polygon_to_pairs(polygon) -> List[List[float]]:
    """
    Convert polygon representation (from DI) to [[x, y], ...] pairs.

    Handles:
      - already-paired points
      - flat list [x1, y1, x2, y2, ...]
    """
    if not polygon:
        return []
    if isinstance(polygon[0], (list, tuple)) and len(polygon[0]) == 2:
        return [[float(pt[0]), float(pt[1])] for pt in polygon]
    if len(polygon) % 2 != 0:
        return []
    out: List[List[float]] = []
    for i in range(0, len(polygon), 2):
        out.append([float(polygon[i]), float(polygon[i + 1])])
    return out


def _typed_scalar(field) -> Dict[str, Any]:
    """
    Build a typed node for a scalar DI field:
      { "type": "scalar", "content": ..., "confidence": ..., "polygon": ... }
    """
    content = field.value if hasattr(field, "value") else field.content
    node: Dict[str, Any] = {
        "type": "scalar",
        "content": content,
        "confidence": getattr(field, "confidence", None),
    }
    if getattr(field, "bounding_regions", None):
        br = field.bounding_regions[0]
        node["polygon"] = _polygon_to_pairs(br.polygon)
    return node


def _typed_number(field) -> Dict[str, Any]:
    """
    Build a typed node for numeric DI fields.
    Same as scalar, but we may post-process knowing it's a number.
    """
    return _typed_scalar(field) | {"type": "number"}


def _typed_array(field) -> Dict[str, Any]:
    """
    Build a typed node for array fields:
      { "type": "array", "valueArray": [typed items...] }
    """
    items: List[Dict[str, Any]] = []
    for v in field.value_array:
        if v.type == "object":
            items.append(_typed_object(v))
        elif v.type == "array":
            items.append(_typed_array(v))
        elif v.type in ("number", "integer"):
            items.append(_typed_number(v))
        else:
            items.append(_typed_scalar(v))
    return {"type": "array", "valueArray": items}


def _typed_object(field) -> Dict[str, Any]:
    """
    Build a typed node for object fields:
      { "type": "object", "valueObject": { name: typed_node, ... } }
    """
    props: Dict[str, Any] = {}
    for name, subfield in field.value_object.items():
        props[name] = _map_document_field(name, subfield)
    return {"type": "object", "valueObject": props}


def _first_region_polygon(field) -> List[List[float]]:
    """
    Helper to grab the first bounding region polygon (if any) and normalize it.
    """
    if not getattr(field, "bounding_regions", None):
        return []
    br = field.bounding_regions[0]
    return _polygon_to_pairs(br.polygon)


def _map_document_field(field_name: str, field) -> Dict[str, Any]:
    """
    Map a DI document field to a typed node based on its type.
    """
    f_type = field.type
    if f_type == "object":
        return _typed_object(field)
    if f_type == "array":
        return _typed_array(field)
    if f_type in ("number", "integer"):
        return _typed_number(field)
    return _typed_scalar(field)


def analyze_result_to_typed_tree(result: Any) -> Dict[str, Any]:
    """
    Convert a DocumentIntelligence analyze result into a typed tree and
    apply content normalization rules.

    Returns a nested dict where each field is represented as:
      { "type": ..., "content"/"valueObject"/"valueArray", "confidence", "polygon" }
    """
    if not result.documents:
        return {}
    doc = result.documents[0]
    out: Dict[str, Any] = {}
    for field_name, field in doc.fields.items():
        out[field_name] = _map_document_field(field_name, field)
    apply_content_patchers_inplace(out)
    return out


# =============================================================================
# I/O helpers
# =============================================================================

def write_json_locally(json_data: Dict[str, Any],
                       output_root: str,
                       container_name: str,
                       blob_name: str) -> str:
    """
    Write JSON to ./json/<container_name>/<blob_name>.json and return the path.
    """
    base_name = os.path.basename(blob_name)
    if base_name.lower().endswith(".pdf"):
        base_name = base_name[:-4]
    out_dir = os.path.join("json", output_root, container_name)
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, f"{base_name}.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(json_data, f, default=serialize, ensure_ascii=False, indent=2)
    return out_path


def upload_file_to_blob_via_aad(storage_account_url: str,
                                container_name: str,
                                blob_name: str,
                                local_path: str) -> None:
    """
    Upload a local file to Azure Blob Storage using AAD (no account key/SAS).

    - storage_account_url: e.g. "https://myaccount.blob.core.windows.net"
    - container_name:      "fia-json"
    - blob_name:           "prefix/path/file.json"
    """
    credential = DefaultAzureCredential()
    service_client = BlobServiceClient(account_url=storage_account_url, credential=credential)
    container_client: ContainerClient = service_client.get_container_client(container_name)
    try:
        container_client.create_container()
    except Exception:
        # Already exists or no permission to create; we continue and try upload.
        pass

    blob_client: BlobClient = container_client.get_blob_client(blob=blob_name)
    with open(local_path, "rb") as data:
        blob_client.upload_blob(data, overwrite=True)


# =============================================================================
# Progress tracking
# =============================================================================

def load_progress(progress_path: str) -> Set[str]:
    """
    Load processed IDs from a TSV progress file, if it exists.

    Each line: "<mode>::<id>\t<PAGES>\t<ISO8601>\tOK"
    Returns a set of "<mode>::<id>" tokens.
    """
    if not os.path.exists(progress_path):
        return set()
    processed: Set[str] = set()
    with open(progress_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            key = line.split("\t", 1)[0]
            processed.add(key)
    return processed


def append_progress(progress_path: str, key_token: str, pages: int) -> None:
    """
    Append a single entry to the progress file.
    """
    ts = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(progress_path), exist_ok=True)
    with open(progress_path, "a", encoding="utf-8") as f:
        f.write(f"{key_token}\t{pages}\t{ts}\tOK\n")


# =============================================================================
# Main result saver
# =============================================================================

def save_analyzed_result(result: Any,
                         output_root: str,
                         container_name: str,
                         blob_name: str,
                         upload_output: bool,
                         storage_account_url: str,
                         output_container: str) -> None:
    """
    Convert an analyze result to a typed tree, write JSON locally,
    and optionally upload it to blob storage.
    """
    tree = analyze_result_to_typed_tree(result)
    local_path = write_json_locally(tree, output_root, container_name, blob_name)

    if upload_output:
        base_name = os.path.basename(local_path)
        blob_name_out = os.path.join(output_container, base_name).replace("\\", "/")
        upload_file_to_blob_via_aad(storage_account_url, output_container, blob_name_out, local_path)


# =============================================================================
# Pipelines: local PDFs and Azure PDFs
# =============================================================================

def analyze_local_pdfs(di_client: DocumentIntelligenceClient,
                       model_id: str,
                       local_directory: str,
                       output_container: str,
                       upload_output: bool,
                       storage_account_url: str,
                       progress_path: str,
                       processed_keys: Set[str]) -> None:
    """
    Process PDFs from a local folder:
      - call DI for each file
      - write JSON output
      - update progress TSV
    """
    for root, _, files in os.walk(local_directory):
        for fname in files:
            if not fname.lower().endswith(".pdf"):
                continue
            full_path = os.path.join(root, fname)
            rel_path = os.path.relpath(full_path, local_directory)
            key_token = f"local::{rel_path}"
            if key_token in processed_keys:
                print(f"[SKIP] {rel_path} already processed.")
                continue

            print(f"[INFO] Analyzing local PDF: {rel_path}")
            with open(full_path, "rb") as f:
                poller = di_client.begin_analyze_document(model_id=model_id, body=f)
                result = poller.result()
            pages = len(result.pages or [])
            save_analyzed_result(
                result=result,
                output_root=output_container,
                container_name="local",
                blob_name=rel_path,
                upload_output=upload_output,
                storage_account_url=storage_account_url,
                output_container=output_container,
            )
            append_progress(progress_path, key_token, pages)


def analyze_azure_pdfs(di_client: DocumentIntelligenceClient,
                       storage_account_url: str,
                       container_name: str,
                       model_id: str,
                       prefix: str,
                       max_pdfs: int | None,
                       upload_output: bool,
                       output_container: str,
                       output_prefix: str,
                       tps_limit: int,
                       progress_path: str,
                       processed_keys: Set[str],
                       max_workers: int,
                       retries: int,
                       download_dir: str) -> None:
    """
    Process PDFs from an Azure Blob container in parallel.

    Steps:
      - list PDF blobs (optionally filtered by prefix, limited by max_pdfs)
      - download each PDF to a temp folder
      - call DI
      - write JSON + optionally upload
      - record progress
    """
    credential = DefaultAzureCredential()
    service_client = BlobServiceClient(account_url=storage_account_url, credential=credential)
    container_client: ContainerClient = service_client.get_container_client(container_name)

    blobs_iter = container_client.list_blobs(name_starts_with=prefix)
    blob_names: List[str] = []
    for b in blobs_iter:
        if not b.name.lower().endswith(".pdf"):
            continue
        blob_names.append(b.name)
        if max_pdfs is not None and len(blob_names) >= max_pdfs:
            break

    print(f"[INFO] Found {len(blob_names)} PDF blobs in {container_name}/{prefix}")
    if not blob_names:
        return

    os.makedirs(download_dir, exist_ok=True)

    rate_limiter = AdaptiveRateLimiter(target_tps=tps_limit)

    def worker(blob_name: str) -> None:
        key_token = f"azure::{blob_name}"
        if key_token in processed_keys:
            print(f"[SKIP] {blob_name} already processed.")
            return

        attempt = 0
        while True:
            attempt += 1
            try:
                rate_limiter.acquire()
                blob_client: BlobClient = container_client.get_blob_client(blob_name)
                local_pdf = os.path.join(download_dir, os.path.basename(blob_name))
                with open(local_pdf, "wb") as f:
                    data = blob_client.download_blob()
                    f.write(data.readall())

                with open(local_pdf, "rb") as f:
                    poller = di_client.begin_analyze_document(model_id=model_id, body=f)
                    result = poller.result()
                pages = len(result.pages or [])

                out_blob_name = os.path.join(output_prefix, os.path.basename(blob_name)).replace("\\", "/")
                save_analyzed_result(
                    result=result,
                    output_root=output_container,
                    container_name=container_name,
                    blob_name=out_blob_name,
                    upload_output=upload_output,
                    storage_account_url=storage_account_url,
                    output_container=output_container,
                )
                append_progress(progress_path, key_token, pages)
                return

            except HttpResponseError as e:
                # Throttling or transient HTTP response
                status = getattr(e, "status_code", None) or getattr(e, "status", None)
                if status in (429, 503) and attempt <= retries:
                    print(f"[WARN] Throttled on {blob_name} (status={status}), retry {attempt}/{retries}")
                    rate_limiter.penalize()
                    time.sleep(2 * attempt)
                    continue
                print(f"[ERR] HttpResponseError on {blob_name}: {e}")
                return
            except (ServiceRequestError, ServiceResponseError) as e:
                # Network issues; retry a few times
                if attempt <= retries:
                    print(f"[WARN] Network error for {blob_name}, retry {attempt}/{retries}: {e}")
                    time.sleep(2 * attempt)
                    continue
                print(f"[ERR] Permanent network error for {blob_name}: {e}")
                return
            except Exception as e:
                print(f"[ERR] Unexpected error for {blob_name}: {e}")
                return

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(worker, name) for name in blob_names]
        for fut in as_completed(futures):
            _ = fut.result()


# =============================================================================
# Config + CLI
# =============================================================================

def load_connections_config() -> ConnectionsConfig:
    """
    Load ./connections.json from the current working directory and
    return connection settings for Document Intelligence and Blob Storage.

    Expected JSON keys (with some tolerant fallbacks):
      - "DI-Endpoint"   (or legacy "DI_Endpoint")
      - "DI-Key"
      - "SA-endpoint"   (or "SA_endpoint" / "SA-Endpoint")
    """
    config_path = os.path.join(os.getcwd(), "connections.json")
    with open(config_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    di_endpoint = data.get("DI-Endpoint") or data.get("DI_Endpoint")
    if not di_endpoint:
        raise ValueError("connections.json missing 'DI-Endpoint' (or 'DI_Endpoint').")

    di_key = data.get("DI-Key")
    if not di_key:
        raise ValueError("connections.json missing 'DI-Key'.")

    storage_account_url = (
        data.get("SA-endpoint")
        or data.get("SA_endpoint")
        or data.get("SA-Endpoint")
    )
    if not storage_account_url:
        raise ValueError(
            "connections.json missing 'SA-endpoint' (or 'SA_endpoint' / 'SA-Endpoint')."
        )

    return ConnectionsConfig(
        di_endpoint=di_endpoint.rstrip("/"),
        di_key=di_key.rstrip(),
        storage_account_url=storage_account_url.rstrip("/"),
    )


def parse_args() -> argparse.Namespace:
    """
    Parse command-line flags and return an argparse.Namespace.

    The options control:
      - input source (local folder or Azure blob container)
      - DI model ID
      - Azure concurrency / throttling
      - where to write JSON + progress
    """
    p = argparse.ArgumentParser(
        description="Analyze PDFs with Azure Document Intelligence, with optional Azure Blob input and progress tracking."
    )
    p.add_argument(
        "--mode",
        choices=["local", "azure"],
        default="local",
        help="Source of PDFs: 'local' folder or 'azure' blob container.",
    )
    p.add_argument(
        "--model-id",
        default="prebuilt-read",
        help="DI model ID (e.g., 'prebuilt-read' or your custom model).",
    )
    p.add_argument(
        "--local-directory",
        default="./tmp_azure_pdfs",
        help="Local folder for PDFs when --mode local.",
    )
    p.add_argument(
        "--container",
        default="pdf",
        help="Azure Blob container for --mode azure.",
    )
    p.add_argument(
        "--max-workers",
        type=int,
        default=12,
        help="Max concurrent workers for download/analyze/upload (azure mode).",
    )
    p.add_argument(
        "--retries",
        type=int,
        default=2,
        help="Retries per file on transient failures (e.g., 429/503).",
    )
    p.add_argument(
        "--prefix",
        default="",
        help="Optional blob name prefix for azure mode (e.g. 'ground-pdf/').",
    )
    p.add_argument(
        "--download-dir",
        default="./tmp_azure_pdfs",
        help="Where to stage PDFs locally before analyze (azure mode).",
    )
    p.add_argument(
        "--max-pdfs",
        type=int,
        default=None,
        help="Max PDFs to process from the container (azure mode).",
    )
    p.add_argument(
        "--upload-output",
        action="store_true",
        help="If set, upload JSON outputs back to storage via AAD.",
    )
    p.add_argument(
        "--output-container",
        default="update_jsons",
        help="Target container name (virtual) for JSON outputs.",
    )
    p.add_argument(
        "--tps-limit",
        type=int,
        default=8,
        help="Simple TPS throttle (results collection) for azure mode.",
    )
    p.add_argument(
        "--progress-file",
        default="./fia_progress_processed.txt",
        help="Path to the progress TSV file.",
    )
    p.add_argument(
        "--reset",
        action="store_true",
        help="Delete the progress file before running.",
    )
    p.add_argument(
        "--output-prefix",
        default="",
        help="Blob name prefix for results, e.g. 'test-json/'.",
    )

    return p.parse_args()


def main() -> None:
    """Entrypoint: load config, honor --reset, and run the chosen pipeline."""
    opts = parse_args()
    cfg = load_connections_config()

    if opts.reset and os.path.exists(opts.progress_file):
        os.remove(opts.progress_file)
        print(f"[INFO] Progress file reset: {opts.progress_file}")

    processed_keys = load_progress(opts.progress_file)

    di_client = DocumentIntelligenceClient(
        endpoint=cfg.di_endpoint,
        credential=AzureKeyCredential(cfg.di_key),
    )

    if opts.mode == "local":
        analyze_local_pdfs(
            di_client=di_client,
            model_id=opts.model_id,
            local_directory=opts.local_directory,
            output_container=opts.output_container,
            upload_output=opts.upload_output,
            storage_account_url=cfg.storage_account_url,
            progress_path=opts.progress_file,
            processed_keys=processed_keys,
        )
    else:
        analyze_azure_pdfs(
            di_client=di_client,
            storage_account_url=cfg.storage_account_url,
            container_name=opts.container,
            model_id=opts.model_id,
            prefix=opts.prefix,
            max_pdfs=opts.max_pdfs,
            upload_output=opts.upload_output,
            output_container=opts.output_container,
            output_prefix=opts.output_prefix,
            tps_limit=opts.tps_limit,
            progress_path=opts.progress_file,
            processed_keys=processed_keys,
            max_workers=opts.max_workers,
            retries=opts.retries,
            download_dir=opts.download_dir,
        )


if __name__ == "__main__":
    main()
