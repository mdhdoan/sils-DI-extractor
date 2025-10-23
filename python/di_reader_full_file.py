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

CHECKBOX_GROUPS = [
    water_level_list,
    weather_brightness_list,
    weather_cloudy_list,
    precipitation_type_list,
    precipitation_intensity_list,
    fish_visibility_list,
    water_clarity_list,
]


# =============================================================================
# Configuration Models
# =============================================================================

@dataclass
class ConnectionsConfig:
    """Connection settings loaded from connections.json."""
    di_endpoint: str
    di_key: str
    storage_account_url: str

@dataclass
class RunOptions:
    """Command-line options controlling a run."""
    mode: str                    # "local" or "azure"
    model_id: str                # DI model ID
    local_directory: str         # local pdf folder (mode=local)
    container_name: str          # source container (mode=azure)
    prefix: str                  # directory within container (mode=azure)
    max_pdfs: int | None         # optional cap (mode=azure)
    max_workers: int
    download_dir: str
    retries: int
    upload_output: bool          # optionally upload JSON to blob
    output_prefix: str
    output_container: str        # target container for outputs
    tps_limit: int               # result-collection throttle (mode=azure)
    progress_file: str           # path to progress TSV
    reset: bool                  # remove progress TSV before run

class AdaptiveRateLimiter:
    """
    Token-bucket limiter with adaptive backoff on 429/503.
    - target_tps: base tokens per second
    - backoff: decreases effective_tps on throttle, gradually recovers
    """
    def __init__(self, target_tps: int, window: float = 1.0,
                 min_tps: int = 1, recovery_half_life_s: float = 15.0):
        self.target_tps = max(1, target_tps)
        self.window = window
        self.min_tps = max(1, min_tps)
        self.bucket = []
        self.lock = threading.Lock()
        self.effective_tps = float(self.target_tps)
        self.recovery_half_life_s = recovery_half_life_s
        self.last_penalty = 0.0
        self.last_update = time.time()

    def _prune(self, now):
        # remove tokens older than window
        cutoff = now - self.window
        while self.bucket and self.bucket[0] < cutoff:
            self.bucket.pop(0)

    def _recover(self, now):
        # exponential recovery toward target_tps
        dt = max(0.0, now - self.last_update)
        if dt > 0:
            k = math.log(2) / self.recovery_half_life_s
            self.effective_tps = min(
                self.target_tps,
                self.min_tps + (self.effective_tps - self.min_tps) * math.exp(-k * dt)
            )
            self.last_update = now

    def acquire(self):
        """
        Blocks until a token is available at current effective_tps.
        """
        while True:
            with self.lock:
                now = time.time()
                self._recover(now)
                self._prune(now)
                capacity = max(self.min_tps, int(self.effective_tps))
                if len(self.bucket) < capacity:
                    self.bucket.append(now)
                    return
                # time until oldest token ages out
                sleep_for = (self.bucket[0] + self.window) - now
            if sleep_for > 0:
                time.sleep(sleep_for)

    def penalize(self):
        """
        Call on 429/503 to reduce effective TPS (floor at min_tps).
        """
        with self.lock:
            self.effective_tps = max(self.min_tps, self.effective_tps * 0.6)  # 40% cut
            self.last_update = time.time()

# =============================================================================
# JSON Serialization
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

# =============================================================================
# Extractor (flat schema: content, confidence, polygon — no spans)
# =============================================================================

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
                simplified[field] = extract_object(inner)
        elif node_type == "array":
            inner = attrs.get("valueArray")
            if isinstance(inner, list):
                simplified[field] = extract_object_array(field, inner)
        else:
            if "content" in attrs:
                simplified[field] = content_field(attrs)
    return simplified

# =============================================================================
# DI Result → Typed Tree Adapter (prebuilt & custom)
# =============================================================================

def _normalize_content_value(val):
    """
    1) Ø/ø -> "0"
    2) :selected:/ :unselected: -> strip colons
    """
    if not isinstance(val, str):
        return val
    v = val.strip()
    if v in O_SLASH_ALIASES:
        return "0"
    # strip colon-wrapped selection tokens
    if v in SELECTION_TOKENS:
        return SELECTION_TOKENS[v]
    # also handle cases where content contains these tokens in longer strings (rare)
    for k, repl in SELECTION_TOKENS.items():
        if k in v:
            v = v.replace(k, repl)
    return v


def _walk_and_patch_content_inplace(node, warnings):
    """
    Recursively traverse the simplified JSON and normalize leaf 'content'.
    A 'leaf' looks like: {"content": <str>, "confidence": <float>, "polygon": [...]}
    """
    if isinstance(node, dict):
        # If this dict *is* a leaf, patch its content
        if "content" in node and isinstance(node["content"], (str, int, float)):
            node["content"] = _normalize_content_value(node["content"])
        # Recurse into children
        for k, v in list(node.items()):
            _walk_and_patch_content_inplace(v, warnings)
    elif isinstance(node, list):
        for item in node:
            _walk_and_patch_content_inplace(item, warnings)


def _collect_group_refs(root_obj: dict, names: list[str]) -> list[tuple[str, dict]]:
    """
    Find references to checkbox fields by *key name* anywhere in the tree.
    Returns a list of (field_name, leaf_dict) where leaf_dict has 'content'/'confidence'.
    """
    found = []

    def _recurse(obj):
        if isinstance(obj, dict):
            for k, v in obj.items():
                # If this key is a target, and the value looks like a leaf, collect it
                if k in names and isinstance(v, dict) and "content" in v:
                    found.append((k, v))
                # Recurse into nested structures
                _recurse(v)
        elif isinstance(obj, list):
            for item in obj:
                _recurse(item)

    _recurse(root_obj)
    return found


def _enforce_single_selection_in_group(root_obj: dict, names: list[str], warnings: list[str]):
    """
    Ensure at most one item in 'names' is 'selected'.
    - If multiple selected: keep the one with highest confidence; flip others to 'unselected'.
    - If none selected: leave as-is (warn).
    """
    refs = _collect_group_refs(root_obj, names)
    if not refs:
        return  # group not present in this document

    # Normalize content for safety (select/unselect tokens)
    for _, leaf in refs:
        if "content" in leaf:
            leaf["content"] = _normalize_content_value(leaf["content"])

    selected = [(name, leaf) for name, leaf in refs if str(leaf.get("content", "")).lower() == "selected"]

    if len(selected) <= 1:
        if len(selected) == 0:
            warnings.append(f"[WARN] No selection in group: {names[0].split(':')[0]} … ({len(names)} options)")
        return

    # multiple selected -> keep one with highest confidence
    def conf(leaf):
        c = leaf[1].get("confidence", 0.0)
        try:
            return float(c) if c is not None else 0.0
        except Exception:
            return 0.0

    # winner is tuple (name, leaf)
    winner = max(selected, key=conf)
    winner_name = winner[0]

    for name, leaf in selected:
        if name != winner_name:
            leaf["content"] = "unselected"

    warnings.append(f"[FIX] Multiple selections in group; kept '{winner_name}', unselected others.")


def apply_content_patchers_inplace(extracted: dict) -> list[str]:
    """
    Run all normalization & checkbox enforcement on the simplified JSON tree.
    Returns a list of warnings/fixes applied.
    """
    warnings: list[str] = []

    # 1) Normalize content globally
    _walk_and_patch_content_inplace(extracted, warnings)

    # 2) Enforce single-selection per checkbox group
    for group in CHECKBOX_GROUPS:
        _enforce_single_selection_in_group(extracted, group, warnings)

    return warnings

def _polygon_to_pairs(poly: Any) -> List[List[float]]:
    """Normalize DI polygon: [x1,y1,x2,y2,...] → [[x,y], ...]."""
    if not poly:
        return []
    if isinstance(poly, list) and poly and isinstance(poly[0], list):
        return poly
    if isinstance(poly, list):
        pairs: List[List[float]] = []
        it = iter(poly)
        for x in it:
            y = next(it, None)
            if y is None:
                break
            pairs.append([x, y])
        return pairs
    return []

def _typed_scalar(content: Any = None,
                  confidence: float | None = None,
                  polygon: List[List[float]] | None = None) -> Dict[str, Any]:
    """Build a typed scalar node with optional confidence and polygon (no spans)."""
    node: Dict[str, Any] = {"type": "string", "content": content}
    if confidence is not None:
        node["confidence"] = confidence
    if polygon is not None:
        node["polygon"] = polygon
    return node

def _typed_number(value: Any,
                  confidence: float | None = None,
                  polygon: List[List[float]] | None = None) -> Dict[str, Any]:
    """Build a typed numeric node with optional confidence and polygon (no spans)."""
    node: Dict[str, Any] = {"type": "number", "content": value}
    if confidence is not None:
        node["confidence"] = confidence
    if polygon is not None:
        node["polygon"] = polygon
    return node

def _typed_array(items: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Build a typed array wrapper."""
    return {"type": "array", "valueArray": items}

def _typed_object(obj: Dict[str, Any]) -> Dict[str, Any]:
    """Build a typed object wrapper."""
    return {"type": "object", "valueObject": obj}

def _first_region_polygon(source) -> List[List[float]] | None:
    """
    Return the first polygon from source.bounding_regions / boundingRegions, normalized
    to [[x,y], ...]. Handles SDK objects (list[Point]) and REST dicts.
    """
    brs = getattr(source, "bounding_regions", None) or getattr(source, "boundingRegions", None) or []
    if not brs:
        return None

    r0 = brs[0]
    # SDK object
    page = getattr(r0, "page_number", None) or getattr(r0, "pageNumber", None)
    poly = getattr(r0, "polygon", None)

    # REST dict shape fallback
    if poly is None and isinstance(r0, dict):
        page = r0.get("pageNumber", r0.get("page_number"))
        poly = r0.get("polygon")

    # Normalize
    if poly and isinstance(poly, list):
        if poly and hasattr(poly[0], "x"):  # list[Point]
            return [[pt.x, pt.y] for pt in poly]
        # list of floats → pairs
        return _polygon_to_pairs(poly)

    return None


def _map_document_field(field_obj: Any) -> Dict[str, Any]:
    """
    Map a DI DocumentField (custom model) into the typed schema.
    Adds polygon from boundingRegions; drops spans.
    """
    try:
        field_type = getattr(field_obj, "type", None) or getattr(field_obj, "value_type", None) or "string"
        content = getattr(field_obj, "content", None)
        confidence = getattr(field_obj, "confidence", None)
        poly = _first_region_polygon(field_obj)  # ⟵ NEW

        if field_type in (
            "string", "date", "time", "phoneNumber", "countryRegion", "currency",
            "integer", "number", "selectionMark", "address"
        ):
            typed_val = None
            for attr in (
                "value_string", "value_date", "value_time", "value_phone_number",
                "value_country_region", "value_currency", "value_integer", "value_number",
                "value_selection_mark", "value_address"
            ):
                if hasattr(field_obj, attr) and getattr(field_obj, attr) is not None:
                    typed_val = getattr(field_obj, attr)
                    break
            if isinstance(typed_val, (int, float)):
                return _typed_number(typed_val, confidence=confidence, polygon=poly)
            return _typed_scalar(content if content is not None else typed_val,
                                 confidence=confidence, polygon=poly)

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

        # Fallback scalar (still attach polygon if present)
        return _typed_scalar(content, confidence=confidence, polygon=poly)

    except Exception:
        return _typed_scalar(getattr(field_obj, "content", None),
                             confidence=getattr(field_obj, "confidence", None),
                             polygon=_first_region_polygon(field_obj))


def analyze_result_to_typed_tree(result: Any) -> Tuple[Dict[str, Any], int]:
    """
    Convert a DI AnalyzeResult to the typed schema with polygons but no spans.
    Returns: (typed_root, page_count)
    """
    # Try custom model first
    try:
        documents = getattr(result, "documents", None)
        if documents and len(documents) > 0 and getattr(documents[0], "fields", None):
            doc0 = documents[0]
            root_fields: Dict[str, Any] = {}

            # Map all fields (each field now carries its own polygon)
            for key, field_obj in doc0.fields.items():
                root_fields[key] = _map_document_field(field_obj)

            # OPTIONAL: carry doc-level info as siblings (minimal schema change)
            doc_type = getattr(doc0, "doc_type", None) or getattr(doc0, "docType", None)
            if doc_type is not None:
                root_fields["_docType"] = _typed_scalar(str(doc_type))

            doc_poly = _first_region_polygon(doc0)  # ⟵ document.boundingRegions[0].polygon
            if doc_poly:
                # Store as a scalar carrying just the polygon (empty content)
                root_fields["_documentPolygon"] = _typed_scalar("", polygon=doc_poly)

            page_count = len(getattr(result, "pages", []) or [])
            return _typed_object(root_fields), page_count

    except Exception:
        pass

    # prebuilt-read shape
    doc_content = getattr(result, "content", "") or ""
    typed_pages: List[Dict[str, Any]] = []

    pages = getattr(result, "pages", []) or []
    for page in pages:
        page_number = getattr(page, "page_number", None) or getattr(page, "page", None)
        page_conf = getattr(page, "confidence", None)
        page_poly = _polygon_to_pairs(getattr(page, "polygon", None))
        page_content = getattr(page, "content", "") or ""

        typed_lines: List[Dict[str, Any]] = []
        for line in getattr(page, "lines", []) or []:
            ln_content = getattr(line, "content", "") or ""
            ln_conf = getattr(line, "confidence", None)
            ln_poly = _polygon_to_pairs(getattr(line, "polygon", None))
            if not ln_poly:
                brs = getattr(line, "bounding_regions", None) or getattr(line, "boundingRegions", None) or []
                if brs and isinstance(brs, list) and isinstance(brs[0], dict):
                    ln_poly = _polygon_to_pairs(brs[0].get("polygon"))

            typed_words: List[Dict[str, Any]] = []
            for word in getattr(line, "words", []) or []:
                w_content = getattr(word, "content", "") or ""
                w_conf = getattr(word, "confidence", None)
                w_poly = _polygon_to_pairs(getattr(word, "polygon", None))
                if not w_poly:
                    brs = getattr(word, "bounding_regions", None) or getattr(word, "boundingRegions", None) or []
                    if brs and isinstance(brs, list) and isinstance(brs[0], dict):
                        w_poly = _polygon_to_pairs(brs[0].get("polygon"))
                typed_words.append(_typed_object({
                    "content": _typed_scalar(w_content, confidence=w_conf, polygon=w_poly)
                }))

            typed_lines.append(_typed_object({
                "content": _typed_scalar(ln_content, confidence=ln_conf, polygon=ln_poly),
                "words": _typed_array(typed_words)
            }))

        typed_pages.append(_typed_object({
            "pageNumber": _typed_number(page_number if page_number is not None else ""),
            "content": _typed_scalar(page_content, confidence=page_conf, polygon=page_poly),
            "lines": _typed_array(typed_lines)
        }))

    typed_root = _typed_object({
        "content": _typed_scalar(doc_content),
        "pages": _typed_array(typed_pages)
    })
    return typed_root, len(pages)

# =============================================================================
# Output + Progress
# =============================================================================

def write_json_locally(payload: Dict[str, Any], output_dir: str, filename: str) -> str:
    """Write JSON payload to disk; returns the full path."""
    os.makedirs(output_dir, exist_ok=True)
    path = os.path.join(output_dir, filename)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2, default=serialize)
    print(f"[OK] Wrote: {path}")
    return path

def upload_file_to_blob_via_aad(storage_account_url: str,
                                container_name: str,
                                local_path: str,
                                blob_prefix: str = "") -> None:
    """Upload a local file to an Azure Blob container using AAD (with optional virtual-folder prefix)."""
    credential = DefaultAzureCredential()
    blob_service = BlobServiceClient(account_url=storage_account_url, credential=credential)
    container_client: ContainerClient = blob_service.get_container_client(container_name)
    try:
        container_client.create_container()
    except Exception:
        pass

    base = os.path.basename(local_path)
    prefix = (blob_prefix or "").strip("/")
    blob_name = f"{prefix}/{base}" if prefix else base

    with open(local_path, "rb") as data:
        container_client.upload_blob(name=blob_name, data=data, overwrite=True)
    print(f"[OK] Uploaded to blob: {container_name}/{blob_name}")


def load_progress(progress_path: str) -> Set[str]:
    """
    Load existing progress TSV and return set of keys that completed successfully.
    Each line: "<mode>::<id>\t<PAGES>\t<ISO8601>\tOK"
    """
    done: Set[str] = set()
    if not os.path.exists(progress_path):
        return done
    with open(progress_path, "r", encoding="utf-8") as f:
        for line in f:
            parts = line.strip().split("\t")
            if len(parts) >= 4 and parts[3].upper() == "OK":
                done.add(parts[0])
    return done

def append_progress(progress_path: str, key_token: str, pages: int) -> None:
    """Append a success line to the progress TSV."""
    ts = datetime.now(timezone.utc).isoformat()
    with open(progress_path, "a", encoding="utf-8") as f:
        f.write(f"{key_token}\t{pages}\t{ts}\tOK\n")

# =============================================================================
# Pipelines
# =============================================================================

def save_analyzed_result(result: Any,
                         output_basename: str,
                         output_root: str,
                         upload_output: bool,
                         storage_account_url: str,
                         output_container: str) -> Tuple[str, int]:
    """
    Convert DI result → typed → simplified JSON, write to disk, optionally upload.
    Returns: (local_json_path, page_count)
    """
    typed_root, page_count = analyze_result_to_typed_tree(result)
    if page_count <= 0:
        raise ValueError("AnalyzeResult reports zero pages.")

    extracted = extract_object(typed_root["valueObject"])
    patch_notes = apply_content_patchers_inplace(extracted)
    for note in patch_notes:
        print(note)
    out_dir = os.path.join(os.getcwd(), "json", output_container)
    local_path = write_json_locally(extracted, out_dir, f"{output_basename}.json")

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
    Run DI on local PDFs (skips non-PDFs), resuming from progress.
    """
    start = datetime.now()
    print(f"[INFO] Local dir: {local_directory} | Model: {model_id}")

    if not os.path.isdir(local_directory):
        raise FileNotFoundError(f"Local directory not found: {local_directory}")

    processed = skipped = errors = 0
    for filename in sorted(os.listdir(local_directory)):
        if not filename.lower().endswith(".pdf"):
            continue

        key_token = f"local::{filename}"
        if key_token in processed_keys:
            skipped += 1
            continue

        path = os.path.join(local_directory, filename)
        try:
            with open(path, "rb") as fh:
                poller = di_client.begin_analyze_document(model_id, fh)
                result = poller.result()

            base = os.path.splitext(filename.replace(" ", ""))[0]
            _, pages = save_analyzed_result(
                result=result,
                output_basename=f"local_{base}",
                output_root=os.path.join("json", output_container),
                upload_output=upload_output,
                storage_account_url=storage_account_url,
                output_container=output_container,
            )
            append_progress(progress_path, key_token, pages)
            processed += 1
            print(f"[OK] {filename} (pages={pages})")
        except Exception as e:
            errors += 1
            print(f"[ERR] {filename}: {e}")

    elapsed = (datetime.now() - start).total_seconds()
    print(f"[OK] Local: processed={processed}, skipped={skipped}, errors={errors} in {elapsed:.2f}s")

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
                       max_workers: int = 12,
                       retries: int = 2,
                       download_dir: str = "./tmp_azure_pdfs") -> None:
    """
    Concurrent Azure pipeline with adaptive rate limiting and parallel uploads.
    """
    os.makedirs(download_dir, exist_ok=True)
    print(f"[INFO] Container={container_name}, Prefix='{prefix}', Model={model_id}, Workers={max_workers}, TPS={tps_limit}")

    cred = DefaultAzureCredential()
    blob_service = BlobServiceClient(account_url=storage_account_url, credential=cred)
    container_client: ContainerClient = blob_service.get_container_client(container_name)

    # Enumerate candidate blobs
    candidates = []
    for blob in container_client.list_blobs(name_starts_with=prefix or None):
        if not blob.name.lower().endswith(".pdf"):
            continue
        key_token = f"azure::{container_name}/{blob.name}"
        if key_token in processed_keys:
            continue
        candidates.append(blob.name)
        if max_pdfs is not None and len(candidates) >= max_pdfs:
            break

    if not candidates:
        print("[INFO] Nothing to process.")
        return

    rate = AdaptiveRateLimiter(target_tps=tps_limit)
    json_paths: List[str] = []
    processed = errors = 0
    start = datetime.now()

    def is_transient(exc: Exception) -> bool:
        if isinstance(exc, (ServiceRequestError, ServiceResponseError)):
            return True
        if isinstance(exc, HttpResponseError):
            code = getattr(exc, "status_code", None) or getattr(exc, "status", None)
            return code in (429, 500, 502, 503, 504)
        return False

    def work(name: str) -> Tuple[str, str, int]:
        """
        Download → rate-limited submit → result → save local JSON (no upload here).
        Retries transient failures 'retries' times with simple backoff.
        """
        key_token = f"azure::{container_name}/{name}"
        attempt = 0
        delay = 1.0
        last_exc = None

        while attempt <= retries:
            try:
                # 1) Download bytes
                pdf = container_client.get_blob_client(name).download_blob().readall()

                # 2) Rate-limited submit
                rate.acquire()
                poller = di_client.begin_analyze_document(model_id, pdf)
                result = poller.result()

                # 3) Save JSON locally (no upload)
                base = os.path.splitext(os.path.basename(name))[0]
                local_path, pages = save_analyzed_result(
                    result=result,
                    output_basename=base,
                    output_root=os.path.join("json", output_container),
                    upload_output=False,  # defer uploads
                    storage_account_url=storage_account_url,
                    output_container=output_container,
                )
                # 4) Progress
                append_progress(progress_path, key_token, pages)
                return name, local_path, pages

            except Exception as e:
                last_exc = e
                # Penalize limiter on throttle responses
                if isinstance(e, HttpResponseError):
                    code = getattr(e, "status_code", None) or getattr(e, "status", None)
                    if code in (429, 503):
                        rate.penalize()
                if attempt < retries and is_transient(e):
                    time.sleep(delay)
                    delay = min(8.0, delay * 2)
                    attempt += 1
                    continue
                raise

        raise last_exc  # should not reach

    # Analyze concurrently
    futures = []
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        for name in candidates:
            futures.append(ex.submit(work, name))

        for fut in as_completed(futures):
            try:
                name, local_path, pages = fut.result()
                json_paths.append(local_path)
                processed += 1
                print(f"[OK] {name} (pages={pages}) → {local_path}")
            except Exception as e:
                errors += 1
                print(f"[ERR] {e}")

    # Parallel uploads (optional)
    if upload_output and json_paths:
        print(f"[INFO] Uploading {len(json_paths)} JSON files to '{output_container}'...")
        def upload_one(path: str):
            upload_file_to_blob_via_aad(storage_account_url, output_container, path, blob_prefix=output_prefix)
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            for _ in ex.map(upload_one, json_paths):
                pass

    elapsed = (datetime.now() - start).total_seconds()
    print(f"[DONE] Azure concurrent: processed={processed}, errors={errors}, elapsed={elapsed:.2f}s")

# =============================================================================
# CLI / Entrypoint
# =============================================================================

def load_connections_config() -> ConnectionsConfig:
    """
    Load ./connections.json (CWD). Expects:
      - DI-Key
      - DI-Endpoint (or legacy DI_Endpoint)
      - SA-endpoint (account URL, https://<storage>.blob.core.windows.net)
    """
    config_path = os.path.join(os.getcwd(), "connections.json")
    with open(config_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    di_endpoint = (data.get("DI-Endpoint") or data.get("DI_Endpoint"))
    if not di_endpoint:
        raise ValueError("connections.json missing 'DI-Endpoint' (or 'DI_Endpoint').")
    di_key = data.get("DI-Key")
    if not di_key:
        raise ValueError("connections.json missing 'DI-Key'.")
    storage_account_url = (data.get("SA-endpoint") or data.get("SA-enpoint"))
    if not storage_account_url:
        raise ValueError("connections.json missing 'SA-endpoint'.")

    return ConnectionsConfig(
        di_endpoint=di_endpoint.rstrip("/"),
        di_key=di_key,
        storage_account_url=storage_account_url.rstrip("/"),
    )

def parse_args() -> RunOptions:
    """Parse CLI flags."""
    p = argparse.ArgumentParser(
        description="Analyze PDFs with Azure Document Intelligence via AAD (optional), with progress tracking."
    )
    p.add_argument("--mode", choices=["local", "azure"], default="local",
                   help="Source of PDFs.")
    p.add_argument("--model-id", required=True,
                   help="DI model ID (e.g., 'prebuilt-read' or your custom model).")
    p.add_argument("--local-directory", default="./tempdata",
                   help="Local folder for PDFs when --mode local.")
    p.add_argument("--container", default="pdf",
                   help="Azure Blob container for --mode azure.")
    p.add_argument("--max-workers", type=int, default=12,
               help="Max concurrent workers for download/analyze/upload.")
    p.add_argument("--retries", type=int, default=2,
                help="Retries per file on transient failures (e.g., 429/503).")
    p.add_argument("--prefix", default="",
                help="Optional blob name prefix (e.g. 'ground-pdf/').")
    p.add_argument("--download-dir", default="./tmp_azure_pdfs",
                help="Where to stage PDFs locally before analyze.")
    p.add_argument("--max-pdfs", type=int, default=None,
                   help="Max PDFs to process from the container.")
    p.add_argument("--upload-output", action="store_true",
                   help="Upload JSON outputs back to storage via AAD.")
    p.add_argument("--output-container", default="updated_json",
                   help="Target container for JSON outputs.")
    p.add_argument("--tps-limit", type=int, default=8,
                   help="Simple TPS throttle (results collection) for azure mode.")
    p.add_argument("--progress-file", default="./progress_processed.txt",
                   help="Path to the progress TSV file.")
    p.add_argument("--reset", action="store_true",
                   help="Delete the progress file before running.")
    p.add_argument("--output-prefix", default="", 
                   help="Blob name prefix for results, e.g. 'test-json/'.")

    a = p.parse_args()
    return RunOptions(
        mode=a.mode,
        model_id=a.model_id,
        local_directory=a.local_directory,
        container_name=a.container,
        prefix=a.prefix,
        max_pdfs=a.max_pdfs,
        upload_output=a.upload_output,
        max_workers=a.max_workers,
        retries=a.retries,
        download_dir=a.download_dir,
        output_prefix=a.output_prefix,
        output_container=a.output_container,
        tps_limit=a.tps_limit,
        progress_file=a.progress_file,
        reset=a.reset,
    )

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
            container_name=opts.container_name,
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
    # Run command:
    # python python/test.py --mode azure --model-id fia-roving-aerial-1 --container fia --prefix aerial-pdf 
    # --output-container fia --output-prefix json-test --upload-output --max-worker 16 --tps-limit 8 --retries 3 --reset

    # python python/test.py --mode azure --model-id wcvi-sil-1 --container wcvi --prefix wcvi-sil-2023/pdf 
    # --output-container wcvi --output-prefix wcvi-sil-2023/update_jsons
    # --upload-output --max-worker 16 --tps-limit 8 --retries 3 --reset