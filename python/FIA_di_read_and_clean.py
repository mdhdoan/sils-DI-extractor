#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Combined FIA Roving/Aerial pipeline:

- Stage 1: Azure Document Intelligence PDF reader with progress tracking
           (originally FIA_di_reader_full_file.py)

- Stage 2: Azure BC16 JSON cleaner/validator on blobs
           (originally FIA_di_cleanup_azure.py)

Usage:

  # Stage 1: extract PDFs → JSON
  python fia_pipeline.py extract --mode azure --model-id fia-roving-aerial-1 \
    --container fia --prefix ground/upload \
    --output-container fia --output-prefix ground/update_jsons \
    --upload-output --max-workers 16 --tps-limit 8 --retries 3 --reset

  # Stage 2: clean/validate JSONs in Azure
  python fia_pipeline.py clean \
    --container fia \
    --src-prefix ground/update_jsons \
    --dst-prefix ground/update_jsons \
    --report-blob ground/update_jsons/

  # Run both stages in one go (Azure only)
  python fia_pipeline.py both \
    --model-id fia-roving-aerial-1 \
    --container fia \
    --pdf-prefix ground/upload \
    --json-prefix ground/update_jsons \
    --max-workers 16 --tps-limit 8 --retries 3 --reset
"""

from __future__ import annotations

import argparse
import json
import math
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Set, Tuple

from urllib.parse import urlparse, quote

import requests

# Azure SDKs
from azure.ai.documentintelligence import DocumentIntelligenceClient
from azure.core.credentials import AzureKeyCredential
from azure.core.exceptions import (
    ServiceRequestError,
    ServiceResponseError,
    HttpResponseError,
)
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, ContainerClient, BlobClient

# =====================================================================
# Shared helpers
# =====================================================================

OK, ERR = "[OK]", "[ERR]"


def rfc1123_now() -> str:
    return datetime.now(timezone.utc).strftime("%a, %d %b %Y %H:%M:%S GMT")


# =====================================================================
# --- PART A: Azure DI reader (from FIA_di_reader_full_file.py)
# =====================================================================

# --- Normalization tables ---
O_SLASH_ALIASES = {"Ø", "ø"}
SELECTION_TOKENS = {":selected:": "selected", ":unselected:": "unselected"}

# --- Shared checkbox label constants (used by both stages) ---
CHECKBOX_SURVEY = [
    "Survey Type: Ground",
    "Survey Type: Aerial",
]

CHECKBOX_WATER_LEVEL = [
    "Water Level: % Bankfill: <25%",
    "Water Level: % Bankfill: 25-50%",
    "Water Level: % Bankfill: 50-75%",
    "Water Level: % Bankfill: 75-100%",
    "Water Level: % Bankfill: +100%",
]

CHECKBOX_WEATHER_BRIGHTNESS = [
    "Weather: Brightness: Full",
    "Weather: Brightness: Bright",
    "Weather: Brightness: Medium",
    "Weather: Brightness: Dark",
]

CHECKBOX_WEATHER_CLOUDY = [
    "Weather: %Cloudy: 0%",
    "Weather: %Cloudy: 25%",
    "Weather: %Cloudy: 50%",
    "Weather: %Cloudy: 75%",
    "Weather: %Cloudy: 100%",
]

CHECKBOX_PRECIPITATION_TYPE = [
    "Precipitation: Type: Rain",
    "Precipitation: Type: Snow",
    "Precipitation: Type: None",
]

CHECKBOX_PRECIPITATION_INTENSITY = [
    "Precipitation: Intensity: Light",
    "Precipitation: Intensity: Medium",
    "Precipitation: Intensity: Heavy",
]

CHECKBOX_FISH_VISIBILITY = [
    "Water Conditions: Fish Visibility: Low",
    "Water Conditions: Fish Visibility: Medium",
    "Water Conditions: Fish Visibility: High",
]

CHECKBOX_WATER_CLARITY = [
    "Water Conditions: Water Clarity: 0-0.25m",
    "Water Conditions: Water Clarity: 0.25-0.5m",
    "Water Conditions: Water Clarity: 0.5-1.0m",
    "Water Conditions: Water Clarity: 1-3m",
    "Water Conditions: Water Clarity: 3m to bottom",
]

# Checkbox groups used for DI extraction normalization
CHECKBOX_GROUPS = [
    CHECKBOX_WATER_LEVEL,
    CHECKBOX_WEATHER_BRIGHTNESS,
    CHECKBOX_WEATHER_CLOUDY,
    CHECKBOX_PRECIPITATION_TYPE,
    CHECKBOX_PRECIPITATION_INTENSITY,
    CHECKBOX_FISH_VISIBILITY,
    CHECKBOX_WATER_CLARITY,
]


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

    def __init__(
        self,
        target_tps: int,
        window: float = 1.0,
        min_tps: int = 1,
        recovery_half_life_s: float = 15.0,
    ):
        self.target_tps = max(1, target_tps)
        self.window = window
        self.min_tps = max(1, min_tps)
        self.bucket: List[float] = []
        self.lock = threading.Lock()
        self.effective_tps = float(self.target_tps)
        self.recovery_half_life_s = recovery_half_life_s
        self.last_penalty = 0.0
        self.last_update = time.time()

    def _prune(self, now: float) -> None:
        cutoff = now - self.window
        while self.bucket and self.bucket[0] < cutoff:
            self.bucket.pop(0)

    def _recover(self, now: float) -> None:
        dt = max(0.0, now - self.last_update)
        if dt > 0:
            k = math.log(2) / self.recovery_half_life_s
            self.effective_tps = min(
                self.target_tps,
                self.min_tps + (self.effective_tps - self.min_tps) * math.exp(-k * dt),
            )
            self.last_update = now

    def acquire(self) -> None:
        """Blocks until a token is available at current effective_tps."""
        while True:
            with self.lock:
                now = time.time()
                self._recover(now)
                self._prune(now)
                capacity = max(self.min_tps, int(self.effective_tps))
                if len(self.bucket) < capacity:
                    self.bucket.append(now)
                    return
                sleep_for = (self.bucket[0] + self.window) - now
            if sleep_for > 0:
                time.sleep(sleep_for)

    def penalize(self) -> None:
        """Call on 429/503 to reduce effective TPS (floor at min_tps)."""
        with self.lock:
            self.effective_tps = max(self.min_tps, self.effective_tps * 0.6)
            self.last_update = time.time()


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
            inner = attrs.get("valueArray")
            if isinstance(inner, list):
                simplified[field] = extract_object_array(field, inner)
        else:
            if "content" in attrs:
                simplified[field] = content_field(attrs)
    return simplified


def _normalize_content_value(val: Any) -> Any:
    """
    1) Ø/ø -> "0"
    2) :selected:/ :unselected: -> strip colons
    """
    if not isinstance(val, str):
        return val
    v = val.strip()
    if v in O_SLASH_ALIASES:
        return "0"
    if v in SELECTION_TOKENS:
        return SELECTION_TOKENS[v]
    for k, repl in SELECTION_TOKENS.items():
        if k in v:
            v = v.replace(k, repl)
    return v


def _walk_and_patch_content_inplace(node: Any, warnings: List[str]) -> None:
    """
    Recursively traverse the simplified JSON and normalize leaf 'content'.
    """
    if isinstance(node, dict):
        if "content" in node and isinstance(node["content"], (str, int, float)):
            node["content"] = _normalize_content_value(node["content"])
        for _, v in list(node.items()):
            _walk_and_patch_content_inplace(v, warnings)
    elif isinstance(node, list):
        for item in node:
            _walk_and_patch_content_inplace(item, warnings)


def _collect_group_refs(
    root_obj: dict, names: List[str]
) -> List[Tuple[str, dict]]:
    """
    Find references to checkbox fields by *key name* anywhere in the tree.
    Returns a list of (field_name, leaf_dict).
    """
    found: List[Tuple[str, dict]] = []

    def _recurse(obj: Any) -> None:
        if isinstance(obj, dict):
            for k, v in obj.items():
                if k in names and isinstance(v, dict) and "content" in v:
                    found.append((k, v))
                _recurse(v)
        elif isinstance(obj, list):
            for item in obj:
                _recurse(item)

    _recurse(root_obj)
    return found


def _enforce_single_selection_in_group(
    root_obj: dict, names: List[str], warnings: List[str]
) -> None:
    """
    Ensure at most one item in 'names' is 'selected'.
    - If multiple selected: keep highest confidence, others → unselected.
    """
    refs = _collect_group_refs(root_obj, names)
    if not refs:
        return

    for _, leaf in refs:
        if "content" in leaf:
            leaf["content"] = _normalize_content_value(leaf["content"])

    selected = [
        (name, leaf)
        for name, leaf in refs
        if str(leaf.get("content", "")).lower() == "selected"
    ]

    if len(selected) <= 1:
        if len(selected) == 0:
            warnings.append(
                f"[WARN] No selection in group: {names[0].split(':')[0]} … ({len(names)} options)"
            )
        return

    def conf(leaf: Tuple[str, dict]) -> float:
        c = leaf[1].get("confidence", 0.0)
        try:
            return float(c) if c is not None else 0.0
        except Exception:
            return 0.0

    winner = max(selected, key=conf)
    winner_name = winner[0]

    for name, leaf in selected:
        if name != winner_name:
            leaf["content"] = "unselected"

    warnings.append(
        f"[FIX] Multiple selections in group; kept '{winner_name}', unselected others."
    )


def apply_content_patchers_inplace(extracted: dict) -> List[str]:
    """
    Run all normalization & checkbox enforcement on the simplified JSON tree.
    Returns a list of warnings/fixes applied.
    """
    warnings: List[str] = []
    _walk_and_patch_content_inplace(extracted, warnings)
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


def _typed_scalar(
    content: Any = None,
    confidence: float | None = None,
    polygon: List[List[float]] | None = None,
) -> Dict[str, Any]:
    """Build a typed scalar node with optional confidence and polygon (no spans)."""
    node: Dict[str, Any] = {"type": "string", "content": content}
    if confidence is not None:
        node["confidence"] = confidence
    if polygon is not None:
        node["polygon"] = polygon
    return node


def _typed_number(
    value: Any,
    confidence: float | None = None,
    polygon: List[List[float]] | None = None,
) -> Dict[str, Any]:
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


def _first_region_polygon(source: Any) -> List[List[float]] | None:
    """
    Return the first polygon from source.bounding_regions / boundingRegions, normalized.
    """
    brs = (
        getattr(source, "bounding_regions", None)
        or getattr(source, "boundingRegions", None)
        or []
    )
    if not brs:
        return None

    r0 = brs[0]
    poly = getattr(r0, "polygon", None)

    if poly is None and isinstance(r0, dict):
        poly = r0.get("polygon")

    if poly and isinstance(poly, list):
        if poly and hasattr(poly[0], "x"):
            return [[pt.x, pt.y] for pt in poly]
        return _polygon_to_pairs(poly)

    return None


def _map_document_field(field_obj: Any) -> Dict[str, Any]:
    """
    Map a DI DocumentField (custom model) into the typed schema.
    Adds polygon from boundingRegions; drops spans.
    """
    try:
        field_type = (
            getattr(field_obj, "type", None)
            or getattr(field_obj, "value_type", None)
            or "string"
        )
        content = getattr(field_obj, "content", None)
        confidence = getattr(field_obj, "confidence", None)
        poly = _first_region_polygon(field_obj)

        if field_type in (
            "string",
            "date",
            "time",
            "phoneNumber",
            "countryRegion",
            "currency",
            "integer",
            "number",
            "selectionMark",
            "address",
        ):
            typed_val = None
            for attr in (
                "value_string",
                "value_date",
                "value_time",
                "value_phone_number",
                "value_country_region",
                "value_currency",
                "value_integer",
                "value_number",
                "value_selection_mark",
                "value_address",
            ):
                if hasattr(field_obj, attr) and getattr(field_obj, attr) is not None:
                    typed_val = getattr(field_obj, attr)
                    break
            if isinstance(typed_val, (int, float)):
                return _typed_number(typed_val, confidence=confidence, polygon=poly)
            return _typed_scalar(
                content if content is not None else typed_val,
                confidence=confidence,
                polygon=poly,
            )

        if field_type == "array":
            items: List[Dict[str, Any]] = []
            arr = getattr(field_obj, "value_array", None)
            if arr:
                for element in arr:
                    items.append(_map_document_field(element))
            return _typed_array(items)

        if field_type == "object":
            obj_map: Dict[str, Any] = {}
            value_obj = getattr(field_obj, "value_object", None) or {}
            for key, val in value_obj.items():
                obj_map[key] = _map_document_field(val)
            return _typed_object(obj_map)

        return _typed_scalar(content, confidence=confidence, polygon=poly)

    except Exception:
        return _typed_scalar(
            getattr(field_obj, "content", None),
            confidence=getattr(field_obj, "confidence", None),
            polygon=_first_region_polygon(field_obj),
        )


def analyze_result_to_typed_tree(result: Any) -> Tuple[Dict[str, Any], int]:
    """
    Convert a DI AnalyzeResult to the typed schema with polygons but no spans.
    Returns: (typed_root, page_count)
    """
    try:
        documents = getattr(result, "documents", None)
        if documents and len(documents) > 0 and getattr(documents[0], "fields", None):
            doc0 = documents[0]
            root_fields: Dict[str, Any] = {}

            for key, field_obj in doc0.fields.items():
                root_fields[key] = _map_document_field(field_obj)

            doc_type = getattr(doc0, "doc_type", None) or getattr(doc0, "docType", None)
            if doc_type is not None:
                root_fields["_docType"] = _typed_scalar(str(doc_type))

            doc_poly = _first_region_polygon(doc0)
            if doc_poly:
                root_fields["_documentPolygon"] = _typed_scalar("", polygon=doc_poly)

            page_count = len(getattr(result, "pages", []) or [])
            return _typed_object(root_fields), page_count

    except Exception:
        pass

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
            typed_words: List[Dict[str, Any]] = []
            for word in getattr(line, "words", []) or []:
                w_content = getattr(word, "content", "") or ""
                w_conf = getattr(word, "confidence", None)
                w_poly = _polygon_to_pairs(getattr(word, "polygon", None))
                typed_words.append(
                    _typed_object(
                        {"content": _typed_scalar(w_content, confidence=w_conf, polygon=w_poly)}
                    )
                )

            typed_lines.append(
                _typed_object(
                    {
                        "content": _typed_scalar(
                            ln_content, confidence=ln_conf, polygon=ln_poly
                        ),
                        "words": _typed_array(typed_words),
                    }
                )
            )

        typed_pages.append(
            _typed_object(
                {
                    "pageNumber": _typed_number(
                        page_number if page_number is not None else ""
                    ),
                    "content": _typed_scalar(
                        page_content, confidence=page_conf, polygon=page_poly
                    ),
                    "lines": _typed_array(typed_lines),
                }
            )
        )

    typed_root = _typed_object(
        {
            "content": _typed_scalar(doc_content),
            "pages": _typed_array(typed_pages),
        }
    )
    return typed_root, len(pages)


def write_json_locally(payload: Dict[str, Any], output_dir: str, filename: str) -> str:
    """Write JSON payload to disk; returns the full path."""
    os.makedirs(output_dir, exist_ok=True)
    path = os.path.join(output_dir, filename)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2, default=serialize)
    print(f"[OK] Wrote: {path}")
    return path


def upload_file_to_blob_via_aad(
    storage_account_url: str,
    container_name: str,
    local_path: str,
    blob_prefix: str = "",
) -> None:
    """Upload a local file to an Azure Blob container using AAD."""
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


def save_analyzed_result(
    result: Any,
    output_basename: str,
    output_root: str,
    upload_output: bool,
    storage_account_url: str,
    output_container: str,
) -> Tuple[str, int]:
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
        upload_file_to_blob_via_aad(
            storage_account_url, output_container, local_path
        )

    return local_path, page_count


def analyze_local_pdfs(
    di_client: DocumentIntelligenceClient,
    model_id: str,
    local_directory: str,
    output_container: str,
    upload_output: bool,
    storage_account_url: str,
    progress_path: str,
    processed_keys: Set[str],
) -> None:
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
    print(
        f"[OK] Local: processed={processed}, skipped={skipped}, errors={errors} in {elapsed:.2f}s"
    )


def analyze_azure_pdfs(
    di_client: DocumentIntelligenceClient,
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
    download_dir: str = "./tmp_azure_pdfs",
) -> None:
    """
    Concurrent Azure pipeline with adaptive rate limiting and parallel uploads.
    """
    os.makedirs(download_dir, exist_ok=True)
    print(
        f"[INFO] Container={container_name}, Prefix='{prefix}', Model={model_id}, "
        f"Workers={max_workers}, TPS={tps_limit}"
    )

    cred = DefaultAzureCredential()
    blob_service = BlobServiceClient(account_url=storage_account_url, credential=cred)
    container_client: ContainerClient = blob_service.get_container_client(container_name)

    candidates: List[str] = []
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
        last_exc: Exception | None = None

        while attempt <= retries:
            try:
                pdf = container_client.get_blob_client(name).download_blob().readall()

                rate.acquire()
                poller = di_client.begin_analyze_document(model_id, pdf)
                result = poller.result()

                base = os.path.splitext(os.path.basename(name))[0]
                local_path, pages = save_analyzed_result(
                    result=result,
                    output_basename=base,
                    output_root=os.path.join("json", output_container),
                    upload_output=False,
                    storage_account_url=storage_account_url,
                    output_container=output_container,
                )
                append_progress(progress_path, key_token, pages)
                return name, local_path, pages

            except Exception as e:  # noqa: BLE001
                last_exc = e
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

        assert last_exc is not None
        raise last_exc

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
            except Exception as e:  # noqa: BLE001
                errors += 1
                print(f"[ERR] {e}")

    if upload_output and json_paths:
        print(
            f"[INFO] Uploading {len(json_paths)} JSON files to '{output_container}'..."
        )

        def upload_one(path: str) -> None:
            upload_file_to_blob_via_aad(
                storage_account_url, output_container, path, blob_prefix=output_prefix
            )

        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            for _ in ex.map(upload_one, json_paths):
                pass

    elapsed = (datetime.now() - start).total_seconds()
    print(
        f"[DONE] Azure concurrent: processed={processed}, errors={errors}, elapsed={elapsed:.2f}s"
    )


def load_connections_config() -> ConnectionsConfig:
    """
    Load ./connections.json and return connection settings for DI + Blob Storage.
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
        di_key=di_key,
        storage_account_url=storage_account_url.rstrip("/"),
    )


def parse_extract_args(argv: List[str] | None = None) -> RunOptions:
    """Parse CLI flags for the 'extract' stage."""
    p = argparse.ArgumentParser(
        prog="fia_pipeline.py extract",
        description="Analyze PDFs with Azure Document Intelligence with progress tracking.",
    )
    p.add_argument(
        "--mode",
        choices=["local", "azure"],
        default="local",
        help="Source of PDFs.",
    )
    p.add_argument(
        "--model-id",
        required=True,
        help="DI model ID (e.g., 'prebuilt-read' or your custom model).",
    )
    p.add_argument(
        "--local-directory",
        default="./tempdata",
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
        help="Max concurrent workers for download/analyze/upload.",
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
        help="Optional blob name prefix (e.g. 'ground-pdf/').",
    )
    p.add_argument(
        "--download-dir",
        default="./tmp_azure_pdfs",
        help="Where to stage PDFs locally before analyze.",
    )
    p.add_argument(
        "--max-pdfs",
        type=int,
        default=None,
        help="Max PDFs to process from the container.",
    )
    p.add_argument(
        "--upload-output",
        action="store_true",
        help="Upload JSON outputs back to storage via AAD.",
    )
    p.add_argument(
        "--output-container",
        default="updated_json",
        help="Target container for JSON outputs.",
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

    a = p.parse_args(argv)
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


def extract_cli(argv: List[str] | None = None) -> None:
    """CLI entry for the 'extract' stage (PDF → JSON)."""
    opts = parse_extract_args(argv)
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


# =====================================================================
# --- PART B: Azure JSON cleaner/validator (from FIA_di_cleanup_azure.py)
# =====================================================================

# Defaults (can be overridden by CLI)
SOURCE_CONTAINER = "test_container"
SOURCE_PREFIX = "test_directory/upload_jsons/"
TARGET_PREFIX = "test_directory/validated_jsons/"
REPORT_BLOB_NAME = TARGET_PREFIX + "report.json"


def die(msg: str, code: int = 1) -> None:
    print(f"{ERR} {msg}")
    sys.exit(code)


def get_aad_token_for_storage() -> str:
    cred = DefaultAzureCredential()
    token = cred.get_token("https://storage.azure.com/.default")
    print(f"{OK} AAD token acquired for storage")
    return token.token


def get_required(d: dict, k: str) -> str:
    v = str(d.get(k, "")).strip()
    if not v:
        die(f"Missing or empty '{k}' in connections.json")
    return v


def validate_url(name: str, value: str) -> str:
    try:
        u = urlparse(value)
    except Exception:
        die(f"{name} is not a valid URL: {value}")
    if u.scheme not in ("https", "http") or not u.netloc:
        die(f"{name} must be an absolute URL, got: {value}")
    return value.rstrip("/")


def list_blobs_aad(
    sa_endpoint: str, container: str, bearer: str, prefix: str | None = None, timeout: int = 15
) -> List[str]:
    base = f"{sa_endpoint}/{container}"
    params = {"restype": "container", "comp": "list", "maxresults": "5000"}
    if prefix:
        params["prefix"] = prefix
    marker: str | None = None
    names: List[str] = []
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


def download_blob_aad(
    sa_endpoint: str, container: str, blob_name: str, bearer: str, timeout: int = 60
) -> bytes:
    url = f"{sa_endpoint}/{container}/{quote(blob_name)}"
    headers = {
        "Authorization": f"Bearer {bearer}",
        "x-ms-version": "2021-10-04",
        "x-ms-date": rfc1123_now(),
    }
    r = requests.get(url, headers=headers, timeout=timeout, stream=True)
    if r.status_code == 200:
        return r.content
    if r.status_code == 404:
        raise FileNotFoundError(f"Blob not found: {blob_name}")
    raise RuntimeError(f"Download failed {r.status_code}: {r.text[:200]}")


def upload_blob_aad(
    sa_endpoint: str,
    container: str,
    blob_name: str,
    bearer: str,
    data: bytes,
    timeout: int = 60,
) -> None:
    """
    Simple PUT blob (BlockBlob) via REST with AAD. Overwrites if exists.
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


# Cleaning / validation logic

NUMERIC_FIELDS = [
    "Live Count (incl. live tags)",
    "Live Count 2 (incl. live tags)",
    "% O.E. (Observer Efficiency)",
    "% Holding / Migrating",
    "% Spawning",
    "% Spawned Out",
    "Dead",
    "Dead 2",
    "Other species",
    "Other species 2",
    "Male",
    "Female % Spawn: N.R.",
    "Female % Spawn: 0%",
    "Female % Spawn: 50%",
    "Female % Spawn: 100%",
    "Unsexed",
    "Female",
    "Jack",
    "Water Level: Gauge",
    "Water Level: Gauge: Area",
    "Water Conditions: Water Temperature",
]

PERCENT_TRIO = [
    "% Holding / Migrating",
    "% Spawning",
    "% Spawned Out",
    "Female % Spawn: N.R.",
    "Female % Spawn: 0%",
    "Female % Spawn: 50%",
    "Female % Spawn: 100%",
]


def clean_selected_flag(v: Any) -> Any:
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


def normalize_zero_like(v: Any) -> Any:
    if not isinstance(v, str):
        return v
    raw = v.strip()
    if raw in ("Ø", "ø", "O", "o"):
        return "0"
    return v


def convert_polygon(poly: Any) -> Any:
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


def clean_node(node: Any, current_key_path: str = "") -> Any:
    if isinstance(node, dict) and "content" in node:
        is_comment = "comment" in current_key_path.lower()
        content_val = clean_selected_flag(node["content"])
        if not is_comment:
            content_val = normalize_zero_like(content_val)
        new_node: Dict[str, Any] = {}
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
        new_obj: Dict[str, Any] = {}
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


def get_content_from_cleaned(data: dict, field_name: str) -> Any:
    v = data.get(field_name)
    if isinstance(v, dict) and "content" in v:
        return v["content"]
    return None


def parse_number_from_content(val: Any) -> float | None:
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


def at_least_one_selected(cleaned_data: dict, field_list: List[str]) -> bool:
    for fname in field_list:
        val = get_content_from_cleaned(cleaned_data, fname)
        val = clean_selected_flag(val) if val is not None else val
        if val == "selected":
            return True
    return False


def validate_precipitation_rule(cleaned_data: dict) -> dict | None:
    none_field_name = CHECKBOX_PRECIPITATION_TYPE[-1]
    none_val = get_content_from_cleaned(cleaned_data, none_field_name)
    none_val = clean_selected_flag(none_val) if none_val is not None else none_val
    if none_val == "unselected":
        if not at_least_one_selected(cleaned_data, CHECKBOX_PRECIPITATION_INTENSITY):
            return {
                "precipitation_type_list": "Precipitation: Type: None is unselected but no precipitation intensity selected."
            }
    return None


def validate_selection_groups(cleaned_data: dict) -> dict:
    field_errors: Dict[str, str] = {}
    groups = [
        ("survey_list", CHECKBOX_SURVEY),
        ("water_level_list", CHECKBOX_WATER_LEVEL),
        ("weather_brightness_list", CHECKBOX_WEATHER_BRIGHTNESS),
        ("weather_cloudy_list", CHECKBOX_WEATHER_CLOUDY),
        ("precipitation_type_list", CHECKBOX_PRECIPITATION_TYPE),
        ("fish_visibility_list", CHECKBOX_FISH_VISIBILITY),
        ("water_clarity_list", CHECKBOX_WATER_CLARITY),
    ]
    for name, fields in groups:
        if not at_least_one_selected(cleaned_data, fields):
            field_errors[name] = f"Group '{name}' has no selected values."
    msg = validate_precipitation_rule(cleaned_data)
    if msg:
        field_errors.update(msg)
    return field_errors


def validate_numeric_fields(cleaned_data: dict) -> dict:
    errs: Dict[str, str] = {}
    for field_name in NUMERIC_FIELDS:
        val = get_content_from_cleaned(cleaned_data, field_name)
        if val is None or val == "":
            continue
        num = parse_number_from_content(val)
        if num is None:
            errs[field_name] = f"Field '{field_name}' should be numeric but got '{val}'."
    return errs


def validate_sk_count_data(cleaned_data: dict) -> dict:
    errs: Dict[str, Any] = {}
    sk1 = cleaned_data.get("SK_Count_data")
    if not isinstance(sk1, list) or not sk1:
        return errs

    for i, row in enumerate(sk1):
        if not isinstance(row, dict):
            continue
        r = row.get("item") if isinstance(row.get("item"), dict) else row
        row_errs: Dict[str, str] = {}
        parsed: Dict[str, float | None] = {}
        for key in NUMERIC_FIELDS:
            cell = r.get(key)
            if not (isinstance(cell, dict) and "content" in cell):
                parsed[key] = None
                continue
            s = cell["content"]
            if s is None:
                parsed[key] = None
                continue
            s = str(s).strip()
            if s == "":
                parsed[key] = None
                continue
            if key.startswith("%"):
                s_clean = s.replace("%", "").strip()
            else:
                s_clean = s
            if any(c in s_clean for c in ["/", "€"]) or any(
                ch.isalpha() for ch in s_clean
            ):
                row_errs[key] = "non-numeric or invalid symbol detected"
                parsed[key] = None
                continue
            try:
                parsed[key] = float(s_clean)
            except Exception:
                row_errs[key] = "not numeric"
                parsed[key] = None

        h = parsed.get("% Holding / Migrating")
        sp = parsed.get("% Spawning")
        so = parsed.get("% Spawned Out")
        if all(isinstance(x, (int, float)) for x in (h, sp, so)):
            total = float(h) + float(sp) + float(so)
            if abs(total - 100.0) > 0.5:
                row_errs["% Sum"] = (
                    f"Percentages do not total 100% (sum={total})"
                )

        if row_errs:
            errs[f"row_{i}"] = row_errs

    return {"Visual Surveys": errs} if errs else {}


def validate_sk_count_data_2(cleaned_data: dict) -> dict:
    errs: Dict[str, Any] = {}
    rows = cleaned_data.get("SK_Count_data_2")
    if not isinstance(rows, list) or not rows:
        return errs

    for i, row in enumerate(rows):
        if not isinstance(row, dict):
            continue
        r = row.get("item") if isinstance(row.get("item"), dict) else row
        row_errs: Dict[str, str] = {}

        keys_all = [
            "Female",
            "Females",
            "female",
            "females",
            "NR",
            "Nr",
            "nr",
            "0%",
            "0 %",
            "Zero",
            "zero",
            "50%",
            "50 %",
            "100%",
            "100 %",
            "Jack",
            "Unsexed",
            "Female % Spawn: 100%",
        ]

        parsed: Dict[str, float | None] = {}
        for k in keys_all:
            cell = r.get(k)
            if not (isinstance(cell, dict) and "content" in cell):
                parsed[k] = None
                continue
            s = cell["content"]
            if s is None:
                parsed[k] = None
                continue
            s = str(s).strip()
            if s == "":
                parsed[k] = None
                continue
            if "%" in s or "/" in s or "€" in s or any(ch.isalpha() for ch in s):
                row_errs[k] = "non-numeric or invalid symbol detected"
                parsed[k] = None
                continue
            v = parse_number_from_content(s)
            if v is None:
                row_errs[k] = "not numeric"
                parsed[k] = None
            else:
                parsed[k] = float(v)

        female = None
        for fk in ["Female", "Females", "female", "females"]:
            if isinstance(parsed.get(fk), (int, float)):
                female = parsed.get(fk)
                break
        parts_keys = [
            "NR",
            "Nr",
            "nr",
            "0%",
            "0 %",
            "Zero",
            "zero",
            "50%",
            "50 %",
            "100%",
            "100 %",
        ]
        part_vals = [
            parsed[k] for k in parts_keys if isinstance(parsed.get(k), (int, float))
        ]
        if isinstance(female, (int, float)) and part_vals:
            total = sum(part_vals)  # type: ignore[arg-type]
            if abs(float(female) - float(total)) > 0.01:
                row_errs["Female"] = (
                    f"Female ({female}) != NR+0%+50%+100% ({total})"
                )

        if row_errs:
            errs[f"row_{i}"] = row_errs

    return {"SOCKEYE UNTAGGED RECOVERIES": errs} if errs else {}


def validate_sk_count_data_3(cleaned_data: dict) -> dict:
    errs: Dict[str, Any] = {}
    rows = cleaned_data.get("SK_Count_data_3")
    if not isinstance(rows, list) or not rows:
        return errs

    for i, row in enumerate(rows):
        if not isinstance(row, dict):
            continue
        r = row.get("item") if isinstance(row.get("item"), dict) else row
        row_errs: Dict[str, str] = {}

        for k, cell in r.items():
            if k == "Area":
                continue
            if not (isinstance(cell, dict) and "content" in cell):
                continue
            s = cell["content"]
            if s is None:
                continue
            s = str(s).strip()
            if s == "":
                continue
            if "%" in s or "/" in s or "€" in s or any(ch.isalpha() for ch in s):
                row_errs[k] = "non-numeric or invalid symbol detected"
                continue
            v = parse_number_from_content(s)
            if v is None:
                row_errs[k] = "not numeric"

        if row_errs:
            errs[f"row_{i}"] = row_errs

    return {"SOCKEYE TAGGED RECOVERIES": errs} if errs else {}


def parse_clean_args(argv: List[str] | None = None):
    """Parse CLI flags for the 'clean' stage."""
    ap = argparse.ArgumentParser(
        prog="fia_pipeline.py clean",
        description="Azure BC16 cleaner/validator (nested) + polygon + numeric checks",
    )
    ap.add_argument(
        "-f",
        "--file",
        default="connections.json",
        help="connections.json path",
    )
    ap.add_argument(
        "--container",
        default=SOURCE_CONTAINER,
        help="Source/target container",
    )
    ap.add_argument(
        "--src-prefix",
        default=SOURCE_PREFIX,
        help="Prefix to read from",
    )
    ap.add_argument(
        "--dst-prefix",
        default=TARGET_PREFIX,
        help="Prefix to write cleaned files to",
    )
    ap.add_argument(
        "--report-blob",
        default=REPORT_BLOB_NAME,
        help="Where to upload the summary report (blob name or folder-like path)",
    )
    ap.add_argument(
        "--report-file",
        default="report.json",
        help="Local filename (unused but kept for compatibility)",
    )
    return ap.parse_args(argv)


def cleanup_cli(argv: List[str] | None = None) -> None:
    """CLI entry for the 'clean' stage (JSON cleanup/validation in Azure)."""
    args = parse_clean_args(argv)

    try:
        with open(args.file, "r", encoding="utf-8") as rf:
            cfg = json.load(rf)
    except Exception as e:  # noqa: BLE001
        die(f"Failed to load connections.json: {e}")

    sa_endpoint = validate_url("SA-endpoint", get_required(cfg, "SA-endpoint"))
    bearer = get_aad_token_for_storage()

    try:
        blobs = list_blobs_aad(
            sa_endpoint, args.container, bearer, prefix=args.src_prefix
        )
    except Exception as e:  # noqa: BLE001
        die(f"Failed to list blobs: {e}")

    json_blobs = [b for b in blobs if b.lower().endswith(".json")]
    print(f"{OK} Found {len(json_blobs)} JSON blobs in {args.container}/{args.src_prefix}")

    report_data: Dict[str, Any] = {}

    for blob_name in json_blobs:
        print(f"[..] Processing {blob_name}")
        try:
            raw_bytes = download_blob_aad(
                sa_endpoint, args.container, blob_name, bearer
            )
            data = json.loads(raw_bytes.decode("utf-8"))
        except Exception as e:  # noqa: BLE001
            print(f"{ERR} {blob_name} -> cannot download/parse: {e}")
            report_data[blob_name] = {"file_error": str(e)}
            continue

        cleaned = clean_node(data)

        errors: Dict[str, Any] = {}
        errors.update(validate_selection_groups(cleaned))
        errors.update(validate_numeric_fields(cleaned))
        errors.update(validate_sk_count_data(cleaned))
        errors.update(validate_sk_count_data_2(cleaned))
        errors.update(validate_sk_count_data_3(cleaned))

        base_name = os.path.basename(blob_name)
        dst_blob_name = f"{args.dst_prefix.rstrip('/')}/{base_name}"
        cleaned_bytes = json.dumps(
            cleaned, ensure_ascii=False, indent=2
        ).encode("utf-8")
        try:
            upload_blob_aad(
                sa_endpoint, args.container, dst_blob_name, bearer, cleaned_bytes
            )
        except Exception as e:  # noqa: BLE001
            print(f"{ERR} Failed to upload cleaned {base_name}: {e}")

        filename = blob_name.split("/")[-1][:-5]
        if errors:
            report_data[filename] = errors
            print(f"{ERR} {blob_name} -> {len(errors)} issues")
        else:
            msg = filename + " passed preliminary check."
            report_data[filename] = {"summary": msg}
            print(f"{OK} {blob_name} -> passes all checks")

    timestamp = datetime.now(timezone.utc).isoformat(timespec="seconds")
    report_data["_generated_at"] = timestamp

    report_bytes = json.dumps(
        report_data, ensure_ascii=False, indent=2
    ).encode("utf-8")

    report_blob_name = args.report_blob.strip("/")
    if not report_blob_name.lower().endswith(".json"):
        report_blob_name = report_blob_name + "/report.json"

    try:
        upload_blob_aad(
            sa_endpoint, args.container, report_blob_name, bearer, report_bytes
        )
        print(f"{OK} Uploaded report to {report_blob_name}")
    except Exception as e:  # noqa: BLE001
        print(f"{ERR} Failed to upload report.json to {report_blob_name}: {e}")


# =====================================================================
# --- BOTH: chained extract → clean
# =====================================================================

def parse_both_args(argv: List[str] | None = None):
    """Parse CLI flags for the 'both' (extract+clean) stage."""
    ap = argparse.ArgumentParser(
        prog="fia_pipeline.py both",
        description="Run extract (Azure DI) then clean (Azure JSON cleanup) in sequence.",
    )
    ap.add_argument(
        "--model-id",
        required=True,
        help="DI model ID for extract stage (e.g. 'fia-roving-aerial-1').",
    )
    ap.add_argument(
        "--container",
        default="fia",
        help="Azure Blob container used for PDFs and JSONs.",
    )
    ap.add_argument(
        "--pdf-prefix",
        default="ground/upload",
        help="Blob prefix where PDFs live (container/pdf-prefix).",
    )
    ap.add_argument(
        "--json-prefix",
        default="ground/update_jsons",
        help="Blob prefix where JSON outputs go and are later cleaned.",
    )
    ap.add_argument(
        "--connections-file",
        default="connections.json",
        help="connections.json path for clean stage.",
    )
    ap.add_argument(
        "--max-workers",
        type=int,
        default=12,
        help="Max concurrent workers for extract stage.",
    )
    ap.add_argument(
        "--tps-limit",
        type=int,
        default=8,
        help="TPS throttle for extract Azure DI stage.",
    )
    ap.add_argument(
        "--retries",
        type=int,
        default=2,
        help="Retries per file on transient failures (extract stage).",
    )
    ap.add_argument(
        "--max-pdfs",
        type=int,
        default=None,
        help="Max PDFs to process in extract stage.",
    )
    ap.add_argument(
        "--progress-file",
        default="./fia_progress_processed.txt",
        help="Progress TSV used in extract stage.",
    )
    ap.add_argument(
        "--reset",
        action="store_true",
        help="Delete the progress file before running extract.",
    )
    ap.add_argument(
        "--report-blob",
        default="",
        help="Report blob name or folder; default json-prefix/report.json.",
    )
    return ap.parse_args(argv)


def both_cli(argv: List[str] | None = None) -> None:
    """
    Run extract (Azure DI) followed by clean (JSON cleanup) in one command.
    """
    args = parse_both_args(argv)

    print("[INFO] Stage 1/2: extract (Azure DI) starting...")
    extract_args: List[str] = [
        "--mode", "azure",
        "--model-id", args.model_id,
        "--container", args.container,
        "--prefix", args.pdf_prefix,
        "--output-container", args.container,
        "--output-prefix", args.json_prefix,
        "--upload-output",
        "--max-workers", str(args.max_workers),
        "--tps-limit", str(args.tps_limit),
        "--retries", str(args.retries),
        "--progress-file", args.progress_file,
    ]
    if args.max_pdfs is not None:
        extract_args.extend(["--max-pdfs", str(args.max_pdfs)])
    if args.reset:
        extract_args.append("--reset")

    extract_cli(extract_args)

    print("[INFO] Stage 2/2: clean (JSON cleanup/validation) starting...")
    report_blob = args.report_blob.strip()
    if not report_blob:
        report_blob = f"{args.json_prefix.rstrip('/')}/report.json"

    cleanup_args: List[str] = [
        "--container", args.container,
        "--src-prefix", args.json_prefix,
        "--dst-prefix", args.json_prefix,
        "--report-blob", report_blob,
        "-f", args.connections_file,
    ]
    cleanup_cli(cleanup_args)


# =====================================================================
# Top-level dispatcher
# =====================================================================

def main() -> None:
    """
    Dispatcher:

      python fia_pipeline.py extract [extract-args...]
      python fia_pipeline.py clean   [clean-args...]
      python fia_pipeline.py both    [both-args...]
    """
    if len(sys.argv) < 2 or sys.argv[1] in ("-h", "--help"):
        print(
            "Usage:\n"
            "  python fia_pipeline.py extract [extract-args...]\n"
            "  python fia_pipeline.py clean   [clean-args...]\n"
            "  python fia_pipeline.py both    [both-args...]\n"
            "\nUse '--help' after a subcommand for its options, e.g.:\n"
            "  python fia_pipeline.py extract --help\n"
            "  python fia_pipeline.py clean   --help\n"
            "  python fia_pipeline.py both    --help"
        )
        sys.exit(0)

    subcommand = sys.argv[1]
    remaining = sys.argv[2:]

    if subcommand == "extract":
        extract_cli(remaining)
    elif subcommand == "clean":
        cleanup_cli(remaining)
    elif subcommand == "both":
        both_cli(remaining)
    else:
        print(f"{ERR} Unknown subcommand: {subcommand}")
        sys.exit(1)


if __name__ == "__main__":
    main()


# python .\python\FIA_di_read_and_clean.py --container fia --src-prefix ground/update_jsons --dst-prefix ground/update_jsons --report-blob ground/
# python python/FIA_di_reader_full_file.py --mode azure --model-id fia-roving-aerial-1 --container fia --prefix aerial/upload --output-container fia --output-prefix aerial/update_jsons --upload-output --max-worker 16 --tps-limit 8 --retries 3