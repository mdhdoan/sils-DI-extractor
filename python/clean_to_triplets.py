#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
clean_to_triplets.py
Transforms Azure DI JSON into a sample triplet shape:

    <field> -> [ value_or_null, {p1:[x,y], p2:[x,y], ...} or {}, confidence_or_null ]

Supported inputs
----------------
1) Custom-model extracted JSON (your di_json_reader.py "extracted" outputs)
   - Leaves look like: {"content": "...", "confidence": 0.97, "polygon": [[x,y],...], ...}
   - We preserve objects/arrays structurally, converting only "scalar" leaves to triplets.

2) Prebuilt-read raw JSON (when --source prebuilt)
   - Expected shape like: {"document":{"pages":[{"pageNumber": "...", "lines":[{content,confidence,polygon,...}, ...]}]}}
   - We output:
       {
         "document": {
           "pages": [
             {
               "pageNumber": [<string_or_null>, {}, <float_or_null>],
               "lines": [
                 [<line_text_or_null>, {p1..}, <float_or_null>],
                 ...
               ]
             },
             ...
           ]
         }
       }

Usage
-----
# Custom model (default)
python clean_to_triplets.py --input ./json/results-json --output ./json_clean/results-json

# Prebuilt-read mode (converts each page/line to triplets)
python clean_to_triplets.py --source prebuilt \
    --input ./raw_prebuilt_json \
    --output ./json_clean_prebuilt
"""

from __future__ import annotations

import argparse
import json
import os
from typing import Any, Dict, List, Tuple, Union

# ----------------------------
# Generic helpers
# ----------------------------

def polygon_pairs_to_dict(poly: Any) -> Dict[str, List[float]]:
    """[[x,y], ...] -> {"p1":[x,y], ...}; non-lists/empty -> {}."""
    if not poly or not isinstance(poly, list):
        return {}
    out: Dict[str, List[float]] = {}
    for i, pt in enumerate(poly, start=1):
        if isinstance(pt, (list, tuple)) and len(pt) >= 2:
            out[f"p{i}"] = [pt[0], pt[1]]
    return out

def triplet(value: Any, polygon: Any = None, confidence: Any = None) -> List[Any]:
    """Build [value_or_null, polygon_dict_or_{}, confidence_or_null]."""
    poly_dict = polygon_pairs_to_dict(polygon)
    conf = None if (confidence in ("", None)) else confidence
    val = None if value in ("", None) else value
    return [val, poly_dict, conf]

# ----------------------------
# Mode 1: Custom-model outputs
# ----------------------------

def looks_like_scalar_leaf(node: Any) -> bool:
    """A 'scalar leaf' in custom-model extracted JSON: dict with 'content' key."""
    return isinstance(node, dict) and "content" in node

def transform_custom(node: Any) -> Any:
    """
    Recursively convert custom-model extracted JSON:
      - dicts with 'content' -> triplet
      - dicts/arrays -> recurse
      - primitives -> passthrough (rare)
    """
    if isinstance(node, dict):
        if looks_like_scalar_leaf(node):
            return triplet(node.get("content", None),
                           node.get("polygon", None),
                           node.get("confidence", None))
        return {k: transform_custom(v) for k, v in node.items()}
    if isinstance(node, list):
        return [transform_custom(v) for v in node]
    return node

# ----------------------------
# Mode 2: Prebuilt-read outputs
# ----------------------------

def get(obj: Any, path: List[Union[str, int]], default=None):
    """Safe getter using a path of keys/indices."""
    cur = obj
    for p in path:
        try:
            if isinstance(p, int):
                cur = cur[p]
            else:
                cur = cur.get(p)
        except Exception:
            return default
        if cur is None:
            return default
    return cur

def transform_prebuilt(raw: Dict[str, Any]) -> Dict[str, Any]:
    """
    Convert prebuilt-read raw to triplets for pageNumber + every line.
    Confidence is often empty; we normalize '' -> null.
    """
    doc = get(raw, ["document"], {}) or {}
    pages = get(doc, ["pages"], []) or []

    out_pages = []
    for pg in pages:
        page_num = get(pg, ["pageNumber"])
        page_conf = get(pg, ["confidence"])
        lines = get(pg, ["lines"], []) or []

        out_lines = []
        for ln in lines:
            out_lines.append(
                triplet(
                    value=get(ln, ["content"]),
                    polygon=get(ln, ["polygon"]),
                    confidence=get(ln, ["confidence"])
                )
            )

        out_pages.append({
            "pageNumber": triplet(page_num, None, page_conf),  # pageNumber rarely has polygon; keep {}
            "lines": out_lines
        })

    return {"document": {"pages": out_pages}}

# ----------------------------
# I/O
# ----------------------------

def process_file(in_path: str, out_path: str, source: str) -> None:
    with open(in_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    if source == "custom":
        shaped = transform_custom(data)
    else:
        shaped = transform_prebuilt(data)

    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(shaped, f, ensure_ascii=False, indent=2)
    print(f"[OK] Wrote: {out_path}")

def walk_and_process(in_root: str, out_root: str, source: str) -> None:
    for root, _, files in os.walk(in_root):
        for fn in files:
            if not fn.lower().endswith(".json"):
                continue
            in_path = os.path.join(root, fn)
            rel = os.path.relpath(in_path, in_root)
            out_path = os.path.join(out_root, rel)
            process_file(in_path, out_path, source)

# ----------------------------
# CLI
# ----------------------------

def main() -> None:
    p = argparse.ArgumentParser(description="Clean Azure DI JSON into [value, {p1..}, confidence] triplets.")
    p.add_argument("--input", required=True, help="Folder of input JSONs.")
    p.add_argument("--output", required=True, help="Folder to write cleaned JSONs.")
    p.add_argument("--source", choices=["custom", "prebuilt"], default="custom",
                   help="Input JSON source type: custom-model extracted (default) or prebuilt-read raw.")
    args = p.parse_args()

    in_root = os.path.abspath(args.input)
    out_root = os.path.abspath(args.output)
    if not os.path.isdir(in_root):
        raise FileNotFoundError(f"Input folder not found: {in_root}")

    walk_and_process(in_root, out_root, args.source)

if __name__ == "__main__":
    main()

# python clean_to_triplets.py \
#   --input ./json/results-json \
#   --output ./json_clean/results-json \
#   --source custom

  
# python clean_to_triplets.py \
#   --input ./raw_prebuilt_json \
#   --output ./json_clean_prebuilt \
#   --source prebuilt
