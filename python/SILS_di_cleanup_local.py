#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Local BC16 cleaner/validator (nested) + polygon + numeric checks + report.json output

- Reads connections.json (for consistency)
- Reads JSON files from ./local_input/
- Cleans:
    - 'content' normalization (selected/unselected, Ø→0 (non-comments))
    - normalizes polygon to [[x,y], ...]
- Validates:
    - required selection groups
    - precipitation special rule
    - numeric field validity
    - % trio (Holding/Migrating + Spawning + Spawned Out == 100 or 1)
    - SK_Count_data_2 female == NR + 0% + 50% + 100%
- Writes cleaned JSONs to ./out_cleaned/
- Writes validation summary as ./report.json, with per-file dictionaries of field:message
"""

import os, json, sys

OK, ERR = "[OK]", "[ERR]"

# ---------------------------------------------------------------------
# Basic helpers
# ---------------------------------------------------------------------
def die(msg, code=1):
    print(f"{ERR} {msg}")
    sys.exit(code)

def ensure_dir(path):
    os.makedirs(path, exist_ok=True)

def get_required(d: dict, k: str) -> str:
    v = str(d.get(k, "")).strip()
    if not v:
        die(f"Missing or empty '{k}' in connections.json")
    return v

# ---------------------------------------------------------------------
# Field groups
# ---------------------------------------------------------------------
survey_list = ['Survey Type: Ground','Survey Type: Aerial']
water_level_list = ['Water Level: % Bankfill: <25%','Water Level: % Bankfill: 25-50%',
                    'Water Level: % Bankfill: 50-75%','Water Level: % Bankfill: 75-100%',
                    'Water Level: % Bankfill: +100%']
weather_brightness_list = ['Weather: Brightness: Full','Weather: Brightness: Bright',
                    'Weather: Brightness: Medium','Weather: Brightness: Dark']
weather_cloudy_list = ['Weather: %Cloudy: 0%','Weather: %Cloudy: 25%',
                    'Weather: %Cloudy: 50%','Weather: %Cloudy: 75%','Weather: %Cloudy: 100%']
precipitation_type_list = ['Precipitation: Type: Rain','Precipitation: Type: Snow','Precipitation: Type: None']
precipitation_intensity_list = ['Precipitation: Intensity: Light','Precipitation: Intensity: Medium','Precipitation: Intensity: Heavy']
fish_visibility_list = ['Water Conditions: Fish Visibility: Low','Water Conditions: Fish Visibility: Medium',
                        'Water Conditions: Fish Visibility: High']
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

# ---------------------------------------------------------------------
# Cleaning utilities
# ---------------------------------------------------------------------
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

# ---------------------------------------------------------------------
# Recursive cleaner
# ---------------------------------------------------------------------
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

# ---------------------------------------------------------------------
# Utility parsers
# ---------------------------------------------------------------------
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

# ---------------------------------------------------------------------
# Validators
# ---------------------------------------------------------------------
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
    ap = argparse.ArgumentParser(description="Local BC16 cleaner/validator with report.json output.")
    ap.add_argument("-f", "--file", default="connections.json", help="connections.json path")
    ap.add_argument("--in-dir", default="local_input", help="Folder with raw JSONs")
    ap.add_argument("--out-dir", default="out_cleaned", help="Folder for cleaned JSONs")
    ap.add_argument("--report", default="report.json", help="Output validation report path")
    args = ap.parse_args()

    try:
        with open(args.file, "r", encoding="utf-8") as rf:
            cfg = json.load(rf)
    except Exception as e:
        die(f"Failed to load connections.json: {e}")

    _ = get_required(cfg, "SA-endpoint")

    ensure_dir(args.out_dir)
    files = [f for f in os.listdir(args.in_dir) if f.lower().endswith(".json")]
    print(f"{OK} Found {len(files)} JSON files in {args.in_dir}")

    report_data = {}

    for name in files:
        src_path = os.path.join(args.in_dir, name)
        try:
            with open(src_path, "r", encoding="utf-8") as f:
                raw = json.load(f)
        except Exception as e:
            print(f"{ERR} {name} -> cannot read JSON: {e}")
            report_data[name] = {"file_error": str(e)}
            continue

        cleaned = clean_node(raw)
        errors = {}
        errors.update(validate_selection_groups(cleaned))
        errors.update(validate_numeric_fields(cleaned))
        errors.update(validate_percentage_trio(cleaned))
        errors.update(validate_sk_count_data_2(cleaned))

        out_path = os.path.join(args.out_dir, name)
        with open(out_path, "w", encoding="utf-8") as wf:
            json.dump(cleaned, wf, ensure_ascii=False, indent=2)

        report_data[name] = errors
        if errors:
            print(f"{ERR} {name} -> {len(errors)} issues")
        else:
            print(f"{OK} {name} -> passes all checks")

    with open(args.report, "w", encoding="utf-8") as vf:
        json.dump(report_data, vf, ensure_ascii=False, indent=2)

    print(f"\n{OK} Validation report written to {args.report}")


if __name__ == "__main__":
    main()

# python clean_local_nested.py --in-dir local_input --out-dir out_cleaned
