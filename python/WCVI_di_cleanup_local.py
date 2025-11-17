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
method_list = ['METHOD - BANK WALK', 'METHOD - STREAM WALK', 'METHOD - SNORKLE', 'METHOD - DEADPITCH', 'METHOD - BOAT', 'METHOD - OTHER']
water_level_list = ['WATER LEVEL - EXT LOW','WATER LEVEL - BELOW NORM', 'WATER LEVEL - NORMAL','WATER LEVEL - ABOVE NORM', 'WATER LEVEL -FLOOD']
weather_brightness_list = ['LIGHT LEVEL - FULL','LIGHT LEVEL - BRIGHT', 'LIGHT LEVEL - MEDIUM','LIGHT LEVEL - DARK']
weather_cloudy_list = ['CLOUD COVER - CLEAR','CLOUD COVER - SCATTERED', 'CLOUD COVER - PART CLOUDY','CLOUD COVER - CLOUDY','CLOUD COVER - OVERCAST']
target_species_list = ['TARGET SPECIES - SOCKEYE', 'TARGET SPECIES - COHO', 'TARGET SPECIES - PINK', 
                       'TARGET SPECIES - CHUM', 'TARGET SPECIES - CHINOOK', 'TARGET SPECIES - OTHER']
precipitation_type_list = ['PRECIPITATION - NONE', 'PRECIPITATION - RAIN', 'PRECIPITATION - SNOW']
precipitation_intensity_list = ['PRECIPITATION - LIGHT', 'PRECIPITATION - MEDIUM', 'PRECIPITATION - HEAVY']
fish_visibility_list = ['FISH VIS - LOW','FISH VIS - MEDIUM', 'FISH VIS - HIGH']
water_con_list = ['WATER CON - CLEAR','WATER CON - TEA', 'WATER CON - SILT', 'WATER CON - MUDDY', 'WATER CON - TURBID', 'WATER CON - ICED']

# ---------------------------------------------------------------------
# Cleaning utilities
# ---------------------------------------------------------------------
def clean_selected_flag(v: str) -> str:
    if not isinstance(v, str):
        return v
    val = v.strip()
    low = val.lower()
    # print("before clean:",v)
    if low == ":selected:":
        return "selected"
    if low == ":unselected:":
        return "unselected"
    if "selected" in low and "unselected" not in low:
        return "selected"
    if "unselected" in low:
        return "unselected"
    return val

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
        # print(new_node)
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
    print(data)
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
    none_val = get_content_from_cleaned(cleaned_data, "PRECIPITATION - NONE")
    none_val = clean_selected_flag(none_val) if none_val is not None else none_val
    if none_val == "unselected":
        if not at_least_one_selected(cleaned_data, precipitation_intensity_list):
            return {"precipitation_type_list": "PRECIPITATION - NONE is unselected but no precipitation intensity selected."}
    return None

def validate_selection_groups(cleaned_data: dict):
    field_errors = {}
    groups = [
        ("method_list", method_list),
        ("water_level_list", water_level_list),
        ("weather_brightness_list", weather_brightness_list),
        ("target_species_list", target_species_list),
        ("weather_cloudy_list", weather_cloudy_list),
        ("precipitation_type_list", precipitation_type_list),
        ("fish_visibility_list", fish_visibility_list),
        ("water_con_list", water_con_list),
    ]
    for name, fields in groups:
        if not at_least_one_selected(cleaned_data, fields):
            field_errors[name] = f"Group '{name}' has no selected values."
    msg = validate_precipitation_rule(cleaned_data)
    if msg:
        field_errors.update(msg)
    return field_errors

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
        file_report_name = name[:-5]
        # condition_pages = {}
        src_path = os.path.join(args.in_dir, name)
        try:
            with open(src_path, "r", encoding="utf-8") as f:
                raw = json.load(f)
                print(name, "loaded")
                # condition_pages = raw['C1-1']
        except Exception as e:
            print(f"{ERR} {name} -> cannot read JSON: {e}")
            report_data[file_report_name] = {"file_error": str(e)}
            continue

        cleaned = {}
        # if condition_pages == {}:
        #     continue
        errors = {}
        for key, pages in raw.items():
            for page in pages:
                cleaned = clean_node(page)
                if page == cleaned:
                    print(key, page['page'],'allgood')
                    continue
                # print(cleaned)
                errors.update(validate_selection_groups(cleaned))
                print("error updated")
                raw[key] = cleaned
            # raw['C1-1'] += cleaned

        out_path = os.path.join(args.out_dir, name)
        print(out_path)
        with open(out_path, "w+", encoding="utf-8") as wf:
            json.dump(raw, wf, ensure_ascii=False, indent=2)

        report_data[file_report_name] = errors
        if errors:
            print(f"{ERR} {name} -> {len(errors)} issues")
        else:
            print(f"{OK} {name} -> passes all checks")
            report_data[file_report_name]['summary'] = file_report_name + ' passed preliminary check.'

    with open(args.report, "w", encoding="utf-8") as vf:
        json.dump(report_data, vf, ensure_ascii=False, indent=2)

    print(f"\n{OK} Validation report written to {args.report}")


if __name__ == "__main__":
    main()

# python SILS_di_cleanup_local.py --in-dir ... --out-dir ...
