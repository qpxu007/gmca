#!/usr/bin/env python3
"""
Generate an interactive map with circles showing user counts per institution.

Usage:
    python institution_map.py input.csv -o map.html
    python institution_map.py input.xlsx --sheet "Sheet1" -o map.html

Input spreadsheet must have columns:
    - Institution name (first column or column named 'institution'/'name'/'university'/'company')
    - User count (second column or column named 'users'/'count'/'number')
"""

import argparse
import json
import math
import os
import re
import sys
import time
from collections import defaultdict

import folium
import pandas as pd
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderUnavailable


CACHE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".geocode_cache.json")


def load_cache():
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, "r") as f:
            return json.load(f)
    return {}


def save_cache(cache):
    with open(CACHE_FILE, "w") as f:
        json.dump(cache, f, indent=2)


def geocode_institution(name, geolocator, cache, retries=3):
    """Geocode an institution name, using cache when available.

    Tries the full name first, then falls back to the stripped name
    (without location suffix) if that fails.
    """
    key = name.strip().lower()
    if key in cache:
        return cache[key]

    # Try full name, then stripped name, then just the core institution name
    variants = [name]
    stripped = strip_location(name)
    if stripped.lower() != name.strip().lower():
        variants.append(stripped)

    for variant in variants:
        for attempt in range(retries):
            try:
                location = geolocator.geocode(variant, timeout=10)
                if location:
                    result = {"lat": location.latitude, "lng": location.longitude, "address": location.address}
                    cache[key] = result
                    return result
                break  # Successfully queried but not found, move to next variant
            except (GeocoderTimedOut, GeocoderUnavailable) as e:
                print(f"\n  Warning: geocoding failed for '{variant}' on attempt {attempt+1}: {e}")
                time.sleep(2 ** attempt)  # Exponential backoff
        time.sleep(1.1)

    cache[key] = None
    return None


def strip_location(name):
    """Strip trailing location suffixes like ', CA, USA' or ', Canada' from institution names."""
    # Match patterns like ", XX, USA" or ", Country" at end
    # Also handles ", City, XX, USA" patterns
    stripped = re.sub(r',\s*([A-Z]{2},\s*USA|USA|Canada|UK|Germany|France|Japan|China|Australia|Switzerland)\s*$', '', name, flags=re.IGNORECASE).strip()
    # Remove trailing state abbreviation if still present (", XX" at end)
    stripped = re.sub(r',\s*[A-Z]{2}\s*$', '', stripped).strip()
    # Remove trailing comma if any
    stripped = stripped.rstrip(',').strip()
    return stripped if stripped else name


def merge_institutions(df, name_col, count_col):
    """Merge institutions sharing the same root name (prefix matching).

    Strips location suffixes before comparing, so 'University Of Pittsburgh, PA, USA'
    and 'University Of Pittsburgh School Of Medicine, PA, USA' both match on
    'University Of Pittsburgh'.
    """
    names = df[name_col].dropna().astype(str).str.strip().tolist()
    counts = df[count_col].tolist()

    # Build (original_name, stripped_name, count) tuples
    entries = [(n, strip_location(n), c) for n, c in zip(names, counts)]
    # Sort by stripped name length (shortest first)
    entries.sort(key=lambda x: len(x[1]))

    groups = {}  # canonical_original -> list of (original_name, count)
    canonical_stripped = {}  # canonical_original -> stripped_name
    canonical_order = []

    for orig, stripped, count in entries:
        stripped_lower = stripped.lower()
        matched = False
        for canon in canonical_order:
            canon_stripped_lower = canonical_stripped[canon].lower()
            if stripped_lower.startswith(canon_stripped_lower):
                rest = stripped_lower[len(canon_stripped_lower):]
                if rest == "" or rest[0] in (" ", ",", ";", "-", "/"):
                    groups[canon].append((orig, count))
                    matched = True
                    break
        if not matched:
            groups[orig] = [(orig, count)]
            canonical_stripped[orig] = stripped
            canonical_order.append(orig)

    merged_rows = []
    for canon in canonical_order:
        members = groups[canon]
        total = sum(c for _, c in members)
        member_names = [n for n, _ in members]
        if len(member_names) > 1:
            print(f"  Merged: {member_names} -> '{canon}' (total: {total})")
        merged_rows.append({name_col: canon, count_col: total})

    return pd.DataFrame(merged_rows)


def detect_columns(df):
    """Auto-detect institution name and user count columns."""
    cols = [c.strip().lower() for c in df.columns]

    name_keywords = ["institution", "name", "university", "company", "organization", "org", "school"]
    count_keywords = ["users", "count", "number", "num", "total", "unique"]

    name_col = None
    count_col = None

    for i, c in enumerate(cols):
        if any(k in c for k in name_keywords):
            name_col = df.columns[i]
            break
    for i, c in enumerate(cols):
        if any(k in c for k in count_keywords):
            count_col = df.columns[i]
            break

    # Fallback: first string-like column and first numeric column
    if name_col is None:
        for col in df.columns:
            if df[col].dtype == object:
                name_col = col
                break
    if count_col is None:
        for col in df.columns:
            if pd.api.types.is_numeric_dtype(df[col]):
                count_col = col
                break

    if name_col is None or count_col is None:
        raise ValueError(
            f"Could not detect columns. Found: {list(df.columns)}. "
            "Need one text column (institution names) and one numeric column (user counts)."
        )

    return name_col, count_col


def calc_radius(count, min_count, max_count, min_radius=5, max_radius=40):
    """Scale circle radius proportionally using sqrt scaling."""
    if max_count == min_count:
        return (min_radius + max_radius) / 2
    normalized = (count - min_count) / (max_count - min_count)
    return min_radius + math.sqrt(normalized) * (max_radius - min_radius)


TILE_PROVIDERS = {
    "dark": ("CartoDB Dark Matter", "https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png"),
    "terrain": ("Stamen Terrain", "https://tiles.stadiamaps.com/tiles/stamen_terrain/{z}/{x}/{y}.png"),
    "openstreetmap": ("OpenStreetMap", "https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"),
    "positron": ("CartoDB Positron", "https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png"),
    "voyager": ("CartoDB Voyager", "https://basemaps.cartocdn.com/rastertiles/voyager/{z}/{x}/{y}.png"),
}


def build_map(df, name_col, count_col, output, base_tile="dark", non_interactive=False, circle_color_theme="blue", size_multiplier=1.0, opacity=0.6, corrections=None):
    geolocator = Nominatim(user_agent="institution_map_generator")
    cache = load_cache()

    records = []
    missing_institutions = []
    total = len(df)
    for i, (_, row) in enumerate(df.iterrows()):
        name = str(row[name_col]).strip()
        count = int(row[count_col])
        if not name or count <= 0:
            continue

        print(f"  [{i+1}/{total}] Geocoding: {name}...", end=" ", flush=True)
        
        loc = None
        correction = corrections.get(name) if corrections else None
        
        if correction:
            print(f" (Using correction: '{correction}') ", end="", flush=True)
            # Check if correction is a coordinate
            if re.match(r'^-?\d+(\.\d+)?\s*,\s*-?\d+(\.\d+)?$', correction):
                try:
                    lat, lng = map(float, correction.split(','))
                    loc = {"lat": lat, "lng": lng, "address": "Manual Entry"}
                    cache[name.strip().lower()] = loc
                except ValueError:
                    print("Invalid coordinates format.", end=" ")
            else:
                loc = geocode_institution(correction, geolocator, cache)
                if loc:
                    # Also associate the result with the original name
                    cache[name.strip().lower()] = loc
        else:
            loc = geocode_institution(name, geolocator, cache)
        
        if not loc and not non_interactive:
            print("NOT FOUND")
            while not loc:
                user_input = input(f"Could not find '{name}'. Provide alternative name/address, 'lat,lng', or press Enter to skip: ").strip()
                if not user_input:
                    print("  Skipping.")
                    break
                
                # Check for lat,lng
                if re.match(r'^-?\d+(\.\d+)?\s*,\s*-?\d+(\.\d+)?$', user_input):
                    try:
                        lat, lng = map(float, user_input.split(','))
                        loc = {"lat": lat, "lng": lng, "address": "Manual Entry"}
                        cache[name.strip().lower()] = loc
                        print(f"  Manual coords accepted: {lat}, {lng}")
                    except ValueError:
                        print("  Invalid coordinates format.")
                else:
                    print(f"  Searching for '{user_input}'...", end=" ", flush=True)
                    loc = geocode_institution(user_input, geolocator, cache)
                    if loc:
                        print(f"OK ({loc['lat']:.2f}, {loc['lng']:.2f})")
                        cache[name.strip().lower()] = loc  # associate original name with new loc
                    else:
                        print("NOT FOUND")
                        # remove the failed user_input from cache so it doesn't pollute it
                        cache.pop(user_input.strip().lower(), None)
                        
        elif not loc and non_interactive:
            print("NOT FOUND")
            missing_institutions.append(name)
        else:
            print(f"OK ({loc['lat']:.2f}, {loc['lng']:.2f})")
            
        if loc:
            records.append({"name": name, "count": count, **loc})

    save_cache(cache)

    if not records:
        print("Error: no institutions could be geocoded.")
        sys.exit(1)

    print(f"\nGeocoded {len(records)}/{total} institutions.")

    counts = [r["count"] for r in records]
    min_c, max_c = min(counts), max(counts)

    # Center map on mean location
    mean_lat = sum(r["lat"] for r in records) / len(records)
    mean_lng = sum(r["lng"] for r in records) / len(records)

    # Set up base tile layer
    tile_name, tile_source = TILE_PROVIDERS.get(base_tile, TILE_PROVIDERS["dark"])
    if tile_source.startswith("http"):
        m = folium.Map(location=[mean_lat, mean_lng], zoom_start=4, tiles=tile_source, attr=tile_name, name=tile_name)
    else:
        m = folium.Map(location=[mean_lat, mean_lng], zoom_start=4, tiles=tile_source)

    # Single base layer is used (controlled via React frontend)

    THEMES = {
        "blue": {"stroke": "#2563eb", "fill": "#3b82f6"},
        "red": {"stroke": "#dc2626", "fill": "#ef4444"},
        "green": {"stroke": "#16a34a", "fill": "#22c55e"},
        "orange": {"stroke": "#ea580c", "fill": "#f97316"},
        "purple": {"stroke": "#9333ea", "fill": "#a855f7"},
    }
    theme = THEMES.get(circle_color_theme, THEMES["blue"])
    circle_stroke = theme["stroke"]
    circle_fill = theme["fill"]

    for r in sorted(records, key=lambda x: x["count"], reverse=True):
        radius = calc_radius(r["count"], min_c, max_c) * size_multiplier
        popup_html = f"<b>{r['name']}</b><br>Users: {r['count']:,}<br><small>{r['address']}</small>"
        folium.CircleMarker(
            location=[r["lat"], r["lng"]],
            radius=radius,
            popup=folium.Popup(popup_html, max_width=300),
            tooltip=f"{r['name']}: {r['count']:,} users",
            color=circle_stroke,
            fill=True,
            fill_color=circle_fill,
            fill_opacity=opacity,
            weight=1.5,
        ).add_to(m)


    # Build scale legend with representative circle sizes
    scale_values = _pick_scale_values(min_c, max_c)
    circles_svg = ""
    y_offset = 30
    for val in scale_values:
        r = calc_radius(val, min_c, max_c) * size_multiplier
        circles_svg += (
            f'<div style="display:flex; align-items:center; margin:4px 0;">'
            f'<svg width="{int(r*2+4)}" height="{int(r*2+4)}" style="flex-shrink:0;">'
            f'<circle cx="{r+2}" cy="{r+2}" r="{r}" fill="{circle_fill}" fill-opacity="{opacity}" stroke="{circle_stroke}" stroke-width="1.5"/>'
            f'</svg>'
            f'<span style="margin-left:8px;">{val:,}</span></div>'
        )

    legend_html = """
    <div id="institution-map-legend" style="position:fixed; bottom:30px; left:30px; z-index:1000;
                background:rgba(255,255,255,0.92); padding:14px 18px; border-radius:8px;
                box-shadow:0 2px 8px rgba(0,0,0,0.25); font-family:sans-serif; font-size:13px;
                max-width:180px; cursor: grab; user-select: none;">
        <div id="institution-map-legend-header" style="font-weight:bold; font-size:14px; padding-bottom:4px; border-bottom:1px solid #ccc; margin-bottom:8px;" title="Drag to move">
            Unique Users
        </div>
        <div>
            {circles}
        </div>
    </div>
    <script>
        (function() {{
            var legend = document.getElementById('institution-map-legend');
            var header = document.getElementById('institution-map-legend-header');
            var isDragging = false;
            var offset = [0, 0];
            
            header.addEventListener('mousedown', function(e) {{
                isDragging = true;
                offset = [
                    legend.offsetLeft - e.clientX,
                    legend.offsetTop - e.clientY
                ];
                legend.style.cursor = 'grabbing';
                header.style.cursor = 'grabbing';
                e.stopPropagation();
            }});
            
            document.addEventListener('mouseup', function() {{
                isDragging = false;
                legend.style.cursor = 'grab';
                header.style.cursor = 'grab';
            }});
            
            document.addEventListener('mousemove', function(e) {{
                if (isDragging) {{
                    e.preventDefault();
                    legend.style.bottom = 'auto';
                    legend.style.right = 'auto';
                    legend.style.left = (e.clientX + offset[0]) + 'px';
                    legend.style.top = (e.clientY + offset[1]) + 'px';
                }}
            }});
            
            legend.addEventListener('mousedown', function(e) {{ e.stopPropagation(); }});
            legend.addEventListener('dblclick', function(e) {{ e.stopPropagation(); }});
            legend.addEventListener('wheel', function(e) {{ e.stopPropagation(); }});
        }})();
    </script>
    """.format(circles=circles_svg)
    m.get_root().html.add_child(folium.Element(legend_html))

    if output:
        m.save(output)
        print(f"Map saved to: {output}")
        return None
    else:
        # Return HTML string, missing institutions list, and successful records list
        return m.get_root().render(), missing_institutions, records


def _pick_scale_values(min_c, max_c, n=4):
    """Pick n representative values for the scale legend."""
    if min_c == max_c:
        return [min_c]
    # Use nice round numbers
    raw = [min_c + (max_c - min_c) * i / (n - 1) for i in range(n)]
    # Round to nice values
    result = []
    for v in raw:
        if v < 5:
            result.append(max(1, round(v)))
        elif v < 50:
            result.append(round(v / 5) * 5)
        elif v < 500:
            result.append(round(v / 10) * 10)
        else:
            result.append(round(v / 50) * 50)
    # Ensure first and last match actual min/max
    result[0] = min_c
    result[-1] = max_c
    # Deduplicate while preserving order
    seen = set()
    deduped = []
    for v in result:
        if v not in seen:
            seen.add(v)
            deduped.append(v)
    return deduped


def main():
    parser = argparse.ArgumentParser(description="Generate institution user map from spreadsheet")
    parser.add_argument("input", help="Input spreadsheet (CSV or Excel)")
    parser.add_argument("-o", "--output", default="institution_map.html", help="Output HTML file (default: institution_map.html)")
    parser.add_argument("--sheet", default=0, help="Excel sheet name or index (default: first sheet)")
    parser.add_argument("--name-col", help="Column name for institution names (auto-detected if omitted)")
    parser.add_argument("--count-col", help="Column name for user counts (auto-detected if omitted)")
    parser.add_argument("--no-merge", action="store_true", help="Disable merging of related institutions")
    parser.add_argument("--non-interactive", "-n", action="store_true", help="Disable interactive prompts for missing locations")
    parser.add_argument("--tiles", default="dark", choices=list(TILE_PROVIDERS.keys()),
                        help="Base map style (default: dark). Options: " + ", ".join(TILE_PROVIDERS.keys()))
    parser.add_argument("--color", default="blue", choices=["blue", "red", "green", "orange", "purple"])
    parser.add_argument("--size", type=float, default=1.0)
    parser.add_argument("--opacity", type=float, default=0.6)
    args = parser.parse_args()

    ext = os.path.splitext(args.input)[1].lower()
    if ext in (".xlsx", ".xls"):
        df = pd.read_excel(args.input, sheet_name=args.sheet)
    elif ext == ".csv":
        df = pd.read_csv(args.input)
    else:
        print(f"Error: unsupported file type '{ext}'. Use .csv, .xlsx, or .xls")
        sys.exit(1)

    print(f"Loaded {len(df)} rows from {args.input}")
    print(f"Columns: {list(df.columns)}")

    if args.name_col and args.count_col:
        name_col, count_col = args.name_col, args.count_col
    else:
        name_col, count_col = detect_columns(df)

    print(f"Using: institution='{name_col}', count='{count_col}'")

    # Drop rows with missing values in key columns
    df = df.dropna(subset=[name_col, count_col])

    if not args.no_merge:
        print("Merging related institutions...")
        df = merge_institutions(df, name_col, count_col)
        print(f"After merge: {len(df)} unique institutions")

    build_map(df, name_col, count_col, args.output, base_tile=args.tiles, non_interactive=args.non_interactive, circle_color_theme=args.color, size_multiplier=args.size, opacity=args.opacity)


if __name__ == "__main__":
    main()
