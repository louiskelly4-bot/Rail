"""
fetch_timetable.py — Trackr timetable generator
Runs nightly via GitHub Actions (free, no API key needed).

Uses Network Rail's open schedule data feed in JSON format via the
OpenTrainTimes mirror (networkrail.opendata.opentraintimes.com).
No registration required. Generates timetable.json for the next 14 days.

The JSON schedule format from Network Rail contains:
  - BS (Basic Schedule) records: one per train service
  - LO (Location Origin): origin stop
  - LI (Location Intermediate): intermediate stops  
  - LT (Location Terminus): final stop

Each stop has a TIPLOC code (not CRS). We map TIPLOC → CRS using the
master station names file (also freely available).

Output timetable.json format matches what the Trackr app expects:
{
  "generated": "2026-05-01",
  "routes": {
    "LDS-TBY-2026-05-02": [
      {
        "std": "07:15", "arr": "08:33",
        "operator": "TransPennine Express", "operatorCode": "TP",
        "serviceId": "W12345",
        "stops": [
          {"crs": "LDS", "name": "Leeds", "dep": "07:15"},
          {"crs": "YRK", "name": "York",  "dep": "07:38"},
          {"crs": "TBY", "name": "Thornaby", "arr": "08:33"}
        ]
      }
    ]
  }
}
"""

import gzip, io, json, os, re, sys, time, urllib.request, zipfile
from collections import defaultdict
from datetime import date, timedelta

# ── CONFIG ───────────────────────────────────────────────────────────────────
DAYS_AHEAD  = 14

# Network Rail open data — full daily JSON schedule (all TOCs).
# Via OpenTrainTimes mirror — no auth needed.
SCHEDULE_URL = "https://networkrail.opendata.opentraintimes.com/mirror/schedule/cif_all_full_daily/toc-full.json.gz"

# TIPLOC → CRS mapping from Network Rail master stations file (free, no auth)
# This maps the 7-char TIPLOC codes in the schedule to 3-char CRS station codes
TIPLOC_URL = "https://networkrail.opendata.opentraintimes.com/mirror/msn/toc-all.msn"

# ATOC code → operator name
ATOC_NAMES = {
    'AW': 'Transport for Wales', 'CC': 'c2c', 'CH': 'Chiltern Railways',
    'CS': 'Caledonian Sleeper', 'EM': 'East Midlands Railway',
    'ES': 'Eurostar', 'GA': 'Greater Anglia', 'GC': 'Grand Central',
    'GN': 'Great Northern', 'GR': 'LNER', 'GW': 'Great Western Railway',
    'GX': 'Gatwick Express', 'HT': 'Hull Trains', 'HX': 'Heathrow Express',
    'IL': 'Island Line', 'LD': 'Lumo', 'LE': 'Greater Anglia',
    'LM': 'West Midlands Trains', 'LN': 'LNER', 'LO': 'London Overground',
    'ME': 'Merseyrail', 'MX': 'Grand Central', 'NT': 'Northern Trains',
    'NY': 'North Yorkshire Moors Railway', 'SE': 'Southeastern',
    'SJ': 'South Western Railway', 'SN': 'Southern', 'SR': 'ScotRail',
    'SW': 'South Western Railway', 'TL': 'Thameslink', 'TP': 'TransPennine Express',
    'TW': 'Transport for Wales', 'VT': 'Avanti West Coast',
    'WM': 'West Midlands Trains', 'XC': 'CrossCountry', 'XR': 'Elizabeth line',
    'ZZ': 'Network Rail',
}

# ── HELPERS ───────────────────────────────────────────────────────────────────
def fetch_url(url, label=""):
    print(f"Downloading {label or url}...")
    req = urllib.request.Request(url, headers={"User-Agent": "Trackr/1.0 (github-actions)"})
    with urllib.request.urlopen(req, timeout=120) as r:
        return r.read()

def fmt_time(t):
    """Convert HHMM or HHMMSS → HH:MM, handling H (half-minute suffix)."""
    if not t or t.strip() == '':
        return None
    t = t.strip().rstrip('H').rstrip('h')
    t = t.zfill(4)[:4]
    if not t.isdigit():
        return None
    h, m = int(t[:2]), int(t[2:4])
    if h > 47 or m > 59:  # overnight trains can exceed 24:00
        h = h % 24
    return f"{h:02d}:{m:02d}"

def date_in_range(start_str, end_str, days_of_week, target_date):
    """Check if target_date falls within schedule validity and day-of-week mask."""
    try:
        start = date(int(start_str[:4]), int(start_str[4:6]), int(start_str[6:8]))
        end   = date(int(end_str[:4]),   int(end_str[4:6]),   int(end_str[6:8]))
    except (ValueError, IndexError):
        return False
    if not (start <= target_date <= end):
        return False
    # days_of_week is "YYYYYYY" for Mon-Sun, Y=runs, N=doesn't
    day_idx = target_date.weekday()  # 0=Mon, 6=Sun
    return len(days_of_week) > day_idx and days_of_week[day_idx] == '1'

# ── LOAD TIPLOC → CRS MAP ─────────────────────────────────────────────────────
print("Loading TIPLOC → CRS map...")
tiploc_to_crs  = {}  # TIPLOC → CRS code
tiploc_to_name = {}  # TIPLOC → station name

try:
    msn_data = fetch_url(TIPLOC_URL, "master stations file")
    for line in msn_data.decode('latin-1').splitlines():
        if line.startswith('A') and len(line) >= 36:
            # MSN format: A + name (26 chars) + spaces + TIPLOC (7) + ... + CRS (3)
            name   = line[1:27].strip().title()
            tiploc = line[36:43].strip()
            crs    = line[49:52].strip()
            if tiploc and crs and len(crs) == 3 and crs.isalpha():
                tiploc_to_crs[tiploc]  = crs.upper()
                tiploc_to_name[tiploc] = name
    print(f"  Loaded {len(tiploc_to_crs)} TIPLOC → CRS mappings")
except Exception as e:
    print(f"  Warning: Could not load TIPLOC map: {e}")
    print("  Falling back to TIPLOC as station code")

# ── LOAD SCHEDULE JSON ────────────────────────────────────────────────────────
print("Loading full schedule JSON (this is large — may take a minute)...")
try:
    raw = fetch_url(SCHEDULE_URL, "Network Rail full schedule")
    data = json.loads(gzip.decompress(raw).decode('utf-8'))
except Exception as e:
    print(f"ERROR downloading schedule: {e}")
    with open("timetable.json", "w") as f:
        json.dump({"generated": date.today().isoformat(), "routes": {}, "error": str(e)}, f)
    sys.exit(0)

print(f"Schedule loaded. Processing...")

# ── ROUTE FILTER ─────────────────────────────────────────────────────────────
# Only index routes between these station pairs to keep timetable.json small.
# Add any CRS pairs you care about. Both directions are indexed automatically.
# The more pairs you add, the larger timetable.json gets (~2KB per route-day).
ROUTE_PAIRS = {
    # Yorkshire / North East
    ("LDS", "TBY"), ("LDS", "MBR"), ("LDS", "NCL"), ("LDS", "YRK"),
    ("LDS", "HUD"), ("LDS", "MAN"), ("LDS", "MCV"), ("LDS", "SHF"),
    ("LDS", "BHM"), ("LDS", "EUS"), ("LDS", "KGX"), ("LDS", "PAD"),
    ("LDS", "WAT"), ("LDS", "VIC"), ("LDS", "LST"),
    # Manchester
    ("MAN", "LDS"), ("MAN", "NCL"), ("MAN", "BHM"), ("MAN", "EUS"),
    ("MAN", "PAD"), ("MAN", "LIV"), ("MAN", "SHF"),
    # London terminals
    ("EUS", "MAN"), ("EUS", "LIV"), ("EUS", "BHM"), ("EUS", "GLC"),
    ("KGX", "NCL"), ("KGX", "EDB"), ("KGX", "YRK"), ("KGX", "LDS"),
    ("PAD", "BRI"), ("PAD", "CDF"), ("PAD", "EXD"),
    ("VIC", "GTW"), ("VIC", "BHV"),
    ("WAT", "SOT"), ("WAT", "BOU"),
    # Scotland
    ("EDB", "GLC"), ("EDB", "GLQ"), ("GLC", "EDB"),
    # Cross-country
    ("NCL", "BHM"), ("NCL", "EDB"), ("BHM", "BRI"), ("BHM", "NOT"),
    # Thornaby connections
    ("TBY", "LDS"), ("TBY", "NCL"), ("TBY", "MBR"), ("TBY", "YRK"),
}

# Build a set of (fc, tc) pairs including both directions
INDEXED_PAIRS = set()
for a, b in ROUTE_PAIRS:
    INDEXED_PAIRS.add((a, b))
    INDEXED_PAIRS.add((b, a))

print(f"Indexing {len(INDEXED_PAIRS)} route directions across {DAYS_AHEAD} days...")

today      = date.today()
date_range = [today + timedelta(days=i) for i in range(1, DAYS_AHEAD + 1)]
output     = defaultdict(list)
seen_keys  = defaultdict(set)  # (from_crs, to_crs, date, service_id) dedup

# ── PARSE SCHEDULE ────────────────────────────────────────────────────────────
# The JSON schedule has a top-level "JsonScheduleV1" array and other record types
schedules = data.get("JsonScheduleV1", [])
print(f"Processing {len(schedules):,} schedule records...")

processed = 0
skipped   = 0

for record in schedules:
    sched = record.get("JsonScheduleV1", {})
    if not sched:
        continue

    # Only passenger trains
    train_category = sched.get("train_category", "")
    if train_category not in ("OO", "XX", "XZ", "OW", "XE", "XI", "XR"):
        # OO = ordinary passenger, XX = express passenger, etc.
        pass  # include all for now, filter by category if needed

    transaction = sched.get("transaction_type", "")
    if transaction == "Delete":
        continue

    sched_segment = sched.get("schedule_segment", {})
    locations     = sched_segment.get("schedule_location", [])
    if not locations or len(locations) < 2:
        skipped += 1
        continue

    # Service metadata
    uid         = sched.get("CIF_train_uid", "").strip()
    start_date  = sched.get("schedule_start_date", "").replace("-", "")
    end_date    = sched.get("schedule_end_date",   "").replace("-", "")
    days_mask   = sched.get("schedule_days_runs",  "0000000")
    atoc        = sched_segment.get("CIF_train_category", "")
    atoc_code   = sched.get("atoc_code", "").strip()
    operator    = ATOC_NAMES.get(atoc_code, atoc_code)
    stp         = sched.get("CIF_stp_indicator", "P")  # P=permanent, O=overlay, C=cancel, N=new

    if stp == "C":  # planned cancellation
        continue

    # Build stop list with CRS codes
    stops = []
    for loc in locations:
        tiploc  = loc.get("tiploc_code", "").strip()
        dep_raw = loc.get("departure", "") or loc.get("public_departure", "")
        arr_raw = loc.get("arrival",   "") or loc.get("public_arrival",   "")
        pass_raw= loc.get("pass", "")  # passing time — not a calling point

        crs  = tiploc_to_crs.get(tiploc,  tiploc[:3].upper() if tiploc else None)
        name = tiploc_to_name.get(tiploc, tiploc)

        dep = fmt_time(dep_raw)
        arr = fmt_time(arr_raw)

        # Skip passing locations (no public stop)
        if not dep and not arr:
            continue
        if not crs:
            continue

        stops.append({"crs": crs, "name": name, "dep": dep, "arr": arr})

    if len(stops) < 2:
        skipped += 1
        continue

    # For each target date, check if this service runs
    for target_date in date_range:
        if not date_in_range(start_date, end_date, days_mask, target_date):
            continue

        date_str = target_date.isoformat()

        # Index every (origin → destination) pair of calling points
        for i, from_stop in enumerate(stops):
            if not from_stop["dep"]:  # must have a departure
                continue
            for to_stop in stops[i+1:]:
                fc = from_stop["crs"]
                tc = to_stop["crs"]
                if fc == tc:
                    continue

                # Only index pre-defined route pairs to keep file size manageable
                if (fc, tc) not in INDEXED_PAIRS:
                    continue

                dedup_key = (fc, tc, date_str, uid)
                if dedup_key in seen_keys[date_str]:
                    continue
                seen_keys[date_str].add(dedup_key)

                route_key = f"{fc}-{tc}-{date_str}"
                output[route_key].append({
                    "std":          from_stop["dep"],
                    "arr":          to_stop["arr"] or to_stop["dep"],
                    "operator":     operator,
                    "operatorCode": atoc_code,
                    "serviceId":    uid,
                    "stops":        stops[i:stops.index(to_stop) + 1],
                })

    processed += 1
    if processed % 10000 == 0:
        print(f"  {processed:,} processed, {len(output):,} route-date pairs so far...")

# Sort each route by departure time
for key in output:
    output[key].sort(key=lambda x: x.get("std") or "")

result = {
    "generated":  today.isoformat(),
    "days_ahead": DAYS_AHEAD,
    "routes":     dict(output),
}

with open("timetable.json", "w") as f:
    json.dump(result, f, separators=(",", ":"))

total = sum(len(v) for v in output.values())
size  = os.path.getsize("timetable.json") / (1024 * 1024)
print(f"\nDone.")
print(f"  {processed:,} schedules processed, {skipped:,} skipped")
print(f"  {len(output):,} route-date pairs, {total:,} total services")
print(f"  timetable.json: {size:.1f} MB")
