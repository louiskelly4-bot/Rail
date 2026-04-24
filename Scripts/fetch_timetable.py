"""
fetch_timetable.py
Runs nightly via GitHub Actions.
Downloads Network Rail's GTFS timetable (free, no API key needed),
extracts the next 14 days of UK rail departures, and saves a compact
timetable.json that the Trackr app reads for date searches.

The output is a dict keyed by "FROM-TO-YYYY-MM-DD":
{
  "LDS-TBY-2026-05-01": [
    { "std": "07:15", "arr": "08:33", "operator": "TransPennine Express",
      "operatorCode": "TP", "serviceId": "...", "stops": [
        { "crs": "LDS", "name": "Leeds", "dep": "07:15" },
        { "crs": "YRK", "name": "York", "dep": "07:38" },
        ...
        { "crs": "TBY", "name": "Thornaby", "arr": "08:33" }
      ]
    }, ...
  ],
  ...
}
"""

import requests, zipfile, io, csv, json, os
from datetime import date, timedelta
from collections import defaultdict

# ── CONFIG ──────────────────────────────────────────────────────────────
# GTFS feed from BODS (Bus Open Data Service) / Traveline — covers full GB rail
GTFS_URL = "https://data.bus-data.dft.gov.uk/timetable/download/gtfs-file/rail/"

# Fallback: Network Rail's open GTFS (requires free registration).
# If the above URL stops working, swap to:
# GTFS_URL = "https://opendata.nationalrail.co.uk/api/staticfeeds/4.0/timetable"

# How many days ahead to generate timetable for
DAYS_AHEAD = 14

# ── HELPERS ─────────────────────────────────────────────────────────────
def mins_to_hhmm(mins):
    """Convert minutes-since-midnight to HH:MM string."""
    if mins is None:
        return None
    m = int(mins) % 1440  # handle times past midnight (>1440)
    return f"{m // 60:02d}:{m % 60:02d}"

def hhmm_to_mins(t):
    """Convert HH:MM:SS or HH:MM string to minutes since midnight."""
    if not t:
        return None
    parts = t.split(":")
    return int(parts[0]) * 60 + int(parts[1])

# ── DOWNLOAD GTFS ────────────────────────────────────────────────────────
print("Downloading GTFS feed...")
try:
    resp = requests.get(GTFS_URL, timeout=120, stream=True)
    resp.raise_for_status()
    zf = zipfile.ZipFile(io.BytesIO(resp.content))
    print(f"GTFS downloaded, files: {zf.namelist()}")
except Exception as e:
    print(f"GTFS download failed: {e}")
    # Write empty timetable so app doesn't break
    with open("timetable.json", "w") as f:
        json.dump({"generated": date.today().isoformat(), "routes": {}}, f)
    raise SystemExit(0)

# ── PARSE ────────────────────────────────────────────────────────────────
print("Parsing GTFS...")

# Load stops (station lookup: stop_id → {name, crs})
stops = {}  # stop_id → {name, crs}
with zf.open("stops.txt") as f:
    for row in csv.DictReader(io.TextIOWrapper(f)):
        crs = row.get("stop_code", "").upper().strip()
        stops[row["stop_id"]] = {
            "name": row.get("stop_name", ""),
            "crs": crs if len(crs) == 3 else None,
        }

# Load routes (route_id → operator info)
routes = {}  # route_id → {operator, operatorCode}
with zf.open("routes.txt") as f:
    for row in csv.DictReader(io.TextIOWrapper(f)):
        routes[row["route_id"]] = {
            "operator": row.get("agency_id", ""),
            "name": row.get("route_long_name", "") or row.get("route_short_name", ""),
        }

# Load agency names
agencies = {}
try:
    with zf.open("agency.txt") as f:
        for row in csv.DictReader(io.TextIOWrapper(f)):
            agencies[row["agency_id"]] = row.get("agency_name", row["agency_id"])
except:
    pass

# Load calendar — which service_ids run on which days
today = date.today()
date_range = [today + timedelta(days=i) for i in range(DAYS_AHEAD + 1)]

# service_id → set of date strings "YYYY-MM-DD"
service_dates = defaultdict(set)

# calendar.txt: service_id, monday..sunday, start_date, end_date
day_cols = ["monday","tuesday","wednesday","thursday","friday","saturday","sunday"]
try:
    with zf.open("calendar.txt") as f:
        for row in csv.DictReader(io.TextIOWrapper(f)):
            start = date.fromisoformat(row["start_date"])
            end   = date.fromisoformat(row["end_date"])
            for d in date_range:
                if start <= d <= end:
                    day_name = day_cols[d.weekday()]
                    if row.get(day_name) == "1":
                        service_dates[row["service_id"]].add(d.isoformat())
except Exception as e:
    print(f"calendar.txt: {e}")

# calendar_dates.txt: exceptions (added/removed dates)
try:
    with zf.open("calendar_dates.txt") as f:
        for row in csv.DictReader(io.TextIOWrapper(f)):
            d = row["date"]
            sid = row["service_id"]
            if row["exception_type"] == "1":
                service_dates[sid].add(d)
            elif row["exception_type"] == "2":
                service_dates[sid].discard(d)
except Exception as e:
    print(f"calendar_dates.txt: {e}")

# Load trips: trip_id → {service_id, route_id, trip_headsign}
trips = {}
with zf.open("trips.txt") as f:
    for row in csv.DictReader(io.TextIOWrapper(f)):
        trips[row["trip_id"]] = {
            "service_id": row["service_id"],
            "route_id": row.get("route_id",""),
            "headsign": row.get("trip_headsign",""),
            "uid": row.get("block_id","") or row["trip_id"],
        }

# Load stop_times: build trip → ordered list of stops
print("Loading stop times (this may take a moment)...")
trip_stops = defaultdict(list)  # trip_id → [{stop_id, arr_mins, dep_mins, seq}]
with zf.open("stop_times.txt") as f:
    for row in csv.DictReader(io.TextIOWrapper(f)):
        trip_stops[row["trip_id"]].append({
            "stop_id": row["stop_id"],
            "arr": hhmm_to_mins(row.get("arrival_time","")),
            "dep": hhmm_to_mins(row.get("departure_time","")),
            "seq": int(row.get("stop_sequence", 0)),
        })

# Sort each trip's stops by sequence
for tid in trip_stops:
    trip_stops[tid].sort(key=lambda x: x["seq"])

# ── BUILD OUTPUT ─────────────────────────────────────────────────────────
print("Building timetable index...")

output = {}  # "FROM-TO-DATE" → [service, ...]

for trip_id, stop_list in trip_stops.items():
    trip = trips.get(trip_id)
    if not trip:
        continue

    svc_id = trip["service_id"]
    running_dates = service_dates.get(svc_id, set())
    if not running_dates:
        continue

    # Get CRS codes for all stops in this trip
    crs_stops = []
    for s in stop_list:
        st = stops.get(s["stop_id"])
        if st and st["crs"]:
            crs_stops.append({
                "crs": st["crs"],
                "name": st["name"],
                "dep": s["dep"],
                "arr": s["arr"],
            })

    if len(crs_stops) < 2:
        continue

    # For each pair of stations in this trip, index the service
    crs_list = [s["crs"] for s in crs_stops]
    route_info = routes.get(trip["route_id"], {})
    operator_id = route_info.get("operator","")
    operator_name = agencies.get(operator_id, operator_id)

    # Only index if both from and to are in the stop list (in order)
    for i, from_stop in enumerate(crs_stops):
        for to_stop in crs_stops[i+1:]:
            key_base = f"{from_stop['crs']}-{to_stop['crs']}"
            dep_time = mins_to_hhmm(from_stop["dep"])
            arr_time = mins_to_hhmm(to_stop["arr"] or to_stop["dep"])
            # Calling points between from and to
            from_idx = crs_stops.index(from_stop)
            to_idx   = crs_stops.index(to_stop)
            calling = [{
                "crs": s["crs"],
                "name": s["name"],
                "dep": mins_to_hhmm(s["dep"]),
                "arr": mins_to_hhmm(s["arr"]),
            } for s in crs_stops[from_idx:to_idx+1]]

            svc_entry = {
                "std": dep_time,
                "arr": arr_time,
                "operator": operator_name,
                "operatorCode": operator_id[:2].upper() if operator_id else "",
                "serviceId": trip["uid"] or trip_id,
                "stops": calling,
            }

            for d in running_dates:
                key = f"{key_base}-{d}"
                if key not in output:
                    output[key] = []
                output[key].append(svc_entry)

# Sort each entry's services by departure time
for key in output:
    output[key].sort(key=lambda x: x["std"] or "")

result = {
    "generated": date.today().isoformat(),
    "days_ahead": DAYS_AHEAD,
    "routes": output,
}

with open("timetable.json", "w") as f:
    json.dump(result, f, separators=(",", ":"))

total_entries = sum(len(v) for v in output.values())
print(f"Done. {len(output)} route-date pairs, {total_entries} total services.")
print(f"timetable.json written ({os.path.getsize('timetable.json') / 1024:.0f} KB)")
