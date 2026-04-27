"""
fetch_timetable.py — Trackr timetable generator
Runs nightly via GitHub Actions.

Uses the Realtime Trains API (api.rtt.io) to fetch the next 14 days of
UK rail timetable data and saves it as timetable.json in the repo root.

Setup:
  1. Sign up free at https://api.rtt.io
  2. In your GitHub repo go to Settings → Secrets and variables → Actions
  3. Add two secrets:
       RTT_USER  = your RTT API username  (e.g. rttapi_yourname)
       RTT_PASS  = your RTT API password
  The GitHub Action passes these as environment variables automatically.

Output format — timetable.json:
{
  "generated": "2026-04-27",
  "routes": {
    "LDS-TBY-2026-04-28": [
      {
        "std": "07:15",
        "arr": "08:33",
        "operator": "TransPennine Express",
        "operatorCode": "TP",
        "serviceId": "W12345",
        "stops": [
          { "crs": "LDS", "name": "Leeds", "dep": "07:15" },
          { "crs": "YRK", "name": "York",  "dep": "07:38" },
          { "crs": "TBY", "name": "Thornaby", "arr": "08:33" }
        ]
      }, ...
    ]
  }
}
"""

import os, json, sys, time, requests
from datetime import date, timedelta

# ── CONFIG ───────────────────────────────────────────────────────────────────
RTT_USER  = os.environ.get('RTT_USER', '')
RTT_PASS  = os.environ.get('RTT_PASS', '')
DAYS_AHEAD = 14        # how many days forward to generate
MAX_RETRIES = 3        # retry on rate limit / transient error
RETRY_DELAY = 5        # seconds between retries

if not RTT_USER or not RTT_PASS:
    print("ERROR: RTT_USER and RTT_PASS environment variables must be set.")
    print("Add them as GitHub Actions secrets (Settings → Secrets → Actions).")
    sys.exit(1)

AUTH = (RTT_USER, RTT_PASS)
BASE = "https://api.rtt.io/api/v1/json"

# ── HELPERS ──────────────────────────────────────────────────────────────────
def rtt_get(path):
    """GET from RTT API with retries."""
    url = f"{BASE}{path}"
    for attempt in range(MAX_RETRIES):
        try:
            r = requests.get(url, auth=AUTH, timeout=20)
            if r.status_code == 429:
                wait = int(r.headers.get('Retry-After', RETRY_DELAY * (attempt + 1)))
                print(f"  Rate limited — waiting {wait}s")
                time.sleep(wait)
                continue
            if r.status_code == 404:
                return None
            r.raise_for_status()
            return r.json()
        except requests.RequestException as e:
            print(f"  Request error (attempt {attempt+1}): {e}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY)
    return None

def fmt_time(t):
    """Format HHmm → HH:MM, or return None."""
    if not t or len(t) < 4:
        return None
    return f"{t[:2]}:{t[2:4]}"

# ── MAIN ─────────────────────────────────────────────────────────────────────
today = date.today()
dates = [today + timedelta(days=i) for i in range(DAYS_AHEAD + 1)]
output = {}

print(f"Generating timetable for {DAYS_AHEAD} days from {today}...")

for d in dates:
    ymd = f"{d.year}/{d.month:02d}/{d.day:02d}"
    date_str = d.isoformat()
    print(f"\n── {date_str} ──")

    # Fetch all stations' departures for this date.
    # RTT search/{CRS}/{YYYY}/{MM}/{DD} returns all departures from that station.
    # We build a lookup of every trip that calls at each station.
    # To keep things efficient, we fetch per origin and index by (origin, dest).

    # We fetch a broad list of stations. For a personal app you can narrow this
    # to just the stations you care about — saves API quota.
    # Currently fetches ALL departures from every station that has had a search
    # in the app (populated from the STATIONS list at build time via a separate
    # step, or hard-coded below for common routes).

    # For now: fetch the 50 busiest UK rail stations by departure volume.
    # The GitHub Action can expand this list as needed.
    STATIONS = [
        "LDS","MAN","MCV","MCO","BHM","LIV","NCL","SHF","BRI","NOT",
        "EDB","GLC","GLQ","WAT","EUS","KGX","PAD","VIC","LST","STP",
        "MAN","LBA","HUD","WKF","YRK","TBY","MBR","SKI","HGT","SAL",
        "CDF","SWA","NWP","EXD","PLY","SOT","BOU","RDG","OXF","CBG",
        "NOR","IPS","COL","CHM","GTW","BHV","LUT","MKC","COV","WVH",
    ]

    for crs in STATIONS:
        path = f"/search/{crs}/{ymd}"
        data = rtt_get(path)
        if not data or not data.get('services'):
            continue

        for svc in data['services']:
            loc = svc.get('locationDetail', {})
            origins = svc.get('origin', [])
            dests   = svc.get('destination', [])
            if not origins or not dests:
                continue

            origin_crs = origins[0].get('tiploc', origins[0].get('crs',''))[:3].upper()
            dest_crs   = dests[-1].get('tiploc', dests[-1].get('crs',''))[:3].upper()

            dep_time = fmt_time(
                loc.get('gbttBookedDeparture') or loc.get('realtimeDeparture')
            )
            if not dep_time:
                continue

            operator     = svc.get('atocName', '')
            operator_code= svc.get('atocCode', '')
            service_uid  = svc.get('serviceUid', '')
            run_date     = svc.get('runDate', date_str)

            # Get detailed calling points via service endpoint
            detail_path = f"/service/{service_uid}/{ymd}"
            detail = rtt_get(detail_path)
            time.sleep(0.1)  # be gentle with the API

            stops = []
            if detail and detail.get('locations'):
                for loc_d in detail['locations']:
                    l_crs  = loc_d.get('crs', loc_d.get('tiploc',''))[:3].upper()
                    l_name = loc_d.get('description', l_crs)
                    l_dep  = fmt_time(
                        loc_d.get('gbttBookedDeparture') or
                        loc_d.get('realtimeDeparture')
                    )
                    l_arr  = fmt_time(
                        loc_d.get('gbttBookedArrival') or
                        loc_d.get('realtimeArrival')
                    )
                    if l_crs:
                        stops.append({
                            'crs':  l_crs,
                            'name': l_name,
                            'dep':  l_dep,
                            'arr':  l_arr,
                        })

            if not stops:
                # Fallback: just origin → this station → destination
                stops = [
                    {'crs': origin_crs, 'name': origin_crs, 'dep': None, 'arr': None},
                    {'crs': crs,        'name': crs,        'dep': dep_time, 'arr': dep_time},
                ]

            # Index every (from_stop → to_stop) pair in this trip
            for i, from_st in enumerate(stops):
                if not from_st['crs']:
                    continue
                for to_st in stops[i+1:]:
                    if not to_st['crs']:
                        continue
                    key = f"{from_st['crs']}-{to_st['crs']}-{date_str}"
                    entry = {
                        'std':          from_st.get('dep') or dep_time,
                        'arr':          to_st.get('arr') or to_st.get('dep'),
                        'operator':     operator,
                        'operatorCode': operator_code,
                        'serviceId':    service_uid,
                        'stops':        stops[i:stops.index(to_st)+1],
                    }
                    if key not in output:
                        output[key] = []
                    # Deduplicate by serviceId
                    if not any(e['serviceId'] == service_uid for e in output[key]):
                        output[key].append(entry)

        print(f"  {crs}: done")

# Sort each route's services by departure time
for key in output:
    output[key].sort(key=lambda x: x.get('std') or '')

result = {
    'generated':  today.isoformat(),
    'days_ahead': DAYS_AHEAD,
    'routes':     output,
}

with open('timetable.json', 'w') as f:
    json.dump(result, f, separators=(',', ':'))

total = sum(len(v) for v in output.values())
size  = os.path.getsize('timetable.json') / 1024
print(f"\nDone. {len(output)} route-date pairs, {total} services. {size:.0f} KB")
