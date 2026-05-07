"""
fetch_timetable.py — Trackr timetable generator
Runs nightly via GitHub Actions.

Data source: National Rail Data Portal (opendata.nationalrail.co.uk)
  - Free registration required
  - Subscribe to the "Timetable" feed after registering
  - Add credentials as GitHub secrets: NR_USER and NR_PASS

Auth flow:
  1. POST to /authenticate → get token
  2. GET /api/staticfeeds/3.0/timetable with X-Auth-Token header → zip file
  3. Unzip → parse CIF format → output timetable.json
"""

import io, json, os, sys, urllib.request, urllib.parse, zipfile
from collections import defaultdict
from datetime import date, timedelta

# ── CREDENTIALS ───────────────────────────────────────────────────────────────
NR_USER = os.environ.get('NR_USER', '')
NR_PASS = os.environ.get('NR_PASS', '')

if not NR_USER or not NR_PASS:
    print("ERROR: NR_USER and NR_PASS secrets not set.")
    print("Register at https://opendata.nationalrail.co.uk/ and subscribe to Timetable feed.")
    sys.exit(1)

BASE     = "https://opendata.nationalrail.co.uk"
DAYS_AHEAD = 14

ATOC_NAMES = {
    'AW': 'Transport for Wales', 'CC': 'c2c', 'CH': 'Chiltern Railways',
    'CS': 'Caledonian Sleeper', 'EM': 'East Midlands Railway',
    'GA': 'Greater Anglia', 'GC': 'Grand Central', 'GN': 'Great Northern',
    'GR': 'LNER', 'GW': 'Great Western Railway', 'GX': 'Gatwick Express',
    'HT': 'Hull Trains', 'HX': 'Heathrow Express', 'IL': 'Island Line',
    'LD': 'Lumo', 'LE': 'Greater Anglia', 'LM': 'West Midlands Trains',
    'LN': 'LNER', 'LO': 'London Overground', 'ME': 'Merseyrail',
    'NT': 'Northern Trains', 'SE': 'Southeastern', 'SN': 'Southern',
    'SR': 'ScotRail', 'SW': 'South Western Railway', 'TL': 'Thameslink',
    'TP': 'TransPennine Express', 'TW': 'Transport for Wales',
    'VT': 'Avanti West Coast', 'WM': 'West Midlands Trains',
    'XC': 'CrossCountry', 'XR': 'Elizabeth line',
}

ROUTE_PAIRS = {
    ("LDS","TBY"),("LDS","MBR"),("LDS","NCL"),("LDS","YRK"),("LDS","HUD"),
    ("LDS","MAN"),("LDS","MCV"),("LDS","SHF"),("LDS","BHM"),("LDS","EUS"),
    ("LDS","KGX"),("LDS","PAD"),("LDS","WAT"),("LDS","VIC"),("LDS","LST"),
    ("MAN","LDS"),("MAN","NCL"),("MAN","BHM"),("MAN","EUS"),("MAN","PAD"),
    ("MAN","LIV"),("MAN","SHF"),("EUS","MAN"),("EUS","LIV"),("EUS","BHM"),
    ("EUS","GLC"),("KGX","NCL"),("KGX","EDB"),("KGX","YRK"),("KGX","LDS"),
    ("PAD","BRI"),("PAD","CDF"),("PAD","EXD"),("VIC","GTW"),("WAT","SOT"),
    ("NCL","BHM"),("NCL","EDB"),("BHM","BRI"),("BHM","NOT"),
    ("TBY","LDS"),("TBY","NCL"),("TBY","MBR"),("TBY","YRK"),
    ("EDB","GLC"),("EDB","GLQ"),("GLC","EDB"),
}
INDEXED_PAIRS = set()
for a, b in ROUTE_PAIRS:
    INDEXED_PAIRS.add((a, b))
    INDEXED_PAIRS.add((b, a))

# ── HELPERS ───────────────────────────────────────────────────────────────────
def fmt_time(t):
    """Convert HHMM or HHMMSS to HH:MM, strip trailing H (half minute)."""
    if not t:
        return None
    t = str(t).strip().rstrip('Hh').zfill(4)[:4]
    if not t.isdigit():
        return None
    return f"{int(t[:2]) % 24:02d}:{t[2:4]}"

def parse_yymmdd(s):
    """Parse YYMMDD string to date object."""
    if not s or len(s) < 6:
        return None
    try:
        y = int(s[0:2])
        y += 2000 if y < 60 else 1900
        return date(y, int(s[2:4]), int(s[4:6]))
    except ValueError:
        return None

def runs_on(days_str, target_date):
    """days_str is 'NYYYYNN' Mon-Sun. Returns True if service runs on target_date."""
    if not days_str or len(days_str) < 7:
        return False
    return days_str[target_date.weekday()] == '1'

# ── STEP 1: AUTHENTICATE ─────────────────────────────────────────────────────
print("Authenticating with National Rail Data Portal...")
auth_data = urllib.parse.urlencode({'username': NR_USER, 'password': NR_PASS}).encode()
auth_req  = urllib.request.Request(
    f"{BASE}/authenticate",
    data=auth_data,
    headers={"Content-Type": "application/x-www-form-urlencoded",
             "User-Agent": "Trackr/1.3 github-actions"},
    method="POST"
)
try:
    with urllib.request.urlopen(auth_req, timeout=30) as r:
        auth_resp = json.loads(r.read())
    token = auth_resp.get('token', '')
    if not token:
        print(f"ERROR: No token in response: {auth_resp}")
        sys.exit(1)
    print(f"Authenticated. Roles: {list(auth_resp.get('roles', {}).keys())}")
except Exception as e:
    print(f"ERROR authenticating: {e}")
    sys.exit(1)

# ── STEP 2: DOWNLOAD TIMETABLE ZIP ───────────────────────────────────────────
print("Downloading timetable zip...")
tt_req = urllib.request.Request(
    f"{BASE}/api/staticfeeds/3.0/timetable",
    headers={"X-Auth-Token": token, "User-Agent": "Trackr/1.3 github-actions"}
)
try:
    with urllib.request.urlopen(tt_req, timeout=300) as r:
        zip_data = r.read()
    print(f"Downloaded {len(zip_data) / 1024 / 1024:.1f} MB")
except Exception as e:
    print(f"ERROR downloading timetable: {e}")
    sys.exit(1)

# ── STEP 3: UNZIP AND FIND CIF FILE ──────────────────────────────────────────
print("Unzipping...")
try:
    zf = zipfile.ZipFile(io.BytesIO(zip_data))
    print(f"Files in zip: {zf.namelist()}")
    # Find the .MCA file (main timetable) and .MSN file (station names)
    mca_name = next((n for n in zf.namelist() if n.upper().endswith('.MCA')), None)
    msn_name = next((n for n in zf.namelist() if n.upper().endswith('.MSN')), None)
    if not mca_name:
        print("ERROR: No .MCA file found in zip")
        sys.exit(1)
    print(f"Timetable file: {mca_name}")
except Exception as e:
    print(f"ERROR unzipping: {e}")
    sys.exit(1)

# ── STEP 4: PARSE MSN (station names → TIPLOC → CRS) ─────────────────────────
tiploc_crs  = {}
tiploc_name = {}
if msn_name:
    print("Parsing station names...")
    with zf.open(msn_name) as f:
        for line in io.TextIOWrapper(f, encoding='latin-1'):
            if line.startswith('A') and len(line) >= 52:
                name   = line[1:27].strip().title()
                tiploc = line[36:43].strip()
                crs    = line[49:52].strip().upper()
                if tiploc and crs and len(crs) == 3 and crs.isalpha():
                    tiploc_crs[tiploc]  = crs
                    tiploc_name[tiploc] = name
    print(f"Loaded {len(tiploc_crs)} TIPLOC→CRS mappings")

# ── STEP 5: PARSE CIF (.MCA) ─────────────────────────────────────────────────
print("Parsing timetable CIF data...")
today      = date.today()
date_range = [today + timedelta(days=i) for i in range(1, DAYS_AHEAD + 1)]
output     = defaultdict(list)
seen       = defaultdict(set)

# CIF state machine: accumulate records per train
current = None  # dict with keys: uid, atoc, start, end, days, stops

def flush(current, date_range, output, seen):
    """Index a completed train service into output."""
    if not current or len(current['stops']) < 2:
        return
    stops = current['stops']
    atoc  = current['atoc']
    uid   = current['uid']

    for target_date in date_range:
        s_date = current['start']
        e_date = current['end']
        if not s_date or not e_date:
            continue
        if not (s_date <= target_date <= e_date):
            continue
        if not runs_on(current['days'], target_date):
            continue
        date_str = target_date.isoformat()

        for i, from_st in enumerate(stops):
            if not from_st['dep']:
                continue
            fc = from_st['crs']
            for to_st in stops[i+1:]:
                tc = to_st['crs']
                if (fc, tc) not in INDEXED_PAIRS:
                    continue
                key   = f"{fc}-{tc}-{date_str}"
                dedup = (fc, tc, uid)
                if dedup in seen[date_str]:
                    continue
                seen[date_str].add(dedup)
                j = stops.index(to_st)
                output[key].append({
                    'std':          from_st['dep'],
                    'arr':          to_st['arr'] or to_st['dep'],
                    'operator':     ATOC_NAMES.get(atoc, atoc),
                    'operatorCode': atoc,
                    'serviceId':    uid,
                    'stops':        stops[i:j+1],
                })

line_count = 0
with zf.open(mca_name) as f:
    for raw_line in io.TextIOWrapper(f, encoding='latin-1'):
        line = raw_line.rstrip('\n')
        if len(line) < 2:
            continue
        rec = line[:2]
        line_count += 1

        if rec == 'BS':  # Basic Schedule — new train
            flush(current, date_range, output, seen)
            stp = line[79] if len(line) > 79 else 'P'
            if stp == 'C':
                current = None
                continue
            current = {
                'uid':   line[3:9].strip(),
                'start': parse_yymmdd(line[9:15]),
                'end':   parse_yymmdd(line[15:21]),
                'days':  line[21:28],
                'atoc':  '',
                'stops': [],
            }

        elif rec == 'BX' and current:  # Extra schedule info — has ATOC code
            current['atoc'] = line[25:27].strip()

        elif rec == 'TI':  # TIPLOC insert — supplement our map
            tip  = line[2:9].strip()
            crs  = line[53:56].strip().upper()
            name = line[56:72].strip().title()
            if tip and crs and len(crs) == 3 and crs.isalpha():
                tiploc_crs.setdefault(tip, crs)
                tiploc_name.setdefault(tip, name)

        elif rec == 'LO' and current:  # Location Origin
            tip = line[2:9].strip()
            crs = tiploc_crs.get(tip, tip[:3].upper() if len(tip) >= 3 else None)
            dep = fmt_time(line[15:19])
            if crs:
                current['stops'].append({
                    'crs': crs, 'name': tiploc_name.get(tip, crs),
                    'dep': dep, 'arr': None,
                })

        elif rec == 'LI' and current:  # Location Intermediate
            tip = line[2:9].strip()
            crs = tiploc_crs.get(tip, tip[:3].upper() if len(tip) >= 3 else None)
            arr = fmt_time(line[10:14])
            dep = fmt_time(line[15:19]) or fmt_time(line[20:24])  # dep or pass
            # Only include if it's a public calling point (arr or dep, not just pass)
            pub_arr = line[25:29].strip()
            pub_dep = line[29:33].strip()
            if crs and (pub_arr or pub_dep):
                current['stops'].append({
                    'crs': crs, 'name': tiploc_name.get(tip, crs),
                    'dep': fmt_time(pub_dep) or dep,
                    'arr': fmt_time(pub_arr) or arr,
                })

        elif rec == 'LT' and current:  # Location Terminus
            tip = line[2:9].strip()
            crs = tiploc_crs.get(tip, tip[:3].upper() if len(tip) >= 3 else None)
            arr = fmt_time(line[10:14])
            if crs:
                current['stops'].append({
                    'crs': crs, 'name': tiploc_name.get(tip, crs),
                    'dep': None, 'arr': arr,
                })
            flush(current, date_range, output, seen)
            current = None

        if line_count % 500000 == 0:
            print(f"  {line_count:,} lines, {len(output):,} route-date pairs...")

flush(current, date_range, output, seen)
print(f"Parsed {line_count:,} CIF lines")

# ── STEP 6: WRITE OUTPUT ─────────────────────────────────────────────────────
for key in output:
    output[key].sort(key=lambda x: x.get('std') or '')

result = {
    'generated':  today.isoformat(),
    'days_ahead': DAYS_AHEAD,
    'routes':     dict(output),
}

with open('timetable.json', 'w') as f:
    json.dump(result, f, separators=(',', ':'))

total = sum(len(v) for v in output.values())
size  = os.path.getsize('timetable.json') / 1024
print(f"\nDone. {len(output):,} route-date pairs, {total:,} services. {size:.0f} KB")
