"""
fetch_timetable.py — Trackr timetable + logos generator
Runs nightly via GitHub Actions.

1. Downloads operator logos from Wikipedia and saves to logos/ folder
2. Downloads National Rail timetable CIF data
3. Parses and outputs timetable.json

Setup:
  - Register at https://opendata.nationalrail.co.uk/
  - Add NR_USER and NR_PASS as GitHub Actions secrets
  - Subscribe to the Timetable feed in your NR account
"""

import gzip, io, json, os, sys, time, urllib.request, urllib.parse, zipfile
from collections import defaultdict
from datetime import date, timedelta

# ── CREDENTIALS ───────────────────────────────────────────────────────────────
NR_USER = os.environ.get('NR_USER', '')
NR_PASS = os.environ.get('NR_PASS', '')

if not NR_USER or not NR_PASS:
    print("ERROR: NR_USER and NR_PASS secrets not set.")
    print("Register at https://opendata.nationalrail.co.uk/ and subscribe to Timetable feed.")
    sys.exit(1)

BASE       = "https://opendata.nationalrail.co.uk"
DAYS_AHEAD = 14

# ── OPERATOR LOGOS ────────────────────────────────────────────────────────────
# Wikipedia thumbnail URL pattern:
# https://upload.wikimedia.org/wikipedia/{project}/thumb/{h1}/{h1h2}/{filename}/{width}px-{filename}.png
# We use the PNG preview at 200px width — clean, small, reliable
OPERATOR_LOGOS = {
    'TP': ('wikipedia/en', 'Northern_Trains.svg', '200'),   # placeholder until TPE confirmed
    'NT': ('wikipedia/en', 'Northern_Trains.svg', '200'),
    'GW': ('wikipedia/commons', 'Great_Western_Railway_(2015)_logo.svg', '200'),
    'VT': ('wikipedia/en', 'Avanti_West_Coast_Logo.svg', '200'),
    'LN': ('wikipedia/commons', 'LNER_Logo_2019.svg', '200'),
    'GR': ('wikipedia/commons', 'LNER_Logo_2019.svg', '200'),
    'XC': ('wikipedia/en', 'CrossCountryTrains.svg', '200'),
    'EM': ('wikipedia/en', 'East_Midlands_Railway_logo.svg', '200'),
    'SR': ('wikipedia/en', 'ScotRail_logo.svg', '200'),
    'SW': ('wikipedia/en', 'South_Western_Railway_logo.svg', '200'),
    'SN': ('wikipedia/en', 'Southern_(train_operating_company)_logo.svg', '200'),
    'SE': ('wikipedia/en', 'Southeastern_trains_logo.svg', '200'),
    'GA': ('wikipedia/en', 'Greater_Anglia_logo.svg', '200'),
    'TL': ('wikipedia/en', 'Thameslink_logo.svg', '200'),
    'GN': ('wikipedia/en', 'Great_Northern_logo.svg', '200'),
    'CH': ('wikipedia/en', 'Chiltern_Railways_logo.svg', '200'),
    'ME': ('wikipedia/en', 'Merseyrail_logo.svg', '200'),
    'CC': ('wikipedia/en', 'C2c_rail_logo.svg', '200'),
    'HX': ('wikipedia/en', 'Heathrow_Express_logo.svg', '200'),
    'LO': ('wikipedia/en', 'London_Overground_roundel.svg', '200'),
    'LD': ('wikipedia/en', 'Lumo_(train_operating_company)_logo.svg', '200'),
    'HT': ('wikipedia/en', 'Hull_Trains_logo.svg', '200'),
    'GC': ('wikipedia/en', 'Grand_Central_Railway_logo.svg', '200'),
    'CS': ('wikipedia/en', 'Caledonian_Sleeper_logo.svg', '200'),
    'TW': ('wikipedia/en', 'Transport_for_Wales_logo.svg', '200'),
    'AW': ('wikipedia/en', 'Transport_for_Wales_logo.svg', '200'),
}

def wiki_thumb_url(project, filename, width):
    """Build a Wikipedia thumbnail URL for an SVG file."""
    import hashlib
    h = hashlib.md5(filename.encode()).hexdigest()
    h1, h2 = h[0], h[0:2]
    return f"https://upload.wikimedia.org/{project}/thumb/{h1}/{h2}/{filename}/{width}px-{filename}.png"

def download_logos():
    """Download operator logos and save to logos/ directory."""
    os.makedirs('logos', exist_ok=True)
    # Wikimedia requires a descriptive User-Agent with contact info
    headers = {
        'User-Agent': 'Trackr/2.0 (UK rail timetable PWA; https://github.com; bot@example.com)',
        'Accept': 'image/png,image/*',
    }
    downloaded = 0
    for code, (project, filename, width) in OPERATOR_LOGOS.items():
        out_path = f'logos/{code}.png'
        if os.path.exists(out_path) and os.path.getsize(out_path) > 500:
            print(f"  {code}: exists, skipping")
            continue
        url = wiki_thumb_url(project, filename, width)
        print(f"  {code}: trying {url}")
        try:
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=15) as r:
                status = r.status
                data = r.read()
            if len(data) > 500:
                with open(out_path, 'wb') as f:
                    f.write(data)
                print(f"  {code}: OK ({len(data)//1024}KB)")
                downloaded += 1
            else:
                print(f"  {code}: response too small ({len(data)}B) status={status}")
        except Exception as e:
            print(f"  {code}: FAILED — {e}")
        time.sleep(0.5)
    print(f"Downloaded {downloaded}/{len(OPERATOR_LOGOS)} logos")

print("=== Downloading operator logos ===")
download_logos()

# ── ATOC NAMES ────────────────────────────────────────────────────────────────
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
    if not t: return None
    t = str(t).strip().rstrip('Hh').zfill(4)[:4]
    if not t.isdigit(): return None
    return f"{int(t[:2]) % 24:02d}:{t[2:4]}"

def parse_yymmdd(s):
    if not s or len(s) < 6: return None
    try:
        y = int(s[0:2]); y += 2000 if y < 60 else 1900
        return date(y, int(s[2:4]), int(s[4:6]))
    except ValueError: return None

def runs_on(days_str, target_date):
    if not days_str or len(days_str) < 7: return False
    return days_str[target_date.weekday()] == '1'

# ── AUTHENTICATE ─────────────────────────────────────────────────────────────
print("\n=== Authenticating with National Rail Data Portal ===")
auth_data = urllib.parse.urlencode({'username': NR_USER, 'password': NR_PASS}).encode()
auth_req  = urllib.request.Request(
    f"{BASE}/authenticate",
    data=auth_data,
    headers={"Content-Type": "application/x-www-form-urlencoded",
             "User-Agent": "Trackr/2.0 github-actions"},
    method="POST"
)
try:
    with urllib.request.urlopen(auth_req, timeout=30) as r:
        auth_resp = json.loads(r.read())
    token = auth_resp.get('token', '')
    if not token:
        print(f"ERROR: No token in response: {auth_resp}")
        sys.exit(1)
    print(f"Authenticated.")
except Exception as e:
    print(f"ERROR authenticating: {e}")
    sys.exit(1)

# ── DOWNLOAD TIMETABLE ────────────────────────────────────────────────────────
print("\n=== Downloading timetable zip ===")
tt_req = urllib.request.Request(
    f"{BASE}/api/staticfeeds/3.0/timetable",
    headers={"X-Auth-Token": token, "User-Agent": "Trackr/2.0 github-actions"}
)
try:
    with urllib.request.urlopen(tt_req, timeout=300) as r:
        zip_data = r.read()
    print(f"Downloaded {len(zip_data)/1024/1024:.1f} MB")
except Exception as e:
    print(f"ERROR downloading timetable: {e}")
    sys.exit(1)

# ── UNZIP ─────────────────────────────────────────────────────────────────────
print("Unzipping...")
zf = zipfile.ZipFile(io.BytesIO(zip_data))
mca_name = next((n for n in zf.namelist() if n.upper().endswith('.MCA')), None)
msn_name = next((n for n in zf.namelist() if n.upper().endswith('.MSN')), None)
if not mca_name:
    print("ERROR: No .MCA file found"); sys.exit(1)
print(f"Timetable: {mca_name}")

# ── PARSE MSN ─────────────────────────────────────────────────────────────────
tiploc_crs, tiploc_name = {}, {}
if msn_name:
    with zf.open(msn_name) as f:
        for line in io.TextIOWrapper(f, encoding='latin-1'):
            if line.startswith('A') and len(line) >= 52:
                name = line[1:27].strip().title()
                tiploc = line[36:43].strip()
                crs = line[49:52].strip().upper()
                if tiploc and crs and len(crs) == 3 and crs.isalpha():
                    tiploc_crs[tiploc] = crs
                    tiploc_name[tiploc] = name
    print(f"Loaded {len(tiploc_crs)} TIPLOC→CRS mappings")

# ── PARSE CIF ─────────────────────────────────────────────────────────────────
print("Parsing CIF data...")
today      = date.today()
date_range = [today + timedelta(days=i) for i in range(1, DAYS_AHEAD + 1)]
output     = defaultdict(list)
seen       = defaultdict(set)
current    = None

def flush(current, date_range, output, seen):
    if not current or len(current['stops']) < 2: return
    stops = current['stops']
    atoc  = current['atoc']
    uid   = current['uid']
    for target_date in date_range:
        s_date, e_date = current['start'], current['end']
        if not s_date or not e_date: continue
        if not (s_date <= target_date <= e_date): continue
        if not runs_on(current['days'], target_date): continue
        date_str = target_date.isoformat()
        for i, from_st in enumerate(stops):
            if not from_st['dep']: continue
            fc = from_st['crs']
            for to_st in stops[i+1:]:
                tc = to_st['crs']
                if (fc, tc) not in INDEXED_PAIRS: continue
                key   = f"{fc}-{tc}-{date_str}"
                dedup = (fc, tc, uid)
                if dedup in seen[date_str]: continue
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
        if len(line) < 2: continue
        rec = line[:2]; line_count += 1

        if rec == 'BS':
            flush(current, date_range, output, seen)
            stp = line[79] if len(line) > 79 else 'P'
            if stp == 'C': current = None; continue
            current = {'uid': line[3:9].strip(), 'start': parse_yymmdd(line[9:15]),
                       'end': parse_yymmdd(line[15:21]), 'days': line[21:28], 'atoc': '', 'stops': []}
        elif rec == 'BX' and current:
            current['atoc'] = line[25:27].strip()
        elif rec == 'TI':
            tip = line[2:9].strip(); crs = line[53:56].strip().upper(); name = line[56:72].strip().title()
            if tip and crs and len(crs) == 3 and crs.isalpha():
                tiploc_crs.setdefault(tip, crs); tiploc_name.setdefault(tip, name)
        elif rec == 'LO' and current:
            tip = line[2:9].strip(); crs = tiploc_crs.get(tip, tip[:3].upper() if len(tip)>=3 else None)
            dep = fmt_time(line[15:19])
            if crs: current['stops'].append({'crs':crs,'name':tiploc_name.get(tip,crs),'dep':dep,'arr':None})
        elif rec == 'LI' and current:
            tip = line[2:9].strip(); crs = tiploc_crs.get(tip, tip[:3].upper() if len(tip)>=3 else None)
            pub_arr = line[25:29].strip(); pub_dep = line[29:33].strip()
            if crs and (pub_arr or pub_dep):
                current['stops'].append({'crs':crs,'name':tiploc_name.get(tip,crs),
                    'dep':fmt_time(pub_dep),'arr':fmt_time(pub_arr)})
        elif rec == 'LT' and current:
            tip = line[2:9].strip(); crs = tiploc_crs.get(tip, tip[:3].upper() if len(tip)>=3 else None)
            arr = fmt_time(line[10:14])
            if crs: current['stops'].append({'crs':crs,'name':tiploc_name.get(tip,crs),'dep':None,'arr':arr})
            flush(current, date_range, output, seen); current = None

        if line_count % 500000 == 0:
            print(f"  {line_count:,} lines, {len(output):,} route-date pairs...")

flush(current, date_range, output, seen)
print(f"Parsed {line_count:,} lines")

# ── WRITE OUTPUT ─────────────────────────────────────────────────────────────
for key in output:
    output[key].sort(key=lambda x: x.get('std') or '')

result = {'generated': today.isoformat(), 'days_ahead': DAYS_AHEAD, 'routes': dict(output)}
with open('timetable.json', 'w') as f:
    json.dump(result, f, separators=(',', ':'))

total = sum(len(v) for v in output.values())
size  = os.path.getsize('timetable.json') / 1024
print(f"\nDone. {len(output):,} route-date pairs, {total:,} services. {size:.0f} KB")
