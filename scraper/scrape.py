#!/usr/bin/env python3
"""
scraper/scrape.py — Holfuy Madroño v2
"""

import re, json, urllib.request
from datetime import datetime, timezone
from pathlib import Path

STATION_ID   = 1761
API_URL      = f'https://api.holfuy.com/live/?s={STATION_ID}&m=JSON&tu=C&su=km/h'
PAGE_URL     = f'https://holfuy.com/es/weather/{STATION_ID}'
DATA_DIR     = Path(__file__).parent.parent / 'data'
DATA_DIR.mkdir(exist_ok=True)
HISTORY_FILE = DATA_DIR / 'holfuy.json'
TODAY_FILE   = DATA_DIR / 'holfuy_today.json'
MAX_RECORDS  = 300

DIR_MAP = {
    'N':'N','NNE':'NNE','NE':'NE','ENE':'ENE','E':'E',
    'ESE':'ESE','SE':'SE','SSE':'SSE','S':'S',
    'SSW':'SSO','SW':'SO','WSW':'OSO','W':'O',
    'WNW':'ONO','NW':'NO','NNW':'NNO',
    'SSO':'SSO','SO':'SO','OSO':'OSO','O':'O',
    'ONO':'ONO','NO':'NO','NNO':'NNO',
}
DIRS = ['N','NNE','NE','ENE','E','ESE','SE','SSE',
        'S','SSO','SO','OSO','O','ONO','NO','NNO']

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                  '(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept-Language': 'es-ES,es;q=0.9',
    'Accept': 'text/html,application/json',
    'Referer': 'https://holfuy.com/',
}

def fetch(url):
    req = urllib.request.Request(url, headers=HEADERS)
    with urllib.request.urlopen(req, timeout=20) as r:
        return r.read().decode('utf-8', errors='replace')

def deg_to_card(deg):
    return DIRS[round(deg / 22.5) % 16]

def to_epoch(iso_date, hhmm):
    try:
        return int(datetime.fromisoformat(f'{iso_date}T{hhmm}:00').timestamp() * 1000)
    except Exception:
        return int(datetime.now(timezone.utc).timestamp() * 1000)

def try_api_json():
    try:
        raw  = fetch(API_URL)
        data = json.loads(raw)
        if 'measurements' in data:
            data = data['measurements'][0]
        today = datetime.now().strftime('%Y-%m-%d')
        dt    = data.get('dateTime', '')
        t     = dt[11:16] if len(dt) >= 16 else datetime.now().strftime('%H:%M')
        wind  = data.get('wind', {})
        deg   = int(wind.get('direction', 0))
        rec   = {
            't': t, 'isoDate': today, 'ts': to_epoch(today, t),
            'v': round(float(wind.get('speed', 0))),
            'g': round(float(wind.get('gust',  0))),
            'd': deg, 'dir': deg_to_card(deg),
            'tmp': float(data['temperature']) if 'temperature' in data else None,
            'r': 0, 'src': 'holfuy-api',
        }
        print(f'[api] {t} v={rec["v"]} g={rec["g"]} d={deg}')
        return [rec]
    except Exception as e:
        print(f'[api] fallo ({e})')
        return None

def extract_cells(row_html):
    cells = re.findall(r'<t[dh][^>]*>(.*?)</t[dh]>', row_html, re.DOTALL | re.IGNORECASE)
    clean = []
    for c in cells:
        text = re.sub(r'<[^>]+>', ' ', c)
        text = text.replace('&nbsp;', ' ').replace('&deg;', '°').strip()
        clean.append(re.sub(r'\s+', ' ', text))
    return clean

def parse_dir(s):
    m = re.match(r'^([NSEWOCB]{1,4})\s+(\d+)', s.strip(), re.IGNORECASE)
    if not m:
        return {'card': '—', 'deg': 0}
    return {'card': DIR_MAP.get(m.group(1).upper(), m.group(1)), 'deg': int(m.group(2))}

def parse_html(html):
    today   = datetime.now().strftime('%Y-%m-%d')
    records = []
    print(f'[html] tamaño={len(html)}')

    tables = re.findall(r'<table[^>]*>(.*?)</table>', html, re.DOTALL | re.IGNORECASE)
    print(f'[html] tablas={len(tables)}')

    for idx, table in enumerate(tables):
        if 'Velocidad' not in table and 'velocidad' not in table:
            continue

        raw_rows = re.findall(r'<tr[^>]*>(.*?)</tr>', table, re.DOTALL | re.IGNORECASE)
        rows = [extract_cells(r) for r in raw_rows]
        print(f'[html] tabla {idx}: {len(rows)} filas')
        for i, row in enumerate(rows[:6]):
            print(f'  [{i}] {row[:5]}')

        time_row = speed_row = gust_row = dir_row = temp_row = None

        for row in rows:
            if not row: continue
            r0 = row[0].lower()
            has_time = any(re.match(r'^\d{2}h$', c.strip()) or
                           re.match(r'^\d{2}:\d{2}', c.strip()) for c in row)
            if has_time and time_row is None:
                time_row = row
            elif any(x in r0 for x in ['velocidad','speed']) and speed_row is None:
                speed_row = row
            elif any(x in r0 for x in ['ráfaga','rafaga','gust']) and gust_row is None:
                gust_row = row
            elif dir_row is None and any(
                re.match(r'^[NSEWOCB]{1,4}\s+\d+', c.strip(), re.IGNORECASE) for c in row):
                dir_row = row
            elif 'temp' in r0 and temp_row is None:
                temp_row = row

        if not time_row or not speed_row or not gust_row:
            print(f'[html] tabla {idx}: faltan filas (time={time_row is not None} speed={speed_row is not None} gust={gust_row is not None})')
            continue

        times  = [c.strip() for c in time_row if re.match(r'^\d{2}(h|:\d{2})', c.strip())]
        speeds = [c.strip() for c in speed_row[1:] if re.match(r'^\d+$', c.strip())]
        gusts  = [c.strip() for c in gust_row[1:]  if re.match(r'^\d+$', c.strip())]
        dirs   = [c.strip() for c in (dir_row or [])
                  if re.match(r'^[NSEWOCB]{1,4}\s+\d+', c.strip(), re.IGNORECASE)]
        temps  = [c.strip() for c in (temp_row[1:] if temp_row else [])
                  if re.match(r'^-?\d+\.?\d*$', c.strip())]

        print(f'[html] times={len(times)} speeds={len(speeds)} gusts={len(gusts)} dirs={len(dirs)} temps={len(temps)}')

        n = min(len(times), len(speeds), len(gusts))
        for k in range(n):
            raw_t  = times[k]
            t      = raw_t.replace('h', ':00') if raw_t.endswith('h') else raw_t[:5]
            parsed = parse_dir(dirs[k] if k < len(dirs) else '')
            records.append({
                't': t, 'isoDate': today, 'ts': to_epoch(today, t),
                'v': int(speeds[k]), 'g': int(gusts[k]),
                'd': parsed['deg'], 'dir': parsed['card'],
                'tmp': float(temps[k]) if k < len(temps) else None,
                'r': 0, 'src': 'holfuy-html',
            })
        if records:
            break

    return records

def load_history():
    if HISTORY_FILE.exists():
        try:
            return json.loads(HISTORY_FILE.read_text())
        except Exception:
            pass
    return []

def save(records):
    if not records:
        print('[save] sin registros')
        return
    history  = load_history()
    existing = {(r['t'], r.get('isoDate','')) for r in history}
    added    = 0
    for rec in records:
        key = (rec['t'], rec.get('isoDate',''))
        if key not in existing:
            history.append(rec)
            existing.add(key)
            added += 1
    history = sorted(history, key=lambda r: (r.get('isoDate',''), r.get('t','')))
    history = history[-MAX_RECORDS:]
    HISTORY_FILE.write_text(json.dumps(history, ensure_ascii=False, indent=2))
    today      = datetime.now().strftime('%Y-%m-%d')
    today_rows = [r for r in history if r.get('isoDate','') == today]
    TODAY_FILE.write_text(json.dumps(today_rows, ensure_ascii=False, indent=2))
    print(f'[save] +{added} · historial={len(history)} · hoy={len(today_rows)}')

if __name__ == '__main__':
    records = try_api_json()
    if not records:
        try:
            html    = fetch(PAGE_URL)
            records = parse_html(html)
        except Exception as e:
            print(f'[error] {e}')
            records = []
    if records:
        save(records)
    else:
        print('[main] sin datos')
