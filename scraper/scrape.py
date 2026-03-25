#!/usr/bin/env python3
"""
scraper/scrape.py
==================
Raspa holfuy.com/es/weather/1761 y guarda los datos en:
  data/holfuy.json       → histórico completo (últimos 200 registros)
  data/holfuy_today.json → solo registros del día actual

Sin dependencias externas — usa solo la librería estándar de Python.
"""

import re
import json
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

URL      = 'https://holfuy.com/es/weather/1761'
DATA_DIR = Path(__file__).parent.parent / 'data'
DATA_DIR.mkdir(exist_ok=True)

HISTORY_FILE = DATA_DIR / 'holfuy.json'
TODAY_FILE   = DATA_DIR / 'holfuy_today.json'
MAX_RECORDS  = 200

DIRS = ['N','NNE','NE','ENE','E','ESE','SE','SSE',
        'S','SSO','SO','OSO','O','ONO','NO','NNO']

DIR_MAP = {
    'N':'N','NNE':'NNE','NE':'NE','ENE':'ENE','E':'E',
    'ESE':'ESE','SE':'SE','SSE':'SSE','S':'S',
    'SSW':'SSO','SW':'SO','WSW':'OSO','W':'O',
    'WNW':'ONO','NW':'NO','NNW':'NNO',
    'SSO':'SSO','SO':'SO','OSO':'OSO','O':'O',
    'ONO':'ONO','NO':'NO','NNO':'NNO',
}

def fetch_html():
    req = urllib.request.Request(
        URL,
        headers={
            'User-Agent': 'Mozilla/5.0 (compatible; MadroniBot/1.0)',
            'Accept-Language': 'es-ES,es;q=0.9',
            'Accept': 'text/html',
        }
    )
    with urllib.request.urlopen(req, timeout=20) as resp:
        return resp.read().decode('utf-8', errors='replace')

def extract_cells(row_html):
    cells = re.findall(r'<t[dh][^>]*>(.*?)</t[dh]>', row_html, re.DOTALL | re.IGNORECASE)
    return [re.sub(r'<[^>]+>', ' ', c).replace('&nbsp;', ' ').strip() for c in cells]

def parse_dir(s):
    m = re.match(r'^([NSEWOCB]{1,4})\s+(\d+)°?$', s.strip(), re.IGNORECASE)
    if not m:
        return {'card': '—', 'deg': 0}
    card = DIR_MAP.get(m.group(1).upper(), m.group(1))
    return {'card': card, 'deg': int(m.group(2))}

def to_epoch(iso_date, hhmm):
    try:
        return int(datetime.fromisoformat(f'{iso_date}T{hhmm}:00').timestamp() * 1000)
    except Exception:
        return int(datetime.now(timezone.utc).timestamp() * 1000)

def parse_html(html):
    today   = datetime.now().strftime('%Y-%m-%d')
    records = []

    tables = re.findall(r'<table[^>]*>(.*?)</table>', html, re.DOTALL | re.IGNORECASE)
    for table in tables:
        if 'Velocidad' not in table and 'velocidad' not in table:
            continue

        raw_rows = re.findall(r'<tr[^>]*>(.*?)</tr>', table, re.DOTALL | re.IGNORECASE)
        rows = [extract_cells(r) for r in raw_rows]

        time_row = speed_row = gust_row = dir_row = temp_row = None

        for row in rows:
            if not row:
                continue
            r0 = row[0].lower()
            has_time = any(
                re.match(r'^\d{2}h$', c.strip()) or re.match(r'^\d{2}:\d{2}$', c.strip())
                for c in row
            )
            if has_time and time_row is None:
                time_row = row
            elif 'velocidad' in r0 and speed_row is None:
                speed_row = row
            elif ('ráfaga' in r0 or 'rafaga' in r0) and gust_row is None:
                gust_row = row
            elif dir_row is None and any(
                re.match(r'^[NSEWOCB]{1,4}\s+\d+°?$', c.strip(), re.IGNORECASE)
                for c in row
            ):
                dir_row = row
            elif 'temp' in r0 and temp_row is None:
                temp_row = row

        if not time_row or not speed_row or not gust_row:
            continue

        times  = [c.strip() for c in time_row  if re.match(r'^\d{2}(h|:\d{2})$', c.strip())]
        speeds = [c.strip() for c in speed_row[1:] if re.match(r'^\d+$', c.strip())]
        gusts  = [c.strip() for c in gust_row[1:]  if re.match(r'^\d+$', c.strip())]
        dirs   = [c.strip() for c in (dir_row or [])
                  if re.match(r'^[NSEWOCB]{1,4}\s+\d+°?$', c.strip(), re.IGNORECASE)]
        temps  = [c.strip() for c in (temp_row[1:] if temp_row else [])
                  if re.match(r'^-?\d+\.?\d*$', c.strip())]

        n = min(len(times), len(speeds), len(gusts))
        for k in range(n):
            raw_t = times[k]
            t     = raw_t.replace('h', ':00') if raw_t.endswith('h') else raw_t
            parsed = parse_dir(dirs[k] if k < len(dirs) else '')
            records.append({
                't':       t,
                'isoDate': today,
                'ts':      to_epoch(today, t),
                'v':       int(speeds[k]),
                'g':       int(gusts[k]),
                'd':       parsed['deg'],
                'dir':     parsed['card'],
                'tmp':     float(temps[k]) if k < len(temps) else None,
                'r':       0,
                'src':     'holfuy',
            })
        break   # primera tabla válida

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
        print('[scraper] Sin registros nuevos')
        return

    # Historial completo
    history = load_history()
    existing = {(r['t'], r.get('isoDate', '')) for r in history}
    added = 0
    for rec in records:
        key = (rec['t'], rec.get('isoDate', ''))
        if key not in existing:
            history.append(rec)
            existing.add(key)
            added += 1

    history = history[-MAX_RECORDS:]
    HISTORY_FILE.write_text(json.dumps(history, ensure_ascii=False))

    # Solo hoy
    today = datetime.now().strftime('%Y-%m-%d')
    today_rows = [r for r in history if r.get('isoDate', '') == today]
    TODAY_FILE.write_text(json.dumps(today_rows, ensure_ascii=False))

    print(f'[scraper] +{added} nuevos · total {len(history)} · hoy {len(today_rows)}')

if __name__ == '__main__':
    try:
        html    = fetch_html()
        records = parse_html(html)
        save(records)
    except Exception as e:
        print(f'[scraper] ERROR: {e}')
        raise
