"""Re-scrape with FTR-first parser, using meet_coverage_v3 to filter East all-5 meets.

Output: /tmp/prize_meet_rows_v2.json (East-coast all-5 only, ~99 meets).
"""
import json, os, re, hashlib
from html import unescape

with open('/tmp/meet_coverage_v3.json') as f:
    cov = json.load(f)


def region_of(name, url):
    nlow = name.lower()
    wa_kw = ['perth','wurgabup','great southern','swdra','northwest dra','egdra',
             'subiaco','metropolitan dra','midlands dra','goldfields','geraldt',
             'esperance','albany','bunbury','chapman valley','bindoon','beverley',
             'carnarvon','northampton','manjimup','wurg','coolilup','wagin',
             'fremantle','barry hughes','golden bullet','barry & sons']
    if any(k in nlow for k in wa_kw): return 'WA'
    if 'wa' in nlow.split() or '(wa)' in nlow: return 'WA'
    if 'chris hope' in nlow: return 'WA'
    return 'East'


CATS = ['TR','F-Open','F-Std','FTR','Sporter']
east_all5 = [r for r in cov
             if all(r['counts'].get(d, 0) >= 5 for d in CATS)
             and region_of(r['name'], r['url']) == 'East']
print(f'East-coast all-5 meets: {len(east_all5)}')


def classify_header(line: str):
    s = line.strip()
    if len(s) > 90 or len(s) < 3: return None
    low = s.lower()
    if re.search(r'\bf[\s/-]?tr\b', low): return 'FTR'
    if 'sporter' in low or 'hunter' in low:
        if 'class' in low or 'production' in low or 'open' in low or 'hunter' in low \
           or re.match(r'^sporter\s*-', low) or low.strip() == 'sporter':
            # determine SO vs SP
            if 'production class - sporter' in low or 'pc' in low and 'pco' not in low: return 'SP'
            if 'pco' in low: return 'SP'  # WA's combined production class open
            if 'open' in low and 'production' in low: return 'SO'
            if 'hunter' in low: return 'SP'  # hunter ~= production-style
            return 'SP'
    if re.search(r'\bf[\s-]?open\b', low) or 'f class open' in low: return 'FO'
    if re.search(r'\bf[\s-]?std\b', low) or 'f standard' in low or 'f-standard' in low: return 'FS'
    if re.match(r'^division\s*\d+\s*$', low): return 'TR'
    if re.search(r'\btarget rifle\b', low): return 'TR'
    if re.match(r'^tr[\s-]', low) or low == 'tr': return 'TR'
    return None


def parse_meet(html: str, meet_name: str):
    blocks = re.findall(
        r'<h2[^>]*>\s*(Match[^<]+)</h2>.*?<table>(.*?)</table>',
        html, flags=re.S | re.I
    )
    for raw_title, table_html in blocks:
        match_title = unescape(re.sub(r'\s+', ' ', raw_title)).strip()
        if 'aggregate' in match_title.lower():
            continue
        current = None
        rows = re.findall(r'<tr[^>]*>(.*?)</tr>', table_html, flags=re.S | re.I)
        for row in rows:
            th = re.search(r'<th\s+colspan=["\']?\d+["\']?[^>]*>\s*(.*?)\s*</th>',
                            row, flags=re.S | re.I)
            if th:
                label = unescape(re.sub(r'<[^>]+>', '', th.group(1))).strip()
                current = classify_header(label)
                continue
            if '<th' in row and '<td' not in row: continue
            if current is None: continue
            tds = re.findall(r'<td[^>]*>(.*?)</td>', row, flags=re.S | re.I)
            if not tds: continue
            cells = [unescape(re.sub(r'<[^>]+>', '', t)).strip() for t in tds]
            if len(cells) < 4: continue
            m = re.match(r'^(\d+)\.(\d+)$', cells[-1])
            if not m: continue
            score_int = int(m.group(1)); centres = int(m.group(2))
            if score_int > 75: continue
            state = ''
            for c in cells[4:-1]:
                if re.match(r'^[A-Z]{2,3}$', c):
                    state = c; break
            yield {
                'meet': meet_name,
                'match': match_title,
                'discipline': current,
                'lastname': cells[1] if len(cells) > 1 else '',
                'prefname': cells[2] if len(cells) > 2 else '',
                'club': cells[3] if len(cells) > 3 else '',
                'state': state,
                'score_int': score_int,
                'centres': centres,
            }


rows = []
for meet in east_all5:
    slug = hashlib.md5(meet['url'].encode()).hexdigest()[:12]
    path = f"/tmp/meet_pages/{meet['year']}_{slug}.html"
    if not os.path.exists(path): continue
    html = open(path).read()
    meet_rows = list(parse_meet(html, meet['name']))
    rows.extend(meet_rows)

with open('/tmp/prize_meet_rows_v2.json', 'w') as f:
    json.dump(rows, f)

from collections import Counter
ct = Counter(r['discipline'] for r in rows)
print(f'\nTotal rows: {len(rows)}')
print(f'Per-discipline: {dict(ct)}')
print(f'Sporter combined (SO+SP): {ct.get("SO",0)+ct.get("SP",0)}')
