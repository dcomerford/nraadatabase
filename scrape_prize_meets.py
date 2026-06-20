"""Scrape shooter-level rows from the 31 all-5-discipline prize meet pages.

Output: /tmp/prize_meet_rows.json with per-match per-discipline shooter scores.

Discipline label normalization to single canonical:
  TR  - Target Rifle, Division 1, Division 2 (WA), TR-A/B/C
  FO  - F Open / F-Open
  FS  - F Standard / F-Std (any A/B grade)
  FTR - FTR / F-TR
  SO  - Sporter Open / Sporter - Production Class OPEN
  SP  - Sporter Production / Sporter - Production Class - Sporter PC / Sporter PCO

Excludes Aggregate matches (sum of others).
"""
import json, os, re, hashlib
from html import unescape

with open('/tmp/meet_coverage_v2.json') as f:
    all_meets = json.load(f)

# Filter to all-5 meets
ALL5 = [m for m in all_meets if all(m['string_lines'][d] >= 5
                                     for d in ['TR','F-Open','F-Std','FTR','Sporter'])]
print(f'Processing {len(ALL5)} all-5-discipline meets')


def normalize_discipline(label: str) -> str | None:
    """Map a raw discipline header to canonical short code, or None to skip."""
    s = label.lower().strip()
    # FTR FIRST (before F-Std, since F-TR could match "F Standard")
    if re.search(r'\bf[\s/-]?tr\b', s):
        return 'FTR'
    # Sporter — try Open vs PC distinction
    if 'sporter' in s:
        if 'open' in s and 'pc' not in s:
            return 'SO'
        if 'pc' in s or 'production class - sporter' in s:
            return 'SP'
        # "Sporter PCO" (WA) - treat as SP (production class open, like SP)
        if 'pco' in s:
            return 'SP'
        return 'SP'  # fall-through default for Sporter
    if 'f open' in s or 'f-open' in s:
        return 'FO'
    if 'f standard' in s or 'f-std' in s or 'f-standard' in s:
        return 'FS'
    # Target rifle variants
    if re.match(r'^division\s*\d+', s):
        return 'TR'
    if re.search(r'\btarget rifle\b', s) or re.match(r'^tr\b', s) or re.search(r'\btr[\s-]', s):
        return 'TR'
    return None


def parse_meet(html: str, meet_name: str):
    """Yield (match_title, discipline, lastname, prefname, club, state, score, centres) tuples."""
    # Strip HTML entities (we'll unescape per cell later)
    # Find each <h2> match title block, then capture the table that follows
    # Pattern: <h2>Match N - title</h2> ... <table> ... </table>
    blocks = re.findall(
        r'<h2[^>]*>\s*(Match[^<]+)</h2>.*?<table>(.*?)</table>',
        html, flags=re.S | re.I
    )
    for raw_title, table_html in blocks:
        match_title = unescape(re.sub(r'\s+', ' ', raw_title)).strip()
        # Skip aggregate matches
        if 'aggregate' in match_title.lower():
            continue

        # Walk table rows; track current discipline from <th colspan> rows
        current_disc = None
        # Split into <tr>...</tr>
        rows = re.findall(r'<tr[^>]*>(.*?)</tr>', table_html, flags=re.S | re.I)
        for row in rows:
            # Discipline header row
            th_header = re.search(r'<th\s+colspan=["\']?\d+["\']?[^>]*>\s*(.*?)\s*</th>',
                                   row, flags=re.S | re.I)
            if th_header:
                label = unescape(re.sub(r'<[^>]+>', '', th_header.group(1))).strip()
                current_disc = normalize_discipline(label)
                continue
            # Skip thead column headers (rows with only <th> column headers, no colspan)
            if '<th' in row and '<td' not in row:
                continue
            if current_disc is None:
                continue
            # Data row: <td>...</td> cells
            tds = re.findall(r'<td[^>]*>(.*?)</td>', row, flags=re.S | re.I)
            if not tds:
                continue
            cells = [unescape(re.sub(r'<[^>]+>', '', t)).strip() for t in tds]
            if len(cells) < 4:
                continue
            # Last cell should be score (decimal)
            score_raw = cells[-1]
            m = re.match(r'^(\d+)\.(\d+)$', score_raw)
            if not m:
                continue
            score_int = int(m.group(1))
            centres = int(m.group(2))
            # Skip aggregate-sized scores (single match max ~75)
            if score_int > 75:
                # aggregate of multiple matches snuck through
                continue
            # Place is cells[0], LastName cells[1], PrefName cells[2], Club cells[3]
            # State / Shots / Info may be in middle cells; we don't need them all
            place = cells[0]
            lastname = cells[1] if len(cells) > 1 else ''
            prefname = cells[2] if len(cells) > 2 else ''
            club = cells[3] if len(cells) > 3 else ''
            state = ''
            for c in cells[4:-1]:
                if re.match(r'^[A-Z]{2,3}$', c):
                    state = c
                    break
            yield {
                'meet': meet_name,
                'match': match_title,
                'discipline': current_disc,
                'lastname': lastname,
                'prefname': prefname,
                'club': club,
                'state': state,
                'score_int': score_int,
                'centres': centres,
            }


rows = []
meet_summary = []
for meet in ALL5:
    slug = hashlib.md5(meet['url'].encode()).hexdigest()[:12]
    path = f"/tmp/meet_pages/{meet['year']}_{slug}.html"
    if not os.path.exists(path):
        print(f"  MISSING: {meet['name']}")
        continue
    html = open(path).read()
    meet_rows = list(parse_meet(html, meet['name']))
    rows.extend(meet_rows)
    # Tally per discipline
    by_d = {}
    for r in meet_rows:
        by_d[r['discipline']] = by_d.get(r['discipline'], 0) + 1
    meet_summary.append({'year': meet['year'], 'name': meet['name'], 'counts': by_d, 'total': len(meet_rows)})
    print(f"  [{meet['year']}] {meet['name']:50s} {by_d} total={len(meet_rows)}")

with open('/tmp/prize_meet_rows.json', 'w') as f:
    json.dump(rows, f)

print(f'\nTotal rows scraped: {len(rows)}')
# Totals per discipline (combined Sporter SO+SP)
from collections import Counter
disc_tot = Counter(r['discipline'] for r in rows)
print(f'Per-discipline totals: {dict(disc_tot)}')
sporter_combined = disc_tot.get('SO', 0) + disc_tot.get('SP', 0)
print(f'Sporter (SO+SP merged): {sporter_combined}')
