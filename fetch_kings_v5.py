"""Fetch NRAA K&Q / Open-Championship match results, write them to a kings-format
CSV (to send to Adrian), and print a quick V5 cross-discipline comparison.

Reusable across days of a multi-day championship: pass the match numbers that
appeared on the new day.

Usage:
    python3 fetch_kings_v5.py --matches 11,13,14
    python3 fetch_kings_v5.py --url <results-url> --matches 17,19,20 --out kings_nswra2026.csv --append

The NRAA results site serves an incomplete TLS chain, so we fetch with an
unverified SSL context (read-only public results page).
"""
import argparse, csv, html, re, ssl, sys, urllib.request
from collections import defaultdict

DEFAULT_URL = 'https://www.results.nraa.com.au/nswra-150th-annual-open-championships-2026-results/'
UA = ('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 '
      '(KHTML, like Gecko) Chrome/124.0 Safari/537.36')

# page discipline header -> (kings discipline code, V5 group, target_max)
DISC_MAP = {
    'Target Rifle - A': ('TR-A', 'TR', 5),
    'Target Rifle - B': ('TR-B', 'TR', 5),
    'Target Rifle - C': ('TR-C', 'TR', 5),
    'F Standard - A': ('F-Std-A', 'F-Standard', 6),
    'F Standard - B': ('F-Std-B', 'F-Standard', 6),
    'F Open - FO': ('F-Open', 'F-Open', 6),
    'F/TR - FTR': ('FTR', 'FTR', 6),
    'Sporter - Production Class OPEN - Open': ('Sporter-Open', 'Sporter', 5),
    'Sporter - Production Class - Sporter PC': ('Sporter-PC', 'Sporter', 5),
}

# V5 formula: Adjusted = (raw*conv + centres*0.7) * factor
V5 = {  # group -> (factor, conversion)
    'TR': (1.412, 1.20), 'F-Open': (1.406, 1.00), 'F-Standard': (1.475, 1.00),
    'FTR': (1.450, 1.00), 'Sporter': (1.383, 1.20),
}
CENTRE_WT = 0.7

CSV_COLS = ['state', 'year', 'competition', 'match_number', 'match_name', 'distance',
            'distance_unit', 'discipline', 'target_max', 'place', 'sid', 'first_name',
            'last_name', 'club', 'raw_score', 'shot_count', 'centres', 'shots_raw', 'info']


def fetch(url):
    if url.startswith('file://'):
        return open(url[7:]).read()
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    req = urllib.request.Request(url, headers={'User-Agent': UA})
    return urllib.request.urlopen(req, context=ctx, timeout=60).read().decode('utf-8', 'replace')


def clean(s):
    return html.unescape(re.sub(r'<[^>]+>', ' ', s)).strip()


def parse(page, want_matches, comp, state, year):
    """Return (rows, meta) where rows are dicts in CSV_COLS order."""
    rows = []
    # split into match blocks by the <h2>Match N - NAME - ...</h2> heading
    blocks = re.split(r'<h2>\s*Match\s+', page)
    for blk in blocks[1:]:
        m = re.match(r'(\d+)\s*-\s*(.*?)\s*(?:-|</h2>)', blk)
        if not m:
            continue
        mnum = int(m.group(1))
        if mnum not in want_matches:
            continue
        mname = clean(m.group(2))
        table = blk.split('</table>', 1)[0]
        disc = None
        for tr in re.findall(r'<tr>(.*?)</tr>', table, re.S):
            th = re.search(r'<th colspan="8"[^>]*>(.*?)</th>', tr, re.S)
            if th:
                disc = clean(th.group(1))
                continue
            tds = re.findall(r'<td[^>]*>(.*?)</td>', tr, re.S)
            if len(tds) < 8 or disc is None:
                continue
            place, last, pref, club, st, shots, info, score = [clean(x) for x in tds[:8]]
            if not score or '.' not in score and not score.isdigit():
                continue
            if '.' in score:
                pts_s, cen_s = score.split('.', 1)
                pts, cen = int(pts_s), int(cen_s)
            else:
                pts, cen = int(score), 0
            code, grp, tmax = DISC_MAP.get(disc, (disc, None, 5 if 'Rifle' in disc or 'Sporter' in disc else 6))
            shot_count = len(re.sub(r'\s', '', shots))
            raw_score = f'{pts}.{str(cen).ljust(3, "0")}'
            rows.append({
                'state': state, 'year': year, 'competition': comp, 'match_number': mnum,
                'match_name': mname, 'distance': '', 'distance_unit': '', 'discipline': code,
                'target_max': tmax, 'place': place, 'sid': '', 'first_name': pref,
                'last_name': last, 'club': club, 'raw_score': raw_score, 'shot_count': shot_count,
                'centres': cen, 'shots_raw': shots, 'info': info,
                '_pts': pts, '_cen': cen, '_grp': grp,
            })
    return rows


def v5_mcsi(pts, cen, grp):
    if grp not in V5:
        return None
    factor, conv = V5[grp]
    return (pts * conv + cen * CENTRE_WT) * factor


def write_csv(rows, out, append):
    mode = 'a' if append else 'w'
    exists = append
    with open(out, mode, newline='') as f:
        w = csv.DictWriter(f, fieldnames=CSV_COLS, extrasaction='ignore')
        if not exists:
            w.writeheader()
        for r in rows:
            w.writerow(r)
    print(f'Wrote {len(rows)} rows to {out} (mode={"append" if append else "new"})')


def report_v5(rows):
    by_match = defaultdict(list)
    for r in rows:
        mcsi = v5_mcsi(r['_pts'], r['_cen'], r['_grp'])
        if mcsi is not None:
            by_match[(r['match_number'], r['match_name'])].append((mcsi, r))
    for (mnum, mname), lst in sorted(by_match.items()):
        lst.sort(key=lambda x: -x[0])
        print(f'\n=== Match {mnum} — {mname} — V5 cross-discipline top 10 ===')
        print(f'{"#":>2} {"Shooter":<22} {"Disc":<13} {"Score":>7} {"V5 MCSI":>8}')
        for i, (mcsi, r) in enumerate(lst[:10], 1):
            name = f'{r["first_name"]} {r["last_name"]}'
            print(f'{i:>2} {name:<22} {r["discipline"]:<13} {r["raw_score"]:>7} {mcsi:>8.2f}')
        # discipline of the winner
        wins = defaultdict(int)
        for _, r in lst[:5]:
            wins[r['_grp']] += 1
        print('   top-5 discipline mix:', dict(wins))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--url', default=DEFAULT_URL)
    ap.add_argument('--matches', default='11,13,14', help='comma-separated match numbers')
    ap.add_argument('--out', default='kings_nswra2026.csv')
    ap.add_argument('--append', action='store_true', help='append to existing CSV (for later days)')
    ap.add_argument('--comp', default='NSWRA 2026')
    ap.add_argument('--state', default='NSWRA')
    ap.add_argument('--year', default='2026')
    a = ap.parse_args()
    want = {int(x) for x in a.matches.split(',') if x.strip()}
    page = fetch(a.url)
    rows = parse(page, want, a.comp, a.state, a.year)
    if not rows:
        print('No rows parsed — check match numbers / page structure.', file=sys.stderr)
        sys.exit(1)
    found = sorted({r['match_number'] for r in rows})
    print(f'Parsed matches {found}: {len(rows)} shooter rows.')
    write_csv(rows, a.out, a.append)
    report_v5(rows)


if __name__ == '__main__':
    main()
