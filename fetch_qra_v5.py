"""Fetch the QRA Kings Prize Shoot from the HEXTA (hexsystems) results site, write
it to the same kings-format CSV layout as fetch_kings_v5.py, and print a quick V5
cross-discipline comparison.

Unlike the NRAA results site, HEXTA serves the whole competition on one page:
each match is a collapsible panel containing one table with a discipline header
row (<th colspan="13">) before each discipline block. Detail matches carry the
distance in the panel title and a per-shot string; aggregate matches carry only
a total (with the component strings in a tooltip), so they are parsed separately
and used to validate the detail strings rather than exported as scores.

Usage:
    python3 fetch_qra_v5.py                       # competition 711, all detail matches
    python3 fetch_qra_v5.py --url .../competition/711 --out kings_qra2026.csv
"""
import argparse, csv, html, re, ssl, sys, urllib.request
from collections import defaultdict

DEFAULT_URL = 'https://shooting.hexsystems.com.au/competition/711'
UA = ('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 '
      '(KHTML, like Gecko) Chrome/124.0 Safari/537.36')

# HEXTA discipline header -> (kings discipline code, V5 group, target_max)
DISC_MAP = {
    'T-Rifle - A': ('TR-A', 'TR', 5),
    'T-Rifle - B': ('TR-B', 'TR', 5),
    'T-Rifle - C': ('TR-C', 'TR', 5),
    'F-Std - A': ('F-Std-A', 'F-Standard', 6),
    'F-Std - B': ('F-Std-B', 'F-Standard', 6),
    'F-Open - A': ('F-Open', 'F-Open', 6),
    'F-TR - A': ('FTR', 'FTR', 6),
    'Sport-PC - A': ('Sporter-PC', 'Sporter', 5),
    'Sport-PCO - A': ('Sporter-Open', 'Sporter', 5),
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
    return urllib.request.urlopen(req, context=ctx, timeout=120).read().decode('utf-8', 'replace')


def clean(s):
    return html.unescape(re.sub(r'<[^>]+>', ' ', s)).replace('\xa0', ' ').strip()


def split_score(score):
    """'50.009' -> (50, 9); '175.027' -> (175, 27); bare '49' -> (49, 0)."""
    score = score.strip()
    if '.' in score:
        p, c = score.split('.', 1)
        return int(p), int(c or 0)
    return int(score), 0


def shots_fired(cell):
    """'10' -> (10, 10); '9/10' -> (9, 10) for a string the shooter did not complete."""
    cell = (cell or '').strip()
    if '/' in cell:
        a, b = cell.split('/', 1)
        return int(a or 0), int(b or 0)
    n = int(cell or 0)
    return n, n


def split_name(full):
    """HEXTA gives one name field; kings CSV wants first/last."""
    parts = full.split()
    if len(parts) < 2:
        return full, ''
    return ' '.join(parts[:-1]), parts[-1]


def parse(page, comp, state, year):
    """Return (detail_rows, agg_rows). Panel titles look like
    'Match 15 - Kings - Day 2 - 800 yds' or 'Match 14 - Kings - Day 1 Agg'."""
    detail, aggs = [], []
    # each panel: heading anchor, then the panel body up to the next panel
    panels = re.split(r'<a class="collapse-toggle[^>]*>', page)[1:]
    for idx, blk in enumerate(panels, 1):
        title = clean(blk.split('</a>', 1)[0])
        m = re.match(r'Match\s+(\d+)\s*-\s*(.*)$', title)
        if not m:
            continue
        mnum, rest = int(m.group(1)), m.group(2).strip()
        if '<tbody>' not in blk:
            continue
        tbody = blk.split('<tbody>', 1)[1].split('</tbody>', 1)[0]
        is_agg = bool(re.search(r'\bAgg\b', rest))
        dm = re.search(r'(\d+)\s*(yds|yards|m)\b', rest, re.I)
        dist, unit = (dm.group(1), dm.group(2).lower()) if dm else ('', '')

        disc = None
        for tr in re.findall(r'<tr[^>]*>(.*?)</tr>', tbody, re.S):
            th = re.search(r'<th colspan="\d+">(.*?)</th>', tr, re.S)
            if th:
                disc = clean(th.group(1))
                continue
            tds = re.findall(r'<td[^>]*>(.*?)</td>', tr, re.S)
            if disc is None or not tds:
                continue
            code, grp, tmax = DISC_MAP.get(disc, (disc, None, 6))
            if is_agg:
                # Place | Comp club | Shooter | Shooter club | Shots | Result(span title=parts)
                if len(tds) < 6:
                    continue
                place, _cc, shooter, club, shots, result = [clean(x) for x in tds[:6]]
                parts = re.search(r'title="([^"]+)"', tds[5])
                try:
                    pts, cen = split_score(result)
                except ValueError:
                    continue
                aggs.append({'match_number': mnum, 'match_name': rest, 'panel': idx,
                             'discipline': code, 'name': shooter, 'club': club,
                             'shot_count': shots_fired(shots)[0], 'pts': pts, 'cen': cen,
                             'parts': html.unescape(parts.group(1)) if parts else ''})
                continue
            # Place | Date | Comp club | Shooter | Shooter club | Target | Shots | String | Result | Group
            if len(tds) < 9:
                continue
            place, dt, _cc, shooter, club, _tgt, nshots, string, result = [clean(x) for x in tds[:9]]
            try:
                pts, cen = split_score(result)
            except ValueError:
                continue
            first, last = split_name(shooter)
            # HEXTA writes '10' for a complete string and '9/10' when shots are missing
            fired, expected = shots_fired(nshots)
            detail.append({
                'state': state, 'year': year, 'competition': comp, 'match_number': mnum,
                'match_name': rest, 'distance': dist, 'distance_unit': unit,
                'discipline': code, 'target_max': tmax, 'place': place, 'sid': '',
                'first_name': first, 'last_name': last, 'club': club,
                'raw_score': f'{pts}.{str(cen).rjust(3, "0")}',
                'shot_count': fired, 'centres': cen, 'shots_raw': string,
                'info': dt if fired == expected else f'{dt} (incomplete {fired}/{expected})',
                '_pts': pts, '_cen': cen, '_grp': grp, '_panel': idx, '_name': shooter,
                '_expected': expected,
            })
    return detail, aggs


def validate(detail, aggs):
    """Reconcile each aggregate row against the sum of that shooter's detail strings.

    Matches are keyed by the day the aggregate names, so a 'Day 2 Agg' is checked
    against every Day-2 detail panel. Returns (checked, mismatches)."""
    def day_of(name):
        d = re.search(r'Day\s+(\d+)', name)
        return d.group(1) if d else None

    by_day = defaultdict(lambda: defaultdict(lambda: [0, 0, 0]))
    for r in detail:
        d = day_of(r['match_name'])
        k = (r['_name'], r['discipline'])
        for scope in ([d] if d else []) + ['ALL']:
            t = by_day[scope][k]
            t[0] += r['_pts']; t[1] += r['_cen']; t[2] += r['shot_count']

    checked, bad = 0, []
    for a in aggs:
        d = day_of(a['match_name'])
        scope = d if d else 'ALL'
        t = by_day[scope].get((a['name'], a['discipline']))
        if not t:
            continue
        checked += 1
        if (t[0], t[1]) != (a['pts'], a['cen']):
            bad.append((a['match_number'], a['match_name'], a['name'], a['discipline'],
                        f'{a["pts"]}.{a["cen"]}', f'{t[0]}.{t[1]}'))
    return checked, bad


def v5_mcsi(pts, cen, grp):
    if grp not in V5:
        return None
    factor, conv = V5[grp]
    return (pts * conv + cen * CENTRE_WT) * factor


def write_csv(rows, out, append):
    mode = 'a' if append else 'w'
    with open(out, mode, newline='') as f:
        w = csv.DictWriter(f, fieldnames=CSV_COLS, extrasaction='ignore')
        if not append:
            w.writeheader()
        for r in rows:
            w.writerow(r)
    print(f'Wrote {len(rows)} rows to {out} (mode={"append" if append else "new"})')


def report_v5(rows):
    by_match = defaultdict(list)
    for r in rows:
        mcsi = v5_mcsi(r['_pts'], r['_cen'], r['_grp'])
        if mcsi is not None:
            by_match[(r['_panel'], r['match_number'], r['match_name'])].append((mcsi, r))
    for (_p, mnum, mname), lst in sorted(by_match.items()):
        lst.sort(key=lambda x: -x[0])
        print(f'\n=== Match {mnum} — {mname} — V5 cross-discipline top 10 ===')
        print(f'{"#":>2} {"Shooter":<22} {"Disc":<13} {"Score":>7} {"V5 MCSI":>8}')
        for i, (mcsi, r) in enumerate(lst[:10], 1):
            print(f'{i:>2} {r["_name"]:<22} {r["discipline"]:<13} {r["raw_score"]:>7} {mcsi:>8.2f}')
        wins = defaultdict(int)
        for _, r in lst[:5]:
            wins[r['_grp']] += 1
        print('   top-5 discipline mix:', dict(wins))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--url', default=DEFAULT_URL)
    ap.add_argument('--out', default='kings_qra2026.csv')
    ap.add_argument('--append', action='store_true')
    ap.add_argument('--comp', default='QRA Kings 2026')
    ap.add_argument('--state', default='QRA')
    ap.add_argument('--year', default='2026')
    ap.add_argument('--quiet', action='store_true', help='skip the per-match top-10 dump')
    a = ap.parse_args()
    page = fetch(a.url)
    detail, aggs = parse(page, a.comp, a.state, a.year)
    if not detail:
        print('No rows parsed — check the URL / page structure.', file=sys.stderr)
        sys.exit(1)
    panels = sorted({(r['_panel'], r['match_number'], r['match_name']) for r in detail})
    print(f'Parsed {len(panels)} scoring matches, {len(detail)} shooter strings, '
          f'{len(aggs)} aggregate rows.')
    for _p, mnum, mname in panels:
        n = sum(1 for r in detail if r['_panel'] == _p)
        print(f'  Match {mnum:<3} {mname:<28} {n:>4} strings')
    checked, bad = validate(detail, aggs)
    print(f'\nValidation: {checked} aggregate rows reconciled against detail strings, '
          f'{len(bad)} mismatches.')
    for b in bad[:15]:
        print('  MISMATCH', b)
    write_csv(detail, a.out, a.append)
    if not a.quiet:
        report_v5(detail)


if __name__ == '__main__':
    main()
