"""Scrape an NRAA results page → CSV in the format import_results.py expects.

Usage:
    python3 scrape_nraa.py <URL> <state_code> <year> --kings-matches 10,11,12,14,15,16,18,19 [-o out.csv]

Notes:
- Each <h2>Match N - ...</h2> heading is followed by exactly one <table>.
- Tables with 7 columns (no Shots) are aggregates. Tables with 8 columns (Shots) are strings.
- Discipline sub-headers are single-cell <tr>s in the middle of the table.
- target_max is auto-detected from the shot string (presence of '6' → 60-pt target).
- Shooter SID is not on the page — the importer (import_results.py) will match by name+club.
"""
import argparse
import csv
import re
import sys
import urllib.request

# ---- Discipline normalisation ------------------------------------------------

DISCIPLINE_RULES = [
    # (regex, canonical_tag)
    (r'target\s*rifle\s*-?\s*a',            'TR-A'),
    (r'target\s*rifle\s*-?\s*b',            'TR-B'),
    (r'target\s*rifle\s*-?\s*c',            'TR-C'),
    (r'target\s*rifle',                     'TR-A'),  # fallback if no class
    (r'f[-/\s]*tr',                         'FTR'),
    (r'f[-\s]*open',                        'F-Open'),
    (r'f[-\s]*class\s*open',                'F-Open'),
    (r'f[-\s]*standard\s*-?\s*a',           'F-Std-A'),
    (r'f[-\s]*standard\s*-?\s*b',           'F-Std-B'),
    (r'f[-\s]*standard\s*open',             'F-Std-Open'),
    (r'f[-\s]*standard',                    'F-Std-Open'),
    (r'sporter.*production.*open',          'Sporter-Open'),
    (r'sporter.*production.*pc',            'Sporter-PC'),
    (r'sporter.*production',                'Sporter-PC'),
    (r'sporter.*open',                      'Sporter-Open'),
    (r'sporter.*pc',                        'Sporter-PC'),
    (r'sporter',                            'Sporter-Combined'),
]


def normalise_discipline(raw):
    raw_l = raw.lower().strip()
    for pat, tag in DISCIPLINE_RULES:
        if re.search(pat, raw_l):
            return tag
    return raw.strip()


# ---- HTML helpers ------------------------------------------------------------

def strip_tags(s):
    return re.sub(r'<[^>]+>', '', s).strip()


def split_score(score_str):
    """'75.11' → (75, 11). '53.1' → (53, 1). '98.10' → (98, 10).
    The decimal digits as-written are the centre count (single number),
    NOT a fractional representation. So '50.10' = 10 centres, '50.1' = 1 centre.
    """
    if not score_str:
        return None, None
    s = score_str.strip()
    if '.' not in s:
        return int(s), 0
    a, b = s.split('.', 1)
    return int(a), int(b)


HEADING_RE = re.compile(
    r'(?P<full><h[1-6][^>]*>(?P<inner>.*?)</h[1-6]>)'
    r'|'
    r'(?P<table><table[^>]*>(?P<tbl>.*?)</table>)',
    re.I | re.S,
)
MATCH_RE = re.compile(r'match\s*(\d+)\s*-\s*(.*)', re.I)
DIST_RE = re.compile(r'(\d{3,4})\s*(yds?|m)\b', re.I)


def parse_page(html):
    """Walk the HTML and yield (match_number, match_name, distance, distance_unit,
    is_aggregate, rows) for every match table found."""
    current_heading = None
    for m in HEADING_RE.finditer(html):
        if m.group('full'):
            current_heading = strip_tags(m.group('inner'))
        elif m.group('table') and current_heading:
            yield current_heading, m.group('tbl')


def parse_match_heading(heading):
    """'Match 10 - 1st 500 Yds - The Jackman Family' → (10, '1st 500 Yds - The Jackman Family', 500, 'Yds', False)
    Aggregate matches return (n, name, None, None, True).
    """
    m = MATCH_RE.match(heading)
    if not m:
        return None
    num = int(m.group(1))
    rest = m.group(2).strip()
    is_agg = bool(re.search(r'aggregate', rest, re.I))
    dist, unit = None, None
    dm = DIST_RE.search(rest)
    if dm:
        dist = int(dm.group(1))
        unit = 'Yds' if dm.group(2).lower().startswith('y') else 'm'
    return num, rest, dist, unit, is_agg


def detect_target_max(shots_raw):
    """60-pt targets allow '6' as a shot value. 50-pt targets max out at '5'/'V'/'X'."""
    if shots_raw and '6' in shots_raw:
        return 6
    return 5


# ---- Row extraction ----------------------------------------------------------

def extract_rows(tbl_html):
    """Returns (header_cells, [(discipline_subheader_or_None, [cells]), ...])."""
    trs = re.findall(r'<tr[^>]*>(.*?)</tr>', tbl_html, re.S)
    header = None
    out = []
    current_discipline_raw = None
    for tr in trs:
        cells = re.findall(r'<t[dh][^>]*>(.*?)</t[dh]>', tr, re.S)
        cleaned = [strip_tags(c) for c in cells]
        if not cleaned:
            continue
        if header is None:
            header = cleaned
            continue
        # Subheader row = single-cell row that doesn't look like a normal data row
        if len(cleaned) == 1:
            current_discipline_raw = cleaned[0]
            continue
        out.append((current_discipline_raw, cleaned))
    return header, out


# ---- Main --------------------------------------------------------------------

def fetch(url):
    """Use curl — Python's bundled CA bundle has issues with this host."""
    import subprocess
    r = subprocess.run(
        ['curl', '-sL', '-A', 'Mozilla/5.0', '--max-time', '60', url],
        capture_output=True, check=True,
    )
    return r.stdout.decode('utf-8', errors='replace')


# Patterns for classifying aggregates.
_APOS = r"(?:\\'|[’‘'`])?"  # straight, smart, backslash-escaped, backtick, or none
KINGS_AGG_RE = re.compile(
    rf"king{_APOS}s|queen{_APOS}s|day\s*[1-9]|[1-9](?:st|nd|rd|th)\s*day", re.I)
KINGS_PRIZE_RE = re.compile(rf"king{_APOS}s|queen{_APOS}s", re.I)
SKIP_AGG_RE = re.compile(r'grand|progressive|championship\s+aggregate', re.I)
NON_KINGS_NAMED = re.compile(
    r"wilson|tiger\s*cup|tiger|duncan|hunt|cowlrick|rolph|president|"
    r"mcintosh|syme|duke|anzac|warm[- ]?up|practice|veteran|junior|"
    r"festival|elizabeth|kaltenberg|mace|sweet|heald|presentation",
    re.I,
)


def auto_detect_kings_matches(html):
    """Identify Kings/Queens string match numbers.

    Strategy:
    1. Find the upper bound = highest-numbered Kings Prize aggregate (e.g. 'The Kings',
       'Kings Aggregate', 'Queens Prize') that isn't a Grand aggregate.
    2. Find the lower fence = highest-numbered NON-Kings parent aggregate before that
       (Wilson, Hunt, Duncan, Syme, ANZAC, Tiger Cup, Rolph, Presidents, McIntosh, Duke).
    3. Kings strings = non-aggregate match numbers in (lower_fence, upper_bound].
    """
    matches = []  # (match_number, name, is_agg)
    for m in re.finditer(r'<h[1-6][^>]*>(.*?)</h[1-6]>', html, re.I | re.S):
        txt = strip_tags(m.group(1))
        parsed = parse_match_heading(txt)
        if parsed:
            num, name, _d, _u, is_agg = parsed
            matches.append((num, name, is_agg))
    matches.sort(key=lambda x: x[0])

    # Upper bound: highest-numbered "Kings/Queens" agg (excluding Grand-style).
    kings_aggs = [
        (num, name) for num, name, ia in matches
        if ia and KINGS_PRIZE_RE.search(name) and not SKIP_AGG_RE.search(name)
    ]
    if not kings_aggs:
        return set()
    upper = max(n for n, _ in kings_aggs)

    # Lower fence: highest-numbered non-Kings parent aggregate before upper.
    lower = 0
    for num, name, ia in matches:
        if ia and num < upper and NON_KINGS_NAMED.search(name):
            lower = max(lower, num)

    kings = set()
    for num, name, ia in matches:
        if ia:
            continue
        if lower < num <= upper:
            kings.add(num)
    return kings


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('url')
    ap.add_argument('state_code', help='e.g. VRA, NSWRA, QRA')
    ap.add_argument('year', type=int)
    ap.add_argument('--kings-matches', default='',
                    help='Comma-separated match numbers that are Kings/Queens strings. '
                         'Strings with these match numbers get is_kings_queens=True. '
                         'If omitted, auto-detected by walking the page structure.')
    ap.add_argument('-o', '--out', default=None)
    args = ap.parse_args()

    competition = f'{args.state_code} {args.year}'
    out_path = args.out or f'/tmp/{args.state_code}_{args.year}.csv'

    print(f'Fetching {args.url}', file=sys.stderr)
    html = fetch(args.url)

    if args.kings_matches.strip():
        kings_set = {int(x) for x in args.kings_matches.split(',')}
        print(f'  using explicit kings-matches: {sorted(kings_set)}', file=sys.stderr)
    else:
        kings_set = auto_detect_kings_matches(html)
        print(f'  auto-detected kings-matches: {sorted(kings_set)}', file=sys.stderr)

    fieldnames = [
        'competition', 'match_number', 'match_name', 'distance', 'distance_unit',
        'discipline', 'place', 'full_name', 'last_name', 'first_name',
        'club', 'state', 'shots', 'info', 'score', 'centres',
        'is_kings_queens', 'target_max', 'is_aggregate',
    ]

    string_count = 0
    agg_count = 0
    skipped_no_score = 0

    with open(out_path, 'w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()

        for heading, tbl in parse_page(html):
            parsed = parse_match_heading(heading)
            if not parsed:
                continue
            match_num, match_name, distance, dist_unit, is_agg = parsed
            header, rows = extract_rows(tbl)
            if not header:
                continue

            # Detect column layout from header
            has_shots = any('shot' in c.lower() for c in header)
            # Map header positions
            colmap = {c.strip().lower(): i for i, c in enumerate(header)}

            def col(row, name):
                i = colmap.get(name.lower())
                return row[i] if i is not None and i < len(row) else ''

            for disc_raw, row in rows:
                if disc_raw is None:
                    # No discipline header — skip (rare; would be a malformed table)
                    continue
                place_raw = col(row, 'place').strip()
                if not place_raw or not place_raw[0].isdigit():
                    continue
                score_raw = col(row, 'score').strip()
                if not score_raw:
                    skipped_no_score += 1
                    continue
                last_name = col(row, 'last name')
                first_name = col(row, 'preferred name') or col(row, 'first name')
                club = col(row, 'club')
                state = col(row, 'state')
                shots = col(row, 'shots') if has_shots else ''
                info = col(row, 'info')

                discipline = normalise_discipline(disc_raw)
                target_max = detect_target_max(shots) if shots else 5
                kings = match_num in kings_set if not is_agg else False
                _pts, centres = split_score(score_raw)

                w.writerow({
                    'competition': competition,
                    'match_number': match_num,
                    'match_name': match_name,
                    'distance': distance if not is_agg else '',
                    'distance_unit': dist_unit if not is_agg else '',
                    'discipline': discipline,
                    'place': int(place_raw),
                    'full_name': f'{first_name} {last_name}'.strip(),
                    'last_name': last_name,
                    'first_name': first_name,
                    'club': club,
                    'state': state,
                    'shots': shots,
                    'info': info,
                    'score': score_raw,
                    'centres': centres if centres is not None else '',
                    'is_kings_queens': 'true' if kings else 'false',
                    'target_max': target_max,
                    'is_aggregate': 'true' if is_agg else 'false',
                })
                if is_agg:
                    agg_count += 1
                else:
                    string_count += 1

    print(f'Wrote {out_path}', file=sys.stderr)
    print(f'  strings:    {string_count}', file=sys.stderr)
    print(f'  aggregates: {agg_count}', file=sys.stderr)
    print(f'  skipped (no score): {skipped_no_score}', file=sys.stderr)
    if kings_set:
        print(f'  kings-tagged matches: {sorted(kings_set)}', file=sys.stderr)


if __name__ == '__main__':
    main()
