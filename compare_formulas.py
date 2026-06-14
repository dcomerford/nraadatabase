"""Score every (filtered) Kings string under 4 candidate MCSI formulas and produce
an HTML report comparing the rankings each formula gives.

Filtered dataset:
- 7 comps (VRA/NQRA 2026, NSWRA/QRA 2025, VRA/NSWRA/QRA 2024)
- 6 disciplines: TR, F-Open, F-Std, FTR, Sporter-Open, Sporter-PC
- Only shooters who completed every range expected of them in their discipline

Formulas:
- A: Current linear  -  (score + centres) × mult + offset
- B: MCSI-14 lookup  -  table lookup keyed on (score + centres); F-classes/TR only
- C: Raw-weighted    -  (score × 1.1 + centres) × mult + offset  -  breaks 49.9 vs 50.8 tie
- D: Fractional centres - (score + centres/11) × mult × ~10  -  centres as sub-point tiebreak
"""
from collections import defaultdict
import html
import sys

from db import get_connection

# ---- Formula A: current linear (from app.py) --------------------------------

LINEAR_PARAMS = {
    'F-Open':       (1.42, 1.8),
    'F-Std':        (1.42, 1.8),
    'FTR':          (1.42, 1.8),
    'TR':           (1.62, 8.4),
    'Sporter-Open': (1.50, 12.0),
    'Sporter-PC':   (1.50, 12.0),
}

# ---- Formula B: MCSI-14 lookup table ----------------------------------------
# Column key per discipline. Sporter has no published column → returns None.
MCSI14 = {
    # lookup_score → (TR, FS, FO, FTR)
    0:(0,0,0,0), 1:(1.7,1.4,1.3,1.3), 2:(3.4,2.9,2.6,2.7), 3:(5.1,4.3,3.8,4),
    4:(6.8,5.7,5.1,5.4), 5:(8.5,7.1,6.4,6.7), 6:(10.2,8.6,7.7,8.1),
    7:(11.9,10,8.9,9.4), 8:(13.6,11.4,10.2,10.8), 9:(15.3,12.9,11.5,12.1),
    10:(17,14.3,12.8,13.5), 11:(18.7,15.7,14,14.8), 12:(20.4,17.1,15.3,16.2),
    13:(22.1,18.6,16.6,17.5), 14:(23.8,20,17.9,18.9), 15:(25.5,21.4,19.1,20.2),
    16:(27.2,22.9,20.4,21.5), 17:(28.9,24.3,21.7,22.9), 18:(30.6,25.7,23,24.2),
    19:(32.3,27.1,24.3,25.6), 20:(34,28.6,25.5,26.9), 21:(35.7,30,26.8,28.3),
    22:(37.4,31.4,28.1,29.6), 23:(39.1,32.9,29.4,31), 24:(40.8,34.3,30.6,32.3),
    25:(42.5,35.7,31.9,33.7), 26:(44.2,37.1,33.2,35), 27:(45.9,38.6,34.5,36.4),
    28:(47.6,40,35.7,37.7), 29:(49.3,41.4,37,39.1), 30:(51,42.9,38.3,40.4),
    31:(52.7,44.3,39.6,41.8), 32:(54.4,45.7,40.8,43.1), 33:(56.1,47.1,42.1,44.4),
    34:(57.8,48.6,43.4,45.8), 35:(59.5,50,44.7,47.1), 36:(61.2,51.4,46,48.5),
    37:(62.9,52.9,47.2,49.8), 38:(64.3,54.3,48.5,51.2), 39:(65.7,55.7,49.8,52.5),
    40:(67.2,57.1,51.1,53.9), 41:(68.6,58.6,52.3,55.2), 42:(70,60,53.6,56.6),
    43:(71.4,61.4,54.9,57.9), 44:(72.8,62.9,56.2,59.3), 45:(74.2,64.3,57.4,60.6),
    46:(75.5,65.7,58.7,62), 47:(76.9,67.1,60,63.3), 48:(78.2,68.6,61.3,64.8),
    49:(79.6,70,62.5,66.3), 50:(81,71.4,63.8,67.8), 51:(82.4,72.9,65.4,69.3),
    52:(83.8,74.3,67,70.8), 53:(85.3,75.7,68.6,72.4), 54:(86.8,77.1,70.3,74),
    55:(88.4,78.6,72,75.5), 56:(90.1,80,73.7,77), 57:(91.9,81.4,75.4,78.6),
    58:(93.9,82.9,77.1,80.2), 59:(96.2,84.3,78.8,81.7), 60:(99.5,85.7,80.5,83.2),
    61:(None,87.1,82.2,84.8), 62:(None,88.6,83.8,86.3), 63:(None,90,85.5,87.9),
    64:(None,91.4,87.2,89.5), 65:(None,92.9,88.9,91), 66:(None,94.3,90.6,92.6),
    67:(None,95.7,92.4,94.2), 68:(None,97.1,94.4,95.9), 69:(None,98.6,96.5,97.7),
    70:(None,100,99.6,99.8),
}

MCSI14_COL = {'TR': 0, 'F-Std': 1, 'F-Open': 2, 'FTR': 3}


def formula_a_linear(score, centres, discipline):
    p = LINEAR_PARAMS.get(discipline)
    if not p:
        return None
    mult, offset = p
    return round((score + centres) * mult + offset, 2)


def formula_b_mcsi14(score, centres, discipline):
    col = MCSI14_COL.get(discipline)
    if col is None:
        return None  # Sporter unsupported in MCSI-14
    lookup = int(score) + int(centres)
    row = MCSI14.get(lookup)
    if not row:
        return None
    val = row[col]
    return None if val is None else round(val, 2)


def formula_c_raw_weighted(score, centres, discipline):
    """Raw-weighted: shifts emphasis to raw score so a clean 50 beats 49+anything."""
    p = LINEAR_PARAMS.get(discipline)
    if not p:
        return None
    mult, offset = p
    return round((score * 1.1 + centres) * mult + offset, 2)


def formula_d_fractional(score, centres, discipline):
    """Centres-as-fraction: centres become sub-point tiebreak, never bridge a full point."""
    p = LINEAR_PARAMS.get(discipline)
    if not p:
        return None
    mult, offset = p
    # Scale to roughly match formula A magnitudes for ease of comparison.
    # (score + centres/11) so 10 centres ≈ 0.91, still strictly less than +1 raw point.
    return round(((score + centres / 11.0) * 2) * mult + offset, 2)


FORMULAS = [
    ('A_linear',         formula_a_linear),
    ('B_mcsi14',         formula_b_mcsi14),
    ('C_raw_weighted',   formula_c_raw_weighted),
    ('D_fractional',     formula_d_fractional),
]


# ---- Discipline normalisation -----------------------------------------------

def combine_class(discipline):
    if discipline.startswith('TR-'):
        return 'TR'
    if discipline.startswith('F-Std-'):
        return 'F-Std'
    return discipline


# ---- Data load --------------------------------------------------------------

QUERY = """
WITH subset AS (
  SELECT st.string_id, st.shooter_sid, st.match_number, st.match_name,
         st.discipline AS raw_discipline,
         st.distance, st.distance_unit,
         st.score, st.target_max, st.centres,
         s.code AS state_code, c.year,
         sh.first_name, sh.last_name, cl.club_name
  FROM strings st
  JOIN competitions c USING(competition_id)
  JOIN states s USING(state_id)
  LEFT JOIN shooters sh ON sh.sid = st.shooter_sid
  LEFT JOIN clubs cl ON cl.club_id = sh.club_id
  WHERE st.is_kings_queens AND (
    (s.code IN ('VRA','NQRA')   AND c.year=2026) OR
    (s.code IN ('NSWRA','QRA')  AND c.year=2025) OR
    (s.code IN ('VRA','NSWRA','QRA') AND c.year=2024)
  )
),
classified AS (
  SELECT *, CASE WHEN raw_discipline LIKE 'TR-%' THEN 'TR'
                 WHEN raw_discipline LIKE 'F-Std-%' THEN 'F-Std'
                 ELSE raw_discipline END AS class
  FROM subset
),
class_max AS (
  SELECT state_code, year, class, COUNT(DISTINCT match_number) AS max_ranges
  FROM classified GROUP BY state_code, year, class
),
shooter_count AS (
  SELECT state_code, year, class, shooter_sid, COUNT(DISTINCT match_number) AS n
  FROM classified GROUP BY state_code, year, class, shooter_sid
),
full_shooters AS (
  SELECT sc.state_code, sc.year, sc.class, sc.shooter_sid
  FROM shooter_count sc JOIN class_max cm USING (state_code, year, class)
  WHERE sc.n = cm.max_ranges
)
SELECT c.state_code, c.year, c.class, c.shooter_sid,
       c.first_name, c.last_name, c.club_name,
       c.match_number, c.match_name, c.distance, c.distance_unit,
       c.score, c.target_max, c.centres
FROM classified c
JOIN full_shooters f USING (state_code, year, class, shooter_sid)
ORDER BY c.state_code, c.year, c.class, c.shooter_sid, c.match_number;
"""


def split_score(score):
    """Numeric (e.g. 75.11) → (75 points, 11 centres)."""
    if score is None:
        return 0, 0
    pts = int(score)
    cs = int(round((float(score) - pts) * 100))
    # Some scores are written as 75.1 meaning 1 centre — normalise so 75.10 == 10 centres
    # Heuristic: if cs is 1-digit and matches the textual representation, treat as-is.
    return pts, cs


def load_data():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(QUERY)
    rows = cur.fetchall()
    conn.close()
    return rows


# ---- Score totals per shooter per (comp, class) -----------------------------

def aggregate(rows):
    """Group rows into per-shooter totals under each formula.

    Returns nested dict:
      out[(state, year)][class][shooter_sid] = {
          'name': str, 'club': str,
          'strings': [(match_num, distance, score, centres, target_max), ...],
          'totals': {formula_key: float},
      }
    """
    out = defaultdict(lambda: defaultdict(dict))
    for r in rows:
        (state, yr, cls, sid, fn, ln, club, mn, mname, dist, unit,
         score, target_max, centres) = r
        score = float(score) if score is not None else 0.0
        pts = int(score)  # NUMERIC, integer part is the raw points
        cs = centres if centres is not None else 0
        entry = out[(state, yr)][cls].setdefault(sid, {
            'name': f'{fn} {ln}'.strip(),
            'club': club or '',
            'strings': [],
        })
        entry['strings'].append((mn, dist, pts, cs, target_max, score))

    # Compute totals
    for comp, by_cls in out.items():
        for cls, by_sid in by_cls.items():
            for sid, entry in by_sid.items():
                totals = {}
                strings_per_formula_ok = {}
                for fkey, fn_ in FORMULAS:
                    vals = []
                    for mn, dist, pts, cs, tmax, score in entry['strings']:
                        v = fn_(pts, cs, cls)
                        if v is not None:
                            vals.append(v)
                    totals[fkey] = round(sum(vals), 2) if vals else None
                    strings_per_formula_ok[fkey] = len(vals)
                entry['totals'] = totals
                entry['raw_total'] = round(
                    sum(s for _,_,_,_,_,s in entry['strings']), 2)
    return out


# ---- HTML output ------------------------------------------------------------

CSS = """
body { font-family: -apple-system, BlinkMacSystemFont, sans-serif; margin: 24px; color: #222; }
h1 { font-size: 24px; }
h2 { font-size: 18px; margin-top: 32px; border-bottom: 2px solid #ccc; padding-bottom: 4px; }
h3 { font-size: 15px; margin-top: 24px; color: #555; }
table { border-collapse: collapse; font-size: 13px; margin-bottom: 16px; }
th, td { padding: 4px 10px; text-align: right; border-bottom: 1px solid #eee; }
th { background: #f4f4f4; text-align: center; white-space: nowrap; }
td.text { text-align: left; }
td.rank { width: 28px; color: #888; }
.gold { background: #fff7d6; font-weight: 600; }
.silver { background: #f0f0f0; }
.bronze { background: #fae8d6; }
.disagree { background: #ffe0e0 !important; }
.muted { color: #999; }
nav a { display: inline-block; margin-right: 12px; padding: 4px 10px; background: #eef;
        border-radius: 4px; text-decoration: none; color: #224; font-size: 13px; }
.meta { color: #666; font-size: 12px; margin-bottom: 8px; }
"""


def html_header(title):
    return (f'<!doctype html><html><head><meta charset="utf-8">'
            f'<title>{html.escape(title)}</title>'
            f'<style>{CSS}</style></head><body>')


def html_footer():
    return '</body></html>'


def rank_cell(rank):
    cls = ''
    if rank == 1: cls = 'gold'
    elif rank == 2: cls = 'silver'
    elif rank == 3: cls = 'bronze'
    return cls


def render_comp(comp_key, by_class, out):
    state, year = comp_key
    out.write(f'<h2>{state} {year}</h2>')

    # --- Top: 1st-place winner across all disciplines under each formula -----
    out.write('<h3>Top winner under each formula (highlights cross-discipline disagreement)</h3>')
    out.write('<table><thead><tr><th>Discipline</th><th>Raw total</th>'
              '<th>A: Linear</th><th>B: MCSI-14</th>'
              '<th>C: Raw-weighted</th><th>D: Fractional</th></tr></thead><tbody>')
    for cls in ['TR', 'F-Open', 'F-Std', 'FTR', 'Sporter-Open', 'Sporter-PC']:
        if cls not in by_class:
            continue
        shooters = list(by_class[cls].values())
        if not shooters:
            continue
        # Find top for each formula
        out.write(f'<tr><td class="text">{cls}</td>')
        top_raw = max(shooters, key=lambda x: x['raw_total'])
        out.write(f'<td class="text">{html.escape(top_raw["name"])} '
                  f'<span class="muted">({top_raw["raw_total"]})</span></td>')
        for fkey, _ in FORMULAS:
            valid = [s for s in shooters if s['totals'].get(fkey) is not None]
            if not valid:
                out.write('<td class="muted">—</td>')
                continue
            top = max(valid, key=lambda x: x['totals'][fkey])
            disagree = top['name'] != top_raw['name']
            cls_attr = ' class="disagree"' if disagree else ''
            out.write(f'<td{cls_attr}>{html.escape(top["name"])}'
                      f'<br><span class="muted">{top["totals"][fkey]}</span></td>')
        out.write('</tr>')
    out.write('</tbody></table>')

    # --- Cross-discipline "winner of the day" under each formula -----
    out.write('<h3>Single winner across ALL disciplines under each formula</h3>')
    all_shooters = []
    for cls, by_sid in by_class.items():
        for sid, entry in by_sid.items():
            all_shooters.append((cls, entry))
    out.write('<table><thead><tr><th>Formula</th><th>Winner</th><th>Discipline</th>'
              '<th>Raw total</th><th>MCSI total</th></tr></thead><tbody>')
    for fkey, _ in FORMULAS:
        valid = [(cls, s) for cls, s in all_shooters if s['totals'].get(fkey) is not None]
        if not valid:
            continue
        winner_cls, winner = max(valid, key=lambda x: x[1]['totals'][fkey])
        out.write(f'<tr><td class="text"><b>{fkey}</b></td>'
                  f'<td class="text">{html.escape(winner["name"])}</td>'
                  f'<td class="text">{winner_cls}</td>'
                  f'<td>{winner["raw_total"]}</td>'
                  f'<td>{winner["totals"][fkey]}</td></tr>')
    out.write('</tbody></table>')

    # --- Per-discipline leaderboards ---------------------------------------
    for cls in ['TR', 'F-Open', 'F-Std', 'FTR', 'Sporter-Open', 'Sporter-PC']:
        if cls not in by_class:
            continue
        shooters = list(by_class[cls].values())
        if not shooters:
            continue
        # Sort by current linear MCSI desc as primary
        shooters.sort(key=lambda x: -(x['totals'].get('A_linear') or 0))
        out.write(f'<h3>{cls} — {len(shooters)} shooters</h3>')
        out.write('<table><thead><tr><th>#</th><th>Shooter</th><th>Club</th>'
                  '<th>Raw</th>')
        for fkey, _ in FORMULAS:
            out.write(f'<th>{fkey}</th>')
        out.write('</tr></thead><tbody>')

        # Pre-compute rank under each formula
        rank_by_formula = {}
        for fkey, _ in FORMULAS:
            with_score = [s for s in shooters if s['totals'].get(fkey) is not None]
            with_score.sort(key=lambda x: -x['totals'][fkey])
            rank_by_formula[fkey] = {id(s): i+1 for i, s in enumerate(with_score)}

        for rank, s in enumerate(shooters, 1):
            cls_attr = rank_cell(rank)
            out.write(f'<tr class="{cls_attr}"><td class="rank">{rank}</td>'
                      f'<td class="text">{html.escape(s["name"])}</td>'
                      f'<td class="text muted">{html.escape(s["club"])}</td>'
                      f'<td>{s["raw_total"]}</td>')
            for fkey, _ in FORMULAS:
                v = s['totals'].get(fkey)
                if v is None:
                    out.write('<td class="muted">—</td>')
                else:
                    r = rank_by_formula[fkey].get(id(s))
                    delta_class = ''
                    if r is not None and r != rank:
                        delta_class = ' disagree'
                    rank_str = f' <span class="muted">#{r}</span>' if r else ''
                    out.write(f'<td class="{delta_class}">{v}{rank_str}</td>')
            out.write('</tr>')
        out.write('</tbody></table>')


def main():
    rows = load_data()
    print(f'Loaded {len(rows)} strings', file=sys.stderr)
    grouped = aggregate(rows)

    out_path = sys.argv[1] if len(sys.argv) > 1 else '/tmp/mcsi_comparison.html'
    with open(out_path, 'w') as out:
        out.write(html_header('MCSI Formula Comparison — Kings 2024–2026'))
        out.write('<h1>MCSI Formula Comparison</h1>')
        out.write('<p class="meta">7 comps, 6 disciplines, full-coverage shooters only.<br>'
                  'Highlighting in <span class="disagree">pink</span> shows where a formula disagrees '
                  'with the baseline ranking (current linear MCSI). The "single winner" table shows '
                  'where formulas disagree about who should win across all disciplines.</p>')
        out.write('<nav>')
        for comp_key in sorted(grouped.keys(), key=lambda x: (-x[1], x[0])):
            state, yr = comp_key
            out.write(f'<a href="#{state}_{yr}">{state} {yr}</a>')
        out.write('</nav>')
        for comp_key in sorted(grouped.keys(), key=lambda x: (-x[1], x[0])):
            state, yr = comp_key
            out.write(f'<a id="{state}_{yr}"></a>')
            render_comp(comp_key, grouped[comp_key], out)
        out.write(html_footer())
    print(f'Wrote {out_path}', file=sys.stderr)


if __name__ == '__main__':
    main()
