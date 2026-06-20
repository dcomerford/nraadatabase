"""V4 formula calibration at top-40% (floor 5) per (state, year, discipline, range).

V4 structure: Adjusted = (Score × Conversion + Centres × 0.7) × Factor
  Conversion = 1.2 for TR/Sporter, 1.0 for F-class.

Reports:
  1. Top-40% mean V4-adjusted MCSI per discipline under CURRENT V4 factors
  2. The factor each discipline WOULD need to hit the F-class mean (fair target)
  3. Spread (max-min mean MCSI) under current vs derived factors

Discipline categorisation matches build_survey_sets.py / app.py:
  TR-*, Division Open → TR
  F-Open → F-Open
  F-Std-* → F-Std
  FTR → FTR
  Sporter-Open → SO
  Sporter-PC, Sporter-Combined → SP
"""
from collections import defaultdict
from statistics import mean
from db import get_connection

TOP_PCT = 0.40
FLOOR_N = 5

V4_FACTORS = {
    'TR':     1.42,
    'F-Open': 1.42,
    'F-Std':  1.46,
    'FTR':    1.45,
    'SO':     1.40,
    'SP':     1.41,
}

CONVERSION = {'TR': 1.2, 'F-Open': 1.0, 'F-Std': 1.0, 'FTR': 1.0, 'SO': 1.2, 'SP': 1.2}
CENTRE_W = 0.7


def v4_adjusted(disc, pts, centres, factor=None):
    f = factor if factor is not None else V4_FACTORS[disc]
    return (pts * CONVERSION[disc] + centres * CENTRE_W) * f


QUERY = """
SELECT st.shooter_sid, st.match_number,
       CASE
         WHEN st.discipline LIKE 'TR-%' OR st.discipline = 'Division Open' THEN 'TR'
         WHEN st.discipline = 'F-Open' THEN 'F-Open'
         WHEN st.discipline LIKE 'F-Std-%' THEN 'F-Std'
         WHEN st.discipline = 'FTR' THEN 'FTR'
         WHEN st.discipline = 'Sporter-Open' THEN 'SO'
         WHEN st.discipline IN ('Sporter-PC','Sporter-Combined') THEN 'SP'
         ELSE NULL
       END AS cat,
       st.score, COALESCE(st.centres, 0) AS centres,
       s.code AS state_code, c.year
FROM strings st
JOIN competitions c USING(competition_id)
JOIN states s USING(state_id)
WHERE st.is_kings_queens AND c.year >= 2024
  AND s.code IN ('NSWRA','VRA','QRA','NQRA','SARA','WARA','NRAA')
"""


def main():
    conn = get_connection(); cur = conn.cursor(); cur.execute(QUERY)
    rows = cur.fetchall(); conn.close()

    buckets = defaultdict(list)
    for sid, mn, cat, score, centres, state, yr in rows:
        if cat is None:
            continue
        pts = int(float(score))
        buckets[(state, yr, cat, mn)].append((pts, int(centres)))

    top_buckets = {}
    for k, vals in buckets.items():
        vals_sorted = sorted(vals, reverse=True)
        n = max(FLOOR_N, int(round(len(vals_sorted) * TOP_PCT)))
        top_buckets[k] = vals_sorted[:min(n, len(vals_sorted))]

    per_cat = defaultdict(list)
    for (state, yr, cat, mn), vals in top_buckets.items():
        for pts, cs in vals:
            per_cat[cat].append((pts, cs))

    print('=' * 92)
    print('V4 CALIBRATION at TOP 40% (floor 5) per (state, year, discipline, range)')
    print('Dataset: K&Q 2024+, states NSWRA/VRA/QRA/NQRA/SARA/WARA/NRAA')
    print('=' * 92)
    print()

    cats = ['TR', 'F-Open', 'F-Std', 'FTR', 'SO', 'SP']
    print(f'{"Cat":<8} {"N":>6} {"Mean raw":>10} {"Mean ctr":>10} '
          f'{"Pre-factor":>12} {"V4 factor":>10} {"V4 MCSI":>10}')
    print('-' * 92)

    cur_means = {}
    pre_means = {}
    for cat in cats:
        vals = per_cat.get(cat, [])
        if not vals:
            continue
        mean_raw = mean(p for p, _ in vals)
        mean_ctr = mean(c for _, c in vals)
        pre = [p * CONVERSION[cat] + c * CENTRE_W for p, c in vals]
        cur = [v4_adjusted(cat, p, c) for p, c in vals]
        pre_means[cat] = mean(pre)
        cur_means[cat] = mean(cur)
        print(f'{cat:<8} {len(vals):>6} {mean_raw:>10.2f} {mean_ctr:>10.2f} '
              f'{mean(pre):>12.3f} {V4_FACTORS[cat]:>10.3f} {mean(cur):>10.3f}')

    # F-class fair target = average of FO, FS, FTR current V4 MCSI
    target = mean(cur_means[c] for c in ('F-Open', 'F-Std', 'FTR') if c in cur_means)
    print()
    print(f'F-class mean V4 MCSI (target):  {target:.3f}')
    spread_current = max(cur_means.values()) - min(cur_means.values())
    print(f'Current V4 spread (max-min):    {spread_current:.3f}')
    print()

    print('Derived factors to equalize each discipline to F-class mean:')
    print()
    print(f'{"Cat":<8} {"Current f":>10} {"Derived f":>10} {"Delta":>8} '
          f'{"At derived":>12}')
    print('-' * 92)
    for cat in cats:
        if cat not in pre_means:
            continue
        f_new = target / pre_means[cat]
        new_mean = pre_means[cat] * f_new
        d = f_new - V4_FACTORS[cat]
        print(f'{cat:<8} {V4_FACTORS[cat]:>10.3f} {f_new:>10.3f} {d:>+8.3f} '
              f'{new_mean:>12.3f}')

    print()
    print('=' * 92)
    print('Worked example — perfect 50.10 (TR / Sporter) and 60.10 (F-class):')
    print('=' * 92)
    for cat in cats:
        if cat not in V4_FACTORS:
            continue
        pts = 50 if CONVERSION[cat] == 1.2 else 60
        adj_cur = v4_adjusted(cat, pts, 10)
        print(f'  {cat:<8} {pts}.10  →  current V4: {adj_cur:.3f}')


if __name__ == '__main__':
    main()
