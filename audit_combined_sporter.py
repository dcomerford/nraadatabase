"""Re-run the fairness audit with all 2024-2026 Kings/Queens comps, treating
Sporter-Open, Sporter-PC and Sporter-Combined as a single 'Sporter' class.

Compares two MCSI formulas:
  ORIGINAL 2014 linear  - Sporter: mult=1.50, offset=12.0
  NEW linear            - Sporter (combined): mult=1.493, offset=12.0
                          TR:                  mult=1.534, offset=8.4

F-classes unchanged in both (mult=1.42, offset=1.8).
"""
from collections import defaultdict
from statistics import mean, median
from db import get_connection

TOP_PCT = 0.40
FLOOR_N = 5

ORIGINAL = {
    'F-Open':  (1.42, 1.8),
    'F-Std':   (1.42, 1.8),
    'FTR':     (1.42, 1.8),
    'TR':      (1.62, 8.4),
    'Sporter': (1.50, 12.0),   # Original 2014 Sporter factor
}

NEW = {
    'F-Open':  (1.42, 1.8),    # Unchanged
    'F-Std':   (1.42, 1.8),    # Unchanged
    'FTR':     (1.42, 1.8),    # Unchanged
    'TR':      (1.534, 8.4),   # Derived from 7-comp analysis
    'Sporter': (1.493, 12.0),  # Average of SO (1.487) and SPC (1.499) from 7-comp analysis
}


def mcsi(pts, centres, cls, params):
    p = params.get(cls)
    if not p:
        return None
    mult, offset = p
    return (pts + centres) * mult + offset


QUERY = """
WITH subset AS (
  SELECT st.shooter_sid, st.match_number,
         CASE WHEN st.discipline LIKE 'TR-%' THEN 'TR'
              WHEN st.discipline LIKE 'F-Std-%' THEN 'F-Std'
              WHEN st.discipline LIKE 'Sporter%' THEN 'Sporter'
              ELSE st.discipline END AS class,
         st.score, COALESCE(st.centres, 0) AS centres,
         s.code AS state_code, c.year
  FROM strings st
  JOIN competitions c USING(competition_id)
  JOIN states s USING(state_id)
  WHERE st.is_kings_queens AND c.year >= 2024
),
class_max AS (
  SELECT state_code, year, class, COUNT(DISTINCT match_number) AS max_ranges
  FROM subset GROUP BY state_code, year, class
),
shooter_count AS (
  SELECT state_code, year, class, shooter_sid, COUNT(DISTINCT match_number) AS n
  FROM subset GROUP BY state_code, year, class, shooter_sid
),
full_shooters AS (
  SELECT sc.state_code, sc.year, sc.class, sc.shooter_sid
  FROM shooter_count sc JOIN class_max cm USING (state_code, year, class)
  WHERE sc.n = cm.max_ranges
)
SELECT s.state_code, s.year, s.class, s.match_number,
       s.shooter_sid, s.score, s.centres
FROM subset s
JOIN full_shooters f USING (state_code, year, class, shooter_sid)
ORDER BY s.state_code, s.year, s.class, s.match_number, s.score DESC;
"""


def load():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(QUERY)
    rows = cur.fetchall()
    conn.close()
    return rows


def main():
    rows = load()
    print(f'Loaded {len(rows)} strings from all 2024-2026 Kings/Queens comps')
    print()

    # Bucket per (comp, class, match)
    buckets = defaultdict(list)
    for state, yr, cls, mn, sid, score, centres in rows:
        pts = int(float(score))
        buckets[(state, yr, cls, mn)].append((pts, int(centres)))

    # Take top 20% with floor 5 per bucket
    top_buckets = {}
    for key, vals in buckets.items():
        n = max(FLOOR_N, int(round(len(vals) * TOP_PCT)))
        top_buckets[key] = vals[:min(n, len(vals))]

    # === Aggregate per class ================================================
    per_class = defaultdict(list)  # cls → [(pts, cs)]
    for (state, yr, cls, mn), vals in top_buckets.items():
        for pts, cs in vals:
            per_class[cls].append((pts, cs))

    print('=' * 84)
    print('OVERALL — top-N mean MCSI by class, original vs new formula')
    print(f'Dataset: all 2024-2026 Kings/Queens comps, Sporter Open+PC+Combined merged')
    print('=' * 84)
    print()
    print(f'{"Class":<12} {"N":>5} {"Mean raw":>10} {"Mean lookup":>12} '
          f'{"Original MCSI":>15} {"New MCSI":>11} {"Δ":>7}')
    print('-' * 84)
    for cls in ['TR', 'F-Open', 'F-Std', 'FTR', 'Sporter']:
        vals = per_class.get(cls, [])
        if not vals:
            continue
        raws = [p + c/10 for p, c in vals]
        looks = [p + c for p, c in vals]
        orig = [mcsi(p, c, cls, ORIGINAL) for p, c in vals]
        new = [mcsi(p, c, cls, NEW) for p, c in vals]
        print(f'{cls:<12} {len(vals):>5} {mean(raws):>10.2f} {mean(looks):>12.2f} '
              f'{mean(orig):>15.2f} {mean(new):>11.2f} {mean(new)-mean(orig):>+7.2f}')
    print()

    # === Per-comp comparison ================================================
    print('=' * 84)
    print('PER-COMP — top-N mean MCSI per class under NEW formula')
    print('=' * 84)
    print()
    print(f'{"Comp":<14} {"TR":>8} {"F-Open":>8} {"F-Std":>8} {"FTR":>8} {"Sporter":>9}')
    print('-' * 84)

    per_comp_class = defaultdict(list)
    for (state, yr, cls, mn), vals in top_buckets.items():
        for pts, cs in vals:
            per_comp_class[(state, yr, cls)].append((pts, cs))

    comp_keys = sorted({(s, y) for (s, y, _) in per_comp_class}, key=lambda x: (-x[1], x[0]))
    for state, yr in comp_keys:
        line = f'{state} {yr:<8}'
        for cls in ['TR', 'F-Open', 'F-Std', 'FTR', 'Sporter']:
            vals = per_comp_class.get((state, yr, cls), [])
            if not vals:
                line += f' {"—":>8}' if cls != 'Sporter' else f' {"—":>9}'
                continue
            v = mean(mcsi(p, c, cls, NEW) for p, c in vals)
            line += f' {v:>8.2f}' if cls != 'Sporter' else f' {v:>9.2f}'
        print(line)
    print()

    print('=' * 84)
    print('PER-COMP — top-N mean MCSI per class under ORIGINAL 2014 formula')
    print('=' * 84)
    print()
    print(f'{"Comp":<14} {"TR":>8} {"F-Open":>8} {"F-Std":>8} {"FTR":>8} {"Sporter":>9}')
    print('-' * 84)
    for state, yr in comp_keys:
        line = f'{state} {yr:<8}'
        for cls in ['TR', 'F-Open', 'F-Std', 'FTR', 'Sporter']:
            vals = per_comp_class.get((state, yr, cls), [])
            if not vals:
                line += f' {"—":>8}' if cls != 'Sporter' else f' {"—":>9}'
                continue
            v = mean(mcsi(p, c, cls, ORIGINAL) for p, c in vals)
            line += f' {v:>8.2f}' if cls != 'Sporter' else f' {v:>9.2f}'
        print(line)
    print()

    # === Re-derive Sporter factor on this bigger dataset ====================
    print('=' * 84)
    print('FAIRNESS CHECK — does the 7-comp-derived NEW formula stand up on the')
    print('full 22-comp dataset with combined Sporter?')
    print('=' * 84)
    print()

    # F-class target = mean of (F-Open, F-Std, FTR) top-N means under NEW
    fclass_means = []
    for cls in ['F-Open', 'F-Std', 'FTR']:
        if cls in per_class:
            fclass_means.append(mean(mcsi(p, c, cls, NEW) for p, c in per_class[cls]))
    fclass_target = mean(fclass_means)
    print(f'F-class top-N mean MCSI (target):  {fclass_target:.2f}')
    print()

    # Derive Sporter and TR factors that hit this target on the bigger dataset
    for cls in ['Sporter', 'TR']:
        vals = per_class.get(cls, [])
        if not vals:
            continue
        mean_lookup = mean(p + c for p, c in vals)
        # Keep offset fixed from NEW, derive mult
        _, offset = NEW[cls]
        needed_mult = (fclass_target - offset) / mean_lookup
        current_mult, _ = NEW[cls]
        diff = needed_mult - current_mult
        print(f'{cls:<10}  mean_lookup={mean_lookup:.2f}  current_mult={current_mult:.3f}  '
              f'needed_mult={needed_mult:.4f}  Δ={diff:+.4f}')


if __name__ == '__main__':
    main()
