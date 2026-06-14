"""Fairness audit of the current 2014 linear MCSI formula.

Premise: a top-of-class shooter in each discipline is near their respective ceiling.
If the formula is fair, top-N MCSI per string should be SIMILAR across disciplines.
If there's a gap, the discipline-specific multiplier/offset is mis-calibrated.

For each (comp, discipline, range):
  - take top 20% with floor of 5
  - compute current MCSI per string
  - report per-discipline mean / median / max of those top-N scores

Then derive what Sporter-Open and Sporter-PC multipliers WOULD have to be to
match the F-class top-N median, taken across all 7 comps.
"""
from collections import defaultdict
from statistics import mean, median
from db import get_connection

LINEAR_PARAMS = {
    'F-Open':       (1.42, 1.8),
    'F-Std':        (1.42, 1.8),
    'FTR':          (1.42, 1.8),
    'TR':           (1.62, 8.4),
    'Sporter-Open': (1.50, 12.0),
    'Sporter-PC':   (1.50, 12.0),
}

TOP_PCT = 0.20
FLOOR_N = 5


def mcsi_current(pts, centres, discipline):
    mult, offset = LINEAR_PARAMS[discipline]
    return (pts + centres) * mult + offset


QUERY = """
WITH subset AS (
  SELECT st.shooter_sid, st.match_number,
         CASE WHEN st.discipline LIKE 'TR-%' THEN 'TR'
              WHEN st.discipline LIKE 'F-Std-%' THEN 'F-Std'
              ELSE st.discipline END AS class,
         st.score, st.centres,
         s.code AS state_code, c.year
  FROM strings st
  JOIN competitions c USING(competition_id)
  JOIN states s USING(state_id)
  WHERE st.is_kings_queens AND (
    (s.code IN ('VRA','NQRA')   AND c.year=2026) OR
    (s.code IN ('NSWRA','QRA')  AND c.year=2025) OR
    (s.code IN ('VRA','NSWRA','QRA') AND c.year=2024)
  )
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
       s.shooter_sid, s.score, COALESCE(s.centres, 0) AS centres
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

    # Bucket per (comp, class, match)
    buckets = defaultdict(list)
    for state, yr, cls, mn, sid, score, centres in rows:
        pts = int(float(score))
        buckets[(state, yr, cls, mn)].append((float(score), pts, int(centres)))

    # Take top 20% with floor 5 from each bucket — these are the top-of-class strings
    top_buckets = {}
    for key, vals in buckets.items():
        # already sorted desc by score from SQL
        n = max(FLOOR_N, int(round(len(vals) * TOP_PCT)))
        n = min(n, len(vals))
        top_buckets[key] = vals[:n]

    # === ANALYSIS 1: per-class MCSI distribution of top-N strings ============
    # Aggregate top-N strings across all comps and ranges into per-class lists
    per_class_mcsi = defaultdict(list)
    per_class_raw = defaultdict(list)
    per_class_lookup = defaultdict(list)
    for (state, yr, cls, mn), vals in top_buckets.items():
        for raw, pts, cs in vals:
            per_class_mcsi[cls].append(mcsi_current(pts, cs, cls))
            per_class_raw[cls].append(raw)
            per_class_lookup[cls].append(pts + cs)

    print('=' * 78)
    print('FAIRNESS AUDIT — current 2014 linear MCSI vs top-of-class performance')
    print('=' * 78)
    print()
    print(f'Top performers = top {int(TOP_PCT*100)}% (floor {FLOOR_N}) per (comp, discipline, range)')
    print(f'Dataset: 7 comps (2024-2026), full-coverage shooters only')
    print()
    print(f'{"Class":<14} {"N":>5} {"Mean MCSI":>10} {"Median":>9} {"Max":>9} '
          f'{"Mean raw":>9} {"Mean lookup":>11}')
    print('-' * 78)
    for cls in ['TR', 'F-Open', 'F-Std', 'FTR', 'Sporter-Open', 'Sporter-PC']:
        if cls not in per_class_mcsi:
            continue
        msci = per_class_mcsi[cls]
        raw = per_class_raw[cls]
        lk = per_class_lookup[cls]
        print(f'{cls:<14} {len(msci):>5} {mean(msci):>10.2f} {median(msci):>9.2f} '
              f'{max(msci):>9.2f} {mean(raw):>9.2f} {mean(lk):>11.2f}')
    print()

    # === ANALYSIS 2: per-comp comparison =====================================
    print('=' * 78)
    print('Per-comp comparison: top-N MEAN MCSI per discipline')
    print('=' * 78)
    print()
    print(f'{"Comp":<12} {"TR":>8} {"F-Open":>8} {"F-Std":>8} {"FTR":>8} '
          f'{"SO":>8} {"SPC":>8}')
    print('-' * 78)
    comp_keys = sorted({(s,y) for (s,y,_,_) in top_buckets}, key=lambda x: (-x[1], x[0]))
    for state, yr in comp_keys:
        line = f'{state} {yr:<6}'
        for cls in ['TR', 'F-Open', 'F-Std', 'FTR', 'Sporter-Open', 'Sporter-PC']:
            vals = []
            for (s, y, c, m), strings in top_buckets.items():
                if s == state and y == yr and c == cls:
                    for raw, pts, cs in strings:
                        vals.append(mcsi_current(pts, cs, cls))
            line += f' {mean(vals):>8.2f}' if vals else f' {"—":>8}'
        print(line)
    print()

    # === ANALYSIS 3: Derive fair Sporter multipliers/offsets ================
    # Target: match the F-class top-N mean MCSI (since F-class formula was calibrated in 2014).
    # F-Open, F-Std, FTR all use mult=1.42 offset=1.8. Use their mean MCSI as target.
    print('=' * 78)
    print('Deriving fair Sporter factors')
    print('=' * 78)
    print()

    # Target benchmark: median of (top-N mean MCSI per class) across F-Open, F-Std, FTR, TR
    targets = {}
    for cls in ['TR', 'F-Open', 'F-Std', 'FTR']:
        if cls in per_class_mcsi:
            targets[cls] = mean(per_class_mcsi[cls])
    fclass_target = mean([targets['F-Open'], targets['F-Std'], targets['FTR']])
    print(f'F-class top-N mean MCSI (target):  {fclass_target:.2f}')
    print(f'TR     top-N mean MCSI:            {targets["TR"]:.2f}')
    print()

    # Sporter current mean
    for cls in ['Sporter-Open', 'Sporter-PC']:
        if cls not in per_class_mcsi:
            continue
        current_mean = mean(per_class_mcsi[cls])
        mean_raw = mean(per_class_raw[cls])
        mean_lookup = mean(per_class_lookup[cls])
        print(f'{cls}:')
        print(f'  current top-N mean MCSI: {current_mean:.2f}  '
              f'(mult=1.50  offset=12.0  mean raw={mean_raw:.2f}  mean lookup={mean_lookup:.2f})')

        # Solve for new mult/offset that hits f-class target.
        # Two unknowns, one equation — need an assumption. Option 1: keep offset fixed, adjust mult.
        # Option 2: keep mult fixed at 1.42 (match F-classes), adjust offset.
        # Option 3: fit (mult, offset) so both target AND a low-end anchor are matched.

        # OPTION A: keep offset at 12.0, adjust multiplier so mean MCSI hits f-class target
        # current MCSI = lookup * mult + 12
        # target MCSI = lookup * new_mult + 12
        # new_mult = (target - 12) / lookup
        new_mult_a = (fclass_target - 12.0) / mean_lookup
        print(f'  → option A (keep offset=12): mult should be {new_mult_a:.3f}')

        # OPTION B: keep multiplier at 1.50, adjust offset
        # target = lookup * 1.50 + new_offset
        new_offset_b = fclass_target - mean_lookup * 1.50
        print(f'  → option B (keep mult=1.50): offset should be {new_offset_b:.2f}')

        # OPTION C: align Sporter to F-class formula entirely (mult=1.42, offset=?)
        # target = lookup * 1.42 + new_offset
        new_offset_c = fclass_target - mean_lookup * 1.42
        print(f'  → option C (mult=1.42 like F-classes): offset should be {new_offset_c:.2f}')
        print()

    # === ANALYSIS 4: validate by simulating new formulas =====================
    print('=' * 78)
    print('Simulation — Sporter top-N mean MCSI under each proposed fix')
    print('=' * 78)
    print()
    print(f'{"Class":<14} {"Current":>10} {"OptA":>10} {"OptB":>10} {"OptC":>10} '
          f'{"Target":>10}')
    print('-' * 78)
    for cls in ['Sporter-Open', 'Sporter-PC']:
        if cls not in per_class_mcsi:
            continue
        mean_lookup = mean(per_class_lookup[cls])
        new_mult_a = (fclass_target - 12.0) / mean_lookup
        new_offset_b = fclass_target - mean_lookup * 1.50
        new_offset_c = fclass_target - mean_lookup * 1.42

        # Simulate each
        scores_curr = per_class_mcsi[cls]
        scores_a = [(l) * new_mult_a + 12.0 for l in per_class_lookup[cls]]
        scores_b = [(l) * 1.50 + new_offset_b for l in per_class_lookup[cls]]
        scores_c = [(l) * 1.42 + new_offset_c for l in per_class_lookup[cls]]
        print(f'{cls:<14} {mean(scores_curr):>10.2f} {mean(scores_a):>10.2f} '
              f'{mean(scores_b):>10.2f} {mean(scores_c):>10.2f} {fclass_target:>10.2f}')


if __name__ == '__main__':
    main()
