"""Test the proposed 50→60 conversion formula:
    MCSI = (raw × 1.2 + centres × 0.7) × factor

Applied to TR, Sporter-Open, Sporter-PC (all 50-pt targets) in our top-20% cohort.
F-classes (60-pt) are the baseline — no conversion needed; they sit at (raw + centres).

The question: does this formula land TR/Sporter at the same top-N mean MCSI as F-classes?
And if not, what factor would?
"""
from collections import defaultdict
from statistics import mean, median
from db import get_connection

TOP_PCT = 0.40
FLOOR_N = 5

# Factor used in the example
EXAMPLE_FACTOR = 1.030

QUERY = """
WITH subset AS (
  SELECT st.shooter_sid, st.match_number,
         CASE WHEN st.discipline LIKE 'TR-%' THEN 'TR'
              WHEN st.discipline LIKE 'F-Std-%' THEN 'F-Std'
              ELSE st.discipline END AS class,
         st.score, st.centres,
         s.code AS state_code, c.year, st.target_max
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
       s.shooter_sid, s.score, COALESCE(s.centres, 0) AS centres, s.target_max
FROM subset s
JOIN full_shooters f USING (state_code, year, class, shooter_sid)
ORDER BY s.state_code, s.year, s.class, s.match_number, s.score DESC;
"""


def is_50pt(target_max):
    return target_max == 5  # 50-pt target → max shot value 5


# Two MCSI candidates for 50-pt disciplines
def new_formula_50pt(raw_pts, centres, factor):
    """Proposed formula. raw_pts is the integer points; centres is the centre count."""
    return (raw_pts * 1.2 + centres * 0.7) * factor


def fclass_baseline(raw_pts, centres):
    """For F-classes (60-pt), no conversion — straight lookup score."""
    return raw_pts + centres


def main():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(QUERY)
    rows = cur.fetchall()
    conn.close()

    # Bucket per (comp, class, match)
    buckets = defaultdict(list)
    for state, yr, cls, mn, sid, score, centres, target_max in rows:
        pts = int(float(score))
        buckets[(state, yr, cls, mn)].append((pts, int(centres), target_max))

    # Take top 20% with floor 5
    top_buckets = {}
    for key, vals in buckets.items():
        n = max(FLOOR_N, int(round(len(vals) * TOP_PCT)))
        top_buckets[key] = vals[:min(n, len(vals))]

    # Accumulate top-N strings per class
    per_class = defaultdict(list)  # cls → [(raw_pts, centres, target_max), ...]
    for (state, yr, cls, mn), vals in top_buckets.items():
        for pts, cs, tm in vals:
            per_class[cls].append((pts, cs, tm))

    # === Baseline: F-class top-N mean (raw + centres) =======================
    fclass_combined = []
    for cls in ['F-Open', 'F-Std', 'FTR']:
        for pts, cs, tm in per_class.get(cls, []):
            fclass_combined.append(pts + cs)
    fclass_target = mean(fclass_combined)
    print(f'F-class top-N mean (raw+centres):  {fclass_target:.3f}')
    print(f'   used as the calibration target for 50-pt disciplines')
    print()

    # === Test proposed formula at example factor 1.030 ======================
    print('=' * 78)
    print(f'PROPOSED FORMULA — (raw × 1.2 + centres × 0.7) × factor')
    print('=' * 78)
    print()
    print(f'{"Discipline":<14} {"N":>5} {"Top-N MCSI @1.030":>18} {"vs F-class target":>20}')
    print('-' * 78)
    for cls in ['TR', 'Sporter-Open', 'Sporter-PC', 'F-Open', 'F-Std', 'FTR']:
        vals = per_class.get(cls, [])
        if not vals:
            continue
        # 50-pt disciplines use the proposed formula; 60-pt use raw+centres
        scores = []
        for pts, cs, tm in vals:
            if is_50pt(tm):
                scores.append(new_formula_50pt(pts, cs, EXAMPLE_FACTOR))
            else:
                scores.append(fclass_baseline(pts, cs))
        m = mean(scores)
        delta = m - fclass_target
        sign = '+' if delta >= 0 else ''
        print(f'{cls:<14} {len(vals):>5} {m:>18.3f} {sign}{delta:>16.3f}')
    print()

    # === Derive the fair factor for each 50-pt discipline ===================
    print('=' * 78)
    print('Deriving the factor that makes each 50-pt discipline hit F-class target')
    print('=' * 78)
    print()
    print(f'{"Discipline":<14} {"Mean pre-factor":>18} {"Needed factor":>15} '
          f'{"vs example 1.030":>17}')
    print('-' * 78)
    for cls in ['TR', 'Sporter-Open', 'Sporter-PC']:
        vals = per_class.get(cls, [])
        if not vals:
            continue
        # Compute pre-factor mean: (raw × 1.2 + centres × 0.7) WITHOUT factor
        pre_factor = [(pts * 1.2 + cs * 0.7) for pts, cs, tm in vals if is_50pt(tm)]
        if not pre_factor:
            continue
        mean_pre = mean(pre_factor)
        # We want mean_pre × factor = fclass_target
        needed = fclass_target / mean_pre
        diff_from_example = needed - EXAMPLE_FACTOR
        sign = '+' if diff_from_example >= 0 else ''
        print(f'{cls:<14} {mean_pre:>18.3f} {needed:>15.4f}   '
              f'{sign}{diff_from_example:>15.4f}')
    print()

    # === Per-comp verification ==============================================
    print('=' * 78)
    print('Per-comp validation: top-N mean MCSI under proposed formula (factor=1.030)')
    print('=' * 78)
    print()
    print(f'{"Comp":<12} {"TR":>8} {"SO":>8} {"SPC":>8} '
          f'{"F-Open":>8} {"F-Std":>8} {"FTR":>8}')
    print('-' * 78)

    # Per (comp, class) top-N collected
    per_comp_class = defaultdict(list)
    for (state, yr, cls, mn), vals in top_buckets.items():
        for pts, cs, tm in vals:
            per_comp_class[(state, yr, cls)].append((pts, cs, tm))

    comp_keys = sorted({(s, y) for (s, y, _) in per_comp_class}, key=lambda x: (-x[1], x[0]))
    for state, yr in comp_keys:
        line = f'{state} {yr:<6}'
        for cls in ['TR', 'Sporter-Open', 'Sporter-PC', 'F-Open', 'F-Std', 'FTR']:
            vals = per_comp_class.get((state, yr, cls), [])
            if not vals:
                line += f' {"—":>8}'
                continue
            scores = []
            for pts, cs, tm in vals:
                if is_50pt(tm):
                    scores.append(new_formula_50pt(pts, cs, EXAMPLE_FACTOR))
                else:
                    scores.append(fclass_baseline(pts, cs))
            line += f' {mean(scores):>8.2f}'
        print(line)


if __name__ == '__main__':
    main()
