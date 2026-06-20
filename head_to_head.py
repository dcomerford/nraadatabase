"""Head-to-head: Original 2014 linear formula vs the proposed (raw×1.2 + centres×0.7)×factor.

Applied to the FULL 22-comp 2024-2026 dataset with Sporter (Open+PC+Combined) merged.

For F-class (60-pt) disciplines: no raw conversion needed — formula becomes (raw + centres) × factor.
For 50-pt disciplines (TR, Sporter): use (raw × 1.2 + centres × 0.7) × factor.

We compute:
1. Top-N mean MCSI per class under each formula
2. The factor each discipline needs to hit the calibration target
"""
from collections import defaultdict
from statistics import mean
from db import get_connection

TOP_PCT = 0.40
FLOOR_N = 5

# Original 2014 linear formula
ORIGINAL = {
    'F-Open':  (1.42, 1.8),
    'F-Std':   (1.42, 1.8),
    'FTR':     (1.42, 1.8),
    'TR':      (1.62, 8.4),
    'Sporter': (1.50, 12.0),
}

# Proposed formula — factor 1.030 as in the example
PROPOSED_FACTOR_50PT = 1.030
PROPOSED_FACTOR_FCLASS = 1.000  # No conversion needed for 60-pt; start with 1.0

def mcsi_original(pts, centres, cls):
    mult, offset = ORIGINAL[cls]
    return (pts + centres) * mult + offset

def mcsi_proposed(pts, centres, cls, factor=None):
    """Apply the proposed conversion structure.
    - 50-pt disciplines (TR, Sporter): (raw × 1.2 + centres × 0.7) × factor
    - 60-pt disciplines (F-class):     (raw × 1.0 + centres × 1.0) × factor
    """
    if cls in ('TR', 'Sporter'):
        f = factor if factor is not None else PROPOSED_FACTOR_50PT
        return (pts * 1.2 + centres * 0.7) * f
    else:
        f = factor if factor is not None else PROPOSED_FACTOR_FCLASS
        return (pts + centres) * f


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


def main():
    conn = get_connection(); cur = conn.cursor(); cur.execute(QUERY)
    rows = cur.fetchall(); conn.close()

    buckets = defaultdict(list)
    for state, yr, cls, mn, sid, score, centres in rows:
        pts = int(float(score))
        buckets[(state, yr, cls, mn)].append((pts, int(centres)))

    top_buckets = {}
    for k, vals in buckets.items():
        n = max(FLOOR_N, int(round(len(vals) * TOP_PCT)))
        top_buckets[k] = vals[:min(n, len(vals))]

    per_class = defaultdict(list)
    for (state, yr, cls, mn), vals in top_buckets.items():
        for pts, cs in vals:
            per_class[cls].append((pts, cs))

    print('=' * 88)
    print('HEAD TO HEAD — 22 comps, Sporter combined, top-40%/floor-5 cohort')
    print('=' * 88)
    print()

    # === Original formula vs Proposed at factor 1.030 (50-pt) / 1.0 (F-class)
    print('1) Original 2014 vs Proposed formula AT EXAMPLE FACTORS')
    print('   50-pt formula:  (raw × 1.2 + centres × 0.7) × 1.030')
    print('   F-class formula:  (raw × 1.0 + centres × 1.0) × 1.000')
    print()
    print(f'{"Class":<10} {"N":>5} {"Mean raw":>10} {"Original MCSI":>15} '
          f'{"Proposed MCSI":>15} {"Δ":>9}')
    print('-' * 88)
    for cls in ['TR', 'F-Open', 'F-Std', 'FTR', 'Sporter']:
        vals = per_class.get(cls, [])
        if not vals:
            continue
        raws = [p + c/10 for p, c in vals]
        orig = [mcsi_original(p, c, cls) for p, c in vals]
        prop = [mcsi_proposed(p, c, cls) for p, c in vals]
        print(f'{cls:<10} {len(vals):>5} {mean(raws):>10.2f} {mean(orig):>15.2f} '
              f'{mean(prop):>15.2f} {mean(prop)-mean(orig):>+9.2f}')
    print()

    # === Derive ideal per-discipline factor under proposed formula ==========
    # First derive the "fair target" — use F-class average under proposed at factor 1.0
    fc_means_pre = {}
    for cls in ['F-Open', 'F-Std', 'FTR']:
        vals = per_class.get(cls, [])
        if vals:
            fc_means_pre[cls] = mean(p + c for p, c in vals)
    target = mean(fc_means_pre.values())
    print(f'2) Calibration target = F-class mean of (raw + centres): {target:.3f}')
    print()
    print('   Derived per-discipline factor to hit target under proposed formula:')
    print()
    print(f'{"Class":<10} {"Formula":<45} {"Pre-factor mean":>16} {"Factor":>8}')
    print('-' * 88)

    final_factors = {}
    for cls in ['TR', 'Sporter']:
        vals = per_class.get(cls, [])
        if not vals:
            continue
        pre = [p * 1.2 + c * 0.7 for p, c in vals]
        mean_pre = mean(pre)
        f = target / mean_pre
        final_factors[cls] = f
        print(f'{cls:<10} {"(raw × 1.2 + centres × 0.7) × factor":<45} '
              f'{mean_pre:>16.3f} {f:>8.4f}')

    for cls in ['F-Open', 'F-Std', 'FTR']:
        vals = per_class.get(cls, [])
        if not vals:
            continue
        pre = [p + c for p, c in vals]
        mean_pre = mean(pre)
        f = target / mean_pre
        final_factors[cls] = f
        print(f'{cls:<10} {"(raw + centres) × factor":<45} '
              f'{mean_pre:>16.3f} {f:>8.4f}')
    print()

    # === Final comparison with derived factors ===========================
    print('3) FINAL — proposed formula with derived per-discipline factors')
    print()
    print(f'{"Class":<10} {"N":>5} {"Original MCSI":>15} {"Proposed MCSI":>15} '
          f'{"Δ":>9} {"Target":>9}')
    print('-' * 88)
    for cls in ['TR', 'F-Open', 'F-Std', 'FTR', 'Sporter']:
        vals = per_class.get(cls, [])
        if not vals:
            continue
        f = final_factors.get(cls, 1.0)
        orig = [mcsi_original(p, c, cls) for p, c in vals]
        prop = [mcsi_proposed(p, c, cls, factor=f) for p, c in vals]
        print(f'{cls:<10} {len(vals):>5} {mean(orig):>15.2f} {mean(prop):>15.2f} '
              f'{mean(prop)-mean(orig):>+9.2f} {target:>9.3f}')
    print()

    # === Worked examples for the panel ====================================
    print('=' * 88)
    print('Worked examples — final proposed formula (with derived factors)')
    print('=' * 88)
    print()
    print(f'{"Score":<14} {"Discipline":<10} {"Formula":<45} {"Result":>10}')
    print('-' * 88)

    examples = [
        # (score description, raw_pts, centres, class)
        ('50.10 perfect', 50, 10, 'TR'),
        ('60.10 perfect', 60, 10, 'F-Open'),
        ('60.10 perfect', 60, 10, 'F-Std'),
        ('60.10 perfect', 60, 10, 'FTR'),
        ('50.10 perfect', 50, 10, 'Sporter'),
        ('49.10',         49, 10, 'TR'),
        ('50.8',          50, 8,  'TR'),
        ('149.10 (3 strings)', 149, 10, 'Sporter'),  # from the example
    ]
    for desc, pts, cs, cls in examples:
        f = final_factors.get(cls, 1.0)
        result = mcsi_proposed(pts, cs, cls, factor=f)
        if cls in ('TR', 'Sporter'):
            formula = f'(({pts}×1.2)+({cs}×0.7))×{f:.4f}'
        else:
            formula = f'(({pts})+({cs}))×{f:.4f}'
        print(f'{desc:<14} {cls:<10} {formula:<45} {result:>10.3f}')


if __name__ == '__main__':
    main()
