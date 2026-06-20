"""V5 calibration: V4 structure but Sporter Open + Sporter Production merged into
a single 'S' category sharing one factor.

Structure: Adjusted = (Score × Conversion + Centres × 0.7) × Factor
  Conversion = 1.2 for TR/S, 1.0 for F-class.

Calibrated at TOP 40% (floor 5) per (state, year, discipline, range) using
K&Q 2024+ data, states NSWRA/VRA/QRA/NQRA/SARA/WARA/NRAA.

Reports:
  1. Sample structure (strings / cohort / distinct shooters / matches) per cat
  2. Bootstrap 95% CI on top-40 mean MCSI under V4 factors
  3. Derived V5 factors that equalise each cat to the F-class fair target
  4. Bootstrap 95% CI under V5 factors
  5. Worked examples
"""
from collections import defaultdict
from statistics import mean, stdev
import random
from db import get_connection

random.seed(42)

TOP_PCT = 0.40
FLOOR_N = 5

# V4 baseline factors (for the diagnostic step)
V4 = {'TR': 1.42, 'F-Open': 1.42, 'F-Std': 1.46, 'FTR': 1.45, 'S': 1.405}  # S = mean of SO/SP V4
CONV = {'TR': 1.2, 'F-Open': 1.0, 'F-Std': 1.0, 'FTR': 1.0, 'S': 1.2}
CENTRE_W = 0.7

QUERY = """
SELECT st.shooter_sid, st.match_number,
       CASE
         WHEN st.discipline LIKE 'TR-%' OR st.discipline = 'Division Open' THEN 'TR'
         WHEN st.discipline = 'F-Open' THEN 'F-Open'
         WHEN st.discipline LIKE 'F-Std-%' THEN 'F-Std'
         WHEN st.discipline = 'FTR' THEN 'FTR'
         WHEN st.discipline IN ('Sporter-Open','Sporter-PC','Sporter-Combined') THEN 'S'
       END AS cat,
       st.score, COALESCE(st.centres, 0) AS centres,
       s.code AS state_code, c.year
FROM strings st
JOIN competitions c USING(competition_id)
JOIN states s USING(state_id)
WHERE st.is_kings_queens AND c.year >= 2024
  AND s.code IN ('NSWRA','VRA','QRA','NQRA','SARA','WARA','NRAA')
"""

conn = get_connection(); cur = conn.cursor(); cur.execute(QUERY)
rows = cur.fetchall(); conn.close()

shooters_per_cat = defaultdict(set)
matches_per_cat = defaultdict(set)
all_per_cat = defaultdict(list)
buckets = defaultdict(list)
for sid, mn, cat, score, centres, state, yr in rows:
    if not cat: continue
    pts = int(float(score))
    shooters_per_cat[cat].add(sid)
    matches_per_cat[cat].add((state, yr, mn))
    all_per_cat[cat].append((pts, int(centres)))
    buckets[(state, yr, cat, mn)].append((pts, int(centres)))

top_per_cat = defaultdict(list)
for (state, yr, cat, mn), vals in buckets.items():
    vals_sorted = sorted(vals, reverse=True)
    n = max(FLOOR_N, int(round(len(vals_sorted) * TOP_PCT)))
    top_per_cat[cat].extend(vals_sorted[:n])

CATS = ['TR', 'F-Open', 'F-Std', 'FTR', 'S']

print('=' * 96)
print('V5 CALIBRATION — Sporter Open + Sporter Production merged into single "S"')
print(f'Top {int(TOP_PCT*100)}% (floor {FLOOR_N}) per (state, year, discipline, range)')
print('K&Q 2024+, states NSWRA/VRA/QRA/NQRA/SARA/WARA/NRAA')
print('=' * 96)
print()
print('STEP 1 — Sample structure')
print('-' * 96)
print(f'{"Cat":<8} {"All strings":>12} {"Top-40 cohort":>15} {"Distinct shooters":>20} '
      f'{"Distinct matches":>18}')
print('-' * 96)
for cat in CATS:
    print(f'{cat:<8} {len(all_per_cat[cat]):>12} {len(top_per_cat[cat]):>15} '
          f'{len(shooters_per_cat[cat]):>20} {len(matches_per_cat[cat]):>18}')
print()


def mcsi(cat, pts, cen, factor=None):
    f = factor if factor is not None else V4[cat]
    return (pts * CONV[cat] + cen * CENTRE_W) * f


def bootstrap_ci(vals, n_boot=2000):
    n = len(vals)
    means = []
    for _ in range(n_boot):
        s = [vals[random.randrange(n)] for _ in range(n)]
        means.append(sum(s) / n)
    means.sort()
    m = sum(means) / len(means)
    se = stdev(means)
    return m, se, means[int(n_boot * 0.025)], means[int(n_boot * 0.975)]


print('STEP 2 — Bootstrap 95% CI on top-40 mean MCSI under V4 baseline')
print('-' * 96)
print(f'{"Cat":<8} {"Mean MCSI":>11} {"Std err":>10} {"95% CI":>22} {"CI width":>10} '
      f'{"Factor σ":>10}')
print('-' * 96)
cur_means_v4 = {}
for cat in CATS:
    vals = [mcsi(cat, p, c) for p, c in top_per_cat[cat]]
    m, se, lo, hi = bootstrap_ci(vals)
    cur_means_v4[cat] = m
    factor_se = se / m * V4[cat]
    print(f'{cat:<8} {m:>11.3f} {se:>10.4f} {lo:>10.3f} - {hi:>8.3f} {hi-lo:>10.3f} '
          f'±{factor_se:>9.4f}')
print()

# F-class fair target = mean of F-Open / F-Std / FTR means
target = mean(cur_means_v4[c] for c in ('F-Open', 'F-Std', 'FTR'))
print(f'F-class fair target (mean of FO/FS/FTR means): {target:.3f}')
print()

print('STEP 3 — Derived V5 factors that equalise each cat to the F-class fair target')
print('-' * 96)
print(f'{"Cat":<8} {"V4 factor":>10} {"V5 factor":>10} {"Δ":>8} {"Pre-factor mean":>18} '
      f'{"@V5 mean MCSI":>15}')
print('-' * 96)
v5 = {}
pre_means = {}
for cat in CATS:
    pre = [p * CONV[cat] + c * CENTRE_W for p, c in top_per_cat[cat]]
    pre_means[cat] = mean(pre)
    f_v5 = target / pre_means[cat]
    v5[cat] = round(f_v5, 3)
    new_m = pre_means[cat] * v5[cat]
    d = v5[cat] - V4[cat]
    print(f'{cat:<8} {V4[cat]:>10.3f} {v5[cat]:>10.3f} {d:>+8.3f} '
          f'{pre_means[cat]:>18.3f} {new_m:>15.3f}')
print()

print('STEP 4 — Bootstrap 95% CI on top-40 mean MCSI under DERIVED V5 factors')
print('-' * 96)
print(f'{"Cat":<8} {"Mean MCSI":>11} {"Std err":>10} {"95% CI":>22} {"CI width":>10} '
      f'{"Factor σ":>10}')
print('-' * 96)
for cat in CATS:
    vals = [mcsi(cat, p, c, factor=v5[cat]) for p, c in top_per_cat[cat]]
    m, se, lo, hi = bootstrap_ci(vals)
    factor_se = se / m * v5[cat]
    print(f'{cat:<8} {m:>11.3f} {se:>10.4f} {lo:>10.3f} - {hi:>8.3f} {hi-lo:>10.3f} '
          f'±{factor_se:>9.4f}')
print()

print('STEP 5 — Worked examples')
print('-' * 96)
for cat in CATS:
    pts = 50 if CONV[cat] == 1.2 else 60
    print(f'  {cat:<8}  {pts}.10 (perfect) → V4: {mcsi(cat,pts,10):.2f}   '
          f'V5: {mcsi(cat,pts,10,v5[cat]):.2f}')
print()
print('=' * 96)
print('V5 FINAL FACTOR TABLE')
print('=' * 96)
print(f'  TR            {v5["TR"]:.3f}')
print(f'  F-Open        {v5["F-Open"]:.3f}')
print(f'  F-Standard    {v5["F-Std"]:.3f}')
print(f'  FTR           {v5["FTR"]:.3f}')
print(f'  Sporter (any) {v5["S"]:.3f}   ← SO and SP share one factor in V5')
