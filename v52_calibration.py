"""V5.2 calibration — K&Q only, EXCLUDING WARA.

Rationale: WA Sporter is unusually strong (regional culture + different conditions)
and WA F-class is weaker than East-coast F-class, especially at prize meets.
Including WA data drags the Sporter factor down artificially. V5.2 uses
only East-coast / national K&Q data for a balanced cross-discipline calibration.

Same V4/V5 structure: Adjusted = (Score × Conversion + Centres × 0.7) × Factor.
Sporter Open + Sporter Production still merged into one S factor.
"""
from collections import defaultdict
from statistics import mean, stdev
import random
from db import get_connection

random.seed(42)

TOP_PCT = 0.40
FLOOR_N = 5

# Existing V5 factors for comparison
V5 = {'TR':1.412, 'F-Open':1.406, 'F-Std':1.475, 'FTR':1.450, 'S':1.383}
V51 = {'TR':1.403, 'F-Open':1.404, 'F-Std':1.484, 'FTR':1.444, 'S':1.325}
CONV = {'TR':1.2, 'F-Open':1.0, 'F-Std':1.0, 'FTR':1.0, 'S':1.2}
CENTRE_W = 0.7
CATS = ['TR','F-Open','F-Std','FTR','S']

# NOTE: WARA excluded; NRAA (National) kept since it's the federal championship pool
QUERY = """
SELECT s.code, c.year, st.match_number,
  CASE
    WHEN st.discipline LIKE 'TR-%' OR st.discipline = 'Division Open' THEN 'TR'
    WHEN st.discipline = 'F-Open' THEN 'F-Open'
    WHEN st.discipline LIKE 'F-Std-%' THEN 'F-Std'
    WHEN st.discipline = 'FTR' THEN 'FTR'
    WHEN st.discipline IN ('Sporter-Open','Sporter-PC','Sporter-Combined') THEN 'S'
  END AS cat,
  st.score, COALESCE(st.centres,0)
FROM strings st
JOIN competitions c USING(competition_id)
JOIN states s USING(state_id)
WHERE st.is_kings_queens AND c.year >= 2024
  AND s.code IN ('NSWRA','VRA','QRA','NQRA','SARA','NRAA')   -- WARA excluded
"""

conn = get_connection(); cur = conn.cursor(); cur.execute(QUERY)
rows = cur.fetchall(); conn.close()
print(f'K&Q East+National strings (excl WARA): {len(rows)}')

buckets = defaultdict(list)
shooters_per_cat = defaultdict(set)
for state, yr, mn, cat, score, cen in rows:
    if not cat: continue
    buckets[(state, yr, cat, mn)].append((int(float(score)), int(cen)))

top_per_cat = defaultdict(list)
for (state, yr, cat, mn), vals in buckets.items():
    if len(vals) < FLOOR_N: continue
    s = sorted(vals, reverse=True)
    n = max(FLOOR_N, int(round(len(s)*TOP_PCT)))
    top_per_cat[cat].extend(s[:n])

print('\nTop-40 cohort sizes:')
for c in CATS:
    print(f'  {c:<8} N={len(top_per_cat[c])}')

def bootstrap(vals, n=1000):
    out = []
    L = len(vals)
    for _ in range(n):
        s = [vals[random.randrange(L)] for _ in range(L)]
        out.append(sum(s)/L)
    out.sort()
    return sum(out)/n, out[int(n*0.025)], out[int(n*0.975)]

# Pre-factor means + target
pre = {}
for c in CATS:
    pre[c] = mean(p*CONV[c] + cen*CENTRE_W for p, cen in top_per_cat[c])

target = mean(pre[c]*V5[c] for c in ('F-Open','F-Std','FTR'))
print(f'\nF-class fair target (mean of FO/FS/FTR post-V5 means): {target:.3f}')

print('\nDerived V5.2 factors:')
print(f'{"Cat":<8} {"Pre-mean":>10} {"V5":>8} {"V5.1":>8} {"V5.2":>8} {"Δ vs V5":>10} {"Δ vs V5.1":>10}')
print('-'*70)
v52 = {}
for c in CATS:
    f = round(target / pre[c], 3)
    v52[c] = f
    print(f'{c:<8} {pre[c]:>10.3f} {V5[c]:>8.3f} {V51[c]:>8.3f} {f:>8.3f} {f-V5[c]:>+10.3f} {f-V51[c]:>+10.3f}')

print('\nBootstrap 95% CI under V5.2 factors:')
print(f'{"Cat":<8} {"Mean MCSI":>10} {"95% CI":>22} {"CI width":>10}')
for c in CATS:
    vals = [(p*CONV[c] + cen*CENTRE_W)*v52[c] for p, cen in top_per_cat[c]]
    m, lo, hi = bootstrap(vals)
    print(f'{c:<8} {m:>10.3f} {lo:>10.3f} - {hi:>8.3f} {hi-lo:>10.3f}')

print('\n' + '='*70)
print('V5.2 FINAL FACTOR TABLE — K&Q East+National only, WARA excluded')
print('='*70)
for c in CATS:
    print(f'  {c:<8} {v52[c]:.3f}')
