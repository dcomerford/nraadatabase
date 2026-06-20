"""Inspect the real sample structure behind each discipline in K&Q 2024+ data.

Raw string count tells only part of the story. For calibration to be defensible we
also want:
  - many distinct shooters (so we're not just measuring 5 people's quirks)
  - many distinct (state, year, range) match cells (so we're not over-weighting one event)
  - top-40% mean stable across bootstrap resamples
"""
from collections import defaultdict
from statistics import mean, stdev
import random
from db import get_connection

random.seed(42)

QUERY = """
SELECT st.shooter_sid, st.match_number,
       CASE
         WHEN st.discipline LIKE 'TR-%' OR st.discipline = 'Division Open' THEN 'TR'
         WHEN st.discipline = 'F-Open' THEN 'F-Open'
         WHEN st.discipline LIKE 'F-Std-%' THEN 'F-Std'
         WHEN st.discipline = 'FTR' THEN 'FTR'
         WHEN st.discipline = 'Sporter-Open' THEN 'SO'
         WHEN st.discipline IN ('Sporter-PC','Sporter-Combined') THEN 'SP'
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

per_cat = defaultdict(list)              # cat -> list of (pts, cen, sid, match_key)
shooters_per_cat = defaultdict(set)
matches_per_cat = defaultdict(set)
buckets = defaultdict(list)
for sid, mn, cat, score, centres, state, yr in rows:
    if not cat: continue
    pts = int(float(score))
    match_key = (state, yr, mn)
    per_cat[cat].append((pts, int(centres), sid, match_key))
    shooters_per_cat[cat].add(sid)
    matches_per_cat[cat].add(match_key)
    buckets[(state, yr, cat, mn)].append((pts, int(centres)))

# Top-40% cohort per (state, year, cat, range)
TOP_PCT = 0.40; FLOOR_N = 5
top_per_cat = defaultdict(list)
for (state, yr, cat, mn), vals in buckets.items():
    vals_sorted = sorted(vals, reverse=True)
    n = max(FLOOR_N, int(round(len(vals_sorted) * TOP_PCT)))
    top_per_cat[cat].extend(vals_sorted[:n])

print('SAMPLE STRUCTURE  —  K&Q 2024+, NSW/VIC/QLD/NQ/SA/WA/NRAA')
print('=' * 92)
print(f'{"Cat":<6} {"All strings":>12} {"Top-40 cohort":>15} {"Distinct shooters":>20} '
      f'{"Distinct matches":>18}')
print('-' * 92)
for cat in ['TR', 'F-Open', 'F-Std', 'FTR', 'SO', 'SP']:
    print(f'{cat:<6} {len(per_cat[cat]):>12} {len(top_per_cat[cat]):>15} '
          f'{len(shooters_per_cat[cat]):>20} {len(matches_per_cat[cat]):>18}')

print()
print('BOOTSTRAP STABILITY of top-40 mean MCSI (1000 resamples, V4 factors)')
print('=' * 92)
print(f'{"Cat":<6} {"Mean":>10} {"Std err":>10} {"95% CI":>22} {"CI width":>12}')
print('-' * 92)

CONV = {'TR':1.2,'F-Open':1.0,'F-Std':1.0,'FTR':1.0,'SO':1.2,'SP':1.2}
V4 = {'TR':1.42,'F-Open':1.42,'F-Std':1.46,'FTR':1.45,'SO':1.40,'SP':1.41}

def mcsi(cat, pts, cen):
    return (pts * CONV[cat] + cen * 0.7) * V4[cat]

for cat in ['TR', 'F-Open', 'F-Std', 'FTR', 'SO', 'SP']:
    cohort = top_per_cat[cat]
    vals = [mcsi(cat, p, c) for p, c in cohort]
    n = len(vals)
    boots = []
    for _ in range(1000):
        sample = [vals[random.randrange(n)] for _ in range(n)]
        boots.append(sum(sample)/n)
    boots.sort()
    m = mean(boots); se = stdev(boots)
    lo, hi = boots[25], boots[975]
    print(f'{cat:<6} {m:>10.3f} {se:>10.4f} {lo:>10.3f} - {hi:>8.3f} {hi-lo:>12.3f}')

print()
print('INTERPRETATION')
print('=' * 92)
print('A 95% CI width of ~0.5 MCSI ⇒ factor uncertainty of ±0.3% — fine for calibration.')
print('A CI width of >2.0 MCSI    ⇒ factor uncertainty of ±1.5%+ — comparable to the')
print('  factor differences we are trying to detect ⇒ unreliable for tuning.')
