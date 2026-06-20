"""V5.3 calibration — K&Q + East-coast prize meets, all events all-5, NO WA.

Pool composition:
  - K&Q events from NSWRA/VRA/QRA/NQRA/SARA/NRAA where all 5 disciplines present
    (13 of 14 qualify; NRAA 2025 dropped — no Sporter strings)
  - Prize meets that are NON-WA AND have all 5 disciplines present
    (1 meet: Atherton PM 2024)

Top-40% (floor 5) per bucket; pooled per discipline; factors equalize each
discipline's mean to F-class fair target.
"""
import json
from collections import defaultdict
from statistics import mean
import random
from db import get_connection

random.seed(42)

CONV = {'TR':1.2,'F-Open':1.0,'F-Std':1.0,'FTR':1.0,'S':1.2}
CATS = ['TR','F-Open','F-Std','FTR','S']
TOP_PCT = 0.40
FLOOR_N = 5

# Existing factors for comparison
V5  = {'TR':1.412, 'F-Open':1.406, 'F-Std':1.475, 'FTR':1.450, 'S':1.383}
V51 = {'TR':1.403, 'F-Open':1.404, 'F-Std':1.484, 'FTR':1.444, 'S':1.325}
V52 = {'TR':1.414, 'F-Open':1.410, 'F-Std':1.472, 'FTR':1.448, 'S':1.366}

# ---------- 1. Pull K&Q, restricted to East+National + all-5 events ----------
conn = get_connection(); cur = conn.cursor()
cur.execute("""
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
  AND s.code IN ('NSWRA','VRA','QRA','NQRA','SARA','NRAA')
""")
kq_rows = cur.fetchall(); conn.close()

# Group strings per (state, year, cat) for all-5 check
event_disc_counts = defaultdict(lambda: defaultdict(int))
for state, yr, mn, cat, sc, cen in kq_rows:
    if not cat: continue
    event_disc_counts[(state, yr)][cat] += 1

qualifying_events = {ev for ev, dc in event_disc_counts.items()
                     if all(dc.get(c, 0) >= 5 for c in CATS)}
print(f'K&Q qualifying events (all 5 disciplines): {len(qualifying_events)}')
for ev in sorted(qualifying_events):
    print(f'  {ev[0]} {ev[1]}')

# Bucket K&Q strings, only from qualifying events
kq_buckets = defaultdict(list)
kq_string_count = 0
for state, yr, mn, cat, sc, cen in kq_rows:
    if not cat: continue
    if (state, yr) not in qualifying_events: continue
    kq_buckets[(state, yr, cat, mn)].append((int(float(sc)), int(cen)))
    kq_string_count += 1
print(f'K&Q strings retained: {kq_string_count}')

# ---------- 2. Pull prize-meet rows, non-WA + all-5 only ----------
with open('/tmp/prize_meet_rows.json') as f:
    pm_rows = json.load(f)
cat_map = {'TR':'TR','FO':'F-Open','FS':'F-Std','FTR':'FTR','SO':'S','SP':'S'}

# Determine each meet's region
meet_states = defaultdict(lambda: defaultdict(int))
for r in pm_rows:
    meet_states[r['meet']][r['state']] += 1
def region(meet):
    states = meet_states[meet]
    wa = states.get('WA', 0)
    east = sum(c for s, c in states.items() if s in ('NSW','QLD','VIC','SA','NT','TAS','ACT'))
    return 'WA' if wa > east else 'East'

# Aggregate per (meet, cat) to check all-5
meet_disc_counts = defaultdict(lambda: defaultdict(int))
for r in pm_rows:
    c = cat_map.get(r['discipline'])
    if not c: continue
    meet_disc_counts[r['meet']][c] += 1

east_all5_meets = []
for meet, dc in meet_disc_counts.items():
    if region(meet) == 'East' and all(dc.get(c, 0) >= 5 for c in CATS):
        east_all5_meets.append(meet)
print(f'\nEast-coast prize meets with all 5 disciplines: {len(east_all5_meets)}')
for m in east_all5_meets:
    print(f'  {m}')

pm_buckets = defaultdict(list)
pm_string_count = 0
for r in pm_rows:
    if r['meet'] not in east_all5_meets: continue
    c = cat_map.get(r['discipline'])
    if not c: continue
    pm_buckets[(r['meet'], c, r['match'])].append((r['score_int'], r['centres']))
    pm_string_count += 1
print(f'Prize-meet strings retained: {pm_string_count}')
print(f'\nTOTAL pool: {kq_string_count + pm_string_count} strings')

# ---------- 3. Combine buckets, derive factors ----------
all_buckets = {}
for k, v in kq_buckets.items(): all_buckets[('kq',) + k] = v
for k, v in pm_buckets.items(): all_buckets[('pm',) + k] = v

def top40(buckets, min_n=5):
    out = defaultdict(list)
    for k, v in buckets.items():
        if len(v) < min_n: continue
        s = sorted(v, reverse=True)
        n = max(FLOOR_N, int(round(len(s)*TOP_PCT)))
        # cat at index 3 for kq (src, state, year, cat, mn), index 2 for pm (src, meet, cat, match)
        if k[0] == 'kq': cat = k[3]
        else: cat = k[2]
        out[cat].extend(s[:n])
    return out

top = top40(all_buckets, min_n=5)
print('\nTop-40 cohort sizes:')
for c in CATS:
    print(f'  {c:<8} N={len(top[c])}')

# Pre-factor means
pre = {c: mean(p*CONV[c] + cen*0.7 for p, cen in top[c]) for c in CATS}

# F-class fair target (use V5 factors)
target = mean(pre[c]*V5[c] for c in ('F-Open','F-Std','FTR'))
print(f'\nF-class fair target (mean FO/FS/FTR post-V5): {target:.3f}')

v53 = {c: round(target / pre[c], 3) for c in CATS}

print('\nV5.3 vs prior factor versions:')
print(f'{"Cat":<8} {"V5":>8} {"V5.1":>8} {"V5.2":>8} {"V5.3":>8} {"Δ vs V5":>10} {"Δ vs V5.2":>11}')
print('-'*70)
for c in CATS:
    print(f'{c:<8} {V5[c]:>8.3f} {V51[c]:>8.3f} {V52[c]:>8.3f} {v53[c]:>8.3f} '
          f'{v53[c]-V5[c]:>+10.3f} {v53[c]-V52[c]:>+11.3f}')

# Bootstrap CIs on post-factor MCSI
def bootstrap(vals, n_boot=2000):
    L = len(vals); means = []
    for _ in range(n_boot):
        s = [vals[random.randrange(L)] for _ in range(L)]
        means.append(sum(s)/L)
    means.sort()
    return sum(means)/n_boot, means[int(n_boot*0.025)], means[int(n_boot*0.975)]

print('\nBootstrap 95% CI on top-40 mean MCSI under V5.3 factors:')
print(f'{"Cat":<8} {"Mean MCSI":>10} {"95% CI":>22} {"CI ±":>10}')
print('-'*60)
for c in CATS:
    vals = [(p*CONV[c] + cen*0.7)*v53[c] for p, cen in top[c]]
    m, lo, hi = bootstrap(vals)
    print(f'{c:<8} {m:>10.3f} {lo:>10.3f} - {hi:>8.3f} ±{(hi-lo)/2:>9.4f}')

print('\n' + '='*70)
print('V5.3 FINAL FACTOR TABLE')
print('K&Q East+National (13 events, all-5) + Atherton PM 2024')
print('='*70)
for c in CATS:
    print(f'  {c:<8} {v53[c]:.3f}')
