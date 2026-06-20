"""V5 re-calibration on COMBINED K&Q + prize-meet data.

Uses the 31 all-5-discipline prize meets scraped from results.nraa.com.au
in addition to the existing K&Q 2024+ strings.

Bucketing: each (event, year, discipline, match_label) is a same-conditions bucket.
Top-40% per bucket (floor 5) selected; all top-40 cohorts pooled per discipline.

Low-number handling: configurable via MIN_BUCKET_N — buckets with fewer than this
many entries in a discipline are skipped (since "top 40%" of 4 = the top 2, which
doesn't represent the relative skill spread). Reports sensitivity across thresholds.
"""
import json
from collections import defaultdict
from statistics import mean, stdev
import random
from db import get_connection

random.seed(42)

TOP_PCT = 0.40
FLOOR_N = 5

V4 = {'TR': 1.42, 'F-Open': 1.42, 'F-Std': 1.46, 'FTR': 1.45, 'S': 1.405}
CURRENT_V5 = {'TR': 1.412, 'F-Open': 1.406, 'F-Std': 1.475, 'FTR': 1.450, 'S': 1.383}
CONV = {'TR': 1.2, 'F-Open': 1.0, 'F-Std': 1.0, 'FTR': 1.0, 'S': 1.2}
CENTRE_W = 0.7
CATS = ['TR', 'F-Open', 'F-Std', 'FTR', 'S']

CAT_MAP = {
    'TR': 'TR', 'FO': 'F-Open', 'FS': 'F-Std', 'FTR': 'FTR', 'SO': 'S', 'SP': 'S',
}

# ---------- 1. Pull K&Q strings ----------
KQ_QUERY = """
SELECT s.code AS state_code, c.year, st.match_number,
       CASE
         WHEN st.discipline LIKE 'TR-%' OR st.discipline = 'Division Open' THEN 'TR'
         WHEN st.discipline = 'F-Open' THEN 'F-Open'
         WHEN st.discipline LIKE 'F-Std-%' THEN 'F-Std'
         WHEN st.discipline = 'FTR' THEN 'FTR'
         WHEN st.discipline IN ('Sporter-Open','Sporter-PC','Sporter-Combined') THEN 'S'
       END AS cat,
       st.score, COALESCE(st.centres, 0) AS centres
FROM strings st
JOIN competitions c USING(competition_id)
JOIN states s USING(state_id)
WHERE st.is_kings_queens AND c.year >= 2024
  AND s.code IN ('NSWRA','VRA','QRA','NQRA','SARA','WARA','NRAA')
"""

conn = get_connection(); cur = conn.cursor(); cur.execute(KQ_QUERY)
kq_rows = cur.fetchall(); conn.close()
print(f'K&Q strings: {len(kq_rows)}')

# bucket: (source='kq', state, year, cat, match_num) -> list[(pts, centres)]
buckets = defaultdict(list)
for state, yr, mn, cat, score, centres in kq_rows:
    if not cat: continue
    pts = int(float(score))
    buckets[('kq', state, yr, cat, mn)].append((pts, int(centres)))

# ---------- 2. Add prize-meet strings ----------
with open('/tmp/prize_meet_rows.json') as f:
    pm_rows = json.load(f)
print(f'Prize-meet strings: {len(pm_rows)}')

for r in pm_rows:
    cat = CAT_MAP.get(r['discipline'])
    if not cat: continue
    buckets[('pm', r['meet'], None, cat, r['match'])].append((r['score_int'], r['centres']))

# ---------- 3. Build top-40 cohorts under varying MIN_BUCKET_N ----------
def top40(buckets_iter, min_bucket_n):
    """Yield (cat, pts, centres) from top-40% (floor 5) of each qualifying bucket."""
    for key, vals in buckets_iter:
        if len(vals) < min_bucket_n:
            continue
        vals_sorted = sorted(vals, reverse=True)
        n = max(FLOOR_N, int(round(len(vals_sorted) * TOP_PCT)))
        for p, c in vals_sorted[:n]:
            yield key, p, c

def calibrate(min_bucket_n):
    top_per_cat = defaultdict(list)
    bucket_counts = defaultdict(lambda: {'kq': 0, 'pm': 0})
    for (src, *rest, cat, _), vals in buckets.items():
        # Re-derive cat from key (5-tuple: src, x, y, cat, match)
        pass
    # Simpler: iterate buckets directly with key unpacking
    top_per_cat = defaultdict(list)
    src_count = {'kq': 0, 'pm': 0}
    bucket_per_cat = defaultdict(lambda: {'kq':0, 'pm':0})
    for key, vals in buckets.items():
        src = key[0]; cat = key[3]
        if len(vals) < min_bucket_n: continue
        vals_sorted = sorted(vals, reverse=True)
        n = max(FLOOR_N, int(round(len(vals_sorted) * TOP_PCT)))
        for p, c in vals_sorted[:n]:
            top_per_cat[cat].append((p, c))
        src_count[src] += 1
        bucket_per_cat[cat][src] += 1
    return top_per_cat, src_count, bucket_per_cat


def derive_factors(top_per_cat):
    pre_means = {}
    for cat in CATS:
        pre = [p*CONV[cat] + c*CENTRE_W for p, c in top_per_cat[cat]]
        if not pre:
            pre_means[cat] = None
            continue
        pre_means[cat] = mean(pre)
    target = mean(pre_means[c] * CURRENT_V5[c] for c in ('F-Open','F-Std','FTR'))
    factors = {}
    for cat in CATS:
        factors[cat] = round(target / pre_means[cat], 3) if pre_means[cat] else None
    return factors, target


def bootstrap_ci(vals, n_boot=1000):
    n = len(vals)
    means = []
    for _ in range(n_boot):
        s = [vals[random.randrange(n)] for _ in range(n)]
        means.append(sum(s)/n)
    means.sort()
    return sum(means)/n_boot, means[int(n_boot*0.025)], means[int(n_boot*0.975)]


print()
print('='*100)
print('SAMPLE SIZE PER DISCIPLINE — by source')
print('='*100)
for cat in CATS:
    kq = sum(len(v) for k,v in buckets.items() if k[0]=='kq' and k[3]==cat)
    pm = sum(len(v) for k,v in buckets.items() if k[0]=='pm' and k[3]==cat)
    print(f'  {cat:<8} K&Q: {kq:>5}   Prize meets: {pm:>5}   COMBINED: {kq+pm:>6}')

print()
print('='*100)
print('SENSITIVITY: V5 factors at different MIN_BUCKET_N thresholds')
print('='*100)
print(f'{"min N":<6} {"buckets-KQ":>10} {"buckets-PM":>10} ' + '  '.join(f'{c:>9}' for c in CATS))

for min_n in [1, 5, 10, 15, 20, 30]:
    top, src_ct, bpc = calibrate(min_n)
    factors, target = derive_factors(top)
    row = f'{min_n:<6} {src_ct["kq"]:>10} {src_ct["pm"]:>10} '
    row += '  '.join(f'{factors[c]:>9.3f}' if factors[c] else f'{"---":>9}' for c in CATS)
    print(row)

# ---------- 4. Final detailed report at MIN_BUCKET_N = 10 ----------
print()
print('='*100)
print('DETAILED RESULTS at MIN_BUCKET_N = 10 (recommended)')
print('='*100)
MIN = 10
top, src_ct, bpc = calibrate(MIN)
factors, target = derive_factors(top)

print(f'\nBuckets retained: K&Q={src_ct["kq"]}  Prize-meet={src_ct["pm"]}\n')

print(f'{"Cat":<8} {"Top-40 N":>10} {"KQ buckets":>11} {"PM buckets":>11} '
      f'{"V4":>8} {"Current V5":>11} {"NEW V5":>9} {"Δ vs cur":>9}')
print('-'*100)
for cat in CATS:
    n_top = len(top[cat])
    f = factors[cat]
    d = (f - CURRENT_V5[cat])
    print(f'{cat:<8} {n_top:>10} {bpc[cat]["kq"]:>11} {bpc[cat]["pm"]:>11} '
          f'{V4[cat]:>8.3f} {CURRENT_V5[cat]:>11.3f} {f:>9.3f} {d:>+9.3f}')

print()
print('Bootstrap 95% CI on top-40 mean (pre-factor) — confidence improvement:')
print(f'{"Cat":<8} {"Mean":>10} {"95% CI":>22} {"CI width":>10}')
print('-'*100)
for cat in CATS:
    pre = [p*CONV[cat] + c*CENTRE_W for p, c in top[cat]]
    if not pre: continue
    m, lo, hi = bootstrap_ci(pre)
    print(f'{cat:<8} {m:>10.3f} {lo:>10.3f} - {hi:>8.3f} {hi-lo:>10.3f}')

print()
print('='*100)
print('FINAL — proposed V5.1 factor table (K&Q + 31 same-conditions prize meets)')
print('='*100)
print(f'  TR            {factors["TR"]:.3f}    (current V5: {CURRENT_V5["TR"]:.3f})')
print(f'  F-Open        {factors["F-Open"]:.3f}    (current V5: {CURRENT_V5["F-Open"]:.3f})')
print(f'  F-Standard    {factors["F-Std"]:.3f}    (current V5: {CURRENT_V5["F-Std"]:.3f})')
print(f'  FTR           {factors["FTR"]:.3f}    (current V5: {CURRENT_V5["FTR"]:.3f})')
print(f'  Sporter (any) {factors["S"]:.3f}    (current V5: {CURRENT_V5["S"]:.3f})')
