"""Does folding NSWRA 2026 + QRA Kings 2026 into the calibration pool change V5?

Re-runs the V5 calibration method (v5_calibration_combined.py) on:
    A. the national K&Q pool alone           -> baseline
    B. the pool + NSWRA 2026 + QRA Kings 2026 -> new factors

Method (unchanged from the original calibration):
    bucket   = (event, year, discipline, match)   — one bucket = same conditions
    cohort   = top 40% of each bucket, floor 5, buckets with < 10 entries dropped
    factor   = target / mean(pre-factor merit of the pooled cohort)
    target   = mean of the three F-class post-factor means (F-class is the anchor)
    merit    = raw x conversion + centres x 0.7

Run BOTH ways, because it matters here:
    'absolute'  — merit as the original calibration pooled it, a whole-string total
    'per-shot'  — merit divided by the number of shots in the string

The archive is 87% ten-shot strings; NSWRA 2026 is 50% fifteen-shot. Pooling
absolute string totals therefore mixes 10- and 15-shot strings at a different
ratio before and after, which shifts every discipline mean for a reason that has
nothing to do with shooting. Per-shot normalisation removes that; it is the
number to trust when the string-length mix changes.

Caveat: the published V5 factors were set on K&Q *plus* ~31 same-conditions prize
meets; that scraped supplement lived in /tmp and is gone, so the baseline here is
K&Q-only. It reproduces published V5 to within 0.008. Because a bigger baseline
pool can only dilute new data further, the movement reported here is an upper
bound on the movement against the true published pool.

Usage:  python3 pool_recalibration.py
"""
import csv
from collections import defaultdict
from statistics import mean
from db import get_connection

TOP_PCT, FLOOR_N, MIN_BUCKET_N = 0.40, 5, 10
CONV = {'TR': 1.2, 'F-Open': 1.0, 'F-Std': 1.0, 'FTR': 1.0, 'S': 1.2}
CENTRE_W = 0.7
CATS = ['TR', 'F-Open', 'F-Std', 'FTR', 'S']
LABEL = {'TR': 'Target Rifle', 'F-Open': 'F-Open', 'F-Std': 'F-Standard',
         'FTR': 'F/TR', 'S': 'Sporter'}
PUBLISHED_V5 = {'TR': 1.412, 'F-Open': 1.406, 'F-Std': 1.475, 'FTR': 1.450, 'S': 1.383}
AGG_BASE = 490          # a full King's series aggregate, in MCSI points

# kings-CSV discipline -> calibration category
CSV_CAT = {'TR-A': 'TR', 'TR-B': 'TR', 'TR-C': 'TR', 'F-Open': 'F-Open',
           'F-Std-A': 'F-Std', 'F-Std-B': 'F-Std', 'FTR': 'FTR',
           'Sporter-Open': 'S', 'Sporter-PC': 'S'}

KQ_QUERY = """
SELECT s.code AS state_code, c.year, st.match_number,
       length(regexp_replace(coalesce(st.shots_raw,''), '\\s', '', 'g')) AS shots,
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


def kq_buckets():
    conn = get_connection(); cur = conn.cursor()
    cur.execute(KQ_QUERY)
    rows = cur.fetchall(); conn.close()
    b = defaultdict(list)
    for state, yr, mn, shots, cat, score, cen in rows:
        if not cat or not shots:
            continue
        b[('kq', state, yr, cat, mn)].append((int(float(score)), int(cen), int(shots)))
    return b, len(rows)


def csv_buckets(path, tag):
    """One bucket per (event, discipline, shoot). match_name is the shoot key —
    QRA runs match 15 twice, so the number alone would merge two shoots."""
    b = defaultdict(list)
    n = 0
    for r in csv.DictReader(open(path)):
        cat = CSV_CAT.get(r['discipline'])
        if not cat:
            continue
        n += 1
        shoot = f"{r['match_number']}|{r['match_name']}"
        b[(tag, tag, None, cat, shoot)].append(
            (int(r['raw_score'].split('.')[0]), int(r['centres']), int(r['shot_count'] or 0)))
    return b, n


def calibrate(buckets, per_shot=False):
    cohort = defaultdict(list)
    kept = defaultdict(int)
    for key, vals in buckets.items():
        if len(vals) < MIN_BUCKET_N:
            continue
        cat = key[3]
        vs = sorted(vals, reverse=True)
        n = max(FLOOR_N, int(round(len(vs) * TOP_PCT)))
        cohort[cat].extend(vs[:n])
        kept[cat] += 1

    def merit(cat, p, x, shots):
        m = p * CONV[cat] + x * CENTRE_W
        return m / shots if (per_shot and shots) else m

    pre = {c: mean(merit(c, p, x, s) for p, x, s in cohort[c]) for c in cohort}
    target = mean(pre[c] * PUBLISHED_V5[c] for c in ('F-Open', 'F-Std', 'FTR'))
    factors = {c: target / pre[c] for c in cohort}
    return factors, {c: len(cohort[c]) for c in cohort}, dict(kept)


def main():
    kq, n_kq = kq_buckets()
    nsw, n_nsw = csv_buckets('kings_nswra2026.csv', 'NSWRA2026')
    qra, n_qra = csv_buckets('kings_qra2026.csv', 'QRA2026')

    print('=' * 92)
    print('DOES ADDING NSWRA 2026 + QRA KINGS 2026 TO THE POOL CHANGE V5?')
    print('=' * 92)
    print(f'National K&Q pool (2024+, 7 associations): {n_kq:,} strings, {len(kq)} buckets')
    print(f'NSWRA 150th Open Championships 2026:       {n_nsw:,} strings, {len(nsw)} buckets')
    print(f'QRA Kings Prize Shoot 2026:                {n_qra:,} strings, {len(qra)} buckets')
    print(f'Combined pool:                             {n_kq + n_nsw + n_qra:,} strings '
          f'(+{100 * (n_nsw + n_qra) / n_kq:.1f}%)')

    # string-length mix — the reason both normalisations are reported
    print('\n' + '-' * 92)
    print('STRING-LENGTH MIX (why per-shot normalisation matters here)')
    print('-' * 92)
    for name, bk in (('National K&Q pool', kq), ('NSWRA 2026', nsw), ('QRA Kings 2026', qra)):
        ct = defaultdict(int)
        for vals in bk.values():
            for _p, _x, s in vals:
                ct[s] += 1
        tot = sum(ct.values())
        print(f'  {name:<20} ' + '  '.join(f'{s}-shot {100 * n / tot:.1f}%'
                                           for s, n in sorted(ct.items()) if n / tot > 0.01))

    both = dict(kq); both.update(nsw); both.update(qra)
    only_nsw = dict(kq); only_nsw.update(nsw)
    only_qra = dict(kq); only_qra.update(qra)

    results = {}
    for mode, per_shot in (('absolute (as originally pooled)', False),
                           ('per-shot normalised', True)):
        base_f, base_n, _ = calibrate(kq, per_shot)
        new_f, new_n, _ = calibrate(both, per_shot)
        f_nsw, _, _ = calibrate(only_nsw, per_shot)
        f_qra, _, _ = calibrate(only_qra, per_shot)
        results[per_shot] = (base_f, new_f, base_n, new_n, f_nsw, f_qra)

        print('\n' + '=' * 92)
        print(f'MERIT POOLED AS: {mode.upper()}')
        print('=' * 92)
        print(f'{"Discipline":<14} {"published":>10} {"baseline":>10} {"repro diff":>11} '
              f'{"+new n":>7} {"with events":>12} {"move":>9} {"MCSI pts":>9}  verdict')
        worst, worst_cat = 0.0, ''
        for c in CATS:
            move = new_f[c] - base_f[c]
            pts = move * AGG_BASE
            if abs(pts) > worst:
                worst, worst_cat = abs(pts), LABEL[c]
            verdict = ('no change' if abs(pts) < 1 else
                       'watch' if abs(pts) < 3 else 'REVIEW')
            print(f'{LABEL[c]:<14} {PUBLISHED_V5[c]:>10.3f} {base_f[c]:>10.4f} '
                  f'{base_f[c] - PUBLISHED_V5[c]:>+11.4f} {new_n[c] - base_n[c]:>+7} '
                  f'{new_f[c]:>12.4f} {move:>+9.4f} {pts:>+9.2f}  {verdict}')
        print(f'\n  Largest movement: {worst_cat}, {worst:.2f} MCSI points on a '
              f'~{AGG_BASE}-point series aggregate.')
        print('  Rounded to the 3 decimals V5 is published at:')
        changed = []
        for c in CATS:
            b, n = round(base_f[c], 3), round(new_f[c], 3)
            tag = 'unchanged' if n == b else f'{b:.3f} -> {n:.3f}'
            if tag != 'unchanged':
                changed.append(LABEL[c])
            print(f'    {LABEL[c]:<14} {tag}')
        print(f'  Moves at all: {", ".join(changed) if changed else "none"}.')
        print(f'\n  Split by event ({"per-shot" if per_shot else "absolute"}):')
        print(f'  {"Discipline":<14} {"+NSWRA only":>12} {"+QRA only":>11} {"+both":>10} '
              f'{"agree?":>8}')
        for c in CATS:
            dn, dq = f_nsw[c] - base_f[c], f_qra[c] - base_f[c]
            print(f'  {LABEL[c]:<14} {dn:>+12.4f} {dq:>+11.4f} '
                  f'{new_f[c] - base_f[c]:>+10.4f} {"yes" if dn * dq > 0 else "no":>8}')

    # ---- which normalisation should be believed? ----
    print('\n' + '=' * 92)
    print('READING THE TWO TABLES')
    print('=' * 92)
    ab, an = results[False][0], results[False][1]
    pb, pn = results[True][0], results[True][1]
    print(f'{"Discipline":<14} {"absolute move":>14} {"per-shot move":>14} {"shrinks by":>12}')
    for c in CATS:
        da, dp = an[c] - ab[c], pn[c] - pb[c]
        shrink = f'{100 * (1 - abs(dp) / abs(da)):.0f}%' if abs(da) > 1e-9 else '-'
        print(f'{LABEL[c]:<14} {da:>+14.4f} {dp:>+14.4f} {shrink:>12}')
    print('\nThe absolute column moves because the new events carry a different mix of')
    print('10- and 15-shot strings to the archive, not because the shooting differed.')
    print('Per-shot is the like-for-like comparison and is the one to act on.')
    return results


if __name__ == '__main__':
    main()
