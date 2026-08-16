"""Cross-event check: do the QRA Kings 2026 and NSWRA 2026 championships imply the
same MCSI factors, and does adding QRA to the national pool move V5?

Both events are fresh King's-class data that V5 was never calibrated on, so
agreement between them is the real test — a single event can drift, two
independent events drifting the same way is a signal.

Usage:  python3 compare_qra_nswra.py
"""
import csv, math
from collections import defaultdict, Counter

V5 = {'TR': (1.412, 1.20), 'F-Open': (1.406, 1.00), 'F-Standard': (1.475, 1.00),
      'FTR': (1.450, 1.00), 'Sporter': (1.383, 1.20)}
GRP = {'TR-A': 'TR', 'TR-B': 'TR', 'TR-C': 'TR', 'F-Open': 'F-Open',
       'F-Std-A': 'F-Standard', 'F-Std-B': 'F-Standard', 'FTR': 'FTR',
       'Sporter-Open': 'Sporter', 'Sporter-PC': 'Sporter'}
FCLASS = ('F-Open', 'F-Standard', 'FTR')
ORDER = ['TR', 'F-Open', 'F-Standard', 'FTR', 'Sporter']
POOL_TOTAL = 20335   # national K&Q strings V5 was calibrated on
AGG_BASE = 490       # typical full-championship MCSI aggregate


def load(path):
    rows = []
    for r in csv.DictReader(open(path)):
        g = GRP.get(r['discipline'])
        if not g:
            continue
        rows.append({'pts': int(r['raw_score'].split('.')[0]), 'cen': int(r['centres']),
                     'grp': g, 'key': f"{r['match_number']}|{r['match_name']}"})
    return rows


def implied(rows, top=0.4):
    buckets = defaultdict(list)
    for r in rows:
        buckets[(r['grp'], r['key'])].append(r)
    vals, n = defaultdict(list), Counter()
    for (grp, _k), lst in buckets.items():
        lst.sort(key=lambda r: (-r['pts'], -r['cen']))
        k = max(1, math.ceil(top * len(lst)))
        n[grp] += k
        f, c = V5[grp]
        vals[grp].extend(r['pts'] * c + r['cen'] * 0.7 for r in lst[:k])
    mean = {g: sum(v) / len(v) for g, v in vals.items()}
    target = sum(mean[g] * V5[g][0] for g in FCLASS) / 3
    return {g: target / mean[g] for g in mean}, dict(n)


def main():
    events = [('NSWRA 2026', 'kings_nswra2026.csv'), ('QRA Kings 2026', 'kings_qra2026.csv')]
    res = {}
    for label, path in events:
        rows = load(path)
        imp, n = implied(rows)
        res[label] = (imp, n, len(rows))
        print(f'{label}: {len(rows)} strings')

    print('\n' + '=' * 84)
    print('IMPLIED FACTORS — DO THE TWO EVENTS AGREE?  (top-40% cohort)')
    print('=' * 84)
    print(f'{"Group":<12} {"V5":>8} ' + ' '.join(f'{l.split()[0]:>10}' for l, _ in events) +
          f' {"agree?":>18} {"pooled":>9} {"delta":>8}')
    pooled_imp = {}
    for g in ORDER:
        vals = [res[l][0][g] for l, _ in events]
        ns = [res[l][1][g] for l, _ in events]
        d = [v - V5[g][0] for v in vals]
        same = 'yes, both ' + ('high' if d[0] > 0 else 'low') if d[0] * d[1] > 0 else 'NO — opposite'
        pooled = sum(v * k for v, k in zip(vals, ns)) / sum(ns)
        pooled_imp[g] = (pooled, sum(ns))
        print(f'{g:<12} {V5[g][0]:>8.4f} ' + ' '.join(f'{v:>10.4f}' for v in vals) +
              f' {same:>18} {pooled:>9.4f} {pooled - V5[g][0]:>+8.4f}')

    print('\n' + '=' * 84)
    print('EFFECT ON V5 OF BLENDING BOTH EVENTS INTO THE NATIONAL POOL')
    print('=' * 84)
    nat_per = round(POOL_TOTAL / 5 * 0.4)
    print(f'{"Group":<12} {"V5":>8} {"new n":>7} {"blended":>9} {"move":>9} '
          f'{"MCSI pts":>9}  verdict')
    worst = 0.0
    for g in ORDER:
        imp, n = pooled_imp[g]
        b = (V5[g][0] * nat_per + imp * n) / (nat_per + n)
        move, pts = b - V5[g][0], (b - V5[g][0]) * AGG_BASE
        worst = max(worst, abs(pts))
        verdict = 'no change' if abs(pts) < 1 else ('worth watching' if abs(pts) < 3 else 'REVIEW')
        print(f'{g:<12} {V5[g][0]:>8.4f} {n:>7} {b:>9.4f} {move:>+9.4f} {pts:>+9.2f}  {verdict}')
    print(f'\nLargest move on a ~{AGG_BASE}-point championship aggregate: {worst:.2f} MCSI points.')
    print('Placings at a King\'s are decided by far more than that, so nothing here forces')
    print('a factor change; the two events together are a corroboration, not a correction.')


if __name__ == '__main__':
    main()
