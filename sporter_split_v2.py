"""Should V5's single Sporter factor (1.383) split into Sporter-Open and Sporter-PC?

Re-runs the 27-Jun head-to-head test on the pooled data now that two fresh 2026
championships are in (NSWRA 2026, QRA 2026).

Method (unchanged, per the June call):
  * HEAD-TO-HEAD ONLY. A match that lists only Sporter-Open OR only Sporter-PC
    almost certainly had both divisions on the mound under one banner, so it is
    not a clean class separation. Use only matches where BOTH appear as distinct
    rows. Sporter-Combined is dropped outright.
  * The match must also carry all three F-classes so it can set its own fair
    target (F-class post-factor mean) -- keeps weather/difficulty out of it.
  * Top-40% cohort per (class, match), merit normalised PER SHOT
    (reference_calibration_string_length: whole-string pooling is skewed by the
    10- vs 15-shot mix).
  * Implied factor per class per match = target / mean per-shot base merit.
  * Gap = PC - Open, SE clustered at match level (matches are the sample unit).

Usage: python3 sporter_split_v2.py
"""
import csv, math, statistics as st
from collections import defaultdict

CSVS = ['kings_queens.csv', 'kings_nswra2026.csv', 'kings_qra2026.csv']

V5 = {'TR': (1.412, 1.20), 'F-Open': (1.406, 1.00), 'F-Standard': (1.475, 1.00),
      'FTR': (1.450, 1.00), 'Sporter': (1.383, 1.20)}
GRP = {'TR-A': 'TR', 'TR-B': 'TR', 'TR-C': 'TR', 'Division Open': 'TR',
       'Division 1': 'TR', 'Division 2': 'TR', 'Division 3': 'TR',
       'F-Open': 'F-Open', 'F-Std-A': 'F-Standard', 'F-Std-B': 'F-Standard',
       'F-Std-Open': 'F-Standard', 'FTR': 'FTR',
       'Sporter-Open': 'SO', 'Sporter-PC': 'SPC'}
FCLASS = ('F-Open', 'F-Standard', 'FTR')
TOP = 0.40


def load():
    seen, rows = set(), []
    dupes = 0
    for path in CSVS:
        for r in csv.DictReader(open(path)):
            d = r['discipline']
            if d not in GRP:
                continue
            key = (r['state'], r['year'], r['competition'], r['match_number'],
                   r['sid'], r['raw_score'])
            if key in seen:
                dupes += 1
                continue
            seen.add(key)
            sc = int(r['shot_count'] or 0)
            if sc <= 0:
                continue
            rows.append({
                'match': (r['state'], r['year'], r['competition'], r['match_number']),
                'grp': GRP[d],
                'pts': int(r['raw_score'].split('.')[0]),
                'cen': int(r['centres'] or 0),
                'shots': sc,
                'sid': r['sid'],
            })
    print(f'{len(rows)} strings loaded from {len(CSVS)} sources ({dupes} duplicate rows dropped)')
    return rows


def base_per_shot(r):
    """V5 pre-factor merit, normalised per shot."""
    conv = 1.20 if r['grp'] in ('TR', 'SO', 'SPC') else 1.00
    return (r['pts'] * conv + r['cen'] * 0.7) / r['shots']


def cohort_mean(lst):
    lst = sorted(lst, key=lambda r: (-r['pts'], -r['cen']))
    k = max(1, math.ceil(TOP * len(lst)))
    return st.fmean(base_per_shot(r) for r in lst[:k]), k


def main():
    rows = load()

    by_match = defaultdict(lambda: defaultdict(list))
    for r in rows:
        by_match[r['match']][r['grp']].append(r)

    qualifying, skipped_no_h2h, skipped_no_f = [], 0, 0
    for m, grps in by_match.items():
        if not ('SO' in grps and 'SPC' in grps):
            if 'SO' in grps or 'SPC' in grps:
                skipped_no_h2h += 1
            continue
        if not all(g in grps for g in FCLASS):
            skipped_no_f += 1
            continue
        qualifying.append((m, grps))

    print(f'Matches with any Sporter: {skipped_no_h2h + skipped_no_f + len(qualifying)}')
    print(f'  dropped, single-class Sporter only : {skipped_no_h2h}')
    print(f'  dropped, no full F-class reference : {skipped_no_f}')
    print(f'  QUALIFYING head-to-head matches    : {len(qualifying)}')

    per_match = []           # (match, f_SO, f_PC, n_so, n_pc, ratio)
    for m, grps in qualifying:
        means = {}
        ns = {}
        for g in FCLASS + ('SO', 'SPC'):
            means[g], ns[g] = cohort_mean(grps[g])
        target = st.fmean(means[g] * V5[g][0] for g in FCLASS)
        f_so, f_pc = target / means['SO'], target / means['SPC']
        per_match.append((m, f_so, f_pc, ns['SO'], ns['SPC'], means['SO'] / means['SPC']))

    if not per_match:
        print('\nNo qualifying matches -- cannot run the test.')
        return

    def report(label, sub):
        n = len(sub)
        so = [x[1] for x in sub]
        pc = [x[2] for x in sub]
        gap = [x[2] - x[1] for x in sub]
        ratio = [x[5] for x in sub]
        se = lambda v: st.stdev(v) / math.sqrt(len(v)) if len(v) > 1 else float('nan')
        print(f'\n{label}  ({n} matches, '
              f'{sum(x[3] for x in sub)} SO / {sum(x[4] for x in sub)} PC cohort strings)')
        print(f'  Sporter-Open implied factor : {st.fmean(so):.4f} +/- {se(so):.4f}')
        print(f'  Sporter-PC   implied factor : {st.fmean(pc):.4f} +/- {se(pc):.4f}')
        g, gse = st.fmean(gap), se(gap)
        print(f'  Gap (PC - Open)             : {g:+.4f} +/- {gse:.4f}  '
              f'=> {abs(g) / gse:.2f} SE' if gse == gse else '')
        print(f'  Paired base ratio Open/PC   : {st.fmean(ratio):.4f} '
              f'(>1 = PC scores lower for equal merit)')
        pos = sum(1 for x in gap if x > 0)
        print(f'  Matches where PC needs the bigger factor: {pos}/{n} ({100*pos/n:.0f}%)')

    report('ALL QUALIFYING MATCHES', per_match)

    # --- per state-year, to see whether the signal replicates event to event ---
    print('\n' + '=' * 74)
    print('PER EVENT (does it replicate?)')
    print('=' * 74)
    print(f'{"Event":<16} {"n":>3} {"SO":>8} {"PC":>8} {"gap":>8} {"SE":>7} {"t":>6}')
    print('-' * 74)
    by_ev = defaultdict(list)
    for x in per_match:
        by_ev[(x[0][0], x[0][1])].append(x)
    for ev in sorted(by_ev, key=lambda e: (e[1], e[0])):
        sub = by_ev[ev]
        so = st.fmean(x[1] for x in sub)
        pc = st.fmean(x[2] for x in sub)
        gap = [x[2] - x[1] for x in sub]
        g = st.fmean(gap)
        sd = st.stdev(gap) / math.sqrt(len(gap)) if len(gap) > 1 else float('nan')
        t = abs(g) / sd if sd == sd and sd > 0 else float('nan')
        print(f'{ev[0] + " " + ev[1]:<16} {len(sub):>3} {so:>8.4f} {pc:>8.4f} '
              f'{g:>+8.4f} {sd:>7.4f} {t:>6.2f}')

    # --- old pool (pre-2026 fresh events) vs everything, to isolate what the new data added ---
    fresh = {('NSWRA', '2026'), ('QRA', '2026')}
    old = [x for x in per_match if (x[0][0], x[0][1]) not in fresh]
    new = [x for x in per_match if (x[0][0], x[0][1]) in fresh]
    print('\n' + '=' * 74)
    print('WHAT THE NEW DATA ADDED')
    print('=' * 74)
    if old:
        report('Pool WITHOUT the two 2026 championships', old)
    if new:
        report('The two 2026 championships ALONE', new)
    report('POOLED (recommendation basis)', per_match)

    # --- what the split would cost/gain in MCSI points on a championship agg ---
    so_f = st.fmean(x[1] for x in per_match)
    pc_f = st.fmean(x[2] for x in per_match)
    merged = st.fmean([so_f, pc_f])
    print('\n' + '=' * 74)
    print('PRACTICAL EFFECT')
    print('=' * 74)
    print(f'  V5 today (merged)      : 1.383')
    print(f'  Implied Sporter-Open   : {so_f:.4f}  ({(so_f-1.383)*350:+.1f} MCSI pts on a ~490 agg)')
    print(f'  Implied Sporter-PC     : {pc_f:.4f}  ({(pc_f-1.383)*350:+.1f} MCSI pts on a ~490 agg)')
    print(f'  Split spread           : {(pc_f-so_f)*350:.1f} MCSI pts between the two divisions')
    print(f'  (implied merged factor from this pool: {merged:.4f})')


if __name__ == '__main__':
    main()
