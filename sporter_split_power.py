"""How much statistical weight does the Sporter Open-vs-PC split actually have,
now that NSWRA 2026 and QRA 2026 are in the pool?

Same head-to-head design as sporter_split_v2.py, but it answers the question the
June run left open: is the gap real once the errors are computed honestly?

Three things the June run did not do:
  1. MIN_FIELD -- a match where a division fielded 1-3 shooters gives a top-40%
     cohort of ONE string. Those matches swing the per-match implied factor by
     +/-0.3 and are pure noise. Sweep the threshold and watch the estimate.
  2. Match-clustered SE. Strings inside one match share wind, mound and squad;
     treating them as independent (string-level SE) understates the error by
     roughly sqrt(strings-per-match).
  3. Event-level SE -- 9 championships as the sample unit. This is the level the
     claim "the split replicates across events" actually lives at.

Usage: python3 sporter_split_power.py
"""
import math, statistics as st
from collections import defaultdict

from sporter_split_v2 import load, cohort_mean, FCLASS, V5, TOP

MIN_FIELDS = [1, 3, 5, 8, 10]


def se(v):
    return st.stdev(v) / math.sqrt(len(v)) if len(v) > 1 else float('nan')


def per_match_estimates(rows, min_field):
    """One implied-factor pair per qualifying match."""
    by_match = defaultdict(lambda: defaultdict(list))
    for r in rows:
        by_match[r['match']][r['grp']].append(r)

    out = []
    for mk, g in by_match.items():
        if 'SO' not in g or 'SPC' not in g:
            continue
        if len(g['SO']) < min_field or len(g['SPC']) < min_field:
            continue
        if not all(x in g for x in FCLASS):
            continue
        mean, n = {}, {}
        for x in FCLASS + ('SO', 'SPC'):
            mean[x], n[x] = cohort_mean(g[x])
        target = st.fmean(mean[x] * V5[x][0] for x in FCLASS)
        out.append({
            'ev': (mk[0], mk[1]), 'match': mk,
            'f_so': target / mean['SO'], 'f_pc': target / mean['SPC'],
            'n_so': n['SO'], 'n_pc': n['SPC'],
            'field_so': len(g['SO']), 'field_pc': len(g['SPC']),
            'ratio': mean['SO'] / mean['SPC'],
        })
    return out


def main():
    rows = load()

    print('\n' + '=' * 78)
    print('1. MIN FIELD SIZE SWEEP  (each division must field >= N shooters in the match)')
    print('=' * 78)
    print(f'{"minN":>5} {"matches":>8} {"events":>7} {"SO":>8} {"PC":>8} {"gap":>9} '
          f'{"SE(match)":>10} {"t":>6} {"PC>Open":>9}')
    print('-' * 78)
    kept = {}
    for mn in MIN_FIELDS:
        est = per_match_estimates(rows, mn)
        if len(est) < 2:
            print(f'{mn:>5} {len(est):>8}   -- too few matches to estimate --')
            continue
        kept[mn] = est
        gap = [e['f_pc'] - e['f_so'] for e in est]
        g, s = st.fmean(gap), se(gap)
        pos = sum(1 for x in gap if x > 0)
        print(f'{mn:>5} {len(est):>8} {len({e["ev"] for e in est}):>7} '
              f'{st.fmean([e["f_so"] for e in est]):>8.4f} '
              f'{st.fmean([e["f_pc"] for e in est]):>8.4f} '
              f'{g:>+9.4f} {s:>10.4f} {g / s if s else 0:>6.2f} '
              f'{f"{pos}/{len(gap)}":>9}')

    # --- pick the defensible threshold and dig in --------------------------
    MN = 5
    est = kept.get(MN)
    if not est:
        print('\nNo matches survive min field 5 -- the data cannot support a split.')
        return

    print('\n' + '=' * 78)
    print(f'2. AT MIN FIELD {MN} -- HOW THE ERROR CHANGES WITH THE SAMPLE UNIT')
    print('=' * 78)
    gap = [e['f_pc'] - e['f_so'] for e in est]
    g = st.fmean(gap)
    s_match = se(gap)
    # string-level: weight each match by its cohort strings, SE as if independent
    strings = sum(e['n_so'] + e['n_pc'] for e in est)
    s_string = s_match * math.sqrt(len(gap)) / math.sqrt(strings)
    by_ev = defaultdict(list)
    for e, x in zip(est, gap):
        by_ev[e['ev']].append(x)
    ev_gaps = [st.fmean(v) for v in by_ev.values()]
    s_event = se(ev_gaps)

    print(f'  gap (PC - Open) = {g:+.4f}')
    print(f'  {"sample unit":<24} {"n":>5} {"SE":>9} {"t":>7}   verdict')
    print('  ' + '-' * 66)
    for label, n, s in [('strings (June method)', strings, s_string),
                        ('matches (clustered)', len(gap), s_match),
                        ('events (replication)', len(ev_gaps), s_event)]:
        t = g / s if s and s == s else float('nan')
        v = 'clears 3 SE' if t >= 3 else ('2-3 SE, weak' if t >= 2 else 'NOT significant')
        print(f'  {label:<24} {n:>5} {s:>9.4f} {t:>7.2f}   {v}')

    print('\n' + '=' * 78)
    print(f'3. PER EVENT AT MIN FIELD {MN} -- does the direction replicate?')
    print('=' * 78)
    print(f'{"Event":<16} {"matches":>8} {"gap":>9} {"PC>Open":>9}')
    print('-' * 78)
    for ev in sorted(by_ev, key=lambda e: (e[1], e[0])):
        v = by_ev[ev]
        pos = sum(1 for x in v if x > 0)
        print(f'{ev[0] + " " + ev[1]:<16} {len(v):>8} {st.fmean(v):>+9.4f} '
              f'{f"{pos}/{len(v)}":>9}')
    pos_ev = sum(1 for v in ev_gaps if v > 0)
    print(f'\n  Events where PC needs the bigger factor: {pos_ev}/{len(ev_gaps)}')
    # sign test across events
    n = len(ev_gaps)
    p = sum(math.comb(n, k) for k in range(pos_ev, n + 1)) / 2 ** n
    print(f'  Sign test across events (one-sided): p = {p:.3f}')

    print('\n' + '=' * 78)
    print(f'4. PAIRED WITHIN-MATCH MERIT RATIO AT MIN FIELD {MN}')
    print('   (Open base / PC base -- >1 means PC scores lower for equal merit,')
    print('    which is the mechanical prediction from tighter Production rules)')
    print('=' * 78)
    ratio = [e['ratio'] for e in est]
    rm, rs = st.fmean(ratio), se(ratio)
    print(f'  ratio = {rm:.4f} +/- {rs:.4f}  ({(rm - 1) / rs:.2f} SE from 1.0)')
    print(f'  implies a factor gap of about {(rm - 1) * 1.39:+.4f}')

    print('\n' + '=' * 78)
    print('5. WHAT THE TWO 2026 CHAMPIONSHIPS CONTRIBUTED')
    print('=' * 78)
    fresh = {('NSWRA', '2026'), ('QRA', '2026')}
    for label, sub in [('pool WITHOUT 2026 events', [e for e in est if e['ev'] not in fresh]),
                       ('the 2026 events alone', [e for e in est if e['ev'] in fresh]),
                       ('pooled', est)]:
        if not sub:
            print(f'  {label:<28} no qualifying matches at this field size')
            continue
        gg = [e['f_pc'] - e['f_so'] for e in sub]
        m_, s_ = st.fmean(gg), se(gg)
        t_ = m_ / s_ if len(gg) > 1 and s_ else float('nan')
        print(f'  {label:<28} {len(gg):>3} matches  gap {m_:>+8.4f} +/- {s_:.4f}  t={t_:.2f}')

    # how many more matches would we need at the observed spread?
    sd = st.stdev(gap)
    need = (3 * sd / g) ** 2 if g > 0 else float('inf')
    print('\n' + '=' * 78)
    print('6. WHAT WOULD IT TAKE TO SETTLE IT?')
    print('=' * 78)
    print(f'  observed gap {g:+.4f}, match-to-match SD {sd:.4f}')
    print(f'  matches needed for a 3-SE result at this effect size: {need:.0f} '
          f'(have {len(gap)})')
    print(f'  ~{need / max(1, len(gap) / len(ev_gaps)):.0f} more championship-events '
          f'worth of head-to-head Sporter matches.')


if __name__ == '__main__':
    main()
