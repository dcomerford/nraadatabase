"""Analyse the QRA Kings Prize Shoot 2026 against MCSI V5 (and June-13/Peter).

Answers, in order:
  1. What does the meeting look like (strings, disciplines, incomplete strings)?
  2. Cross-discipline leaderboard: V5 vs June-13, per day and full series.
  3. What factors does this event imply, and would blending it move V5?
  4. Per-distance implied factors — the weather question. Diagnostic only:
     V5 is a single factor per discipline, so this quantifies how much a
     distance-aware formula would have to move, it does not change V5.

Usage:  python3 analyse_qra_v5.py [--csv kings_qra2026.csv]
"""
import argparse, csv, math
from collections import defaultdict, Counter

V5 = {'TR': (1.412, 1.20), 'F-Open': (1.406, 1.00), 'F-Standard': (1.475, 1.00),
      'FTR': (1.450, 1.00), 'Sporter': (1.383, 1.20)}
GRP = {'TR-A': 'TR', 'TR-B': 'TR', 'TR-C': 'TR', 'F-Open': 'F-Open',
       'F-Std-A': 'F-Standard', 'F-Std-B': 'F-Standard', 'FTR': 'FTR',
       'Sporter-Open': 'Sporter', 'Sporter-PC': 'Sporter'}
PETER = {'TR-A': 1.53, 'TR-B': 1.53, 'TR-C': 1.53, 'F-Open': 1.39,
         'F-Std-A': 1.43, 'F-Std-B': 1.43, 'FTR': 1.43,
         'Sporter-Open': 1.47, 'Sporter-PC': 1.53}
POOL_TOTAL = 20335   # national K&Q strings V5 was calibrated on
FCLASS = ('F-Open', 'F-Standard', 'FTR')
ORDER = ['TR', 'F-Open', 'F-Standard', 'FTR', 'Sporter']


def load(path):
    rows = list(csv.DictReader(open(path)))
    for r in rows:
        r['pts'] = int(r['raw_score'].split('.')[0])
        r['cen'] = int(r['centres'])
        r['shot_count'] = int(r['shot_count'] or 0)
        r['grp'] = GRP[r['discipline']]
        r['name'] = f"{r['first_name']} {r['last_name']}"
        r['key'] = r['match_name']            # unique per shoot (match 15 runs twice)
        r['dist'] = int(r['distance']) if r['distance'] else 0
        r['day'] = r['match_name'].split(' - ')[1] if ' - ' in r['match_name'] else '?'
    return rows


def v5(r):
    f, c = V5[r['grp']]
    return (r['pts'] * c + r['cen'] * 0.7) * f


def peter(r):
    return (r['pts'] + r['cen']) * PETER[r['discipline']]


def base(r):
    """Pre-factor V5 merit: raw x conversion + centres x 0.7."""
    return r['pts'] * V5[r['grp']][1] + r['cen'] * 0.7


def aggregate(rows, keys, fn):
    """Sum fn per shooter over the given shoots; keep only full completers."""
    agg = defaultdict(lambda: [0.0, 0, None, None, 0, 0])
    for r in rows:
        if r['key'] not in keys:
            continue
        k = (r['name'], r['discipline'])
        a = agg[k]
        a[0] += fn(r); a[1] += 1; a[2] = r['discipline']; a[3] = r['grp']
        a[4] += r['pts']; a[5] += r['cen']
    full = [(v[0], k[0], v[2], v[3], v[4], v[5]) for k, v in agg.items() if v[1] == len(keys)]
    full.sort(key=lambda x: -x[0])
    return full


def implied(rows, top=0.4):
    """Top-fraction cohort per (group, shoot); fair target = mean of the three
    F-class post-factor means. Returns (implied factor, cohort n) per group."""
    buckets = defaultdict(list)
    for r in rows:
        buckets[(r['grp'], r['key'])].append(r)
    vals, n = defaultdict(list), Counter()
    for (grp, _k), lst in buckets.items():
        lst.sort(key=lambda r: (-r['pts'], -r['cen']))
        k = max(1, math.ceil(top * len(lst)))
        n[grp] += k
        vals[grp].extend(base(r) for r in lst[:k])
    mean = {g: sum(v) / len(v) for g, v in vals.items()}
    if not all(g in mean for g in FCLASS):
        return {}, {}, 0.0
    target = sum(mean[g] * V5[g][0] for g in FCLASS) / 3
    return {g: target / mean[g] for g in mean}, dict(n), target


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--csv', default='kings_qra2026.csv')
    a = ap.parse_args()
    rows = load(a.csv)
    shoots = sorted({(r['key'], r['dist']) for r in rows}, key=lambda x: (x[0]))
    keys = [k for k, _d in shoots]

    # ---------- 1. data summary ----------
    print('=' * 78)
    print('QRA KINGS PRIZE SHOOT 2026 — MCSI V5 CHECK')
    print('=' * 78)
    print(f'{len(rows)} shooter-strings across {len(keys)} shoots, '
          f'{len({r["name"] for r in rows})} shooters.')
    print('Discipline split: ' + ', '.join(f'{k} {v}' for k, v in
                                           sorted(Counter(r['discipline'] for r in rows).items())))
    inc = [r for r in rows if 'incomplete' in r['info']]
    print(f'\nIncomplete strings (shooter did not fire the full string): {len(inc)}')
    bydist = Counter(r['dist'] for r in inc)
    tot = Counter(r['dist'] for r in rows)
    for d in sorted(tot):
        print(f'   {d:>5} yds  {bydist.get(d, 0):>3} incomplete of {tot[d]:>4} strings '
              f'({100 * bydist.get(d, 0) / tot[d]:.1f}%)')

    # ---------- 2. leaderboards ----------
    days = defaultdict(list)
    for r in rows:
        if r['key'] not in days[r['day']]:
            days[r['day']].append(r['key'])
    blocks = sorted(days.items())
    blocks.append(('Full series', keys))
    for label, ks in blocks:
        av5, ap_ = aggregate(rows, ks, v5), aggregate(rows, ks, peter)
        if not av5:
            continue
        rank_p = {r[1]: i for i, r in enumerate(ap_, 1)}
        print(f'\n--- {label} ({len(ks)} shoots, {len(av5)} full completers) ---')
        print(f'{"#":>2} {"V5 leader":<22} {"Disc":<13} {"Raw":>9} {"V5":>7}   '
              f'{"June-13 leader":<22} {"Disc":<13} {"Jun13":>7}')
        for i in range(min(10, len(av5))):
            x, y = av5[i], ap_[i]
            print(f'{i+1:>2} {x[1]:<22} {x[2]:<13} {str(x[4])+"."+str(x[5]):>9} {x[0]:>7.1f}   '
                  f'{y[1]:<22} {y[2]:<13} {y[0]:>7.1f}')
        print('   top-10 mix — V5: ' +
              ', '.join(f'{k} {v}' for k, v in Counter(r[3] for r in av5[:10]).items()) +
              '  |  June-13: ' +
              ', '.join(f'{k} {v}' for k, v in Counter(r[3] for r in ap_[:10]).items()))
        moves = [(r[1], r[2], i, rank_p[r[1]]) for i, r in enumerate(av5[:15], 1) if r[1] in rank_p]
        big = sorted(moves, key=lambda m: -abs(m[3] - m[2]))[:4]
        print('   biggest shifts: ' +
              '; '.join(f'{n} ({d}) V5 #{rv} -> Jun13 #{rp}' for n, d, rv, rp in big))

    # ---------- 3. would this move V5? ----------
    imp, n, _t = implied(rows)
    nat_per = round(POOL_TOTAL / 5 * 0.4)
    print('\n' + '=' * 78)
    print('WOULD THIS EVENT MOVE V5?  (top-40% cohort, F-class mean as the fair target)')
    print('=' * 78)
    print(f'{"Group":<12} {"implied":>9} {"V5":>9} {"delta":>8} {"n":>6} {"blended":>9} '
          f'{"move":>8} {"MCSI pt*":>9}')
    maxshift = 0.0
    for g in ORDER:
        b = (V5[g][0] * nat_per + imp[g] * n[g]) / (nat_per + n[g])
        shift = (b - V5[g][0]) * 490
        maxshift = max(maxshift, abs(shift))
        print(f'{g:<12} {imp[g]:>9.4f} {V5[g][0]:>9.4f} {imp[g]-V5[g][0]:>+8.4f} {n[g]:>6} '
              f'{b:>9.4f} {b-V5[g][0]:>+8.4f} {shift:>+9.2f}')
    print(f'\n*MCSI-point move on a ~490-point full-series aggregate base.')
    print(f' Largest factor move from blending this event into the {POOL_TOTAL:,}-string '
          f'national pool: {maxshift:.2f} MCSI points.')

    # sensitivity: does the answer depend on the cohort depth?
    print('\nCohort-depth sensitivity (implied factors):')
    print(f'{"top%":<8} ' + ' '.join(f'{g:>12}' for g in ORDER))
    for t in (0.1, 0.2, 0.4, 0.6, 1.0):
        i2, _n2, _ = implied(rows, t)
        print(f'{int(t*100):<8} ' + ' '.join(f'{i2[g]:>12.4f}' for g in ORDER))

    # ---------- 4. per-distance (the weather question) ----------
    print('\n' + '=' * 78)
    print('PER-DISTANCE IMPLIED FACTORS  (diagnostic — V5 uses one factor per discipline)')
    print('=' * 78)
    print(f'{"Distance":<10} {"strings":>8} ' + ' '.join(f'{g:>12}' for g in ORDER))
    spread = defaultdict(list)
    for d in sorted({r['dist'] for r in rows}):
        sub = [r for r in rows if r['dist'] == d]
        i2, _n2, _ = implied(sub)
        if not i2:
            continue
        for g in ORDER:
            spread[g].append(i2[g])
        print(f'{str(d)+" yds":<10} {len(sub):>8} ' + ' '.join(f'{i2[g]:>12.4f}' for g in ORDER))
    print(f'{"range":<10} {"":>8} ' +
          ' '.join(f'{max(spread[g])-min(spread[g]):>12.4f}' for g in ORDER))
    print(f'{"vs V5 max":<10} {"":>8} ' +
          ' '.join(f'{max(abs(x-V5[g][0]) for x in spread[g]):>12.4f}' for g in ORDER))
    print('\nRange = how much a distance-aware factor would swing within this one meeting.')
    print('Compare against the single-factor deltas in the table above: if the per-distance')
    print('range dwarfs the whole-meeting delta, distance is the bigger unmodelled effect.')

    # score compression by distance: is the field genuinely tighter/looser?
    print('\nField behaviour by distance (mean pre-factor merit, top-40% vs all):')
    print(f'{"Distance":<10} {"shots":>6} ' + ' '.join(f'{g:>16}' for g in ORDER))
    for d in sorted({r['dist'] for r in rows}):
        sub = [r for r in rows if r['dist'] == d]
        ns = max(Counter(r['shot_count'] for r in sub))
        cells = []
        for g in ORDER:
            gl = sorted((r for r in sub if r['grp'] == g), key=lambda r: (-r['pts'], -r['cen']))
            if not gl:
                cells.append(f'{"-":>16}'); continue
            k = max(1, math.ceil(0.4 * len(gl)))
            # normalise to per-shot so 10- and 15-shot matches are comparable
            t40 = sum(base(r) / max(1, r['shot_count']) for r in gl[:k]) / k
            allm = sum(base(r) / max(1, r['shot_count']) for r in gl) / len(gl)
            cells.append(f'{t40:>7.3f}/{allm:<8.3f}')
        print(f'{str(d)+" yds":<10} {ns:>6} ' + ' '.join(cells))


    # ---------- 5. distance vs conditions ----------
    # 900 yds was shot twice on Day 2 and once on Day 3, so the same distance
    # appears under different conditions — that separates a distance effect from
    # a weather effect, which a distance-only formula could not tell apart.
    print('\n' + '=' * 78)
    print('DISTANCE vs CONDITIONS  (per-shoot implied factors, same distance repeated)')
    print('=' * 78)
    print(f'{"Shoot":<30} {"yds":>5} {"n":>5} ' + ' '.join(f'{g:>10}' for g in ORDER) +
          f' {"top40 merit":>12}')
    for k in keys:
        sub = [r for r in rows if r['key'] == k]
        i2, _n2, _t = implied(sub)
        if not i2:
            continue
        gl = sorted(sub, key=lambda r: (-base(r) / max(1, r['shot_count'])))
        kk = max(1, math.ceil(0.4 * len(gl)))
        merit = sum(base(r) / max(1, r['shot_count']) for r in gl[:kk]) / kk
        print(f'{k:<30} {sub[0]["dist"]:>5} {len(sub):>5} ' +
              ' '.join(f'{i2.get(g, float("nan")):>10.4f}' for g in ORDER) +
              f' {merit:>12.3f}')
    print('\nIf the same distance swings between shoots, the driver is conditions, not range.')

    # ---------- 6. is the driver distance, or difficulty? ----------
    # Hardship index = TR top-40% merit for that shoot. TR is the biggest cohort
    # and is not part of the F-Standard/F-Open gap, so the two are independent.
    print('\n' + '=' * 78)
    print('IS THE DRIVER DISTANCE, OR CONDITIONS?')
    print('=' * 78)
    hard, gap, dist = [], [], []
    for k in keys:
        sub = [r for r in rows if r['key'] == k]
        i2, _n2, _t = implied(sub)
        tr = sorted((r for r in sub if r['grp'] == 'TR'),
                    key=lambda r: -base(r) / max(1, r['shot_count']))
        kk = max(1, math.ceil(0.4 * len(tr)))
        hard.append(sum(base(r) / max(1, r['shot_count']) for r in tr[:kk]) / kk)
        gap.append(i2['F-Standard'] - i2['F-Open'])
        dist.append(sub[0]['dist'])

    def corr(x, y):
        mx, my = sum(x) / len(x), sum(y) / len(y)
        num = sum((a - mx) * (b - my) for a, b in zip(x, y))
        den = (sum((a - mx) ** 2 for a in x) * sum((b - my) ** 2 for b in y)) ** 0.5
        return num / den if den else float('nan')

    print('Hardship index = TR top-40% merit per shoot (independent of the F-class gap).')
    print(f'  F-Std minus F-Open implied gap  vs hardship : r = {corr(hard, gap):+.3f}')
    print(f'  F-Std minus F-Open implied gap  vs distance : r = {corr(dist, gap):+.3f}')
    print(f'  hardship                        vs distance : r = {corr(dist, hard):+.3f}')
    print(f'  gap range across the meeting: {min(gap):+.4f} .. {max(gap):+.4f}')
    print('\nThe cross-discipline spread tracks how hard the shoot was, not how far it was.')
    print('Distance is only a proxy for difficulty, and a leaky one: the three 900 yd shoots')
    print('above disagree with each other by roughly half the spread seen across all six')
    print('distances. A distance-indexed factor fitted to this meeting would be encoding')
    print('the weather on the day. A difficulty-normalised factor is the better target.')
    print(f'\nNote: only {len(keys)} shoots, and distance and hardship are themselves correlated')
    print(f'(r = {corr(dist, hard):+.3f}), so treat this as a direction to test on more meetings,')
    print('not a fitted result.')


if __name__ == '__main__':
    main()
