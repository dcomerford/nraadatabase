"""Apply Adrian's MCSI formula to historical K&Q strings and check fairness
across disciplines.

Adrian's formula (per shoot):
    Equalised   = Raw × 1.2   (TR & Sporter; converts 50/150 -> 60/180 basis)
                = Raw × 1.0   (F-Open, F-Std, FTR — already on 60/180 basis)
    CentreBonus = Centres × 0.7
    Adjusted    = (Equalised + CentreBonus) × Equipment_Factor

Equipment factors: F-Open 1.44, F-Std 1.46, FTR 1.46, TR 1.45, Sporter 1.43.
"""
import argparse
from collections import defaultdict, Counter
from db import get_connection

EQUIPMENT_FACTORS = {
    'TR-A': 1.45, 'TR-B': 1.45, 'TR-C': 1.45, 'Division Open': 1.45,
    'Division 1': 1.45, 'Division 2': 1.45, 'Division 3': 1.45,
    'F-Open': 1.44,
    'F-Std-A': 1.46, 'F-Std-B': 1.46, 'F-Std-Open': 1.46,
    'FTR': 1.46,
    'Sporter-Open': 1.43, 'Sporter-PC': 1.43, 'Sporter-Combined': 1.43,
}

TR_LIKE = {'TR-A','TR-B','TR-C','Division Open','Division 1','Division 2','Division 3',
           'Sporter-Open','Sporter-PC','Sporter-Combined'}
F_LIKE = {'F-Open','F-Std-A','F-Std-B','F-Std-Open','FTR'}

CATEGORY = {
    **{d: 'TR'      for d in ('TR-A','TR-B','TR-C','Division Open','Division 1','Division 2','Division 3')},
    'F-Open': 'F-Open',
    **{d: 'F-Std'   for d in ('F-Std-A','F-Std-B','F-Std-Open')},
    'FTR': 'FTR',
    **{d: 'Sporter' for d in ('Sporter-Open','Sporter-PC','Sporter-Combined')},
}


def count_centres(shots_raw, target_max):
    if not shots_raw:
        return 0
    mark = 'V' if target_max == 5 else 'X'
    return sum(1 for ch in shots_raw if ch == mark)


def adrian_adjusted(discipline, raw_score, centres):
    if discipline not in EQUIPMENT_FACTORS:
        return None
    raw = float(raw_score)
    score_pts = int(raw)              # 50.7 -> 50 (centres encoded in decimal,
                                       # but we use the V/X count instead)
    if discipline in F_LIKE:
        equalised = score_pts
    else:
        equalised = score_pts * 1.2
    centre_bonus = centres * 0.7
    return round((equalised + centre_bonus) * EQUIPMENT_FACTORS[discipline], 3)


def fetch_strings(min_year=2024, states=('NSWRA','VRA','QRA','NQRA','SARA','WARA','NRAA')):
    cur = get_connection().cursor()
    cur.execute("""
        SELECT s.string_id, s.competition_id, s.match_number, s.match_name,
               s.distance, s.distance_unit, s.discipline, s.score, s.shots_raw,
               s.target_max, c.year, st.code, sh.first_name, sh.last_name,
               (SELECT COUNT(*) FROM shots WHERE string_id=s.string_id) AS n_shots
        FROM strings s
        JOIN competitions c ON c.competition_id=s.competition_id
        JOIN states st ON st.state_id=c.state_id
        LEFT JOIN shooters sh ON sh.sid=s.shooter_sid
        WHERE s.is_kings_queens=TRUE AND s.score IS NOT NULL
          AND c.year >= %s AND st.code = ANY(%s)
          AND s.discipline IS NOT NULL
    """, (min_year, list(states)))
    return cur.fetchall()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--min-year', type=int, default=2024)
    ap.add_argument('--top-n', type=int, default=15,
                    help='Show top-N per match in the per-match leaderboard')
    ap.add_argument('--matches', type=int, default=10,
                    help='How many high-participation matches to show')
    args = ap.parse_args()

    rows = fetch_strings(args.min_year)
    print(f'Loaded {len(rows)} K&Q strings ({args.min_year}+, NSW/VIC/QLD/SA/WA/NRAA)')

    # Compute Adrian-adjusted score per row; restrict to 10-shot strings
    # AND require target_max matches expected for the discipline (excludes
    # the Vic Kings 2025 Sporter-on-6-bull anomaly etc.)
    EXPECTED_TMAX = {**{d: 5 for d in TR_LIKE}, **{d: 6 for d in F_LIKE}}
    enriched = []
    for r in rows:
        (sid, comp_id, mnum, mname, dist, unit, disc, score, shots_raw,
         tmax, year, state, fn, ln, n_shots) = r
        if n_shots != 10: continue
        if disc not in EQUIPMENT_FACTORS: continue
        if tmax != EXPECTED_TMAX.get(disc): continue
        if int(float(score)) > (5 if disc in TR_LIKE else 6) * n_shots: continue
        centres = count_centres(shots_raw, tmax)
        adj = adrian_adjusted(disc, score, centres)
        if adj is None: continue
        enriched.append({
            'comp_id': comp_id, 'match_number': mnum, 'match_name': mname,
            'distance': dist, 'distance_unit': unit, 'discipline': disc,
            'category': CATEGORY.get(disc, disc),
            'raw': float(score), 'centres': centres,
            'adjusted': adj,
            'shooter': f'{fn or ""} {ln or ""}'.strip() or 'unknown',
            'state': state, 'year': year,
        })

    # ============ Fairness check: avg adjusted per category, top-N per cat ============
    print('\n' + '='*78)
    print('FAIRNESS CHECK 1 — Top-of-category averages across all 10-shot strings')
    print('='*78)
    by_cat = defaultdict(list)
    for e in enriched: by_cat[e['category']].append(e['adjusted'])
    print(f"{'Category':<10}{'n':>6}{'mean':>10}{'p90':>10}{'p99':>10}{'max':>10}")
    for cat in sorted(by_cat):
        xs = sorted(by_cat[cat])
        n = len(xs)
        mean = sum(xs)/n
        p90 = xs[int(n*0.9)]; p99 = xs[int(n*0.99)]; mx = xs[-1]
        print(f"{cat:<10}{n:>6d}{mean:>10.2f}{p90:>10.2f}{p99:>10.2f}{mx:>10.2f}")

    # ============ Fairness check 2: per-match leaderboards ============
    print('\n' + '='*78)
    print(f'FAIRNESS CHECK 2 — Top-{args.top_n} per match, biggest {args.matches} matches')
    print('='*78)
    print('If formula is fair, each discipline should sometimes win, sometimes lose')
    print('— winners should not be dominated by any one discipline.')

    by_match = defaultdict(list)
    for e in enriched:
        key = (e['comp_id'], e['match_number'], e['distance'], e['distance_unit'])
        by_match[key].append(e)

    # Show matches with most participants
    big_matches = sorted(by_match.items(), key=lambda kv: -len(kv[1]))[:args.matches]
    winners = Counter()
    for (key, entries) in big_matches:
        entries.sort(key=lambda e: -e['adjusted'])
        first = entries[0]
        winners[first['category']] += 1
        comp_id, mnum, dist, unit = key
        label = f'{first["state"]} {first["year"]} ' \
                f'Match{mnum} {dist}{unit} (n={len(entries)})'
        print(f'\n--- {label} ---')
        print(f"  {'cat':<8}{'discipline':<14}{'raw':>7}{'cen':>5}{'adj':>9}  shooter")
        for e in entries[:args.top_n]:
            print(f"  {e['category']:<8}{e['discipline']:<14}{e['raw']:>7.1f}"
                  f"{e['centres']:>5d}{e['adjusted']:>9.2f}  {e['shooter']}")

    # ============ Per-match: who wins on adjusted vs raw ============
    print('\n' + '='*78)
    print('FAIRNESS CHECK 3 — Winner-by-category distribution across ALL big matches')
    print('='*78)
    cross_match = Counter()
    for entries in by_match.values():
        if len(entries) < 20:  # skip small matches
            continue
        winner = max(entries, key=lambda e: e['adjusted'])
        cross_match[winner['category']] += 1
    total = sum(cross_match.values())
    print(f'Of {total} matches with ≥20 shooters, the Adrian-adjusted winner was:')
    for cat, n in cross_match.most_common():
        print(f'  {cat:<10} {n:>4} ({100*n/total:.1f}%)')

    # ============ Compare ranking inside a small set across disciplines ============
    print('\n' + '='*78)
    print('FAIRNESS CHECK 4 — Top-1 per category per match: who beats whom?')
    print('='*78)
    head_to_head = Counter()
    pairs = 0
    cats = ('TR','F-Open','F-Std','FTR','Sporter')
    for entries in by_match.values():
        if len(entries) < 5: continue
        # top of each category
        tops = {}
        for e in entries:
            cur = tops.get(e['category'])
            if cur is None or e['adjusted'] > cur['adjusted']:
                tops[e['category']] = e
        if len(tops) < 2: continue
        # Pairwise
        present = [c for c in cats if c in tops]
        for i in range(len(present)):
            for j in range(i+1, len(present)):
                a, b = present[i], present[j]
                pairs += 1
                if tops[a]['adjusted'] > tops[b]['adjusted']:
                    head_to_head[(a, b)] += 1
                elif tops[b]['adjusted'] > tops[a]['adjusted']:
                    head_to_head[(b, a)] += 1
    print(f'Total pairwise comparisons (top-of-category per match): {pairs}')
    print(f"\n{'A beats B':<26}{'wins':>8}{'total':>8}{'A win%':>9}")
    for (a, b) in [(x,y) for x in cats for y in cats if x != y]:
        a_wins = head_to_head[(a,b)]
        b_wins = head_to_head[(b,a)]
        tot = a_wins + b_wins
        if tot < 5: continue
        pct = 100 * a_wins / tot
        flag = '  ⚠ skew' if pct >= 70 or pct <= 30 else ''
        print(f'  {a:<10} > {b:<10}{a_wins:>8d}{tot:>8d}{pct:>8.1f}%{flag}')


if __name__ == '__main__':
    main()
