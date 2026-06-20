"""Compare Adrian's and the President's MCSI multiplier sets against real K&Q data.

Both use the same formula structure:
    Adjusted = (Score × Conversion + Centres × 0.7) × Equipment_Factor
where Conversion = 1.2 for TR/Sporter, 1.0 for F-class.

Only the equipment factors differ.
"""
from collections import defaultdict, Counter
from db import get_connection

# Equipment factor sets
ADRIAN = {
    'TR': 1.45, 'Sporter-PC': 1.43, 'Sporter-Open': 1.43,
    'F-Open': 1.44, 'F-Std': 1.46, 'FTR': 1.46,
}
PRESIDENT = {
    'TR': 1.56, 'Sporter-PC': 1.55, 'Sporter-Open': 1.46,
    'F-Open': 1.28, 'F-Std': 1.42, 'FTR': 1.42,
}

TR_LIKE = {'TR-A','TR-B','TR-C','Division Open'}
F_LIKE  = {'F-Open','F-Std-A','F-Std-B','F-Std-Open','FTR'}

def to_category(disc):
    if disc in TR_LIKE: return 'TR'
    if disc in ('Sporter-Open',): return 'Sporter-Open'
    if disc in ('Sporter-PC',):   return 'Sporter-PC'
    if disc == 'F-Open':          return 'F-Open'
    if disc.startswith('F-Std'):  return 'F-Std'
    if disc == 'FTR':             return 'FTR'
    if disc == 'Sporter-Combined': return 'Sporter-PC'   # fold combined into PC for testing
    return None

def count_centres(shots_raw, target_max):
    if not shots_raw: return 0
    mark = 'V' if target_max == 5 else 'X'
    return sum(1 for ch in shots_raw if ch == mark)

def adjust(category, raw_int, centres, factors):
    if category not in factors: return None
    equalised = raw_int * (1.2 if category in ('TR','Sporter-Open','Sporter-PC') else 1.0)
    return round((equalised + centres * 0.7) * factors[category], 3)

def fetch():
    cur = get_connection().cursor()
    cur.execute("""
        SELECT s.competition_id, s.match_number, s.match_name, s.distance,
               s.distance_unit, s.discipline, s.score, s.shots_raw, s.target_max,
               c.year, st.code, sh.first_name, sh.last_name
        FROM strings s
        JOIN competitions c ON c.competition_id=s.competition_id
        JOIN states st ON st.state_id=c.state_id
        LEFT JOIN shooters sh ON sh.sid=s.shooter_sid
        WHERE s.is_kings_queens=TRUE AND s.score IS NOT NULL AND c.year>=2024
          AND st.code = ANY(%s) AND s.discipline IS NOT NULL
          AND (SELECT COUNT(*) FROM shots WHERE string_id=s.string_id)=10
    """, (['NSWRA','VRA','QRA','NQRA','SARA','WARA','NRAA'],))
    return cur.fetchall()

def main():
    rows = fetch()
    enriched = []
    EXPECTED_TMAX = {'TR':5,'Sporter-Open':5,'Sporter-PC':5,'F-Open':6,'F-Std':6,'FTR':6}
    for (comp, mnum, mname, dist, unit, disc, score, shots, tmax,
         year, state, fn, ln) in rows:
        cat = to_category(disc)
        if not cat: continue
        if tmax != EXPECTED_TMAX[cat]: continue
        raw_int = int(float(score))
        max_score = 50 if cat in ('TR','Sporter-Open','Sporter-PC') else 60
        if raw_int > max_score: continue
        centres = count_centres(shots, tmax)
        a = adjust(cat, raw_int, centres, ADRIAN)
        p = adjust(cat, raw_int, centres, PRESIDENT)
        enriched.append({
            'comp':comp,'mnum':mnum,'dist':dist,'unit':unit,'disc':disc,'cat':cat,
            'raw':float(score),'centres':centres,'adrian':a,'pres':p,
            'shooter':f'{fn or ""} {ln or ""}'.strip(),'state':state,'year':year,
        })

    print(f'Loaded {len(enriched)} 10-shot K&Q strings (2024+).\n')

    # ---- TOP 10 OVERALL by each formula ----
    print('='*88)
    print('TOP 10 SHOOTS — RANKED BY ADRIAN vs PRESIDENT')
    print('='*88)
    for name, key in [('ADRIAN',  'adrian'), ('PRESIDENT', 'pres')]:
        print(f'\n— Top 10 by {name} —')
        top = sorted(enriched, key=lambda e: -e[key])[:10]
        print(f"  {'cat':<13}{'raw':>7}{'cen':>5}{'adrian':>9}{'pres':>9}  shooter / event")
        for e in top:
            print(f"  {e['cat']:<13}{e['raw']:>7.1f}{e['centres']:>5d}"
                  f"{e['adrian']:>9.2f}{e['pres']:>9.2f}  "
                  f"{e['shooter'][:24]:<24}  {e['state']} {e['year']} M{e['mnum']}")

    # ---- TOP 10 PER CATEGORY ----
    print('\n' + '='*88)
    print('TOP-RANKED SHOOT IN EACH CATEGORY — does each formula crown the same one?')
    print('='*88)
    by_cat = defaultdict(list)
    for e in enriched: by_cat[e['cat']].append(e)
    print(f"\n  {'category':<14}{'adrian #1':>32}{'president #1':>34}")
    for cat in sorted(by_cat):
        ad_top = max(by_cat[cat], key=lambda e:e['adrian'])
        pr_top = max(by_cat[cat], key=lambda e:e['pres'])
        ad_s = f"{ad_top['shooter'][:18]:<18} {ad_top['adrian']:>6.2f}"
        pr_s = f"{pr_top['shooter'][:18]:<18} {pr_top['pres']:>6.2f}"
        print(f"  {cat:<14}{ad_s:>32}{pr_s:>34}")

    # ---- CATEGORY MAX (theoretical ceiling) ----
    print('\n' + '='*88)
    print('THEORETICAL MAX (perfect-with-all-centres) per category')
    print('='*88)
    print(f"  {'category':<14}{'adrian':>10}{'president':>12}{'diff':>8}")
    for cat in ['TR','Sporter-Open','Sporter-PC','F-Open','F-Std','FTR']:
        max_score = 50 if cat in ('TR','Sporter-Open','Sporter-PC') else 60
        eq = max_score * (1.2 if cat in ('TR','Sporter-Open','Sporter-PC') else 1.0)
        a_max = (eq + 10*0.7) * ADRIAN[cat]
        p_max = (eq + 10*0.7) * PRESIDENT[cat]
        print(f"  {cat:<14}{a_max:>10.2f}{p_max:>12.2f}{p_max-a_max:>+8.2f}")

    # ---- HEAD-TO-HEAD per match for each formula ----
    print('\n' + '='*88)
    print('HEAD-TO-HEAD — top-of-category per match, who beats whom?')
    print('='*88)
    by_match = defaultdict(list)
    for e in enriched:
        by_match[(e['comp'], e['mnum'], e['dist'], e['unit'])].append(e)

    def collect_h2h(metric):
        h2h = Counter()
        cats_seq = ('TR','F-Open','F-Std','FTR','Sporter-Open','Sporter-PC')
        for entries in by_match.values():
            if len(entries) < 5: continue
            tops = {}
            for e in entries:
                cur = tops.get(e['cat'])
                if cur is None or e[metric] > cur[metric]:
                    tops[e['cat']] = e
            present = [c for c in cats_seq if c in tops]
            for i in range(len(present)):
                for j in range(i+1, len(present)):
                    a, b = present[i], present[j]
                    if tops[a][metric] > tops[b][metric]:  h2h[(a,b)] += 1
                    elif tops[b][metric] > tops[a][metric]: h2h[(b,a)] += 1
        return h2h

    h_a = collect_h2h('adrian')
    h_p = collect_h2h('pres')
    cats_seq = ('TR','F-Open','F-Std','FTR','Sporter-Open','Sporter-PC')
    print(f"\n  {'A beats B':<26}{'adrian':>10}{'president':>12}")
    for a in cats_seq:
        for b in cats_seq:
            if a == b: continue
            tot_a = h_a[(a,b)]+h_a[(b,a)]
            tot_p = h_p[(a,b)]+h_p[(b,a)]
            if max(tot_a, tot_p) < 5: continue
            ap = 100*h_a[(a,b)]/tot_a if tot_a else 0
            pp = 100*h_p[(a,b)]/tot_p if tot_p else 0
            flag_a = '⚠' if ap >= 70 or ap <= 30 else ' '
            flag_p = '⚠' if pp >= 70 or pp <= 30 else ' '
            print(f"  {a:<11} > {b:<10}{ap:>8.1f}%{flag_a}{pp:>10.1f}%{flag_p}")

    # ---- WINNER-BY-CATEGORY across big matches ----
    print('\n' + '='*88)
    print('WHO WINS BIG K&Q MATCHES (≥20 shooters)?')
    print('='*88)
    a_wins = Counter(); p_wins = Counter()
    matches = 0
    for entries in by_match.values():
        if len(entries) < 20: continue
        matches += 1
        a_wins[max(entries, key=lambda e:e['adrian'])['cat']] += 1
        p_wins[max(entries, key=lambda e:e['pres'])  ['cat']] += 1
    print(f"\n  Of {matches} big matches, the winner was:")
    print(f"  {'category':<14}{'adrian':>10}{'president':>12}")
    for cat in cats_seq:
        a = a_wins[cat]; p = p_wins[cat]
        print(f"  {cat:<14}{a:>5d}({100*a/matches:>3.0f}%){p:>7d}({100*p/matches:>3.0f}%)")


if __name__ == '__main__':
    main()
