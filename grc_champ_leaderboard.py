"""GRC Club Championship leaderboard.

Pulls per-range, per-shooter scores from Bullet Impacts for the GRC point
shoots, applies Adrian v3 MCSI to each string, takes each shooter's BEST 2
scores at each distance, sums their adjusted totals, and ranks the field.

Usage:
    python3 grc_champ_leaderboard.py
"""
import json
import urllib.request
from collections import defaultdict

# Comps we can access (third in user's list returned 403 Not publicly available)
COMP_IDS = [
    'd2a4e092-a9cd-47ab-8997-593499363e17',  # May 2nd
    'd6d4e0e5-618d-41ad-8dda-b33f7d2f1d39',  # April 11
]

# Adrian v3 factors
ADRIAN_V3 = {
    'TR': 1.42, 'SP': 1.41, 'SO': 1.40,
    'FO': 1.43, 'FS': 1.46, 'FTR': 1.46,
}
CENTRE = 0.7

def to_short(discipline_name):
    if not discipline_name: return None
    d = discipline_name.lower()
    if 'target' in d and 'rifle' in d: return 'TR'
    if 'division open' in d:           return 'TR'
    if 'f-tr' in d or 'ftr' in d:      return 'FTR'
    if 'f-open' in d or 'fopen' in d or 'f open' in d: return 'FO'
    if 'f-standard' in d or 'f standard' in d or 'fstandard' in d: return 'FS'
    if 'production' in d:              return 'SP'
    if 'sporter open' in d:            return 'SO'
    if d.strip() == 'sporter':         return 'SP'   # bare 'Sporter' often = production
    return None

def adjusted(disc, total, centres):
    f = ADRIAN_V3.get(disc)
    if f is None: return None
    eq = total if disc in ('FO','FS','FTR') else total * 1.2
    return round((eq + centres * CENTRE) * f, 3)

def fetch(comp_id):
    url = f'https://bulletimpacts.com/api/competitions/{comp_id}/results'
    return json.load(urllib.request.urlopen(url))

# Collect every (shooter, distance, discipline) → list of (raw, centres, adj)
strings = []   # one row per shoot
for cid in COMP_IDS:
    d = fetch(cid)
    comp_name = d['competition']['name']
    comp_date = d['competition']['startDate']
    for rr in d['rangeResults']:
        dist = rr['name']
        for grp in rr['results']:
            short = to_short(grp.get('disciplineName'))
            for e in grp.get('entries', []):
                name = f"{e.get('firstName','').strip()} {e.get('lastName','').strip()}".strip()
                raw = int(e.get('total') or 0)
                centres = int(e.get('centers') or 0)
                if not name or not raw or not short:
                    continue
                adj = adjusted(short, raw, centres)
                strings.append({
                    'comp': comp_name, 'date': comp_date, 'dist': dist,
                    'shooter': name, 'disc': short,
                    'raw': raw, 'centres': centres, 'adj': adj,
                })

# Per shooter: keep best 2 adjusted scores per distance, sum them.
# A shooter who only has 1 score at a distance contributes that one.
by_shooter = defaultdict(lambda: defaultdict(list))   # shooter -> dist -> [(adj, raw, cen, comp)]
disc_of = {}
for s in strings:
    by_shooter[s['shooter']][s['dist']].append((s['adj'], s['raw'], s['centres'], s['comp']))
    disc_of[s['shooter']] = s['disc']   # assume one discipline per shooter

leaderboard = []
for shooter, by_dist in by_shooter.items():
    total_adj = 0.0
    dist_breakdown = {}
    for dist, lst in by_dist.items():
        best2 = sorted(lst, key=lambda x: -x[0])[:2]
        contributed = sum(b[0] for b in best2)
        total_adj += contributed
        dist_breakdown[dist] = (contributed, len(lst), len(best2))
    leaderboard.append({
        'shooter': shooter, 'disc': disc_of[shooter],
        'total_adj': round(total_adj, 3), 'distances': dist_breakdown,
    })

leaderboard.sort(key=lambda r: -r['total_adj'])

# ===== output =====
print('='*100)
print('GRC CLUB CHAMPIONSHIP LEADERBOARD (Adrian v3 MCSI)')
print('Best 2 scores per distance summed. Source: 2 of 3 Bullet Impacts comps (third locked)')
print('='*100)
print(f"  {'place':<5}{'shooter':<28}{'disc':<5}{'total adj':>11}    distances [adj | shots taken/avail]")
print('-'*100)
for i, e in enumerate(leaderboard, 1):
    dist_str = ', '.join(
        f'{d}:{v[0]:.1f}({v[2]}/{v[1]})'
        for d, v in sorted(e['distances'].items())
    )
    print(f"  {i:<5}{e['shooter'][:26]:<28}{e['disc']:<5}{e['total_adj']:>11.2f}    {dist_str}")

print(f"\n{len(strings)} individual range scores across {len(set(s['comp'] for s in strings))} comps")
print(f"{len(leaderboard)} shooters on the leaderboard")
