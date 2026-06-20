"""Populate survey_sets / survey_set_items with up to N comparison sets.

A comparison set = one match (competition + match_number + distance + unit)
with the TOP score from each of TR-A / FO / F-STD / FTR / Sporter present.
Sets are sampled randomly; matches missing categories still qualify if at
least MIN_CATEGORIES are present.

Usage:
    python3 build_survey_sets.py            # default 100 sets, min 3 cats
    python3 build_survey_sets.py --n 50 --min-categories 4 --reset
"""
import argparse
import random
import re

from db import get_connection

# Allowed state codes (the survey wants only competitions with strong fields)
ALLOWED_STATES = ('NSWRA', 'VRA', 'QRA', 'NQRA', 'SARA', 'WARA', 'NRAA')

# Some states leave the distance column NULL but encode it in match_name
DIST_RE = re.compile(r'(\d{3,4})\s*(yds?|m|metres|yards)', re.I)


def parse_distance_from_name(name):
    if not name:
        return (None, None)
    m = DIST_RE.search(name)
    if not m:
        return (None, None)
    n = int(m.group(1))
    return (n, 'Yds' if m.group(2).lower().startswith('y') else 'm')

# (category_label, discipline_predicate, target_max_required, per_shot_max)
# Sporter is combined (Open + PC + Combined all roll into one category;
# the best Sporter shoot wins). per_shot_max * shot_count is the score cap,
# which excludes mis-scored shoots (e.g. Vic Kings 2025 sporter on 6-bull).
CATEGORY_RULES = [
    # All TR sub-grades roll up to TR-A; WA's Division Open is also TR.
    ('TR-A',    lambda d: d in ('TR-A', 'TR-B', 'TR-C', 'Division Open'), 5, 5),
    ('F-Open',  lambda d: d == 'F-Open',                                  6, 6),
    ('F-STD',   lambda d: d.startswith('F-Std'),                          6, 6),
    ('FTR',     lambda d: d == 'FTR',                                     6, 6),
    ('Sporter', lambda d: d.startswith('Sporter'),                        5, 5),
]
CATEGORY_ORDER = [c for c, *_ in CATEGORY_RULES]
ALLOWED_SHOT_COUNTS = (10, 15)
TOP_N_PER_CATEGORY = 5     # pool size to randomly sample one shoot from

# Adrian's CURRENT factors (FO 1.42, FTR 1.45, FS 1.46, TR 1.42, SO 1.40, SP 1.41)
# Same formula structure: (raw × conversion + centres × 0.7) × factor
# conversion = 1.2 for TR/Sporter, 1.0 for F-class
def adrian_factor(category, discipline):
    if category in ('F-Open','FO'): return 1.42
    if category in ('F-STD','FS'):  return 1.46
    if category in ('FTR',):        return 1.45
    if category in ('TR-A','TR'):   return 1.42
    # Sporter — split by underlying discipline
    if discipline == 'Sporter-Open':                  return 1.40
    if discipline in ('Sporter-PC','Sporter-Combined'): return 1.41
    return 1.41  # default sporter

def adrian_adjusted(category, discipline, raw_score_int, centres):
    is_f = category in ('F-Open','F-STD','FTR')
    eq = raw_score_int if is_f else raw_score_int * 1.2
    return round((eq + centres * 0.7) * adrian_factor(category, discipline), 3)

# Peter's formula (used at June 13 Geelong shoot)
# Different structure: (raw + centres) × factor  — no 1.2 conversion, no 0.7 centre weight
def peter_factor(category, discipline):
    if category in ('F-Open','FO'): return 1.39
    if category in ('F-STD','FS'):  return 1.43
    if category in ('FTR',):        return 1.39
    if category in ('TR-A','TR'):   return 1.53
    if discipline == 'Sporter-Open':                  return 1.47
    if discipline in ('Sporter-PC','Sporter-Combined'): return 1.53
    return 1.53  # default sporter

def peter_adjusted(category, discipline, raw_score_int, centres):
    return round((raw_score_int + centres) * peter_factor(category, discipline), 3)

# V5 — top-40% calibrated, Sporter merged (SO and SP share one factor)
# Structure same as V4: (raw × conversion + centres × 0.7) × factor
def v5_factor(category, discipline):
    if category in ('F-Open','FO'): return 1.406
    if category in ('F-STD','FS'):  return 1.475
    if category in ('FTR',):        return 1.450
    if category in ('TR-A','TR'):   return 1.412
    return 1.383  # Sporter Open + Sporter Production merged

def v5_adjusted(category, discipline, raw_score_int, centres):
    is_f = category in ('F-Open','F-STD','FTR')
    eq = raw_score_int if is_f else raw_score_int * 1.2
    return round((eq + centres * 0.7) * v5_factor(category, discipline), 3)


def categorise(disc, target_max, score, n_shots):
    """Returns category label if the row qualifies, else None.

    Score cap is on the INTEGER part of the score — NRAA scores are stored
    as 'X.Y' where Y is the centre count, not a fraction. So 50.9 = a perfect
    50 with 9 centres (a 'possible') and must pass a '<=50' cap.
    """
    if n_shots not in ALLOWED_SHOT_COUNTS:
        return None
    score_int = int(float(score))
    for cat, pred, tmax, per_shot in CATEGORY_RULES:
        if pred(disc) and target_max == tmax and score_int <= per_shot * n_shots:
            return cat
    return None


def fetch_candidate_matches(cur, min_categories, min_year=None):
    """Return list of qualified match-buckets. Filters to 10-shot strings,
    optionally restricts to year >= min_year."""
    cur.execute("""
        SELECT s.competition_id, s.match_number, s.match_name,
               s.distance, s.distance_unit, s.discipline, s.score, s.string_id,
               s.target_max, c.year, st.code, s.shots_raw,
               (SELECT COUNT(*) FROM shots WHERE string_id = s.string_id) AS n_shots
        FROM strings s
        JOIN competitions c ON c.competition_id = s.competition_id
        JOIN states st ON st.state_id = c.state_id
        WHERE s.is_kings_queens = TRUE
          AND s.score IS NOT NULL
          AND s.discipline IS NOT NULL
          AND (%s::int IS NULL OR c.year >= %s)
          AND st.code = ANY(%s)
    """, (min_year, min_year, list(ALLOWED_STATES)))
    rows = cur.fetchall()

    # Bucket by match key. For each (match, category) keep the TOP_N best
    # shoots; the survey will sample one of them per variant.
    # For states that leave the distance column NULL, parse it from match_name.
    buckets = {}
    for (comp_id, mnum, mname, dist, unit, disc, score, sid, tmax,
         year, state, shots_raw, n_shots) in rows:
        cat = categorise(disc, tmax, score, n_shots)
        if cat is None:
            continue
        if not dist:
            dist, unit = parse_distance_from_name(mname)
        if not dist:
            continue
        # Count centres from shots_raw (V for TR/Sporter, X for F-class)
        mark = 'V' if tmax == 5 else 'X'
        centres = sum(1 for ch in (shots_raw or '') if ch == mark)
        key = (comp_id, mnum, dist, unit)
        b = buckets.setdefault(key, {
            'meta': (comp_id, mnum, mname, dist, unit, f'{state} {year}'),
            'pools': {},   # category -> list of candidate entries
        })
        b['pools'].setdefault(cat, []).append({
            'discipline': disc,
            'score': float(score),
            'string_id': sid,
            'centres': centres,
            'n_shots': n_shots,
        })

    # Trim each pool to its top N by score, drop matches with too few categories
    qualified = []
    for b in buckets.values():
        for cat, entries in b['pools'].items():
            entries.sort(key=lambda e: e['score'], reverse=True)
            b['pools'][cat] = entries[:TOP_N_PER_CATEGORY]
        if len(b['pools']) >= min_categories:
            qualified.append(b)
    return qualified


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--n', type=int, default=100,
                    help='Number of comparison sets to insert')
    ap.add_argument('--min-categories', type=int, default=3,
                    help='Minimum categories a match must have to qualify')
    ap.add_argument('--reset', action='store_true',
                    help='Truncate survey_sets and survey_set_items first')
    ap.add_argument('--seed', type=int, default=None,
                    help='Random seed for reproducible sampling')
    ap.add_argument('--min-year', type=int, default=None,
                    help='Only consider matches from this year onwards')
    args = ap.parse_args()

    if args.seed is not None:
        random.seed(args.seed)

    conn = get_connection()
    cur = conn.cursor()

    if args.reset:
        cur.execute("TRUNCATE survey_set_items, survey_sets RESTART IDENTITY CASCADE")
        print("Reset survey_sets/survey_set_items")

    qualified = fetch_candidate_matches(cur, args.min_categories, args.min_year)
    yr = f", year >= {args.min_year}" if args.min_year else ""
    print(f"Qualified matches (>= {args.min_categories} cats{yr}): {len(qualified)}")

    if not qualified:
        print("No qualified matches found — aborting")
        return

    # Distribute the N target sets across the qualified matches as evenly as
    # possible. Each set is one "variant" = a random sample of one entry from
    # each category's top-5 pool. With ~3,125 (=5^5) variants per match, we
    # have plenty of unique combinations even at 300 sets / ~114 matches.
    plan = []  # list of bucket references, length == args.n
    base = args.n // len(qualified)
    extra = args.n - base * len(qualified)
    bucket_list = list(qualified)
    random.shuffle(bucket_list)
    for i, b in enumerate(bucket_list):
        copies = base + (1 if i < extra else 0)
        plan.extend([b] * copies)
    random.shuffle(plan)
    print(f"Sampling {len(plan)} variants across {len(bucket_list)} matches")

    seen_variants = set()
    for bucket in plan:
        comp_id, mnum, mname, dist, unit, comp_label = bucket['meta']

        # Sample one entry from each category's pool; avoid producing the
        # exact same variant for the same match twice (very unlikely but
        # protects against tiny pools).
        for _ in range(20):
            picks = {cat: random.choice(pool) for cat, pool in bucket['pools'].items()}
            sig = (comp_id, mnum, tuple(sorted((c, p['string_id']) for c, p in picks.items())))
            if sig not in seen_variants:
                seen_variants.add(sig)
                break

        cur.execute(
            """INSERT INTO survey_sets
                  (competition_id, competition_label, match_number, match_name, distance, distance_unit)
               VALUES (%s,%s,%s,%s,%s,%s) RETURNING set_id""",
            (comp_id, comp_label, mnum, mname, dist, unit),
        )
        set_id = cur.fetchone()[0]

        pos = 0
        for cat in CATEGORY_ORDER:
            entry = picks.get(cat)
            if entry is None:
                continue
            pos += 1
            raw_int = int(entry['score'])
            adrian = adrian_adjusted(cat, entry['discipline'], raw_int, entry['centres'])
            peter  = peter_adjusted (cat, entry['discipline'], raw_int, entry['centres'])
            v5     = v5_adjusted    (cat, entry['discipline'], raw_int, entry['centres'])
            cur.execute(
                """INSERT INTO survey_set_items
                      (set_id, category, discipline, distance, distance_unit,
                       score, string_id, position, centres, adrian_v3, peter_score, v5_score)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                (set_id, cat, entry['discipline'], dist, unit,
                 entry['score'], entry['string_id'], pos,
                 entry['centres'], adrian, peter, v5),
            )

    conn.commit()
    cur.execute("SELECT COUNT(*) FROM survey_sets")
    print(f"survey_sets total rows: {cur.fetchone()[0]}")
    cur.execute("SELECT COUNT(*) FROM survey_set_items")
    print(f"survey_set_items total rows: {cur.fetchone()[0]}")


if __name__ == '__main__':
    main()
