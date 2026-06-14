"""Import a scrape_nraa.py CSV into the local nraadb.

Reads CSV produced by scrape_nraa.py. Splits rows by is_aggregate and inserts
into aggregates / strings, propagating is_kings_queens + target_max.

Unmatched shooters land in unmatched_results so nothing is silently dropped.
"""
import csv
import sys

from db import get_connection
from psycopg2.extras import execute_values


def parse_competition_name(name):
    parts = name.split()
    return parts[0], int(parts[1])


def get_or_create_competition(cur, state_code, year):
    cur.execute("SELECT state_id FROM states WHERE code = %s;", (state_code,))
    r = cur.fetchone()
    if not r:
        raise ValueError(f'Unknown state code: {state_code}')
    state_id = r[0]
    cur.execute("SELECT competition_id FROM competitions WHERE state_id=%s AND year=%s;",
                (state_id, year))
    r = cur.fetchone()
    if r:
        return r[0]
    cur.execute("""INSERT INTO competitions (state_id, year, name)
                   VALUES (%s,%s,%s) RETURNING competition_id;""",
                (state_id, year, f'{state_code} {year}'))
    return cur.fetchone()[0]


SYNTH_SID_START = 900000  # Auto-created shooters get SIDs from here upward.


def build_shooter_lookup(conn):
    cur = conn.cursor()
    cur.execute("""SELECT sid, first_name, last_name, pref_name, c.club_name
                   FROM shooters s LEFT JOIN clubs c ON s.club_id=c.club_id;""")
    with_club = {}
    name_only = {}
    for sid, fn, ln, pn, club in cur.fetchall():
        fn_l = (fn or '').lower().strip()
        ln_l = (ln or '').lower().strip()
        pn_l = (pn or '').lower().strip()
        cl_l = (club or '').lower().strip()
        with_club[(fn_l, ln_l, cl_l)] = sid
        if pn_l and pn_l != fn_l:
            with_club[(pn_l, ln_l, cl_l)] = sid
        name_only.setdefault((fn_l, ln_l), sid)
        if pn_l and pn_l != fn_l:
            name_only.setdefault((pn_l, ln_l), sid)
    return with_club, name_only


def next_synth_sid(cur):
    cur.execute("SELECT COALESCE(MAX(sid), %s - 1) + 1 FROM shooters WHERE sid >= %s;",
                (SYNTH_SID_START, SYNTH_SID_START))
    return cur.fetchone()[0]


def get_or_create_club(cur, club_name, cache):
    if not club_name:
        return None
    if club_name in cache:
        return cache[club_name]
    cur.execute("SELECT club_id FROM clubs WHERE club_name = %s;", (club_name,))
    r = cur.fetchone()
    if r:
        cache[club_name] = r[0]
        return r[0]
    cur.execute("INSERT INTO clubs (club_name) VALUES (%s) RETURNING club_id;",
                (club_name,))
    cid = cur.fetchone()[0]
    cache[club_name] = cid
    return cid


def auto_create_shooter(cur, first, last, club, club_cache, sid_counter):
    """Create a new shooter with a synthesized SID. Returns the SID."""
    club_id = get_or_create_club(cur, club, club_cache)
    sid = sid_counter[0]
    sid_counter[0] += 1
    cur.execute(
        """INSERT INTO shooters (sid, first_name, last_name, pref_name, club_id)
           VALUES (%s, %s, %s, %s, %s);""",
        (sid, first, last, first, club_id),
    )
    return sid


def match_shooter(first, last, club, with_club, name_only):
    fn = (first or '').lower().strip()
    ln = (last or '').lower().strip()
    cl = (club or '').lower().strip()
    sid = with_club.get((fn, ln, cl))
    if sid:
        return sid
    return name_only.get((fn, ln))


def parse_shots(shots_raw):
    return [(i + 1, s.upper()) for i, s in enumerate(shots_raw)] if shots_raw else []


def import_csv(csv_path):
    conn = get_connection()
    cur = conn.cursor()

    with_club, name_only = build_shooter_lookup(conn)
    print(f'Shooter lookup: {len(with_club)} with-club, {len(name_only)} name-only')

    sid_counter = [next_synth_sid(cur)]
    club_cache = {}
    auto_created = 0

    aggregates, strings = [], []
    comp_cache = {}

    with open(csv_path, newline='', encoding='utf-8') as f:
        for row in csv.DictReader(f):
            comp = row['competition']
            if comp not in comp_cache:
                state, year = parse_competition_name(comp)
                comp_cache[comp] = get_or_create_competition(cur, state, year)
                conn.commit()
            comp_id = comp_cache[comp]

            sid = match_shooter(row['first_name'], row['last_name'], row['club'],
                                with_club, name_only)
            if sid is None:
                sid = auto_create_shooter(cur, row['first_name'], row['last_name'],
                                          row['club'], club_cache, sid_counter)
                fn_l = row['first_name'].lower().strip()
                ln_l = row['last_name'].lower().strip()
                cl_l = (row['club'] or '').lower().strip()
                with_club[(fn_l, ln_l, cl_l)] = sid
                name_only.setdefault((fn_l, ln_l), sid)
                auto_created += 1

            is_agg = row['is_aggregate'] == 'true'
            if is_agg:
                aggregates.append({**row, 'comp_id': comp_id, 'sid': sid})
            else:
                strings.append({**row, 'comp_id': comp_id, 'sid': sid})

    if auto_created:
        conn.commit()
        print(f'Auto-created {auto_created} new shooter records (synthesized SIDs)')

    # --- aggregates --------------------------------------------------------
    agg_rows = [
        (
            r['comp_id'],
            int(r['match_number']) if r['match_number'] else None,
            r['match_name'],
            r['discipline'],
            int(r['place']) if r['place'] else None,
            r['sid'],
            r['state'],
            r['info'],
            float(r['score']) if r['score'] else None,
        )
        for r in aggregates
    ]
    if agg_rows:
        execute_values(
            cur,
            """INSERT INTO aggregates
               (competition_id, match_number, match_name, discipline, place,
                shooter_sid, state, info, score) VALUES %s;""",
            agg_rows, page_size=1000,
        )
        conn.commit()

    # --- strings -----------------------------------------------------------
    str_rows = [
        (
            r['comp_id'],
            int(r['match_number']) if r['match_number'] else None,
            r['match_name'],
            int(r['distance']) if r['distance'] else None,
            r['distance_unit'] or None,
            r['discipline'],
            int(r['place']) if r['place'] else None,
            r['sid'],
            r['state'],
            r['shots'],
            r['info'],
            float(r['score']) if r['score'] else None,
            r['is_kings_queens'] == 'true',
            int(r['target_max']),
        )
        for r in strings
    ]

    cur.execute('SELECT COALESCE(MAX(string_id),0) FROM strings;')
    start_id = cur.fetchone()[0]

    if str_rows:
        execute_values(
            cur,
            """INSERT INTO strings
               (competition_id, match_number, match_name, distance, distance_unit,
                discipline, place, shooter_sid, state, shots_raw, info, score,
                is_kings_queens, target_max) VALUES %s;""",
            str_rows, page_size=1000,
        )
        conn.commit()

    # --- shots -------------------------------------------------------------
    cur.execute(
        """SELECT string_id, shots_raw FROM strings
           WHERE string_id > %s AND shots_raw IS NOT NULL AND shots_raw != '';""",
        (start_id,),
    )
    shot_rows = []
    for sid_, shots_raw in cur.fetchall():
        shot_rows.extend((sid_, n, v) for n, v in parse_shots(shots_raw))
    if shot_rows:
        execute_values(
            cur,
            "INSERT INTO shots (string_id, shot_number, shot_value) VALUES %s;",
            shot_rows, page_size=5000,
        )
        conn.commit()

    print(f'Imported: aggregates={len(agg_rows)}, strings={len(str_rows)}, '
          f'shots={len(shot_rows)}, auto_created={auto_created}')
    conn.close()


if __name__ == '__main__':
    csv_path = sys.argv[1] if len(sys.argv) > 1 else '/tmp/vra_2026.csv'
    import_csv(csv_path)
