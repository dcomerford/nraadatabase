import csv
import sys
from db import get_connection
from psycopg2.extras import execute_values

# Pass CSV path as argument or use default
CSV_PATH = sys.argv[1] if len(sys.argv) > 1 else "/Users/dancomerford/Desktop/results/nraa_results-2025.csv"


def get_or_create_competition(cur, state_code, year):
    """Get or create a competition for a state/year combo."""
    # Get state_id
    cur.execute("SELECT state_id FROM states WHERE code = %s;", (state_code,))
    result = cur.fetchone()
    if not result:
        raise ValueError(f"Unknown state code: {state_code}")
    state_id = result[0]

    # Check if competition exists
    cur.execute(
        "SELECT competition_id FROM competitions WHERE state_id = %s AND year = %s;",
        (state_id, year)
    )
    result = cur.fetchone()
    if result:
        return result[0]

    # Create new competition
    cur.execute(
        "INSERT INTO competitions (state_id, year, name) VALUES (%s, %s, %s) RETURNING competition_id;",
        (state_id, year, f"{state_code} {year}")
    )
    return cur.fetchone()[0]


def parse_competition_name(name):
    """Parse 'QRA 2025' into (state_code, year)."""
    parts = name.split()
    state_code = parts[0]

    # Handle variations
    if len(parts) == 1:
        # No year provided, will need to infer from filename or context
        raise ValueError(f"No year in competition name: {name}")

    # Extract year (might have suffix like "NRAA 2023 FOS")
    year_str = parts[1]
    year = int(year_str)

    return state_code, year


def build_shooter_lookup(conn):
    """Build lookup dict for matching shooters."""
    cur = conn.cursor()

    cur.execute("""
        SELECT sid, first_name, last_name, pref_name, c.club_name
        FROM shooters s
        JOIN clubs c ON s.club_id = c.club_id;
    """)

    lookup = {}
    for sid, first_name, last_name, pref_name, club_name in cur.fetchall():
        fn_lower = (first_name or '').lower().strip()
        ln_lower = (last_name or '').lower().strip()
        pn_lower = (pref_name or '').lower().strip()
        club_lower = (club_name or '').lower().strip()

        lookup[(fn_lower, ln_lower, club_lower)] = sid
        if pn_lower and pn_lower != fn_lower:
            lookup[(pn_lower, ln_lower, club_lower)] = sid

    cur.execute("SELECT sid, first_name, last_name, pref_name FROM shooters;")

    name_only_lookup = {}
    for sid, first_name, last_name, pref_name in cur.fetchall():
        fn_lower = (first_name or '').lower().strip()
        ln_lower = (last_name or '').lower().strip()
        pn_lower = (pref_name or '').lower().strip()

        key = (fn_lower, ln_lower)
        if key not in name_only_lookup:
            name_only_lookup[key] = sid
        if pn_lower and pn_lower != fn_lower:
            key2 = (pn_lower, ln_lower)
            if key2 not in name_only_lookup:
                name_only_lookup[key2] = sid

    return lookup, name_only_lookup


def match_shooter(first_name, last_name, club, lookup, name_only_lookup):
    """Try to match a shooter, return SID or None."""
    fn = (first_name or '').lower().strip()
    ln = (last_name or '').lower().strip()
    cl = (club or '').lower().strip()

    sid = lookup.get((fn, ln, cl))
    if sid:
        return sid

    sid = name_only_lookup.get((fn, ln))
    return sid


def parse_shots(shots_raw):
    """Parse shots string into list of individual shots."""
    if not shots_raw:
        return []
    return [(i + 1, shot.upper()) for i, shot in enumerate(shots_raw)]


def import_data(conn):
    """Import results data from CSV."""
    cur = conn.cursor()

    print(f"Importing from: {CSV_PATH}")

    # Build shooter lookup
    print("Building shooter lookup...")
    lookup, name_only_lookup = build_shooter_lookup(conn)
    print(f"Lookup has {len(lookup)} entries (with club), {len(name_only_lookup)} entries (name only)")

    # Read CSV and organize data
    print("Reading CSV...")
    comp_map = {}  # competition name -> competition_id
    aggregates = []
    strings = []
    unmatched = []

    with open(CSV_PATH, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            comp_name = row['competition']

            # Get or create competition
            if comp_name not in comp_map:
                state_code, year = parse_competition_name(comp_name)
                comp_id = get_or_create_competition(cur, state_code, year)
                comp_map[comp_name] = comp_id
                conn.commit()

            is_aggregate = 'Aggregate' in row['match_name']

            shooter_sid = match_shooter(
                row['first_name'],
                row['last_name'],
                row['club'],
                lookup,
                name_only_lookup
            )

            if shooter_sid is None:
                unmatched.append({**row, 'is_aggregate': is_aggregate})
            elif is_aggregate:
                aggregates.append({**row, 'sid': shooter_sid})
            else:
                strings.append({**row, 'sid': shooter_sid})

    print(f"Found {len(comp_map)} competitions")
    print(f"Aggregates: {len(aggregates)}, Strings: {len(strings)}, Unmatched: {len(unmatched)}")

    # Insert aggregates
    print("Inserting aggregates...")
    agg_data = [
        (
            comp_map[row['competition']],
            int(row['match_number']) if row['match_number'] else None,
            row['match_name'],
            row['discipline'],
            int(row['place']) if row['place'] else None,
            row['sid'],
            row['state'],
            row['info'],
            float(row['score']) if row['score'] else None
        )
        for row in aggregates
    ]

    if agg_data:
        execute_values(
            cur,
            """INSERT INTO aggregates
               (competition_id, match_number, match_name, discipline, place, shooter_sid, state, info, score)
               VALUES %s;""",
            agg_data,
            page_size=1000
        )
        conn.commit()
    print(f"Inserted {len(agg_data)} aggregates.")

    # Insert strings
    print("Inserting strings...")
    string_records = []
    for row in strings:
        string_records.append((
            comp_map[row['competition']],
            int(row['match_number']) if row['match_number'] else None,
            row['match_name'],
            int(row['distance']) if row['distance'] else None,
            row['distance_unit'],
            row['discipline'],
            int(row['place']) if row['place'] else None,
            row['sid'],
            row['state'],
            row['shots'],
            row['info'],
            float(row['score']) if row['score'] else None
        ))

    # Track which string_ids we're adding
    cur.execute("SELECT COALESCE(MAX(string_id), 0) FROM strings;")
    start_string_id = cur.fetchone()[0]

    if string_records:
        execute_values(
            cur,
            """INSERT INTO strings
               (competition_id, match_number, match_name, distance, distance_unit, discipline, place, shooter_sid, state, shots_raw, info, score)
               VALUES %s;""",
            string_records,
            page_size=1000
        )
        conn.commit()
    print(f"Inserted {len(string_records)} strings.")

    # Insert shots for NEW strings only
    print("Parsing and inserting shots...")
    cur.execute(
        "SELECT string_id, shots_raw FROM strings WHERE string_id > %s AND shots_raw IS NOT NULL AND shots_raw != '';",
        (start_string_id,)
    )

    shots_data = []
    for string_id, shots_raw in cur.fetchall():
        for shot_num, shot_val in parse_shots(shots_raw):
            shots_data.append((string_id, shot_num, shot_val))

    if shots_data:
        execute_values(
            cur,
            "INSERT INTO shots (string_id, shot_number, shot_value) VALUES %s;",
            shots_data,
            page_size=5000
        )
        conn.commit()
    print(f"Inserted {len(shots_data)} individual shots.")

    # Insert unmatched
    if unmatched:
        print(f"Inserting {len(unmatched)} unmatched results...")
        unmatched_data = [
            (
                row['competition'],
                int(row['match_number']) if row['match_number'] else None,
                row['match_name'],
                int(row['distance']) if row['distance'] else None,
                row['distance_unit'],
                row['discipline'],
                int(row['place']) if row['place'] else None,
                row['full_name'],
                row['last_name'],
                row['first_name'],
                row['club'],
                row['state'],
                row['shots'],
                row['info'],
                float(row['score']) if row['score'] else None,
                row['is_aggregate']
            )
            for row in unmatched
        ]

        execute_values(
            cur,
            """INSERT INTO unmatched_results
               (competition, match_number, match_name, distance, distance_unit, discipline, place,
                full_name, last_name, first_name, club, state, shots, info, score, is_aggregate)
               VALUES %s;""",
            unmatched_data,
            page_size=1000
        )
        conn.commit()


def verify_import(conn):
    """Show current database stats."""
    cur = conn.cursor()

    print("\n=== Database Stats ===")

    # Competitions by year
    cur.execute("""
        SELECT s.code, c.year, COUNT(DISTINCT a.aggregate_id) as aggs, COUNT(DISTINCT st.string_id) as strings
        FROM competitions c
        JOIN states s ON c.state_id = s.state_id
        LEFT JOIN aggregates a ON a.competition_id = c.competition_id
        LEFT JOIN strings st ON st.competition_id = c.competition_id
        GROUP BY s.code, c.year
        ORDER BY c.year, s.code;
    """)
    print("\nCompetitions:")
    for code, year, aggs, strings in cur.fetchall():
        print(f"  {code} {year}: {aggs} aggregates, {strings} strings")

    # Totals
    tables = ['competitions', 'aggregates', 'strings', 'shots', 'unmatched_results']
    print("\nTotals:")
    for table in tables:
        cur.execute(f"SELECT COUNT(*) FROM {table};")
        print(f"  {table}: {cur.fetchone()[0]}")


if __name__ == "__main__":
    conn = get_connection()
    import_data(conn)
    verify_import(conn)
    conn.close()
