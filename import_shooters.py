import csv
from db import get_connection

CSV_PATH = "/Users/dancomerford/Desktop/results/nraa_shooters_complete.csv"


def create_tables(conn):
    """Create clubs and shooters tables."""
    cur = conn.cursor()

    # Drop existing tables if they exist
    cur.execute("DROP TABLE IF EXISTS shooters CASCADE;")
    cur.execute("DROP TABLE IF EXISTS clubs CASCADE;")

    # Create clubs table
    cur.execute("""
        CREATE TABLE clubs (
            club_id SERIAL PRIMARY KEY,
            club_name VARCHAR(255) UNIQUE NOT NULL
        );
    """)

    # Create shooters table
    cur.execute("""
        CREATE TABLE shooters (
            sid INTEGER PRIMARY KEY,
            last_name VARCHAR(100),
            first_name VARCHAR(100),
            pref_name VARCHAR(100),
            club_id INTEGER REFERENCES clubs(club_id)
        );
    """)

    conn.commit()
    print("Tables created successfully.")


def import_data(conn):
    """Import data from CSV."""
    cur = conn.cursor()

    # Read CSV and collect unique clubs
    clubs = set()
    shooters = []

    with open(CSV_PATH, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            clubs.add(row['Club'])
            shooters.append(row)

    print(f"Found {len(clubs)} unique clubs and {len(shooters)} shooters.")

    # Insert clubs
    for club_name in clubs:
        cur.execute(
            "INSERT INTO clubs (club_name) VALUES (%s) ON CONFLICT DO NOTHING;",
            (club_name,)
        )
    conn.commit()
    print("Clubs inserted.")

    # Build club name -> id mapping
    cur.execute("SELECT club_id, club_name FROM clubs;")
    club_map = {name: cid for cid, name in cur.fetchall()}

    # Insert shooters in batch
    shooter_data = [
        (
            int(row['SID']),
            row['Last Name'],
            row['First Name'],
            row['Pref Name'],
            club_map.get(row['Club'])
        )
        for row in shooters
    ]

    from psycopg2.extras import execute_values
    execute_values(
        cur,
        """INSERT INTO shooters (sid, last_name, first_name, pref_name, club_id)
           VALUES %s ON CONFLICT (sid) DO NOTHING;""",
        shooter_data,
        page_size=1000
    )
    conn.commit()
    print("Shooters inserted.")


def verify_import(conn):
    """Verify the import."""
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM clubs;")
    club_count = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM shooters;")
    shooter_count = cur.fetchone()[0]

    print(f"\nImport complete:")
    print(f"  Clubs: {club_count}")
    print(f"  Shooters: {shooter_count}")

    # Sample query
    cur.execute("""
        SELECT s.sid, s.first_name, s.last_name, c.club_name
        FROM shooters s
        JOIN clubs c ON s.club_id = c.club_id
        LIMIT 5;
    """)
    print("\nSample data:")
    for row in cur.fetchall():
        print(f"  {row[0]}: {row[1]} {row[2]} - {row[3]}")


if __name__ == "__main__":
    conn = get_connection()
    create_tables(conn)
    import_data(conn)
    verify_import(conn)
    conn.close()
