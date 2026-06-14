"""Bootstrap clubs+shooters tables from shooters.txt.

Line format:
    ID: 41748 | Denis Aarons (Denis) | Pacific Rifle Club
"""
import re
import sys
from db import get_connection
from psycopg2.extras import execute_values

TXT_PATH = sys.argv[1] if len(sys.argv) > 1 else 'shooters.txt'
LINE_RE = re.compile(r'^ID:\s*(\d+)\s*\|\s*(.+?)\s*\(([^)]*)\)\s*\|\s*(.+?)\s*$')


def split_name(full):
    parts = full.split()
    if len(parts) == 1:
        return '', parts[0]
    return ' '.join(parts[:-1]), parts[-1]


def main():
    rows = []
    with open(TXT_PATH, encoding='utf-8') as f:
        for line in f:
            m = LINE_RE.match(line)
            if not m:
                continue
            sid = int(m.group(1))
            first_part, pref, club = m.group(2), m.group(3), m.group(4)
            first_name, last_name = split_name(first_part)
            rows.append((sid, last_name, first_name, pref, club))

    print(f'Parsed {len(rows)} shooters from {TXT_PATH}')
    clubs = sorted({r[4] for r in rows})
    print(f'Found {len(clubs)} unique clubs')

    conn = get_connection()
    cur = conn.cursor()

    execute_values(
        cur,
        'INSERT INTO clubs (club_name) VALUES %s ON CONFLICT (club_name) DO NOTHING;',
        [(c,) for c in clubs],
        page_size=1000,
    )
    conn.commit()

    cur.execute('SELECT club_id, club_name FROM clubs;')
    club_map = {name: cid for cid, name in cur.fetchall()}

    shooter_rows = [
        (sid, last, first, pref, club_map[club])
        for sid, last, first, pref, club in rows
    ]
    execute_values(
        cur,
        '''INSERT INTO shooters (sid, last_name, first_name, pref_name, club_id)
           VALUES %s ON CONFLICT (sid) DO NOTHING;''',
        shooter_rows,
        page_size=1000,
    )
    conn.commit()

    cur.execute('SELECT COUNT(*) FROM shooters;')
    print(f'  shooters table: {cur.fetchone()[0]} rows')
    cur.execute('SELECT COUNT(*) FROM clubs;')
    print(f'  clubs table:    {cur.fetchone()[0]} rows')
    conn.close()


if __name__ == '__main__':
    main()
