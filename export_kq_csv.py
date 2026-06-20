"""Export the Kings & Queens dataset as CSV.

One row per string (10/15-shot range result). Includes shooter info,
competition/match details, raw score, V/X centre count parsed from shots_raw,
and the raw shot string.

Usage:
    python3 export_kq_csv.py [-o kings_queens.csv] [--min-year 2024]
"""
import argparse
import csv
from db import get_connection


def count_centres(shots_raw, target_max):
    if not shots_raw:
        return 0
    mark = 'V' if target_max == 5 else 'X'
    return sum(1 for ch in shots_raw if ch == mark)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('-o', '--output', default='kings_queens.csv')
    ap.add_argument('--min-year', type=int, default=None,
                    help='Restrict to year >= N (default: all years)')
    args = ap.parse_args()

    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT
            st.code            AS state,
            c.year             AS year,
            c.name             AS competition,
            s.match_number,
            s.match_name,
            s.distance,
            s.distance_unit,
            s.discipline,
            s.target_max,
            s.place,
            s.shooter_sid,
            sh.first_name,
            sh.last_name,
            cl.club_name,
            s.score,
            s.shots_raw,
            (SELECT COUNT(*) FROM shots WHERE string_id = s.string_id) AS shot_count,
            s.info
        FROM strings s
        JOIN competitions c ON c.competition_id = s.competition_id
        JOIN states       st ON st.state_id     = c.state_id
        LEFT JOIN shooters sh ON sh.sid          = s.shooter_sid
        LEFT JOIN clubs    cl ON cl.club_id      = sh.club_id
        WHERE s.is_kings_queens = TRUE
          AND (%s::int IS NULL OR c.year >= %s)
        ORDER BY c.year DESC, st.code, c.name, s.match_number, s.place NULLS LAST
    """, (args.min_year, args.min_year))

    header = [
        'state', 'year', 'competition',
        'match_number', 'match_name', 'distance', 'distance_unit',
        'discipline', 'target_max',
        'place', 'sid', 'first_name', 'last_name', 'club',
        'raw_score', 'shot_count', 'centres', 'shots_raw',
        'info',
    ]
    n = 0
    with open(args.output, 'w', newline='') as f:
        w = csv.writer(f)
        w.writerow(header)
        for r in cur:
            (state, year, comp, mnum, mname, dist, unit, disc, tmax, place,
             sid, fn, ln, club, score, shots_raw, shot_count, info) = r
            centres = count_centres(shots_raw, tmax)
            w.writerow([
                state, year, comp, mnum, mname, dist, unit,
                disc, tmax, place, sid, fn, ln, club,
                score, shot_count, centres, shots_raw, info,
            ])
            n += 1
    print(f'Wrote {args.output}: {n} rows')


if __name__ == '__main__':
    main()
