"""Backfill the centres column on strings + aggregates by re-parsing the saved CSVs.

The original text score (e.g. '50.10' = 10 centres) is in /tmp/{STATE}_{YEAR}.csv,
but the DB column is NUMERIC so '50.10' collapsed to 50.1 (same as 50.1 = 1 centre).
We re-read the text and update centres explicitly.
"""
import csv
import glob
from db import get_connection


def split_centres(score_str):
    s = (score_str or '').strip()
    if '.' not in s:
        return 0 if s else None
    try:
        return int(s.split('.', 1)[1])
    except ValueError:
        return None


def main():
    conn = get_connection()
    cur = conn.cursor()

    csvs = sorted(glob.glob('/tmp/*_*.csv'))
    total_updated_strings = 0
    total_updated_aggs = 0

    for path in csvs:
        with open(path, newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        comp_name = rows[0]['competition'] if rows else None
        if not comp_name:
            continue
        state_code, year_str = comp_name.split()[0], comp_name.split()[1]
        year = int(year_str)

        # Map competition_id
        cur.execute("""SELECT competition_id FROM competitions
                       WHERE state_id=(SELECT state_id FROM states WHERE code=%s) AND year=%s;""",
                    (state_code, year))
        r = cur.fetchone()
        if not r:
            continue
        comp_id = r[0]

        # Update strings — match by (competition_id, match_number, place, discipline)
        str_updates = 0
        agg_updates = 0
        for row in rows:
            score_txt = row.get('score') or ''
            centres = split_centres(score_txt)
            if centres is None:
                continue
            mn = int(row['match_number']) if row['match_number'] else None
            place = int(row['place']) if row['place'] else None
            disc = row['discipline']
            if row['is_aggregate'] == 'true':
                cur.execute(
                    """UPDATE aggregates SET centres=%s
                       WHERE competition_id=%s AND match_number=%s
                       AND place=%s AND discipline=%s;""",
                    (centres, comp_id, mn, place, disc))
                agg_updates += cur.rowcount
            else:
                cur.execute(
                    """UPDATE strings SET centres=%s
                       WHERE competition_id=%s AND match_number=%s
                       AND place=%s AND discipline=%s;""",
                    (centres, comp_id, mn, place, disc))
                str_updates += cur.rowcount
        conn.commit()
        total_updated_strings += str_updates
        total_updated_aggs += agg_updates
        print(f'  {comp_name:<12} strings={str_updates:>5}  aggs={agg_updates:>5}')

    print(f'\nTotal updated: strings={total_updated_strings} aggs={total_updated_aggs}')
    conn.close()


if __name__ == '__main__':
    main()
