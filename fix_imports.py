"""Re-scrape + re-import the 11 events that needed manual Kings/Queens match lists.

Wipes each comp's data first, then reimports with explicit --kings-matches.
"""
import subprocess

FIXES = [
    ('ACTRA', 2024, 'https://www.results.nraa.com.au/act-championships-2024-results/',
        '11,12,13,16,17,18,21,22'),
    ('NSWRA', 2024, 'https://www.results.nraa.com.au/nswra-148th-open-championships-2024-nsw-results/',
        '12,13,16,17,18,19,20,22,23,24'),
    ('WARA',  2024, 'https://www.results.nraa.com.au/wara-kings-prize-2024-results/',
        '8,12,13,21,23,24'),
    ('WARA',  2023, 'https://www.results.nraa.com.au/wara-kings-prize-2023-results/',
        '8,12,13,21,23,24'),
    ('NTRA',  2024, 'https://www.results.nraa.com.au/nt-2024-kings-series-results/',
        '10,11,12,13,15,16,17,18,20,21'),
    ('TRA',   2024, 'https://www.results.nraa.com.au/tra-126th-tasmanian-championship-and-100th-kings-prize-results/',
        '9,10,11,12,14,15,16,17,19,20'),
    ('TRA',   2023, 'https://www.results.nraa.com.au/tasmanian-125th-annual-championships-and-kings-prize-meeting-2023-results/',
        '9,10,11,12,14,15,16,17,19,20'),
    ('NRAA',  2022, 'https://www.results.nraa.com.au/nraa-50th-national-rifle-championships-2022-results/',
        '11,12,13,14,16,17,18,19,21,22,23'),
    ('SARA',  2022, 'https://www.results.nraa.com.au/sara-queens-series-2022-1086-results/',
        '10,11,12,13,19,20,24,25,26,32,33'),
    ('NTRA',  2022, 'https://www.results.nraa.com.au/ntra-queens-series-2022-results/',
        '10,11,12,13,15,16,17,18,20,21'),
    ('TRA',   2022, 'https://www.results.nraa.com.au/tra-queens-prize-2022-results/',
        '8,9,10,11,13,14,15,16,18,19'),
    ('WARA',  2022, 'https://www.results.nraa.com.au/wara-queen-elizabeth-the-second-memorial-prize-meeting-results/',
        '7,13,21,23,24'),
]

PSQL = '/opt/homebrew/opt/postgresql@16/bin/psql'

def wipe_comp(code, year):
    sql = f"""
DELETE FROM shots WHERE string_id IN
  (SELECT string_id FROM strings WHERE competition_id IN
    (SELECT competition_id FROM competitions WHERE state_id=
      (SELECT state_id FROM states WHERE code='{code}') AND year={year}));
DELETE FROM strings WHERE competition_id IN
  (SELECT competition_id FROM competitions WHERE state_id=
    (SELECT state_id FROM states WHERE code='{code}') AND year={year});
DELETE FROM aggregates WHERE competition_id IN
  (SELECT competition_id FROM competitions WHERE state_id=
    (SELECT state_id FROM states WHERE code='{code}') AND year={year});
DELETE FROM competitions WHERE state_id=
  (SELECT state_id FROM states WHERE code='{code}') AND year={year};
"""
    subprocess.run([PSQL, '-d', 'nraadb', '-c', sql], capture_output=True)


def main():
    print(f'{"Comp":<12} {"Strings":>8} {"Kings":>6} {"Sporter":>8}')
    print('-' * 40)
    for code, year, url, kings in FIXES:
        wipe_comp(code, year)
        out_csv = f'/tmp/{code}_{year}.csv'
        subprocess.run(['python3', 'scrape_nraa.py', url, code, str(year),
                        '--kings-matches', kings, '-o', out_csv],
                       capture_output=True)
        subprocess.run(['python3', 'import_scraped.py', out_csv], capture_output=True)
        q = subprocess.run(
            [PSQL, '-d', 'nraadb', '-At', '-c',
             f"""SELECT
                   (SELECT COUNT(*) FROM strings st JOIN competitions c USING(competition_id)
                    JOIN states s USING(state_id)
                    WHERE s.code='{code}' AND c.year={year}),
                   COUNT(*) FILTER (WHERE is_kings_queens),
                   COUNT(*) FILTER (WHERE is_kings_queens AND discipline LIKE 'Sporter%%')
                 FROM strings st JOIN competitions c USING(competition_id)
                 JOIN states s USING(state_id)
                 WHERE s.code='{code}' AND c.year={year};"""],
            capture_output=True, text=True)
        try:
            total, kn, sp = (int(x) for x in q.stdout.strip().split('|'))
        except Exception:
            total = kn = sp = 0
        print(f'{code} {year:<6} {total:>8} {kn:>6} {sp:>8}')


if __name__ == '__main__':
    main()
