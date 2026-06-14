"""Scrape + import a batch of NRAA Kings/Queens events using auto-detected match numbers.

Usage:
    python3 bulk_import.py
"""
import subprocess
import sys

# (state_code, year, url) -- all Kings/Queens events for 2024, 2023, 2022
EVENTS = [
    # 2024
    ('ACTRA', 2024, 'https://www.results.nraa.com.au/act-championships-2024-results/'),
    ('NQRA',  2024, 'https://www.results.nraa.com.au/nqra-kings-2024-results/'),
    ('NRAA',  2024, 'https://www.results.nraa.com.au/nraa-52nd-national-rifle-championships-2024-results/'),
    ('NSWRA', 2024, 'https://www.results.nraa.com.au/nswra-148th-open-championships-2024-nsw-results/'),
    ('NTRA',  2024, 'https://www.results.nraa.com.au/nt-2024-kings-series-results/'),
    ('QRA',   2024, 'https://www.results.nraa.com.au/qra-state-championships-duncan-kings-2024-results/'),
    ('SARA',  2024, 'https://www.results.nraa.com.au/sara-kings-series-2024-results/'),
    ('TRA',   2024, 'https://www.results.nraa.com.au/tra-126th-tasmanian-championship-and-100th-kings-prize-results/'),
    ('VRA',   2024, 'https://www.results.nraa.com.au/victorian-rifle-association-2024-kings-series-results/'),
    ('WARA',  2024, 'https://www.results.nraa.com.au/wara-kings-prize-2024-results/'),
    # 2023
    ('ACTRA', 2023, 'https://www.results.nraa.com.au/act-championships-2023-results/'),
    ('NQRA',  2023, 'https://www.results.nraa.com.au/nqra-kings-2023-results/'),
    ('NRAA',  2023, 'https://www.results.nraa.com.au/nraa-festival-of-shooting-2023-51st-national-rifle-championships-results/'),
    ('NSWRA', 2023, 'https://www.results.nraa.com.au/nswra-147th-annual-open-championships-2023-nsw-results/'),
    ('QRA',   2023, 'https://www.results.nraa.com.au/qra-state-championships-duncan-and-kings-2023-results/'),
    ('SARA',  2023, 'https://www.results.nraa.com.au/sara-kings-results/'),
    ('TRA',   2023, 'https://www.results.nraa.com.au/tasmanian-125th-annual-championships-and-kings-prize-meeting-2023-results/'),
    ('VRA',   2023, 'https://www.results.nraa.com.au/victorian-rifle-association-2023-kings-series-results/'),
    ('WARA',  2023, 'https://www.results.nraa.com.au/wara-kings-prize-2023-results/'),
    # 2022 (Queens)
    ('ACTRA', 2022, 'https://www.results.nraa.com.au/act-championships-2022-results/'),
    ('NQRA',  2022, 'https://www.results.nraa.com.au/nqra-queens-2022-results/'),
    ('NRAA',  2022, 'https://www.results.nraa.com.au/nraa-50th-national-rifle-championships-2022-results/'),
    ('NSWRA', 2022, 'https://www.results.nraa.com.au/nswra-146th-annual-open-championships-2022-nsw-results/'),
    ('NTRA',  2022, 'https://www.results.nraa.com.au/ntra-queens-series-2022-results/'),
    ('SARA',  2022, 'https://www.results.nraa.com.au/sara-queens-series-2022-1086-results/'),
    ('TRA',   2022, 'https://www.results.nraa.com.au/tra-queens-prize-2022-results/'),
    ('VRA',   2022, 'https://www.results.nraa.com.au/victorian-rifle-association-2022-queens-series-results/'),
    ('WARA',  2022, 'https://www.results.nraa.com.au/wara-queen-elizabeth-the-second-memorial-prize-meeting-results/'),
]


def run(cmd):
    return subprocess.run(cmd, capture_output=True, text=True)


def main():
    print(f'{"Comp":<12} {"Strings":>8} {"Kings":>6} {"Sporter":>8} {"Auto-detected matches":<60}')
    print('-' * 100)
    totals = {'strings': 0, 'kings': 0, 'sporter': 0, 'failed': []}

    for code, year, url in EVENTS:
        out_csv = f'/tmp/{code}_{year}.csv'
        scrape = run(['python3', 'scrape_nraa.py', url, code, str(year), '-o', out_csv])
        if scrape.returncode != 0:
            print(f'{code} {year}: SCRAPE FAILED')
            print(scrape.stderr[-500:])
            totals['failed'].append(f'{code} {year} (scrape)')
            continue

        # Parse stats from scraper output
        scr_out = scrape.stderr
        detected = ''
        kings_n = strings_n = 0
        for line in scr_out.splitlines():
            line = line.strip()
            if line.startswith('strings:'):
                strings_n = int(line.split(':',1)[1].strip())
            elif line.startswith('auto-detected'):
                detected = line.split(':',1)[1].strip()

        imp = run(['python3', 'import_scraped.py', out_csv])
        if imp.returncode != 0:
            print(f'{code} {year}: IMPORT FAILED')
            print(imp.stderr[-500:])
            totals['failed'].append(f'{code} {year} (import)')
            continue

        # Pull Kings/Sporter counts from psql
        q = run(['/opt/homebrew/opt/postgresql@16/bin/psql', '-d', 'nraadb', '-At', '-c',
                 f"""SELECT
                       COUNT(*) FILTER (WHERE is_kings_queens),
                       COUNT(*) FILTER (WHERE is_kings_queens AND discipline LIKE 'Sporter%')
                     FROM strings st JOIN competitions c USING(competition_id)
                     JOIN states s USING(state_id)
                     WHERE s.code='{code}' AND c.year={year};"""])
        try:
            kings_n, sporter_n = (int(x) for x in q.stdout.strip().split('|'))
        except Exception:
            kings_n, sporter_n = 0, 0

        totals['strings'] += strings_n
        totals['kings'] += kings_n
        totals['sporter'] += sporter_n
        print(f'{code} {year:<6} {strings_n:>8} {kings_n:>6} {sporter_n:>8}   {detected}')

    print('-' * 100)
    print(f"{'TOTAL':<12} {totals['strings']:>8} {totals['kings']:>6} {totals['sporter']:>8}")
    if totals['failed']:
        print('\nFailures:')
        for f in totals['failed']:
            print(' ', f)


if __name__ == '__main__':
    main()
