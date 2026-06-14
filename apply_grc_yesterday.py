"""Apply our three candidate formulas to yesterday's GRC Point Shoot competition
and compare against the formula that was actually used on the day.

Yesterday's formula (multiplier-only, no offset):
    MCSI = (raw + centres) × mult
    TR=1.53  FS=1.43  FO=1.39  SP=1.53  SO=1.47

Three candidates from our analysis:
    A) Original 2014 linear
    B) New linear (7-comp derived)
    C) Proposed formula (raw×1.2 + centres×0.7) × factor   for 50-pt
                        (raw + centres) × factor             for 60-pt
"""
import csv

CSV_PATH = '/Users/dancomerford/Downloads/GRC_-_Point_Shoot_-_June_13th_2026-06-13_scores.csv'

# Yesterday's formula
YESTERDAY = {
    'Target Rifle': 1.53,
    'Sporter':      1.53,   # SP = Sporter Premier / Production Class
    'Sporter Open': 1.47,
    'F-Standard':   1.43,
    'F-Open':       1.39,
    'FTR':          1.43,   # FTR not in the list — using FS factor as proxy
}

# Original 2014 linear (mult, offset)
ORIGINAL = {
    'Target Rifle': (1.62, 8.4),
    'Sporter':      (1.50, 12.0),
    'Sporter Open': (1.50, 12.0),
    'F-Standard':   (1.42, 1.8),
    'F-Open':       (1.42, 1.8),
    'FTR':          (1.42, 1.8),
}

# New linear (derived from 7-comp split-Sporter analysis)
NEW_LINEAR = {
    'Target Rifle': (1.534, 8.4),
    'Sporter':      (1.499, 12.0),  # Sporter-PC factor
    'Sporter Open': (1.487, 12.0),  # Sporter-Open factor
    'F-Standard':   (1.42, 1.8),
    'F-Open':       (1.42, 1.8),
    'FTR':          (1.42, 1.8),
}

# Proposed formula factors (derived from 22-comp combined-Sporter analysis)
PROPOSED_FACTORS = {
    'Target Rifle': 0.9796,
    'Sporter':      1.0060,   # Combined Sporter factor
    'Sporter Open': 1.0060,   # Combined Sporter factor
    'F-Standard':   1.0146,
    'F-Open':       0.9708,
    'FTR':          1.0160,
}


def is_50pt(discipline):
    """TR, Sporter, Sporter Open shoot on 50-pt targets per the CSV's 'Shooting' column."""
    return discipline in ('Target Rifle', 'Sporter', 'Sporter Open')


def mcsi_yesterday(raw, centres, discipline):
    mult = YESTERDAY[discipline]
    return (raw + centres) * mult


def mcsi_original(raw, centres, discipline):
    mult, offset = ORIGINAL[discipline]
    return (raw + centres) * mult + offset


def mcsi_new_linear(raw, centres, discipline):
    mult, offset = NEW_LINEAR[discipline]
    return (raw + centres) * mult + offset


def mcsi_proposed(raw, centres, discipline):
    factor = PROPOSED_FACTORS[discipline]
    if is_50pt(discipline):
        return (raw * 1.2 + centres * 0.7) * factor
    return (raw + centres) * factor


def main():
    rows = []
    with open(CSV_PATH) as f:
        reader = csv.DictReader(f)
        for r in reader:
            disc = r['Discipline']
            if is_50pt(disc):
                raw = int(r['Total TR score'])
                centres = int(r['Total TR V'])
                scoring = 'TR (50-pt)'
            else:
                raw = int(r['Total F-Class score'])
                centres = int(r['Total F-Class X'])
                scoring = 'F-Class (60-pt)'

            yest = mcsi_yesterday(raw, centres, disc)
            orig = mcsi_original(raw, centres, disc)
            new = mcsi_new_linear(raw, centres, disc)
            prop = mcsi_proposed(raw, centres, disc)

            rows.append({
                'name': r['Shooter'],
                'disc': disc,
                'scoring': scoring,
                'raw': raw,
                'cs': centres,
                'score_str': f'{raw}.{centres}',
                'yesterday': yest,
                'original': orig,
                'new_linear': new,
                'proposed': prop,
            })

    # Sort by yesterday's MCSI to match the printed sheet
    rows.sort(key=lambda x: -x['yesterday'])

    print('=' * 130)
    print('GRC POINT SHOOT — 13 JUN 2026 — Four formulas head-to-head')
    print('=' * 130)
    print()
    print(f'{"":>3} {"Shooter":<20} {"Discipline":<14} {"Score":<10} '
          f'{"Yesterday":>11} {"Original":>11} {"New Linear":>11} {"Proposed":>11}')
    print(f'{"":>3} {"":<20} {"":<14} {"raw.V":<10} '
          f'{"(used)":>11} {"(2014)":>11} {"(7-comp)":>11} {"(22-comp)":>11}')
    print('-' * 130)
    for i, r in enumerate(rows, 1):
        print(f'{i:>3} {r["name"]:<20} {r["disc"]:<14} {r["score_str"]:<10} '
              f'{r["yesterday"]:>11.2f} {r["original"]:>11.2f} {r["new_linear"]:>11.2f} '
              f'{r["proposed"]:>11.2f}')
    print()

    # === Rankings under each formula ========================================
    print('=' * 130)
    print('RANKINGS — who wins under each formula?')
    print('=' * 130)
    print()
    print(f'{"Rank":<5} {"Yesterday":<35} {"Original 2014":<35} '
          f'{"New Linear":<35} {"Proposed":<35}')
    print('-' * 130)
    by_y = sorted(rows, key=lambda x: -x['yesterday'])
    by_o = sorted(rows, key=lambda x: -x['original'])
    by_n = sorted(rows, key=lambda x: -x['new_linear'])
    by_p = sorted(rows, key=lambda x: -x['proposed'])
    for i in range(min(15, len(rows))):
        py = f'{by_y[i]["name"]} ({by_y[i]["disc"][:4]}) {by_y[i]["yesterday"]:.2f}'
        po = f'{by_o[i]["name"]} ({by_o[i]["disc"][:4]}) {by_o[i]["original"]:.2f}'
        pn = f'{by_n[i]["name"]} ({by_n[i]["disc"][:4]}) {by_n[i]["new_linear"]:.2f}'
        pp = f'{by_p[i]["name"]} ({by_p[i]["disc"][:4]}) {by_p[i]["proposed"]:.2f}'
        print(f'{i+1:<5} {py:<35} {po:<35} {pn:<35} {pp:<35}')

    # === Focus: Dan Comerford specifically ===================================
    print()
    print('=' * 130)
    print('DAN COMERFORD vs LISA DONNELLY — the case from the original question')
    print('=' * 130)
    print()
    for r in rows:
        if r['name'] in ('Dan Comerford', 'Lisa Donnelly'):
            print(f'  {r["name"]:<20} {r["disc"]:<14} {r["score_str"]:<10} ('
                  f'F-Class total {r["raw"]}.{r["cs"]})')
            print(f'    Yesterday:  {r["yesterday"]:>8.2f}')
            print(f'    Original:   {r["original"]:>8.2f}')
            print(f'    New Linear: {r["new_linear"]:>8.2f}')
            print(f'    Proposed:   {r["proposed"]:>8.2f}')
            print()


if __name__ == '__main__':
    main()
