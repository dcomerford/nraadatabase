"""Build the QRA Kings 2026 scores CSV to send to Adrian.

Takes the raw pipeline export (kings_qra2026.csv, same column layout as every
previous K&Q export) and adds the scored columns so it is usable without
re-deriving anything: V5 adjusted, June-13 (Peter) adjusted, and per-shot merit
so 10- and 15-shot matches can be compared like-for-like.

Usage:  python3 build_qra_scores_csv.py
"""
import csv

SRC = 'kings_qra2026.csv'
OUT = 'QRA_Kings_2026_scores.csv'

V5 = {'TR': (1.412, 1.20), 'F-Open': (1.406, 1.00), 'F-Standard': (1.475, 1.00),
      'FTR': (1.450, 1.00), 'Sporter': (1.383, 1.20)}
GRP = {'TR-A': 'TR', 'TR-B': 'TR', 'TR-C': 'TR', 'F-Open': 'F-Open',
       'F-Std-A': 'F-Standard', 'F-Std-B': 'F-Standard', 'FTR': 'FTR',
       'Sporter-Open': 'Sporter', 'Sporter-PC': 'Sporter'}
PETER = {'TR-A': 1.53, 'TR-B': 1.53, 'TR-C': 1.53, 'F-Open': 1.39,
         'F-Std-A': 1.43, 'F-Std-B': 1.43, 'FTR': 1.43,
         'Sporter-Open': 1.47, 'Sporter-PC': 1.53}
CENTRE_W = 0.7

EXTRA = ['v5_group', 'v5_factor', 'v5_conversion', 'merit_pre_factor',
         'merit_per_shot', 'v5_adjusted', 'jun13_adjusted']


def main():
    rows = list(csv.DictReader(open(SRC)))
    cols = list(rows[0].keys()) + EXTRA
    with open(OUT, 'w', newline='') as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in rows:
            grp = GRP[r['discipline']]
            factor, conv = V5[grp]
            pts = int(r['raw_score'].split('.')[0])
            cen = int(r['centres'])
            shots = int(r['shot_count'] or 0)
            merit = pts * conv + cen * CENTRE_W
            r.update({
                'v5_group': grp,
                'v5_factor': f'{factor:.3f}',
                'v5_conversion': f'{conv:.2f}',
                'merit_pre_factor': f'{merit:.2f}',
                'merit_per_shot': f'{merit / shots:.4f}' if shots else '',
                'v5_adjusted': f'{merit * factor:.2f}',
                'jun13_adjusted': f'{(pts + cen) * PETER[r["discipline"]]:.2f}',
            })
            w.writerow(r)
    print(f'Wrote {len(rows)} rows to {OUT}')
    print('Columns:', ', '.join(cols))


if __name__ == '__main__':
    main()
