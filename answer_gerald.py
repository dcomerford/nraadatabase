"""Produce the exact numbers that answer Gerald's specific technical objections,
using the ACTUAL V5 factors currently presented to the club.

V5:  Adjusted = (Score*Conv + Centres*0.7) * Factor
"""
FACTOR = {'TR': 1.412, 'F-Open': 1.406, 'F-Std': 1.475, 'FTR': 1.450, 'Sporter': 1.383}
CONV   = {'TR': 1.20,  'F-Open': 1.00,  'F-Std': 1.00,  'FTR': 1.00,  'Sporter': 1.20}
MAXPTS = {'TR': 50,     'F-Open': 60,   'F-Std': 60,    'FTR': 60,    'Sporter': 50}
# pre-factor top-40 cohort means straight from v5_calibration.py STEP 3
PREMEAN = {'TR': 66.907, 'F-Open': 67.198, 'F-Std': 64.062, 'FTR': 65.165, 'Sporter': 68.292}
TARGET = 94.471
W = 0.7
ORDER = ['TR', 'F-Open', 'F-Std', 'FTR', 'Sporter']

def mcsi(disc, pct, centres, w=W):
    pts = MAXPTS[disc] * pct
    return (pts * CONV[disc] + centres * w) * FACTOR[disc]

print("="*82)
print("PART A — The factor is NOT a magic number.  Factor = 94.471 / (cohort mean)")
print("="*82)
print(f"{'Discipline':<10}{'top-40 cohort mean':>20}{'94.471 / mean':>16}{'V5 factor':>12}")
print("-"*82)
for d in ORDER:
    print(f"{d:<10}{PREMEAN[d]:>20.3f}{TARGET/PREMEAN[d]:>16.3f}{FACTOR[d]:>12.3f}")
print("Reads: the discipline whose strong scores sit FURTHEST below the shared 67 ceiling")
print("(F-Std, 64.06) earns the LARGEST factor; the one closest to it (Sporter, 68.29) the")
print("smallest. The factor simply lifts every discipline's TOP-COHORT AVERAGE to 94.47.")
print()

print("="*82)
print("PART B — Gerald's equal-relative-score matrix (equal % of possible, equal centres)")
print("="*82)
for centres in (10, 6):
    print(f"\n  --- every shooter on the SAME % of possible with {centres} centres ---")
    print(f"  {'% poss':>7} " + "".join(f"{d:>10}" for d in ORDER) + "   winner")
    for pct in (1.00, 0.98, 0.96, 0.94, 0.90):
        vals = {d: mcsi(d, pct, centres) for d in ORDER}
        win = max(vals, key=vals.get)
        print(f"  {pct*100:>6.0f}% " + "".join(f"{vals[d]:>10.2f}" for d in ORDER) + f"   {win}")
print()
print("  F-Std leads EVERY equal-score row. This is real and follows directly from Part A:")
print("  because the factor equalises each discipline's cohort MEAN (~mid-90s), the discipline")
print("  with the largest factor (F-Std) gets the biggest lift when you feed it a score at or")
print("  near the MAXIMUM — i.e. above its own cohort average. It is a property of mean-")
print("  equalisation, not a hidden preference for F-Std.")
print()

print("="*82)
print("PART C — Section 9: does a 'possible' always beat a 'near-possible'?  NO.")
print("="*82)
print("  Within one discipline, how many extra centres let a 49 overtake a 50?")
print(f"  Break-even = 1 primary point / centre-weight = 1 / {W} = {1/W:.2f} centres.")
print("  Worked (F-Std, factor 1.475):")
for (s1,c1),(s2,c2) in [((50,3),(49,5)), ((50,3),(49,6)), ((50,5),(49,6))]:
    a = (s1*1 + c1*W)*FACTOR['F-Std']; b = (s2*1 + c2*W)*FACTOR['F-Std']
    print(f"    {s1}.{c1:<2} = {a:6.2f}   vs   {s2}.{c2:<2} = {b:6.2f}   -> "
          f"{'50 wins' if a>b else '49 wins'}")
print("  So a possible with few centres CAN be beaten by a near-possible with 2+ more centres.")
print("  Adrian's original 'a possible always wins' wording is therefore too strong.")
print()

print("="*82)
print("PART D — Section 7: how sensitive is the ranking to the 0.7 centre weight?")
print("="*82)
print("  A representative close contest: F-Std 59.8 vs FTR 59.9, both 8 centres.")
print(f"  {'w':>5}{'F-Std':>10}{'FTR':>10}   winner")
for w in (0.0, 0.5, 0.6, 0.7, 0.8, 1.0):
    a = mcsi('F-Std', 59.8/60, 8, w); b = mcsi('FTR', 59.9/60, 8, w)
    print(f"  {w:>5.2f}{a:>10.2f}{b:>10.2f}   {'F-Std' if a>b else 'FTR'}")
print("  The winner does not flip across the whole 0.0-1.0 range here: 0.7 is a stable policy")
print("  choice, not a fragile magic number. Gerald's own finding (small effect) confirms this.")
