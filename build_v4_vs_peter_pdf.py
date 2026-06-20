"""Build a PDF report comparing GRC MCSI V4 to Peter's method, in the same
style as Adrian's report.
"""
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import cm
from reportlab.lib import colors
from reportlab.platypus import (SimpleDocTemplate, Paragraph, Spacer, Table,
                                TableStyle, PageBreak, HRFlowable)
from reportlab.lib.enums import TA_LEFT

OUT = '/Users/dancomerford/Desktop/claude/nraadatabase/GRC_MCSI_V4_vs_Peter.pdf'

doc = SimpleDocTemplate(OUT, pagesize=A4,
    leftMargin=2*cm, rightMargin=2*cm, topMargin=2*cm, bottomMargin=2*cm)

styles = getSampleStyleSheet()
NAVY = colors.HexColor('#1F4E78')
title = ParagraphStyle('title', parent=styles['Heading1'],
    fontSize=18, textColor=NAVY, spaceAfter=10, leading=22)
h2 = ParagraphStyle('h2', parent=styles['Heading2'],
    fontSize=14, textColor=NAVY, spaceBefore=14, spaceAfter=6)
h3 = ParagraphStyle('h3', parent=styles['Heading3'],
    fontSize=12, textColor=NAVY, spaceBefore=10, spaceAfter=4)
body = ParagraphStyle('body', parent=styles['BodyText'],
    fontSize=10.5, leading=14, spaceAfter=6, alignment=TA_LEFT)
small = ParagraphStyle('small', parent=body, fontSize=9, textColor=colors.grey)

def hr(): return HRFlowable(width="100%", thickness=0.5, color=colors.lightgrey,
                             spaceBefore=6, spaceAfter=10)

def tbl(data, header=True, col_widths=None):
    t = Table(data, colWidths=col_widths, repeatRows=1 if header else 0)
    s = [
        ('FONTNAME', (0,0), (-1,-1), 'Helvetica'),
        ('FONTSIZE', (0,0), (-1,-1), 9.5),
        ('VALIGN', (0,0), (-1,-1), 'MIDDLE'),
        ('LINEBELOW', (0,0), (-1,0), 0.75, NAVY),
        ('LINEABOVE', (0,0), (-1,0), 0.75, NAVY),
        ('LINEBELOW', (0,-1), (-1,-1), 0.5, colors.lightgrey),
        ('BOTTOMPADDING', (0,0), (-1,-1), 5),
        ('TOPPADDING', (0,0), (-1,-1), 5),
        ('BACKGROUND', (0,0), (-1,0), colors.HexColor('#E8F0F7')),
        ('TEXTCOLOR', (0,0), (-1,0), NAVY),
        ('FONTNAME', (0,0), (-1,0), 'Helvetica-Bold'),
    ]
    t.setStyle(TableStyle(s))
    return t

flow = []

# ---- Title ----
flow.append(Paragraph("Why the GRC MCSI V4 System is Better Than Peter's Method", title))
flow.append(Paragraph(
    "A data-driven comparison using 17,024 Kings &amp; Queens range scores "
    "(2024–2026, 7 state associations) and four seasons of GRC club data",
    small))
flow.append(hr())

# ---- Executive Summary ----
flow.append(Paragraph("Executive Summary", h2))
flow.append(Paragraph(
    "The GRC MCSI V4 system produces more equitable cross-discipline scores than Peter's method. "
    "Peter's formula systematically over-rewards F-class shooters relative to Target Rifle and "
    "Sporter shooters: across 121 big Kings &amp; Queens matches, F-class disciplines win "
    "<b>97%</b> of matches under Peter's, while TR and Sporter combined win <b>2%</b>. "
    "V4 reduces that imbalance considerably (F-class 65%, TR + Sporter 35%) and produces "
    "median scores that are 2.6× tighter across disciplines.", body))
flow.append(Paragraph(
    "This document presents five arguments, each backed by actual K&amp;Q data — not normalised "
    "projections.", body))
flow.append(hr())

# ---- Argument 1 ----
flow.append(Paragraph("Argument 1: Peter's Formula Heavily Favours F-Class at K&amp;Q Level", h2))
flow.append(Paragraph("Across 17,024 individual range scores from K&amp;Q events 2024–2026:", body))

data = [
    ['Discipline', 'n', 'Median raw', 'Median Peter MCSI', 'Median V4 MCSI'],
    ['F-Open',             '3,465', '59.10', '86.18', '86.90'],
    ['F-Standard',         '3,278', '57.20', '85.80', '85.85'],
    ['FTR',                '2,069', '57.40', '83.40', '85.69'],
    ['Sporter Production',   '963', '49.50', '82.62', '87.56'],
    ['Sporter Open',         '720', '49.70', '80.85', '88.20'],
    ['TR',                 '6,529', '48.70', '79.56', '86.48'],
]
flow.append(tbl(data, col_widths=[4.5*cm, 1.7*cm, 2.5*cm, 3.5*cm, 3.5*cm]))
flow.append(Spacer(1, 6))
flow.append(Paragraph(
    "<b>Peter's median spread</b>: 6.62 points (F-Open 86.18 → TR 79.56)<br/>"
    "<b>V4 median spread</b>: 2.50 points (Sporter Open 88.20 → FTR 85.69)", body))
flow.append(Paragraph(
    "V4's spread is <b>2.6× tighter</b> than Peter's. Under Peter's, the median TR shooter "
    "scores <b>6.6 points below</b> the median F-Open shooter for an equivalent placing "
    "within their discipline.", body))
flow.append(hr())

# ---- Argument 2 ----
flow.append(Paragraph("Argument 2: Peter's Hands K&amp;Q Wins Almost Exclusively to F-Class", h2))
flow.append(Paragraph(
    "Of the 121 big K&amp;Q matches (≥20 shooters) in 2024–2026:", body))

data = [
    ['Discipline', 'V4 wins', "Peter's wins"],
    ['F-Open',                '8%',  '40%'],
    ['F-Standard',           '45%',  '54%'],
    ['FTR',                  '12%',   '3%'],
    ['TR',                   '26%',   '2%'],
    ['Sporter Open',          '3%',   '0%'],
    ['Sporter Production',    '6%',   '0%'],
    ['F-class combined',     '65%',  '97%'],
    ['TR + Sporter combined','35%',   '2%'],
]
flow.append(tbl(data, col_widths=[5.5*cm, 3*cm, 3*cm]))
flow.append(Spacer(1, 6))
flow.append(Paragraph(
    "Under Peter's, a TR or Sporter shooter has effectively no path to a K&amp;Q match win "
    "regardless of how well they shoot. V4 still leans F-class but leaves meaningful room "
    "for the other disciplines.", body))
flow.append(hr())

# ---- Argument 3 ----
flow.append(Paragraph(
    "Argument 3: Peter's Method Conflates Scale Conversion with Equipment Difficulty", h2))
flow.append(Paragraph("V4 separates two distinct corrections:", body))
flow.append(Paragraph(
    "<b>1. Score Conversion (1.2)</b>: TR, SO, SP shoot a 50-point string; F-class shoot a "
    "60-point string. Multiplying TR/Sporter raw scores by 1.2 puts everyone on the same "
    "60-point scale before any equipment correction.", body))
flow.append(Paragraph(
    "<b>2. Centre Bonus (0.7)</b>: Partial credit for V/X precision without letting "
    "centres dominate.", body))
flow.append(Paragraph(
    "<b>3. Equipment Factor</b>: Per-discipline correction for equipment advantage.", body))
flow.append(Paragraph(
    "Peter's formula <i>(raw + centres) × factor</i> collapses scale conversion, centre weighting "
    "and equipment factor into a single multiplier. The same factor is doing all three jobs at "
    "once, so it cannot be tuned for one without changing the others.", body))
flow.append(Paragraph(
    "A practical consequence: under Peter's, a 50-point TR perfect produces "
    "<i>(50 + 10) × 1.53 = 91.80</i>. A 60-point F-Open perfect produces "
    "<i>(60 + 10) × 1.39 = 97.30</i>. The F-Open shooter ends 5.5 points ahead "
    "<b>at the very ceiling</b> — not because F-Open is harder, but because the 50→60 scale "
    "gap is baked into the factor.", body))
flow.append(hr())

# ---- Argument 4 ----
flow.append(Paragraph(
    "Argument 4: Peter's Weights Centres as Full Points; V4 Weights Them as 0.7", h2))
flow.append(Paragraph(
    "Peter's effectively gives every centre a full point of score (since it's added to raw "
    "before multiplying). V4 weights centres at 0.7, recognising precision without letting it "
    "dominate the numeric score.", body))
flow.append(Paragraph(
    "Why this matters: F-class shooters at elite K&amp;Q level routinely hit 60.7–60.8 "
    "(7–8 X's). TR shooters routinely hit 50.9–50.10. The full-centre weighting in Peter's "
    "combined with F-class's higher raw base compounds the F-class advantage.", body))
flow.append(hr())

# ---- Argument 5 ----
flow.append(Paragraph("Argument 5: V4 is Transparent and Reproducible", h2))
flow.append(Paragraph("V4 has three explicit, auditable parameters:", body))
data = [
    ['Parameter', 'Value', 'Purpose'],
    ['Score Conversion', '1.2 (TR/Sporter)',     'Normalises 50→60 scale'],
    ['Centre Bonus',     '0.7 per centre',       'Partial precision credit'],
    ['Equipment Factor', '1.40 – 1.46',          'Equipment difficulty adjustment'],
]
flow.append(tbl(data, col_widths=[4.5*cm, 4*cm, 6*cm]))
flow.append(Spacer(1, 6))
flow.append(Paragraph(
    "Each step is independently adjustable. Any member can verify their score in four lines "
    "of arithmetic.", body))
flow.append(Paragraph(
    "Peter's uses a single combined multiplier per discipline with no decomposition. It "
    "cannot be adjusted for one component without affecting the others, and there is no "
    "documented derivation showing where the factors came from.", body))
flow.append(hr())

# ---- Honest Limitations ----
flow.append(Paragraph("A Note on Honest Limitations", h2))
flow.append(Paragraph("V4 is not perfect. On the same 121-match K&amp;Q dataset:", body))
flow.append(Paragraph(
    "• F-Standard still wins 45% of matches under V4 (over-represented vs an ideal ~17%)<br/>"
    "• Sporter Open wins only 3%", body))
flow.append(Paragraph(
    "V4 is <b>better than Peter's</b>, but it is not yet fully balanced. The remaining bias "
    "is structural — F-class shooters earn more centres per perfect shoot than TR/Sporter "
    "shooters can, and the 0.7 centre bonus rewards that disproportionately. Future iterations "
    "may need to drop the centre weight further (0.7 → 0.4–0.5) or apply per-discipline centre "
    "weights.", body))
flow.append(hr())

# ---- Summary table ----
flow.append(Paragraph("Summary", h2))
data = [
    ['Criterion', 'V4', "Peter's"],
    ['Median MCSI spread across disciplines (K&Q data)',     '2.50 pts', '6.62 pts'],
    ['F-class share of big K&Q match wins',                  '65%',      '97%'],
    ['TR + Sporter share of big K&Q match wins',             '35%',      '2%'],
    ['Score-conversion methodology',                          'Separate, explicit', 'Baked into multiplier'],
    ['Centre bonus weight',                                   '0.7 (partial)', '1.0 (full point)'],
    ['Reproducibility',                                       'Three auditable parameters', 'Single multiplier'],
    ['Honest about residual bias',                            'Yes (F-Std still 45%)', 'Not addressed'],
]
flow.append(tbl(data, col_widths=[8*cm, 4.2*cm, 4.3*cm]))
flow.append(Spacer(1, 8))
flow.append(Paragraph(
    "V4 is the better current option. Both formulas still over-favour F-class at the elite "
    "K&amp;Q level — V4 just does so much less than Peter's.", body))
flow.append(Spacer(1, 12))
flow.append(Paragraph(
    "<i>Analysis based on Kings &amp; Queens 2024–2026 data: 17,024 individual 10-shot range "
    "scores from NSWRA, VRA, QRA, NQRA, SARA, WARA and NRAA championships, plus 270 GRC "
    "club results across four shoots in 2026.</i>", small))

doc.build(flow)
print(f'Wrote {OUT}')
import os; print(f'  size: {os.path.getsize(OUT):,} bytes')
