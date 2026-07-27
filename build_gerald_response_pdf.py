"""Render the short Response to Gerald's V5 MCSI review as a PDF.

Style mirrors NRAA_MCSI_V5_Committee_Report.pdf (same navy/grey palette,
table styling, fonts, hr rule).
"""
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import cm
from reportlab.lib import colors
from reportlab.platypus import (SimpleDocTemplate, Paragraph, Spacer, Table,
                                TableStyle, PageBreak, HRFlowable)
from reportlab.lib.enums import TA_LEFT

OUT = '/Users/dancomerford/Desktop/claude/nraadatabase/GRC_MCSI_Response_to_Gerald.pdf'

doc = SimpleDocTemplate(OUT, pagesize=A4,
    leftMargin=2*cm, rightMargin=2*cm, topMargin=2*cm, bottomMargin=2*cm,
    title="Technical Response — Review of the V5 MCSI",
    author='Geelong Rifle Club')

styles = getSampleStyleSheet()
NAVY = colors.HexColor('#1F4E78')
GREY = colors.HexColor('#444444')
title = ParagraphStyle('title', parent=styles['Heading1'],
    fontSize=18, textColor=NAVY, spaceAfter=8, leading=22)
subtitle = ParagraphStyle('subtitle', parent=styles['Heading2'],
    fontSize=12, textColor=GREY, spaceAfter=16, leading=15)
h2 = ParagraphStyle('h2', parent=styles['Heading2'],
    fontSize=14, textColor=NAVY, spaceBefore=16, spaceAfter=6)
h3 = ParagraphStyle('h3', parent=styles['Heading3'],
    fontSize=12, textColor=NAVY, spaceBefore=10, spaceAfter=4)
body = ParagraphStyle('body', parent=styles['BodyText'],
    fontSize=10.5, leading=14, spaceAfter=6, alignment=TA_LEFT)
small = ParagraphStyle('small', parent=body, fontSize=9, textColor=colors.grey)
bullet = ParagraphStyle('bullet', parent=body, leftIndent=16, bulletIndent=4, spaceAfter=3)

def hr(): return HRFlowable(width="100%", thickness=0.5, color=colors.lightgrey,
                            spaceBefore=6, spaceAfter=10)
def p(text, style=body): return Paragraph(text, style)

cell_style = ParagraphStyle('cell', parent=body, fontSize=9.5, leading=12,
                            spaceAfter=0, spaceBefore=0)
cell_head = ParagraphStyle('cellhead', parent=cell_style,
                           fontName='Helvetica-Bold', textColor=NAVY)
cell_right = ParagraphStyle('cellr', parent=cell_style, alignment=2)  # right

def _wrap_cells(data, header=True, right_from=None):
    out = []
    for r, row in enumerate(data):
        new_row = []
        for c, cell in enumerate(row):
            if isinstance(cell, str):
                if header and r == 0:
                    style = cell_head
                elif right_from is not None and c >= right_from:
                    style = cell_right
                else:
                    style = cell_style
                new_row.append(Paragraph(cell, style))
            else:
                new_row.append(cell)
        out.append(new_row)
    return out

def tbl(data, col_widths=None, header=True, highlight_rows=None, right_from=None):
    t = Table(_wrap_cells(data, header=header, right_from=right_from), colWidths=col_widths)
    style = [
        ('FONT', (0,0), (-1,-1), 'Helvetica', 9.5),
        ('VALIGN', (0,0), (-1,-1), 'MIDDLE'),
        ('TOPPADDING', (0,0), (-1,-1), 5),
        ('BOTTOMPADDING', (0,0), (-1,-1), 5),
        ('LINEBELOW', (0,0), (-1,0), 0.75, NAVY),
        ('LINEBELOW', (0,-1), (-1,-1), 0.5, colors.lightgrey),
        ('LINEABOVE', (0,1), (-1,1), 0.25, colors.lightgrey),
    ]
    if header:
        style += [
            ('FONT', (0,0), (-1,0), 'Helvetica-Bold', 9.5),
            ('TEXTCOLOR', (0,0), (-1,0), NAVY),
            ('BACKGROUND', (0,0), (-1,0), colors.HexColor('#EEF3F8')),
        ]
    if highlight_rows:
        for r in highlight_rows:
            style.append(('BACKGROUND', (0,r), (-1,r), colors.HexColor('#FFF9E6')))
    t.setStyle(TableStyle(style))
    return t

def callout(text, bg='#EEF3F8', border=NAVY):
    """Left-bordered tinted box for the key-principle blockquotes."""
    inner = ParagraphStyle('callout', parent=body, fontSize=10.5, leading=14,
                           textColor=colors.HexColor('#1a1a1a'), spaceAfter=0)
    t = Table([[Paragraph(text, inner)]], colWidths=[17*cm])
    t.setStyle(TableStyle([
        ('BACKGROUND', (0,0), (-1,-1), colors.HexColor(bg)),
        ('LINEBEFORE', (0,0), (0,-1), 3, border),
        ('LEFTPADDING', (0,0), (-1,-1), 10),
        ('RIGHTPADDING', (0,0), (-1,-1), 10),
        ('TOPPADDING', (0,0), (-1,-1), 8),
        ('BOTTOMPADDING', (0,0), (-1,-1), 8),
    ]))
    return t

flow = []

# ── Header ───────────────────────────────────────────────────────────────────
flow.append(p("Technical Response — Review of the V5 MCSI", title))
flow.append(p('Geelong Rifle Club — Multiple Category Score Index, Version 5 · '
              'Prepared for the GRC committee · July 2026', subtitle))
flow.append(hr())
flow.append(p(
    'This document responds to the technical review of the V5 MCSI. Most of the points '
    'raised are fair, several improve the model, and a few rest on a misunderstanding of '
    'what the MCSI is for. Each is answered directly below, with the actual numbers from '
    'our dataset.', body))

# ── 1. What the MCSI is ──────────────────────────────────────────────────────
flow.append(p('1. What the MCSI is — and what it is not', h2))
flow.append(p(
    'This is the most important point, because several objections dissolve once it is clear. '
    'The MCSI was <b>not</b> built as a club-level tool and it does <b>not</b> compare GRC '
    'members against elite shooters as individuals. The original MCSI was built on '
    'King’s/Queen’s competition data from <b>2014 and earlier</b>, and the club has used that '
    'formula for the last four years. The current V5 work is an <b>update</b> of it using '
    'recent competition data.', body))
flow.append(callout(
    '<b>The large external dataset is used to estimate the shape and relative behaviour of '
    'each discipline’s score distribution — not to create individual shooter ratings.</b>'))
flow.append(Spacer(1, 0.2*cm))
flow.append(p(
    'No GRC member is being told their score equals a national champion’s. The model asks '
    '<i>“how are scores distributed within each discipline — how compressed, how often '
    'is a possible reached, how do the top performers cluster?”</i> It does <b>not</b> ask '
    '<i>“is this member as skilled as a King’s medallist?”</i> Because King’s events produce '
    'genuinely large, statistically significant samples in each discipline, we can measure '
    'those distributions reliably — which is exactly why that data is used.', body))
flow.append(p('Its purpose is narrow and worth stating plainly:', body))
flow.append(callout(
    'To convert performances recorded under different scoring systems (50-point vs 60-point) '
    'and equipment conditions into a <b>common index</b> that gives a reasonably equitable '
    'basis for comparing <b>relative</b> performance across GRC disciplines.'))
flow.append(Spacer(1, 0.2*cm))
flow.append(p(
    '<b>Two things have changed since the original formula, and they are the reason for the V5 '
    'update.</b> First, <b>Sporter was never part of the 2014 build.</b> For the last few years '
    'it has been carried in the championship using an <i>estimated</i> factor that was, in '
    'effect, a placeholder rather than a calibrated value. We now have enough recent '
    'King’s/Queen’s data to calibrate Sporter properly — and we have demonstrated that the '
    'sample is large enough to do so (Section 3). Second, <b>equipment has moved on</b> — most '
    'visibly in F-Open — so the older factors no longer reflect current scoring.', body))
flow.append(p(
    'The V5 update therefore uses the recent dataset to produce factors representative of the '
    'present era. One honest limitation: <b>Sporter Open and Sporter (Production) were combined '
    'in the earlier records</b>, so at this stage they cannot be cleanly separated — V5 uses a '
    'single merged Sporter factor. That can be revisited once the two are recorded distinctly '
    'for long enough to calibrate separately.', body))
flow.append(p(
    'None of this re-normalises the formula on GRC’s own small sample. The aim is to (a) properly '
    'include the smaller classes — especially Sporter — which were previously under-represented '
    'or estimated, and (b) rebalance the disciplines so no single class holds a structural '
    'advantage. Celebrating individual discipline champions in their own right remains entirely '
    'compatible with this — the two are not mutually exclusive.', body))

# ── 2. Black box ─────────────────────────────────────────────────────────────
flow.append(p('2. “The factors are a black box / magic numbers” — they are not', h2))
flow.append(p(
    'This is a fair criticism of the <i>report</i>, not the <i>calculator</i>. The derivation was '
    'never published; here it is. The V5 formula is:', body))
flow.append(callout(
    '<b>Adjusted MCSI = (Score × Conversion + Centres × 0.7) × Factor</b>',
    bg='#F4F4F4', border=GREY))
flow.append(Spacer(1, 0.2*cm))
flow.append(p(
    'Each discipline’s factor is <b>not chosen by eye</b>. It is the single number that lifts '
    'that discipline’s top-40% cohort average onto one common target (94.471, the average of '
    'the three F-class cohorts):', body))
flow.append(callout(
    '<b>Factor = 94.471 ÷ (that discipline’s top-40% cohort mean converted score)</b>'))
flow.append(Spacer(1, 0.25*cm))
flow.append(tbl([
    ['Discipline', 'Top-40% cohort mean', '94.471 ÷ mean', 'V5 factor'],
    ['Target Rifle', '66.907', '1.412', '<b>1.412</b>'],
    ['F-Open', '67.198', '1.406', '<b>1.406</b>'],
    ['F-Standard', '64.062', '1.475', '<b>1.475</b>'],
    ['F/TR', '65.165', '1.450', '<b>1.450</b>'],
    ['Sporter (Open + Prod. merged)', '68.292', '1.383', '<b>1.383</b>'],
], col_widths=[7.0*cm, 4.0*cm, 3.0*cm, 3.0*cm], right_from=1))
flow.append(Spacer(1, 0.2*cm))
flow.append(p(
    'The factor is fully reproducible — it can be recomputed independently from the cohort '
    'means. The discipline whose strong scores average <b>lowest</b> (F-Standard, 64.06) gets the '
    '<b>largest</b> factor; the one that averages <b>highest</b> (Sporter, 68.29) gets the '
    '<b>smallest</b>. There is nothing magic in it: it is one division per discipline. '
    '<b>The final report will publish this table with the underlying record counts.</b>', body))

# ── 3. Why K&Q ───────────────────────────────────────────────────────────────
flow.append(p('3. “Why King’s/Queen’s data, not club data?” — sample size', h2))
flow.append(p(
    'The instinct to use our own club data is reasonable, but the numbers make it impossible '
    'today. The V5 calibration uses <b>~19,800 individual range scores across 135 championship '
    'days</b>:', body))
flow.append(tbl([
    ['Discipline', 'Scores', 'Distinct shooters'],
    ['Target Rifle', '7,510', '351'],
    ['F-Open', '4,082', '222'],
    ['F-Standard', '3,731', '193'],
    ['F/TR', '2,437', '124'],
    ['Sporter', '2,018', '152'],
], col_widths=[9.0*cm, 4.0*cm, 4.0*cm], right_from=1))
flow.append(Spacer(1, 0.2*cm))
flow.append(p(
    'A stable calibration needs roughly 3,000–4,000 scores <b>per discipline</b>. Geelong '
    'produces a small fraction of that in a season. A GRC-only factor wouldn’t describe '
    '<i>the discipline</i> — it would describe <b>the two or three regulars who happened to '
    'turn out</b>, and it would lurch year to year. We would need to roughly <b>5× our '
    'participation</b> before a club-only model became statistically meaningful — and for '
    'Sporter, not even then.', body))
flow.append(p(
    '<b>We have also tested this directly.</b> We applied the V5 factors to the most recent New '
    'South Wales King’s data — <b>data not used to derive them</b> — and the factors held: the '
    'results were consistent. When that new block of championship data was folded into the '
    'existing dataset, the factors <b>barely moved</b> (a marginal shift on F-Standard only). '
    'This matters: if our sample were too small to be statistically meaningful, adding a fresh '
    'season of championship data would have swung the factors noticeably. It did not — which is '
    'direct evidence the sample is large enough to rely on.', body))
flow.append(p(
    'This is exactly why the external data is used for the <i>distribution shape</i> while GRC data '
    'is the <i>local sanity check</i>. Elite data isn’t a perfect mirror of club shooting; the '
    'honest long-term answer is a <b>hybrid</b> — anchor on the national distribution now, and '
    'give GRC data progressively more weight as it accumulates (credibility weighting).', body))

# ── 4. F-Std wins ────────────────────────────────────────────────────────────
flow.append(p('4. “All-else-equal, F-Standard always wins” — correct, and here’s why', h2))
flow.append(p(
    'This is a valid observation. When every discipline is put on the same percentage of possible '
    'with the same centres, F-Standard leads every row. Reproducing the test exactly (perfect '
    'score, 10 centres):', body))
flow.append(tbl([
    ['% of possible', 'TR', 'F-Open', 'F-Std', 'FTR', 'Sporter', 'Wins'],
    ['100%', '94.60', '94.20', '<b>98.83</b>', '97.15', '92.66', 'F-Std'],
    ['98%',  '92.91', '92.51', '<b>97.06</b>', '95.41', '91.00', 'F-Std'],
    ['96%',  '91.22', '90.83', '<b>95.28</b>', '93.67', '89.34', 'F-Std'],
    ['90%',  '86.13', '85.77', '<b>89.98</b>', '88.45', '84.36', 'F-Std'],
], col_widths=[2.8*cm, 1.9*cm, 2.2*cm, 2.2*cm, 1.9*cm, 2.2*cm, 1.8*cm], right_from=1))
flow.append(Spacer(1, 0.15*cm))
flow.append(p(
    'Order: <b>F-Std → FTR → TR → F-Open → Sporter</b>.', small))
flow.append(p(
    '<b>Why it happens:</b> the factor equalises each discipline’s <i>cohort average</i> '
    '(Section 2), not its <i>maximum</i>. F-Standard’s top shooters sit furthest below their '
    'ceiling, so F-Std earns the biggest multiplier. When you then feed in a score at or near the '
    '<b>maximum</b> — i.e. above F-Std’s own cohort average — that large multiplier '
    'over-rewards it. It is a mathematical property of <b>mean-equalisation</b>, not a hidden '
    'preference for F-Standard.', body))
flow.append(p('This must be acknowledged as a real trade-off. Two honest fixes, if the committee '
              'finds it unacceptable:', body))
flow.append(p('• <b>Factor shrinkage</b> — pull all factors part-way toward a common '
              'value, keeping half the calibration but reducing the equal-score spread; or', bullet))
flow.append(p('• calibrate to a <b>top-cohort / possibles</b> target instead of the mean '
              '(this equalises the elite tail rather than the average — a different, '
              'defensible choice).', bullet))
flow.append(Spacer(1, 0.2*cm))
flow.append(callout(
    '<b>NOTE — factor consistency:</b> an earlier draft circulated an F-Standard factor of '
    '<b>1.440</b>; the current calibration uses <b>1.475</b>. At 1.475, F-Std has the largest '
    'factor — which is exactly the behaviour described above. Every figure in this document uses '
    '<b>1.475</b>; the final report must too.',
    bg='#FFF9E6', border=colors.HexColor('#C9A227')))

# ── 5. Possible vs near-possible ─────────────────────────────────────────────
flow.append(p('5. Does a possible always beat a near-possible? — no, but only a 2-centre '
              'swing flips it', h2))
flow.append(p(
    'This is also correct, and the earlier claim that “a possible always beats a near-possible” '
    'should be withdrawn. Within a single discipline, ranking is by '
    '(score + 0.7 × centres) — the factor cancels, so the comparison is identical in every '
    'class. A dropped point is worth <b>1</b>; a centre is worth <b>0.7</b>. The break-even is '
    '1 ÷ 0.7 = <b>1.43 centres</b> — so a near-possible must carry at least <b>2 more '
    'centres</b> than the possible to overtake it:', body))
flow.append(tbl([
    ['Possible', 'Near-possible', 'score + 0.7 × centres', 'Winner'],
    ['50.8', '49.9', '55.6 vs 55.3', '<b>50 holds</b>'],
    ['50.7', '49.9', '54.9 vs 55.3', '<b>49 wins</b>'],
    ['50.10', '49.10', '57.0 vs 56.0', '<b>50 wins</b>'],
], col_widths=[3.0*cm, 3.5*cm, 5.5*cm, 5.0*cm]))
flow.append(p('<i>Shown for a 50-max discipline; the same 2-centre rule applies in F-class — '
              '60 vs 59.</i>', small))
flow.append(p('So a possible with a normal centre count is <b>robust</b>: only a near-possible '
              'carrying two extra centres — an uncommon result — overturns it. Whether even that '
              'should be allowed is a <b>policy choice</b>, not a maths error:', body))
flow.append(p('• <b>Policy 1 (primary absolute):</b> a higher score always wins; centres only '
              'break ties.', bullet))
flow.append(p('• <b>Policy 2 (combined performance — current V5):</b> a large enough centre '
              'advantage can outweigh one dropped point.', bullet))
flow.append(p(
    'The committee should pick one deliberately. The report will be corrected to say: <i>the system '
    'strongly rewards primary score and generally favours possibles, but does not guarantee that '
    'every possible outranks every near-possible.</i>', body))

# ── 6. Distance ──────────────────────────────────────────────────────────────
flow.append(p('6. Distance — correct in principle, but deliberately not adopted', h2))
flow.append(p(
    'It is correct that a 50.10 at 900m is, on average, harder than at 500m, and V5 uses one '
    'factor per discipline across all distances. <b>We have looked at this.</b> Two reasons it '
    'is not in V5:', body))
flow.append(p(
    '• <b>Data.</b> A distance-specific factor means <b>6 disciplines × 5 distances = 30 '
    'cells</b>, each needing ~100+ scores from 15+ shooters to be stable. We have that depth for '
    'some disciplines but not others — <b>Sporter in particular</b> — and there is a real risk of '
    '<b>double-counting difficulty</b> that is already reflected in the observed score '
    'distribution.', bullet))
flow.append(p(
    '• <b>Usability.</b> A separate factor for every distance, in both <b>metres and yards</b>, '
    'across every discipline, would turn the formula into exactly the “black box” the review warns '
    'against. We want the MCSI to stay <b>usable — calculable by any shooter by hand.</b> One '
    'factor per discipline keeps it transparent.', bullet))
flow.append(p(
    'This is a legitimate <b>future (V6) development item</b>, subject to sample-size thresholds — '
    'not a V5 defect.', body))

# ── 7. Evolve ────────────────────────────────────────────────────────────────
flow.append(p('7. “Factors should evolve” — agreed, under governance', h2))
flow.append(p(
    'Yes. Equipment (barrels, projectiles, F-Open cartridge design) has changed markedly in a decade, '
    'and the factors should track it. But they must <b>not</b> drift continuously, or a January result '
    'wouldn’t compare with a November one. The process should be: <b>lock factors before each '
    'season → collect scores → annual post-season review → test proposed changes against '
    'history → committee approval → apply from the next season only.</b> Every factor set '
    'versioned, historical results never rewritten. We’ll add this as a formal governance '
    'section.', body))

# ── 8. Even possible ─────────────────────────────────────────────────────────
flow.append(p('8. “Is the MCSI goal even possible?”', h2))
flow.append(p(
    'In a strict scientific sense, there is no single “best shooter” across disciplines that '
    'use different equipment and skills — that is philosophically correct. But the MCSI never '
    'claimed that. It is a <b>constructed index</b>, in exactly the same family as golf handicaps, the '
    'decathlon scoring tables, and motorsport balance-of-performance. None of those prove two '
    'performances are physically identical; they provide an agreed, transparent framework for '
    'comparison. The right description of the title is:', body))
flow.append(callout(
    '<b>GRC Multiple Category Champion under the adopted MCSI rules</b> — not “the '
    'objectively best shooter.”'))
flow.append(Spacer(1, 0.2*cm))
flow.append(p(
    'Four years of work shouldn’t be discarded over the apples-to-oranges objection; and '
    'separately celebrating each discipline’s own champion costs us nothing.', body))

# ── 9. Amendments ────────────────────────────────────────────────────────────
flow.append(p('9. What we will change in the report', h2))
for i, item in enumerate([
    '<b>Publish the factor derivation</b> — the table in Section 2, with record counts, so the '
    'factors are reproducible, not “magic.”',
    '<b>Fix the F-Standard factor inconsistency</b> — use <b>1.475</b> throughout.',
    '<b>Add a centre-weight sensitivity test</b> — show rankings are stable across w = 0.5–0.8; '
    'describe 0.7 as a calibrated policy value, not a universal constant.',
    '<b>Correct the possible/near-possible wording</b> (Section 5) and state the chosen policy explicitly.',
    '<b>Acknowledge the equal-score F-Std lead</b> (Section 4) and offer factor-shrinkage as the '
    'mitigation if the committee wants it.',
    '<b>Add a distance-analysis section</b> recording why per-distance factors are deferred to V6 '
    '(data gaps + usability), subject to sample-size thresholds.',
    '<b>Add a factor-governance section</b> — annual review, seasonal locking, versioning, no '
    'retrospective changes.',
], 1):
    flow.append(p(f'{i}. {item}', bullet))
flow.append(Spacer(1, 0.3*cm))
flow.append(hr())
flow.append(p(
    'The review doesn’t show V5 is wrong — it shows exactly where the '
    '<i>explanation</i> needs strengthening before we present it as final. We’re happy to walk '
    'through any of the above.', body))

doc.build(flow)
print(f'Wrote {OUT}')
