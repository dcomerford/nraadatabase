# Technical Response — Review of the V5 MCSI

**Geelong Rifle Club — Multiple Category Score Index (MCSI), Version 5**
Prepared for the GRC committee · July 2026

This document responds to the technical review of the V5 MCSI. Most of the points raised are fair, several improve the model, and a few rest on a misunderstanding of what the MCSI is for. Each is answered directly below, with the actual numbers from our dataset.

---

## 1. What the MCSI is — and what it is not

This is the most important point, because several objections dissolve once it is clear.

The MCSI was **not** built as a club-level tool and it does **not** compare GRC members against elite shooters as individuals. The original MCSI was built on King's/Queen's competition data from **2014 and earlier**, and the club has used that formula for the last four years. The current V5 work is an **update** of it using recent competition data.

> **The large external dataset is used to estimate the *shape and relative behaviour of each discipline's score distribution* — not to create individual shooter ratings.**

No GRC member is being told their score equals a national champion's. The model asks *"how are scores distributed within each discipline — how compressed, how often is a possible reached, how do the top performers cluster?"* It does **not** ask *"is this member as skilled as a King's medallist?"* Because King's events produce genuinely large, statistically significant samples in each discipline, we can measure those distributions reliably — which is exactly why that data is used.

Its purpose is narrow and worth stating plainly:

> To convert performances recorded under different scoring systems (50-point vs 60-point) and equipment conditions into a **common index** that gives a *reasonably equitable* basis for comparing **relative** performance across GRC disciplines.

**Two things have changed since the original formula, and they are the reason for the V5 update.** First, **Sporter was never part of the 2014 build.** For the last few years it has been carried in the championship using an *estimated* factor that was, in effect, a placeholder rather than a calibrated value. We now have enough recent King's/Queen's data to calibrate Sporter properly — and we have demonstrated that the sample is large enough to do so (Section 3). Second, **equipment has moved on** — most visibly in F-Open — so the older factors no longer reflect current scoring.

The V5 update therefore uses the recent dataset to produce factors representative of the present era. One honest limitation: **Sporter Open and Sporter (Production) were combined in the earlier records**, so at this stage they cannot be cleanly separated — V5 uses a single merged Sporter factor. That can be revisited once the two are recorded distinctly for long enough to calibrate separately.

None of this re-normalises the formula on GRC's own small sample. The aim is to (a) properly include the smaller classes — especially Sporter — which were previously under-represented or estimated, and (b) rebalance the disciplines so no single class holds a structural advantage. Celebrating individual discipline champions in their own right remains entirely compatible with this — the two are not mutually exclusive.

---

## 2. "The factors are a black box / magic numbers" — they are not

This is a fair criticism of the *report*, not the *calculator*. The derivation was never published; here it is.

The V5 formula is:

```
Adjusted MCSI = (Score × Conversion + Centres × 0.7) × Factor
```

Each discipline's factor is **not chosen by eye**. It is the single number that lifts that discipline's *top-40% cohort average* onto one common target (94.471, the average of the three F-class cohorts). In other words:

> **Factor = 94.471 ÷ (that discipline's top-40% cohort mean converted score)**

| Discipline | Top-40% cohort mean | 94.471 ÷ mean | **V5 factor** |
|---|---:|---:|---:|
| Target Rifle | 66.907 | 1.412 | **1.412** |
| F-Open | 67.198 | 1.406 | **1.406** |
| F-Standard | 64.062 | 1.475 | **1.475** |
| F/TR | 65.165 | 1.450 | **1.450** |
| Sporter (Open + Prod. merged) | 68.292 | 1.383 | **1.383** |

The factor is fully reproducible — it can be recomputed independently from the cohort means. The discipline whose strong scores average **lowest** (F-Standard, 64.06) gets the **largest** factor; the one that averages **highest** (Sporter, 68.29) gets the **smallest**. There is nothing magic in it: it is one division per discipline. **The final report will publish this table with the underlying record counts.**

---

## 3. "Why King's/Queen's data, not club data?" — sample size

The instinct to use our own club data is reasonable, but the numbers make it impossible today.

The V5 calibration uses **~19,800 individual range scores across 135 championship days**:

| Discipline | Scores | Distinct shooters |
|---|---:|---:|
| Target Rifle | 7,510 | 351 |
| F-Open | 4,082 | 222 |
| F-Standard | 3,731 | 193 |
| F/TR | 2,437 | 124 |
| Sporter | 2,018 | 152 |

A stable calibration needs roughly 3,000–4,000 scores **per discipline**. Geelong produces a small fraction of that in a season. A GRC-only factor wouldn't describe *the discipline* — it would describe **the two or three regulars who happened to turn out**, and it would lurch year to year. We would need to roughly **5× our participation** before a club-only model became statistically meaningful — and for Sporter, not even then.

**We have also tested this directly.** We applied the V5 factors to the most recent New South Wales King's data — **data not used to derive them** — and the factors held: the results were consistent. When that new block of championship data was folded into the existing dataset, the factors **barely moved** (a marginal shift on F-Standard only). This matters: if our sample were too small to be statistically meaningful, adding a fresh season of championship data would have swung the factors noticeably. It did not — which is direct evidence the sample is large enough to rely on.

This is exactly why the external data is used for the *distribution shape* while GRC data is the *local sanity check*. Elite data isn't a perfect mirror of club shooting; the honest long-term answer is a **hybrid** — anchor on the national distribution now, and give GRC data progressively more weight as it accumulates (credibility weighting).

---

## 4. "All-else-equal, F-Standard always wins" — correct, and here's why

This is a valid observation. When every discipline is put on the same percentage of possible with the same centres, F-Standard leads every row. Reproducing the test exactly (perfect score, 10 centres):

| % of possible | TR | F-Open | F-Std | FTR | Sporter | Wins |
|---:|---:|---:|---:|---:|---:|---|
| 100% | 94.60 | 94.20 | **98.83** | 97.15 | 92.66 | F-Std |
| 98% | 92.91 | 92.51 | **97.06** | 95.41 | 91.00 | F-Std |
| 96% | 91.22 | 90.83 | **95.28** | 93.67 | 89.34 | F-Std |
| 90% | 86.13 | 85.77 | **89.98** | 88.45 | 84.36 | F-Std |

Order: **F-Std → FTR → TR → F-Open → Sporter**.

**Why it happens:** the factor equalises each discipline's *cohort average* (Section 2), not its *maximum*. F-Standard's top shooters sit furthest below their ceiling, so F-Std earns the biggest multiplier. When you then feed in a score at or near the **maximum** — i.e. *above* F-Std's own cohort average — that large multiplier over-rewards it. It is a mathematical property of **mean-equalisation**, not a hidden preference for F-Standard.

**This must be acknowledged as a real trade-off**, and there are two honest fixes if the committee finds it unacceptable:
- **Factor shrinkage** — pull all factors part-way toward a common value, keeping half the calibration but reducing the equal-score spread; or
- calibrate to a **top-cohort / possibles** target instead of the mean (this equalises the *elite tail* rather than the average — a different, defensible choice).

> **NOTE — factor consistency:** an earlier draft circulated an F-Standard factor of **1.440**; the current calibration uses **1.475**. At 1.475, F-Std has the largest factor — which is exactly the behaviour described above. Every figure in this document uses **1.475**; the final report must too.

---

## 5. Does a possible always beat a near-possible? — no, but only a 2-centre swing flips it

This is also correct, and the earlier claim that "a possible always beats a near-possible" should be withdrawn.

Within a single discipline, ranking is by `(score + 0.7 × centres)` — the factor cancels, so the comparison is identical in every class. A dropped point is worth **1**; a centre is worth **0.7**. The break-even is `1 ÷ 0.7 = 1.43` centres — so a near-possible must carry at least **2 more centres** than the possible to overtake it:

| Possible | Near-possible | score + 0.7 × centres | Winner |
|---|---|---|---|
| 50.8 | 49.9 | 55.6 vs 55.3 | **50 holds** |
| 50.7 | 49.9 | 54.9 vs 55.3 | **49 wins** |
| 50.10 | 49.10 | 57.0 vs 56.0 | **50 wins** |

*(Shown for a 50-max discipline; the same 2-centre rule applies in F-class — 60 vs 59.)*

So a possible with a normal centre count is **robust**: only a near-possible carrying two extra centres — an uncommon result — overturns it. Whether even that should be allowed is a **policy choice**, not a maths error:
- **Policy 1 (primary absolute):** a higher score always wins; centres only break ties.
- **Policy 2 (combined performance — current V5):** a large enough centre advantage can outweigh one dropped point.

The committee should pick one deliberately. The report will be corrected to say: *the system strongly rewards primary score and generally favours possibles, but does not guarantee that every possible outranks every near-possible.*

---

## 6. Distance — correct in principle, but deliberately not adopted

It is correct that a 50.10 at 900m is, on average, harder than at 500m, and V5 uses one factor per discipline across all distances. **We have looked at this.** Two reasons it is not in V5:

- **Data.** A distance-specific factor means **6 disciplines × 5 distances = 30 cells**, each needing ~100+ scores from 15+ shooters to be stable. We have that depth for some disciplines but not others — **Sporter in particular** — and there is a real risk of **double-counting difficulty** that is already reflected in the observed score distribution.
- **Usability.** A separate factor for every distance, in both **metres and yards**, across every discipline, would turn the formula into exactly the "black box" the review warns against. We want the MCSI to stay **usable — calculable by any shooter by hand.** One factor per discipline keeps it transparent.

This is a legitimate **future (V6) development item**, subject to sample-size thresholds — not a V5 defect.

---

## 7. "Factors should evolve" — agreed, under governance

Yes. Equipment (barrels, projectiles, F-Open cartridge design) has changed markedly in a decade, and the factors should track it. But they must **not** drift continuously, or a January result wouldn't compare with a November one. The process should be: **lock factors before each season → collect scores → annual post-season review → test proposed changes against history → committee approval → apply from the next season only.** Every factor set versioned, historical results never rewritten. We'll add this as a formal governance section.

---

## 8. "Is the MCSI goal even possible?"

In a strict scientific sense, there is no single "best shooter" across disciplines that use different equipment and skills — that is philosophically correct. But the MCSI never claimed that. It is a **constructed index**, in exactly the same family as golf handicaps, the decathlon scoring tables, and motorsport balance-of-performance. None of those prove two performances are physically identical; they provide an *agreed, transparent framework for comparison*. The right description of the title is:

> **GRC Multiple Category Champion under the adopted MCSI rules** — not "the objectively best shooter."

Four years of work shouldn't be discarded over the apples-to-oranges objection; and separately celebrating each discipline's own champion costs us nothing.

---

## 9. What we will change in the report

1. **Publish the factor derivation** — the table in Section 2, with record counts, so the factors are reproducible, not "magic."
2. **Fix the F-Standard factor consistency** — use **1.475** throughout.
3. **Add a centre-weight sensitivity test** — show rankings are stable across w = 0.5–0.8; describe 0.7 as a calibrated policy value, not a universal constant.
4. **Correct the possible/near-possible wording** (Section 5) and state the chosen policy explicitly.
5. **Acknowledge the equal-score F-Std lead** (Section 4) and offer factor-shrinkage as the mitigation if the committee wants it.
6. **Add a distance-analysis section** recording why per-distance factors are deferred to V6 (data gaps + usability), subject to sample-size thresholds.
7. **Add a factor-governance section** — annual review, seasonal locking, versioning, no retrospective changes.

The review doesn't show V5 is wrong — it shows exactly where the *explanation* needs strengthening before we present it as final. We're happy to walk through any of the above.
