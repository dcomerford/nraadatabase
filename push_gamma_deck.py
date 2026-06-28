"""Summarise the NRAA MCSI V5 committee report into a Gamma slide deck.
Reads GAMMA_API_KEY from the blue-merino .env, POSTs to the Gamma Generations
API, polls until complete, and prints the resulting deck URL.
"""
import os, re, json, time, urllib.request, urllib.error

ENV = '/Users/dancomerford/Desktop/claude/blue-merino/.env'


def load_key():
    with open(ENV) as f:
        for line in f:
            m = re.match(r'\s*GAMMA_API_KEY\s*=\s*(.+)\s*$', line)
            if m:
                return m.group(1).strip().strip('"').strip("'")
    raise SystemExit('GAMMA_API_KEY not found in ' + ENV)


# One card per "---" break.
DECK = """# MCSI Formula V5
## Committee Briefing — June 2026
Calibrating fair cross-discipline club-championship scoring against national King's & Queen's data.

---
# Recommendation
**Adopt V5 as the active MCSI formula for club championship scoring.**

- Calibrated against **20,335 K&Q championship strings** (2024+), across all **seven** Australian state & national associations
- Confidence intervals tight on every discipline (**±0.015 factor SE or better**)
- A measurement of fairness — not a tuned local outcome

---
# The V5 Formula
**Adjusted MCSI = (Raw Score × Conversion + Centres × 0.7) × Discipline Factor**

- **Conversion** equalises iron-sight to optical: 1.20 for TR & Sporter, 1.00 for F-class
- **0.7 centre weight** applied uniformly across all five disciplines
- **Discipline Factor** calibrated from the national K&Q pool

---
# V5 Factors
| Discipline | Factor | Conversion | Centre wt |
|---|---|---|---|
| Target Rifle (TR) | 1.412 | 1.20 | 0.70 |
| F-Open | 1.406 | 1.00 | 0.70 |
| F-Standard | 1.475 | 1.00 | 0.70 |
| F/TR | 1.450 | 1.00 | 0.70 |
| Sporter (Open + Production) | 1.383 | 1.20 | 0.70 |

**Headline finding:** the five factors span only **0.092** (1.383–1.475). The disciplines are far closer in difficulty than intuition suggests — fairness is mostly about centre-rewarding and conversion, not large equipment penalties.

---
# Why King's & Queen's Is the Anchor
K&Q is the only Australian event class that satisfies all three structural requirements:

- **Same conditions** — all five disciplines fire on the same days (13 of 14 events)
- **Cross-state depth** — ~1,000+ shooters from many clubs, minimising per-club bias
- **Adequate sample** — 75× larger than any single-club season can produce
- **Per-state consistency** — Sporter/F-class ratio sits 1.00–1.06 across all regions

---
# Why Single-Club Data Fails
Worked example: the full Geelong 2026 season is only **270 strings / 36 shooters**.

- Confidence intervals **8.7× wider** than national (scales with 1/√n)
- Sporter factor uncertain by **±0.13** — true value could be anywhere 1.25–1.51
- **F/TR unusable** — only 9 strings from 2 shooters
- **Feedback loop** — shooters scored against a benchmark they themselves set

---
# Why Prize-Meet (OPM) Data Fails
200+ meets scraped — two structural biases disqualified them:

- **Regional bias (WA):** 29 of 31 qualifying meets WA-dominated. WA Sporter/F ratio = **1.19** vs the normal 1.00–1.06 — including it drops the Sporter factor 0.058 (~30 leaderboard points)
- **Per-club specialisation (east coast):** clubs specialise by discipline, distorting TR & Sporter top-cohort means by ~0.07 each

---
# Statistical Validation
- **Tight CIs:** TR ±0.0077, F-Open ±0.0111, F-Std ±0.0104, F/TR ±0.0140, Sporter ±0.0152
- **Method-robust:** top-40% and middle-20% methods agree within **0.02** factor points
- **Win-rate tested:** every discipline can win under V5 (TR 26%, F-Std 45%, F-Open 8%, F/TR 12%, Sporter 9%) — none excluded

---
# Objective A vs Objective B
A key committee decision — not a methodology contest:

- **Objective A — Measure performance.** "How hard was this score?" → a single **national** factor is correct (like golf handicaps / chess ratings). **V5 is built for this.**
- **Objective B — Engineer an outcome.** Deliberately compress factors so each discipline stays visible. Legitimate **policy**, but it's championship engineering, not measurement.

A separate compression layer can sit on top of V5 — but it should be an explicit policy override, debated as such.

---
# Recommendation & Next Steps
**Adopt V5 as the active MCSI formula.**

- Satisfies same-conditions, bias-free, large-sample calibration
- 95% CIs tighter than ±1.05 MCSI points on every discipline
- Validated by two independent metrics + historical K&Q win-rates
- **Re-validate annually** as new K&Q data arrives — script is fully reproducible
"""


def main():
    key = load_key()
    body = {
        'inputText': DECK,
        'textMode': 'preserve',
        'format': 'presentation',
        'cardSplit': 'inputTextBreaks',
        'additionalInstructions': (
            'Professional committee briefing deck for a rifle-shooting association. '
            'Clean, data-forward, conservative styling. Keep tables and numbers intact.'),
    }
    req = urllib.request.Request(
        'https://public-api.gamma.app/v1.0/generations',
        data=json.dumps(body).encode(),
        headers={'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36', 'X-API-KEY': key, 'Content-Type': 'application/json',
                 'accept': 'application/json'},
        method='POST')
    try:
        resp = json.load(urllib.request.urlopen(req))
    except urllib.error.HTTPError as e:
        print('POST failed', e.code, e.read().decode())
        raise SystemExit(1)
    gen_id = resp.get('generationId') or resp.get('id')
    print('generationId:', gen_id)

    poll_url = f'https://public-api.gamma.app/v1.0/generations/{gen_id}'
    for i in range(60):
        time.sleep(5)
        pr = urllib.request.Request(poll_url, headers={'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36', 'X-API-KEY': key, 'accept': 'application/json'})
        try:
            data = json.load(urllib.request.urlopen(pr))
        except urllib.error.HTTPError as e:
            print('poll error', e.code, e.read().decode()); continue
        status = data.get('status')
        print(f'[{i*5+5}s] status={status}')
        if status in ('completed', 'succeeded', 'done'):
            print('GAMMA URL:', data.get('gammaUrl') or data.get('url'))
            print(json.dumps(data, indent=2)[:1500])
            return
        if status in ('failed', 'error'):
            print('FAILED:', json.dumps(data, indent=2)); return
    print('Timed out waiting for generation.')


if __name__ == '__main__':
    main()
