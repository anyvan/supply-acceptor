# Supply Acceptor Algorithm — Documentation

**File:** `supply_acceptor.py`
**Purpose:** Decides which *pending* TP (Transport Provider) reservations to accept for each zone on each pickup date.

---

## The Problem

Every day, TPs submit reservations saying "I'm available in Zone X on Date Y."
We need to decide which ones to **accept** — enough to cover expected demand, but not so many that we oversupply and waste cost.

The algorithm runs on two input files:

| File | What it contains |
|---|---|
| `FURN_Supply_Summary - Detailed view.csv` | Demand data — confirmed jobs per zone, date, and job type (1-man / 2-man) |
| `FURN_Supply_Summary - reservations.csv` | All reservations — pending, accepted, and return, with TP quality data |

---

## Step-by-Step Logic

### Step 1 — Calculate Confirmed Demand

For each **(zone, pickup date)** pair, sum `realized_lane_level_jobs` from the demand file.

> **Why "realized" jobs?**
> At D-3 (3 days before pickup), only ~60% of final demand has arrived. The remaining 40% comes in later and naturally fills **EI (Express Interest)** slots. So we only need to cover what's confirmed now — EI handles the rest organically.

**Example:**
- London, 14 March: 204 confirmed jobs
- Sheffield, 14 March: 35 confirmed jobs

---

### Step 2 — Estimate Journeys Needed

```
total_journeys = realized_jobs / jobs_per_journey (JPJ)
```

JPJ varies by zone density:

| Zone Type | JPJ | Zones |
|---|---|---|
| Dense — London | 5.8 | london |
| Dense — others | 5.3 | manchester, oxford, birmingham |
| Light | 5.0 | all other zones |

**Example:**
- Sheffield: 35 jobs ÷ 5.3 = **6.6 journeys**
- London: 204 jobs ÷ 5.8 = **35.2 journeys**

---

### Step 3 — Set Target Reservations

```
target = round(total_journeys × coverage_ratio)
```

**Default coverage = 100%** — accept enough TPs to cover all confirmed journeys.

Some zones have structural supply excess (too many TPs available) and use reduced coverage to avoid over-accepting:

| Zone | Normal Coverage | Month-Edge Coverage (days 1–3 and 24–31) |
|---|---|---|
| Birmingham | 50% | 75% |
| Manchester | 75% | 75% |
| Peterborough | 50% | 50% |
| Oxford | 50% | 100% |
| Salisbury | 75% | 100% |
| Sheffield | 75% | 75% |
| All others | 100% | 100% |

**Example — Birmingham, 14 March (normal day):**
- 35 jobs ÷ 5.3 = 6.6 journeys
- 6.6 × 50% = **target = 3 reservations**

**Example — Oxford, 1 March (month start, peak override):**
- target uses 100% coverage instead of 50%

---

### Step 4 — Calculate the Gap

```
gap = max(0, target − already_accepted_reservations)
```

Return reservations (TPs travelling home after a national job) are **excluded** from the pending candidate pool — they are handled separately. But they *do* count in the accepted total.

**Example — Salisbury, 14 March:**
- Target = 3, Already accepted = 2 → **Gap = 1**
- We need to find 1 more TP to accept

---

### Step 5 — Overflow Credits

If a zone has **more accepted TPs than it needs**, the integer excess is shared with nearby zones to reduce their gap.

Excess is calculated differently depending on whether the zone has a coverage override:

- **Zones with coverage override** (Birmingham, Manchester, etc.):
  `excess = floor(accepted − journeys × 0.85)`
  We're intentionally under-accepting here, so only a meaningful surplus above 85% of journeys counts as true excess.

- **All other zones:**
  `excess = accepted − round(total_journeys)`

Overflow flows in this order:

| Source Zone | Overflows To |
|---|---|
| Birmingham | Oxford → Peterborough |
| Manchester | Sheffield → North Wales → Lake District |
| London | Oxford → Peterborough → Kent → Brighton |
| Brighton | Salisbury → Kent |
| Peterborough | Oxford → Norwich |
| Edinburgh-Glasgow | North Lake District → Newcastle |

**Example — 14 March:**
- Birmingham: 8 accepted, 6.6 journeys → `floor(8 − 6.6×0.85)` = `floor(8 − 5.6)` = **+2 excess**
- Oxford gap was 2, Birmingham sends 2 → Oxford **gap reduced to 0** (no new TPs needed)
- Manchester: 9 accepted, 5.7 journeys → `floor(9 − 5.7×0.85)` = **+4 excess** → flows to Sheffield

---

### Step 6 — Score Every Pending TP

Each pending TP is scored 0–1 using a weighted formula:

```
score = (rating_score × 0.45) + (deallo_score × 0.34) + (vat_score × 0.06) + (cap_score × 0.10) + (type_score × 0.05)
```

#### Rating Score (45% weight)
Linear scale anchored at 4.5 = 0.60 and 5.0 = 1.00:

| Rating | Score |
|---|---|
| 4.4 | 0.52 |
| 4.5 | 0.60 |
| 4.7 | 0.76 |
| 5.0 | 1.00 |
| 6.0 (new TP) | 0.90 (bonus) |

TPs with rating below **4.4** are hard-filtered out before scoring — they are never recommended.

#### Deallocation Rate Score (34% weight)
Lower deallo rate = better (TP less likely to cancel):

| Deallo Rate | Score |
|---|---|
| 0% | 1.00 |
| ≤ 5% | 0.90 |
| ≤ 10% | 0.70 |
| ≤ 20% | 0.40 |
| > 20% | max(0, 1 − deallo×4) |

#### VAT Score (6% weight)
Non-VAT registered TPs are preferred — they save ~20% on TP cost:

| VAT Status | Score |
|---|---|
| Not VAT registered | 1.0 |
| VAT registered | 0.0 |

#### Capacity Score (10% weight)
Higher van capacity is better, especially for 2-man jobs:

| Capacity | Score |
|---|---|
| < 8 | 0.0 (also hard-filtered) |
| 8–9 | 0.3 |
| 10–14 | 0.5 |
| ≥ 15 | 0.65 |

#### Type Fitness Score (5% weight)

| Type | Consider | Score |
|---|---|---|
| 12-man | any | 1.00 |
| custom | local | 0.80 |
| local or national | — | 0.70 |
| custom | national | 0.20 |

**Worked example — KOWALLTD:**
- Rating 4.92 → `0.60 + (4.92−4.5)×0.80` = 0.936 → × 0.45 = 0.421
- Deallo 0.000 → 1.00 → × 0.34 = 0.340
- VAT=No → 1.00 → × 0.06 = 0.060
- Cap 20 → 0.65 → × 0.10 = 0.065
- Type local → 0.70 → × 0.05 = 0.035
- **Total score = 0.921**

---

### Step 7 — Select the Best TPs (Standard Zones)

Selection combines three layers:

#### Layer 1 — 1-man TP Filter
If a zone has ≤ 9 confirmed 1-man jobs, pure 1-man TPs are **excluded** from candidates.
Reason: a 1-man TP can only serve 1-man journeys. If there aren't enough 1-man jobs to form a journey, accepting a 1-man TP is wasteful. 2-man TPs can serve both.

**Example:** Cornwall has 4 1-man jobs → 1-man TPs filtered out.

#### Layer 2 — Versatility (12-man preference)
If **≥ 30% of confirmed jobs are 1-man**, 12-man TPs are preferred over all others.
12-man TPs are uniquely flexible — they can serve both 1-man AND 2-man journeys.

The selection fills **12-man slots first**, then fills remaining slots with all other TPs.

**Example — Salisbury, 14 March:**
- 1-man ratio = 35% → versatility kicks in
- TAONASHE (12-man) selected first, then DAVI2024 (2-man) fills remaining slot

#### Layer 3 — Diversity Sampling
To avoid always recommending the same top TPs and give lower-tier TPs a chance to improve, candidates are split into **3 equal tiers by score** and each pick is drawn probabilistically:

| Random roll | Tier drawn from |
|---|---|
| r < 0.50 | Top third (highest scores) |
| 0.50 ≤ r < 0.80 | Middle third |
| r ≥ 0.80 | Bottom third |

If the chosen tier is exhausted, it falls back to the next available tier.

**Example:** 9 candidates, gap = 3
- Tier 1 (top 3): KOWALLTD (0.92), FENTINFO (0.82), ADRIANCONSTANTIN75 (0.82)
- Tier 2 (mid 3): FAST87 (0.76), TOMI1983 (0.75), GOCARRY (0.69)
- Tier 3 (bot 3): TAONASHE (0.60), VATJAERA (0.61), ...

Pick 1: r=0.31 → draw from Tier 1 → KOWALLTD
Pick 2: r=0.65 → draw from Tier 2 → FAST87
Pick 3: r=0.12 → draw from Tier 1 → FENTINFO

This means a high-scoring TP doesn't always get in, but has the highest probability.

#### Layer 4 — Username Deduplication
If a TP username is already accepted for that zone+date (e.g. they have two reservations), the algorithm fills from TPs **not yet accepted first**. Only if there are no other options does it consider an already-accepted username.

---

### Step 8 — Tightness Override (Non-London Coverage Zones)

Zones that normally run below 100% coverage (Birmingham, Manchester, Oxford, etc.) can automatically switch to **100% coverage** if supply is tight.

**Tightness condition:**
```
full_target = round(total_journeys × 1.0)
is_tight = total_available (accepted + pending) < full_target
```

If the total supply pool cannot even reach the 100% target, we switch coverage to 100% and flag the zone as `[TIGHT]` in the output.

**Example — Oxford, 14 March:**
- Normal coverage = 50% → target = 2
- But total_available = 3, full_target = 5
- 3 < 5 → **Oxford switches to 100%, target = 5, shown as `oxford [TIGHT]`**

The `[TIGHT]` label is printed in the summary next to the zone name so it's immediately visible.

---

### Step 9 — London Special Logic

London uses a different approach due to its size and complexity.

#### Tightness Check
First, London determines if supply is **tight** or **oversupplied**:

```
base_journeys = round(realized_jobs / 5.8)
is_tight = total_available < 1.3 × base_journeys
```

| Mode | Target formula |
|---|---|
| TIGHT | `round(jobs / 5.8)` — accept to fill all journeys |
| OVERSUP | `round(jobs / 6.0) − 5` — be more conservative |

#### Three-Pass Selection

**Pass 1 — South London Quota (16% of target)**
South London postcodes (SE, SW, KT, CR, TW, BR, SM) are underserved.
`round(target × 16%)` slots are reserved for South London TPs first.

**Pass 2 — 1-man Bucket**
```
one_man_target = round(one_man_jobs × 0.66 / 5.5)
```
Filled from TPs with NUMBER_OF_MEN = 1 or 12. Cap ≥ 8 is acceptable here.

**1-man shortfall compensation:**
If fewer 1-man TPs are available than needed, up to **4 extra 2-man TPs** are added to Pass 3 to compensate.

**Pass 3 — 2-man Bucket**
Remaining slots split 50/50 between national and local TPs. Cap ≥ 10 required.

#### London 1-man Composition Warning
Even when London's gap = 0 (already fully accepted), the algorithm checks whether the accepted pool has enough 1-man/12-man TPs and flags a warning if not:

```
⚠ London 2026-03-14: 1-man shortfall — need 13, accepted 3 (shortfall=10)
```

This means 28 TPs are accepted but only 3 are capable of serving 1-man journeys.

---

## Zone Minimum Jobs Threshold

Some low-volume zones require a minimum number of confirmed jobs before any reservations are accepted at all. Below this threshold, demand is too uncertain to commit supply.

| Zone | Minimum Jobs Required |
|---|---|
| East Yorkshire | 10 |
| Kent | 8 |
| North Wales | 8 |
| North Lake District | 8 |
| Norwich | 8 |

**Example:** East Yorkshire has 6 confirmed jobs on 14 March → skipped entirely.

---

## Output

### Summary Table
Printed for each pickup date:

```
Zone                    Jobs  Jrnys  Cov%  Target  Accepted  Excess  OvflCrd   Gap  NewAcc  Unfill
--------------------------------------------------------------------------------------------------
birmingham                35    6.6   50%       3         8     +2→              0       0       0
cardiff                   20    4.0  100%       4         0                      4       2       2 ⚠
london [OVERSUP]         204   35.2   76%      29        29                      0       0       0
oxford [TIGHT]            27    5.1  100%       5         1               -2     2       1       1 ⚠
salisbury                 20    4.0   75%       3         2                      1       1       0
```

Zone name suffixes:

| Suffix | Meaning |
|---|---|
| *(none)* | Normal — coverage ratio applied as configured |
| `[TIGHT]` | Coverage-override zone switched to 100% — total supply was below the full target |
| `[OVERSUP]` | London in oversupply mode — more conservative target applied |
| `[TIGHT]` *(London)* | London in tight mode — full journey-matching target applied |

| Column | Meaning |
|---|---|
| Jobs | Confirmed realized jobs for this zone+date |
| Jrnys | Estimated journeys (Jobs ÷ JPJ) |
| Cov% | Coverage ratio applied |
| Target | Target number of reservations to hold |
| Accepted | Already accepted reservations |
| Excess | Surplus TPs available to overflow to other zones |
| OvflCrd | Gap reduction received from an overflow source |
| Gap | How many more TPs we need to accept |
| NewAcc | How many we're recommending to accept now |
| Unfill | Slots we couldn't fill (not enough qualifying candidates) ⚠ |

### Recommended TPs Table
Lists every TP recommended to accept, with full quality data and rank within their zone.

---

## Running the Script

```bash
# Uses default file paths configured in the script
python3 supply_acceptor.py

# Specify custom input files
python3 supply_acceptor.py path/to/demand.csv path/to/reservations.csv

# Specify output file path too
python3 supply_acceptor.py demand.csv reservations.csv output.csv
```

Default file names are set at the top of the script (`DEMAND_FILE`, `RES_FILE`) and should be updated whenever new CSV exports are dropped in the folder.

---

## Key Parameters Quick Reference

| Parameter | Value | Description |
|---|---|---|
| `DENSE_JPJ` | 5.3 | Jobs per journey — dense zones (excl. London) |
| `LIGHT_JPJ` | 5.0 | Jobs per journey — light zones |
| `ZONE_JPJ_OVERRIDES['london']` | 5.8 | London-specific JPJ |
| `DEFAULT_COVERAGE` | 1.00 | Default coverage ratio (100%) |
| `MIN_RATING` | 4.4 | Hard floor — TPs below this are never recommended |
| `MIN_CAPACITY` | 8 | Hard floor — vans below this capacity excluded |
| `MIN_1MAN_JOBS_FOR_1MAN_TP` | 9 | Min 1-man jobs needed before accepting a 1-man TP |
| `VERSATILITY_1MAN_THRESHOLD` | 0.30 | 1-man ratio threshold to trigger 12-man preference |
| `OVERSUPPLIED_EXCESS_THRESHOLD` | 0.85 | Overflow threshold for coverage-override zones |
| `DIVERSITY_T1_PCT` | 0.50 | Probability of drawing from top-score tier |
| `DIVERSITY_T2_PCT` | 0.30 | Probability of drawing from mid-score tier |
| `W_RATING` | 0.45 | Scoring weight — rating |
| `W_DEALLO` | 0.34 | Scoring weight — deallocation rate |
| `W_VAT` | 0.06 | Scoring weight — VAT status |
| `W_CAP` | 0.10 | Scoring weight — van capacity |
| `W_TYPE` | 0.05 | Scoring weight — reservation type fitness |
| `LONDON_SOUTH_QUOTA_PCT` | 0.16 | Fraction of London target reserved for South London TPs |
| `LONDON_1MAN_FACTOR` | 0.66 | Fraction of 1-man jobs expected to form journeys |
| `LONDON_1MAN_JPJ` | 5.5 | JPJ for 1-man London journeys |
| `LONDON_OVERSUPPLY_RATIO` | 1.3 | Available/base-journeys ratio that triggers oversupply mode |
| `LONDON_OVERSUPPLY_JPJ` | 6.0 | JPJ divisor in oversupply mode |
| `LONDON_OVERSUPPLY_BUFFER` | 5 | Subtracted from target in oversupply mode |
