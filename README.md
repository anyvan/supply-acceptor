# AnyVan Supply Acceptor (V2)

Automated daily tool that decides which Transport Provider (TP) reservations to accept for each AnyVan pickup zone and date. Given a pickup date, the tool fetches confirmed demand and pending reservations from Snowflake, targets how many 1-man and 2-man TPs each zone needs, scores all pending TPs, and produces a vetted recommendation list — with EI balancing and dedup handling built in.

---

## How It Works — Quick Summary

Each day, AnyVan receives job bookings across multiple UK zones (London, Manchester, Edinburgh-Glasgow, etc.). Each zone needs a certain number of Transport Providers (TPs) to cover those jobs. TPs reserve capacity in advance. This tool:

1. Pulls confirmed jobs from Snowflake to compute per-zone TP targets
2. Pulls all pending reservations for the pickup date
3. Scores each pending TP (rating 45%, deallo rate 34%, VAT 6%, capacity 10%, type 5%)
4. Selects the best TPs to accept — separately targeting 1-man and 2-man slots
5. Applies EI (Early Internet) reservation quotas — holds back a fixed number of TPs per zone for the EI channel
6. Handles duplicates: replaces with next-best TP where possible
7. Produces two output files: `recommended_tps_YYYY-MM-DD.csv` (full algorithm output) and `vetted_tps_YYYY-MM-DD.csv` (final recommendations after EI holds and dedup)

**Mode is selected automatically based on days out:**
| Days out | UK time | Mode |
|---|---|---|
| D-1 | any | Actuals — uses confirmed Snowflake jobs |
| D-2 | before 09:00 UK | Forecast — uses S3 demand forecast files |
| D-2 | from 09:00 UK | Actuals |
| D-3+ | any | Forecast |

---

## Prerequisites

### 1. Python 3.9+

```bash
python3 --version
```

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

### 3. Snowflake access

You need a Snowflake account with access to the AnyVan data warehouse. The tool authenticates via SSO (browser auth — a browser window will open on first run, then the session is cached).

**Connection details** (already configured in `fetch_and_run_v2.py`):
- Account: `pu40889.eu-west-1`
- Authenticator: `externalbrowser`
- Role: `MART_ROUTE_OPT_GROUP`
- Database: `HARMONISED`, Schema: `PRODUCTION`

No password needed — just log in with your AnyVan Google account when the browser window opens.

### 4. AWS credentials (forecast mode only — D-2 or earlier)

Forecast mode fetches demand forecast files from S3 (`supply-acceptor-data-production` bucket). You need temporary AWS credentials. Export them before running:

```bash
export AWS_ACCESS_KEY_ID="..."
export AWS_SECRET_ACCESS_KEY="..."
export AWS_SESSION_TOKEN="..."
```

Actuals mode (D-1) does not need AWS credentials.

---

## Running the Tool

**Always use `integrated_supply_acceptor_v2.py`** — it auto-selects actuals or forecast mode.

```bash
python3 integrated_supply_acceptor_v2.py YYYY-MM-DD
```

### Examples

```bash
# Recommendations for tomorrow (D-1 — actuals mode)
python3 integrated_supply_acceptor_v2.py 2026-04-08

# Recommendations for 3 days out (D-3 — forecast mode, needs AWS creds)
export AWS_ACCESS_KEY_ID="..."
export AWS_SECRET_ACCESS_KEY="..."
export AWS_SESSION_TOKEN="..."
python3 integrated_supply_acceptor_v2.py 2026-04-10
```

The tool prints a full zone summary table and TP recommendation list to the terminal, and saves output files to the current directory.

---

## Output Files

| File | Description |
|---|---|
| `recommended_tps_YYYY-MM-DD.csv` | All TPs the algorithm recommends — before EI holds and dedup vetting |
| `vetted_tps_YYYY-MM-DD.csv` | **Final recommendations** — after EI reservation holds and dedup handling |
| `*_zone_summary_YYYY-MM-DD_HHMM.csv` | Per-zone targeting data (targets, accepted counts, gaps, etc.) |

### vetted_tps statuses

| Status | Meaning |
|---|---|
| `ACCEPT` | Accept this TP |
| `HOLD_EI` | Hold for EI (Early Internet) channel — do not accept |
| `REPLACE_DEDUP` | TP already accepted once; this slot replaced by the next-best TP |
| `REMOVE_DEDUP` | Duplicate with no available replacement — do not accept |

---

## Zone Summary Table — Column Reference

When you run the tool, it prints a table like this to the terminal:

```
Zone       1MJobs  2MJobs  Rem  Tgt1M  Tgt2M  Target  Acc  A1M  A2M  A12M  Pend  Excess  Gap  G1M  G2M  NewAcc  Unfill
london        107      64    7     13     16      29    24    6   14     4    38       0     5    3    2       5       0
manchester     22      27    7      2      7       9     7    0    5     2    11       0     2    1    1       2       0
...
```

| Column | Meaning |
|---|---|
| `1MJobs` | Confirmed 1-man furniture jobs in zone |
| `2MJobs` | Confirmed 2-man furniture jobs in zone |
| `Rem` | Confirmed removal jobs (any crew size) |
| `Tgt1M` / `Tgt2M` | Algorithm target: how many 1-man / 2-man TPs the zone needs |
| `Target` | Total TP target for the zone |
| `Acc` | Already accepted TPs (before this run) |
| `A1M` | Accepted dedicated 1-man TPs |
| `A2M` | Accepted dedicated 2-man TPs |
| `A12M` | Accepted 12-man (flexible) TPs |
| `Pend` | Pending TPs in pool |
| `Excess` | Over-accepted TPs (already accepted more than target) |
| `OvflCrd` | Overflow credits received from neighbouring zones |
| `Gap` | Total unfilled slots |
| `G1M` / `G2M` | Unfilled 1-man / 2-man slots specifically |
| `NewAcc` | New TPs recommended this run |
| `Unfill` | Slots that remain unfilled even after all recommendations (no suitable TP available) |

**Forecast mode only — additional columns:**

| Column | Meaning |
|---|---|
| `C1M` / `C2M` / `CRem` | Confirmed jobs (actuals so far) vs the forecast |
| `aT1M` / `aT2M` | Actuals-based targets (computed from confirmed jobs — floor on eTgt) |
| `eT1M` / `eT2M` | Effective targets (conservative at D-3+; raised by aTgt floor) |

---

## EI Vetting Line

At the bottom of the zone table, the tool prints:

```
EI Vetting (Wednesday): 506 jobs ÷ 5.12 (v2 predicted JPJ) = 98.9 expected journeys
Post-acceptance TPs: 67 | → Journeys to EI: 31.9 ✓ within range (25–40)
```

**EI (Early Internet)** is a channel where journeys are handled by TPs who are not pre-accepted — they pick up jobs on the day. The tool ensures enough jobs flow to EI to keep that channel healthy.

| Day | Target EI range |
|---|---|
| Monday – Saturday | 25–40 journeys |
| Sunday | 20–30 journeys |

If EI is above range, the algorithm is under-accepting (leaving too much for EI). If below range, it is over-accepting.

---

## EI Reservation Quotas

Certain high-volume zones always hold back a fixed number of TPs for EI, regardless of the current EI level. These are configured in `integrated_supply_acceptor_v2.py`:

| Zone | Quota held for EI | Preference |
|---|---|---|
| London | 5 TPs | Prefer 2-man |
| Birmingham | 3 TPs | Any |
| Manchester | 3 TPs | Any |
| Sheffield | 2 TPs | Any |
| Peterborough | 2 TPs | Any |

These appear in `vetted_tps_YYYY-MM-DD.csv` with status `HOLD_EI`.

---

## Algorithm Overview (V2)

### Step 1 — Demand split

Demand is split into three buckets per zone:
- **1-man furniture jobs** (`number_of_men = 1`)
- **2-man furniture jobs** (`number_of_men = 2`)
- **Removal jobs** (`category_id = 2`, encoded as `number_of_men = 12`)

### Step 2 — Per-zone TP targets

Each zone has empirically derived JPJ (Jobs Per Journey) parameters in `jpj_parameters.csv`, fitted from 30 days of actual anyroute journey data. These give separate targets for 1-man and 2-man TPs:

```
target_1m = floor(eff_1m_jobs × pct_1m_1j / jpj_1m)
target_2m = round((eff_1m_jobs × pct_1m_2j + eff_2m_jobs + eff_rem_jobs) / jpj_2m)
target    = round((target_1m + target_2m) × coverage_ratio)
```

Coverage ratios are zone-specific (e.g. London 100%, Birmingham 50%, Peterborough 30%) and increase at month edges (days 1–3, 24–31).

### Step 3 — 12-man TP redistribution

Accepted 12-man TPs are flexible — they can serve both 1-man and 2-man jobs. The algorithm tracks them separately (`A12M`) and redistributes each one to whichever bucket (1M or 2M) has the larger unfilled gap. Ties go to the 2M bucket.

This means `G1M` and `G2M` (the gap columns) correctly reflect which specific TP type is still needed after redistribution.

### Step 4 — TP scoring

Every pending TP is scored 0–1:

| Component | Weight | Details |
|---|---|---|
| Rating | 45% | Linear scale: 4.5→0.60, 5.0→1.00. Rating=6 (new TP) → 0.90 |
| Deallo rate | 34% | 0% → 1.00, ≤5% → 0.90, ≤10% → 0.70, ≤20% → 0.40, >20% → penalised |
| VAT status | 6% | Non-VAT → 1.00 (cheaper for AnyVan), VAT-registered → 0.0 |
| Capacity | 10% | ≥15 → 0.65, ≥10 → 0.50, ≥8 → 0.30, <8 → excluded |
| Type | 5% | 12-man → 1.00, custom local → 0.80, local/national → 0.70 |

### Step 5 — Two-pass selection

- **Pass 1**: fill `G1M` slots from the 1-man pool (`NUMBER_OF_MEN` = 1 or 12)
- **Pass 2**: fill `G2M` slots from the 2-man pool (`NUMBER_OF_MEN` = 2 or 12)

Selection uses **probabilistic tier sampling**: candidates are split into top/mid/bottom score tiers. Each pick has a 50% chance of drawing from the top tier, 30% mid, 20% bottom. This introduces controlled diversity — the highest-scoring TP is usually selected but not guaranteed on every run.

### Step 6 — Overflow

Zones with confirmed excess supply can send credits to neighbouring zones with gaps. For example, Peterborough excess helps Oxford.

### Step 7 — EI vetting

The tool checks how many journeys will flow to EI using a fitted linear model for JPJ (Jobs Per Journey), then adjusts acceptance if EI is outside the target range.

### Step 8 — Dedup and EI quota holds

- Duplicates (same TP, same day, multiple reservations) are resolved in `vetted_tps`: replaced with the next-best TP, or removed if none available
- EI reservation quotas (see above) are applied unconditionally before EI-level balancing

---

## Configuration Reference

All configuration is in `integrated_supply_acceptor_v2.py` at the top of the file.

| Config | Variable | Current value |
|---|---|---|
| EI reservation quotas | `EI_RESERVATION_QUOTA` | London=5, Birmingham=3, Manchester=3, Sheffield=2, Peterborough=2 |
| Deep pool zones (Layer 1 EI balancing) | `DEEP_POOL_ZONES` | London, Birmingham, Manchester, Peterborough |
| EI cut fraction per zone | `CUT_FRACTION` | 0.13 (13%) |
| EI target range (Mon–Sat) | `EI_TARGET_MIN` / `EI_TARGET_MAX` | 25 / 40 |
| EI target range (Sunday) | `EI_SUN_MIN` / `EI_SUN_MAX` | 20 / 30 |
| London South quota | `LONDON_SOUTH_QUOTA_PCT` | 16% |
| Min TP rating | `MIN_RATING` | 4.4 |
| Min van capacity | `MIN_CAPACITY` | 8 |

JPJ parameters are in `jpj_parameters.csv` — regenerate with:

```bash
python3 compute_jpj_parameters.py
```

(Requires AWS credentials for the `anyroute-results-data-production` S3 bucket.)

---

## Flags in the Output

The terminal output includes a flags section after the zone table. In priority order:

1. **Unfilled slots** — zones where no suitable TP was found in the pool. Needs human review.
2. **London 1-man shortfall** — London has separate 1-man and 2-man sub-targets. Flag if 1-man coverage is short.
3. **High deallo rate** (>20%) — reliability risk on the day.
4. **Low rating** (4.4–4.5) — quality concern.
5. **London South quota short** — <16% of London target from South London postcodes (SE/SW/KT/CR/TW/BR/SM).
6. **TIGHT zones** — confirmed supply below 100% of target.
7. **EI vetting status** — whether EI is within range.

---

## File Structure

```
supply-acceptor/
├── integrated_supply_acceptor_v2.py   # Main entry point — run this
├── supply_acceptor_v2.py              # Core V2 algorithm
├── supply_acceptor_forecast_v2.py     # Forecast mode wrapper (D-2+)
├── fetch_and_run_v2.py                # Snowflake data fetcher
├── compute_jpj_parameters.py          # Regenerate JPJ params from S3 data
├── jpj_parameters.csv                 # Per-zone empirical JPJ parameters (required)
├── requirements.txt
├── docs/
│   ├── BUSINESS_CONTEXT.md            # Business background and zone descriptions
│   ├── SUPPLY_ACCEPTOR_EXPLAINED.md   # Detailed algorithm explanation
│   ├── SUPPLY_ACCEPTOR_GUIDE.md       # Operational guide
│   ├── FLAG_GENERATION_LOGIC.md       # How flags are generated
│   └── snowflake-schema.md            # Snowflake table schemas
└── legacy/                            # V1 scripts — deprecated, do not use
    ├── supply_acceptor.py
    ├── supply_acceptor_forecast.py
    ├── fetch_and_run.py
    └── integrated_supply_acceptor.py
```

---

## Updating JPJ Parameters

JPJ parameters should be refreshed periodically (every few weeks) to keep them aligned with actual journey patterns:

```bash
# Requires AWS credentials
export AWS_ACCESS_KEY_ID="..."
export AWS_SECRET_ACCESS_KEY="..."
export AWS_SESSION_TOKEN="..."

python3 compute_jpj_parameters.py          # last 30 days (default)
python3 compute_jpj_parameters.py --days 14  # last 14 days
```

This overwrites `jpj_parameters.csv`.

---

## Troubleshooting

**"No AWS credentials found"**
You're running in forecast mode (D-2 or earlier) without AWS credentials. Export them first — see Prerequisites section.

**Snowflake browser auth doesn't open**
Try running the command in a terminal with a display (not a headless environment). The first run opens a browser; subsequent runs use a cached session.

**Zone shows `[v1]` in output**
The zone is missing empirical JPJ parameters and falls back to fixed JPJ=5.5. Run `compute_jpj_parameters.py` to regenerate. If the zone consistently has insufficient data, V1 fallback is expected.

**TP not recommended despite being in the pending pool**
The algorithm uses probabilistic tier sampling — the top scorer is not guaranteed to be selected on every run. If you believe a specific TP should have been selected, check its score components (rating, deallo, VAT, capacity) and compare against what was selected. Re-running may select it.
