# Supply Acceptor — Full Agent Guide

This document captures the complete context, process, and vetting rules for running the Supply Acceptor and generating TP acceptance recommendations. It is intended to give a Claude agent (clawdbot) everything it needs to run the system independently and present results correctly.

---

## 1. What the System Does

The Supply Acceptor decides which Transport Providers (TPs) to accept for each pickup date, zone by zone. It calculates how many TPs are needed (the **target**) based on predicted or confirmed job volume, compares that to how many are already accepted, and recommends the best pending TPs to fill the gap.

There are two modes:
- **Actuals mode (D-1)**: uses confirmed job counts from Snowflake. JPJ ×1.0.
- **Forecast mode (D-2+)**: uses forecast CSV files. JPJ ×1.2 (conservative — fewer TPs accepted further out).

---

## 2. Files Required

### Core scripts (all must be in the same directory)
| File | Purpose |
|---|---|
| `integrated_supply_acceptor.py` | Main entry point — decides mode, runs algorithm, dedup vetting, writes CSV |
| `supply_acceptor.py` | Actuals mode algorithm |
| `supply_acceptor_forecast.py` | Forecast mode algorithm |
| `fetch_and_run.py` | Snowflake data fetching (TP quality, reservations, demand) |

### Forecast data directory
```
../updated_forecast/production_v5/
```
Must contain the latest `v5_furniture_cluster_lt200km_YYYY-MM-DD.csv` and `v5_removals_cluster_lt200km_YYYY-MM-DD.csv` files. The script picks the most recent file automatically.

### Outputs (auto-generated per run)
| File | Contents |
|---|---|
| `recommended_tps_YYYY-MM-DD.csv` | Final recommendations in reservations CSV format |
| `recommended_reservations_YYYY-MM-DD_HHMM.csv` | Raw reservations fetched from Snowflake |
| `supply_acceptor_output_YYYY-MM-DD_HHMM.csv` | Full algorithm output (all TPs, all statuses) |
| `supply_acceptor_forecast_output_YYYY-MM-DD_HHMM.csv` | Same, for forecast runs |

---

## 3. How to Run

```bash
cd /path/to/supply_acceptor_claude

# Single date
python3 integrated_supply_acceptor.py 2026-04-01

# Multiple dates
python3 integrated_supply_acceptor.py 2026-04-01 2026-04-02

# Dry run (no Snowflake, no algorithm — just prints mode decision)
python3 integrated_supply_acceptor.py --dry-run 2026-04-01

# Default (no args) = tomorrow
python3 integrated_supply_acceptor.py
```

Snowflake requires browser-based Google SSO authentication. A browser window will open automatically.

---

## 4. Mode & Multiplier Decision Logic

| Days until pickup (D) | UK time | Mode | JPJ multiplier |
|---|---|---|---|
| D-1 or less | any | Actuals | ×1.0 |
| D-2 | before 09:00 | Forecast | ×1.2 |
| D-2 | from 09:00 | Actuals | ×1.0 |
| D-3 | any | Forecast | ×1.2 |
| D-4+ | any | Forecast | ×1.2 |

The ×1.2 multiplier in forecast mode makes JPJ (jobs per journey) higher, so the target number of TPs is lower — intentionally conservative when the forecast may be off.

### Zone-specific JPJ multipliers (forecast mode only)

| Zone | Additional multiplier applied on top of base ×1.2 |
|---|---|
| Birmingham | ×1.3 |
| Manchester | ×1.3 |
| London (last 4 days of month) | ×1.3 |
| London (all other dates) | ×1.5 |

**Base JPJ values (before multipliers):**
- Dense zones: 5.5
- Light zones: 5.3
- London: 5.8 (overridden separately)
- London 1-man: 5.5
- London OVERSUP: 6.0

---

## 5. Algorithm Overview

### Step 1 — Build zone-day demand table
- **Actuals**: confirmed routable jobs from Snowflake
- **Forecast**: `pred_d1_routable` from V5 forecast CSVs (furniture + removals combined)

### Step 2 — Calculate journey count
```
journeys = realized_jobs / JPJ
```

### Step 3 — Apply coverage ratio to get target
Default coverage is 100%. Overrides:

| Zone | Default coverage | Notes |
|---|---|---|
| Birmingham | 50% | 75% days 1–3 and 24–31 of month |
| Manchester | 75% | |
| Peterborough | 30% | |
| Edinburgh-Glasgow | 75% | |
| Oxford | 50% | 100% days 1–3 and 24–31 |
| Salisbury | 75% | 100% days 1–3 and 24–31 |
| Sheffield | 75% | |
| All others | 100% | |

**TIGHT override**: if total available TPs (accepted + pending) < full 100% target, coverage is automatically raised to 100%.

### Step 4 — London special logic

London is classified as either **TIGHT** or **OVERSUP**:
- TIGHT: `total_available < 1.2 × base_journeys` → use standard journeys-based target
- OVERSUP: `total_available ≥ 1.2 × base_journeys` → `target = round(jobs / OVERSUP_JPJ) - 5`

The buffer of 5 is always applied in OVERSUP mode (regardless of how far out the pickup date is).

London also has a **1-man quota**: `round(one_man_jobs × 0.66 / 5.5)`. If 1-man supply is short, extra 2-man TPs are added to compensate (up to 4 extra).

London also has a **South London quota**: 16% of target slots reserved for TPs with SE/SW/KT/CR/TW/BR/SM postcodes.

### Step 5 — Score and rank pending TPs

```
score = 0.45×rating_norm + 0.34×(1-deallo) + 0.06×vat + 0.10×capacity_norm + 0.05×type_bonus
```

Where:
- `rating_norm` = (rating - 1) / 4  (scale 1–5 → 0–1)
- `deallo` = Deallo Rate Overall
- `vat` = 1 if VAT registered, else 0
- `capacity_norm` = min(capacity, 30) / 30
- `type_bonus` = 1 if `consider_res_type == RES_TYPE` else 0

Hard filters applied before scoring:
- Minimum rating: 4.4 (new TPs with rating 6.00 are exempt — 6.00 is a placeholder for no rating history)
- Minimum capacity: 8 cubes
- 1-man TPs only accepted if zone has >9 predicted 1-man jobs

### Step 6 — Diversified selection

TPs are sampled with diversity rules:
- Top 50% of slots filled from same `USERNAME` (type 1 diversity)
- Next 30% from same username group
- Remaining from full pool
- USERNAME deduplication is bypassed when `n > unique_count` (intentional — rare zones with few TPs)

### Step 7 — Skip conditions

Zones are skipped entirely if predicted/confirmed jobs are below threshold:

| Zone | Min jobs |
|---|---|
| East Yorkshire | 10 |
| Kent | 10 |
| North Wales | 10 |
| North Lake District | 10 |
| Norwich | 10 |
| Northwest Scotland | 10 |
| Northeast Scotland | 10 |
| All others | 8 |

---

## 6. Reading the Full Picture Table

```
Zone                    Jobs  Jrnys  Cov%  Target   Acc  Pend  Excess  OvflCrd   Gap  NewAcc  Unfill
```

| Column | Meaning |
|---|---|
| Jobs | Confirmed/predicted routable jobs |
| Jrnys | Jobs ÷ JPJ = expected journeys |
| Cov% | Coverage ratio applied |
| Target | How many TPs needed total |
| Acc | Already accepted TPs |
| Pend | Pending TPs available |
| Excess | TPs above target (positive = oversupplied) |
| OvflCrd | Overflow credit received from another zone |
| Gap | Target − Acc (slots to fill) |
| NewAcc | New TPs recommended this run |
| Unfill | Slots that could not be filled (not enough qualifying pending TPs) |

A `—` in Cov%/Target/Gap means the zone was skipped (below minimum job threshold or no reservations).

**Forecast mode table** has additional columns: `PredFurn`, `PredRem`, `PredTotal`, `CRFurn`, `CRRem`, `CRout` (confirmed routable from Snowflake alongside forecast).

---

## 7. Reading the Recommended TPs Output

The script prints recommendations grouped by zone, then by TP type (1-man/12-man first, then 2-man national, then 2-man local). Each line shows:

```
Username    Men  Type  Rating  Deallo  VAT
```

Flags printed inline:
- `⚠ high deallo` — Deallo Rate Overall > 20%
- `★ new TP` — rating == 6.00 (no prior rating history, new to platform)
- `⚠ rating X.XX — below 4.5 soft threshold`

The `recommended_tps_YYYY-MM-DD.csv` file is written automatically at the end of each run. It is in the same format as the reservations input CSV:
```
DATE, ID, USERNAME, IRES_STATUS, RES_TYPE, NUMBER_OF_MEN, START_POSTCODE,
RESERVATION_CAPACITY, HOURS_AVAILABLE, sourcezone, consider_res_type,
rating, Deallo Rate, Deallo Rate Overall, VAT_STATUS
```

---

## 8. EI Vetting

Printed at the bottom of each date's full picture table:

```
EI Vetting (Weekday): X pred jobs ÷ 5.5 = Y expected journeys | Post-acceptance TPs: Z | → Journeys to EI: W
```

**Formula**: `EI_journeys = (predicted_jobs / 5.5) - post_acceptance_TPs`

**Acceptable ranges**:
| Day | Range |
|---|---|
| Sunday | 20–30 |
| Monday–Saturday | 25–40 |

If EI journeys is **above range** → under-reserved (not enough TPs accepted, EI will be overloaded).
If EI journeys is **below range** → over-reserved (too many TPs accepted, not enough jobs for EI).

---

## 9. Dedup Vetting Report

Runs automatically after each recommendation. Cap = **1 per TP per day** across accepted + newly recommended.

For each TP appearing more than once:
1. Identify excess recommended slots (accepted slots cannot be changed)
2. Search for a same-type (`NUMBER_OF_MEN`) replacement from the pending pool
3. New TPs (rating == 6.00) are prioritised as replacements
4. If a replacement is found: suggest it, with quality warnings if needed
5. If no replacement: suggest "leave for EI"

**Quality warnings on replacements**:
- Rating < 4.2 (and not a new TP) → `⚠ LOW QUALITY`
- Capacity < 8 cubes → `⚠ LOW QUALITY`

**Cases where no action is possible**:
- Both slots already accepted → "Both slots already accepted — cannot replace either"

---

## 10. How to Present Recommendations (Agent Instructions)

1. **Always show the full picture table** — do not crop any rows, even zones showing `—`.
2. **Show the recommended TPs section** exactly as printed by the script.
3. **Show all flags** for the requested pickup date.
4. **Show the dedup vetting report** in full.
5. **Do not reformat** any of the above into your own tables or markdown. Paste the raw output.
6. **Mention the CSV path** written at the end of the run.
7. If the EI vetting is outside range, note it clearly.

---

## 11. Key Business Rules to Know

- **TAONASHE in Salisbury**: frequently recommended for both 12-man and 2-man slots on the same day. Dedup vetting always drops the 2-man and keeps the 12-man (higher capacity, more valuable).
- **MARIUSGB in Sheffield**: often already accepted once when the run happens. Dedup vetting will flag and drop the extra recommended slot.
- **GREEN199 in Brighton**: limited pending pool means GREEN199 is often the only available TP, and they may already be accepted. Drop extra slot, leave for EI.
- **FINCO in London**: rating 5.00 but deallo 50% — always flagged. Accept cautiously.
- **YEG23 in London**: rating 4.47, below the 4.5 soft threshold — always flagged.
- **Oxford**: consistently thin pending pool, often 5+ unfilled slots. Normal — not a bug.
- **London 1-man shortfall**: if fewer 1-man TPs than needed, the algorithm adds extra 2-mans. This is expected behaviour.
- **New TPs (rating 6.00)**: these have no prior rating history. The 6.00 is a placeholder. They are given priority in dedup replacement suggestions and are exempt from the 4.4 minimum rating filter.
- **Dedup vetting is read-only**: it does not modify any CSV. It is advisory only.

---

## 12. Forecast Version

As of 2026-03-30, the system uses **V5 forecast** files from:
```
../updated_forecast/production_v5/
```

The file finder in `supply_acceptor_forecast.py` tries V5 first, then falls back to V3/V2. When a new forecast version is released, update `FORECAST_DIR` in `integrated_supply_acceptor.py` to point to the new production directory.

---

## 13. Common Issues

| Symptom | Cause | Fix |
|---|---|---|
| `FileNotFoundError: No furniture cluster forecast found` | `FORECAST_DIR` points to wrong directory, or new version prefix not recognised | Update `FORECAST_DIR` in `integrated_supply_acceptor.py`; add new version prefix to `find_best()` in `supply_acceptor_forecast.py` |
| Zone missing from full picture (actuals mode) | Zone has reservations but no confirmed demand — not in demand table | Fixed in `supply_acceptor.py` — adds zero-demand rows for reservation-only zones |
| TP appearing multiple times in recommendations | `diversified_sample()` bypasses dedup when pool is smaller than target | Expected. Caught and reported by dedup vetting. |
| Snowflake auth loop | Browser window did not open or SSO timed out | Re-run script; ensure Google SSO session is active |
