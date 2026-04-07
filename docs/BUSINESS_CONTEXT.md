# Supply Acceptor — Business Context

This document captures the business logic, operational workflows, and vetting rules that inform how the supply acceptor algorithm is used day-to-day. It is intended for anyone running or reviewing the algorithm output.

---

## What Are We Trying to Do?

Every day, Transport Providers (TPs) submit **reservations** saying "I'm available in Zone X on Date Y." We need to decide which ones to **accept** — enough to cover expected demand, but not so many that we oversupply and waste cost.

The algorithm runs at **D-3 to D-1** (3 days to 1 day before pickup). The hard acceptance cutoff is **D-1 at 11am London time** — no new acceptances after that.

---

## The Daily Workflow

### 1. Demand at D-3/D-4 Is Only ~60% of Final

When the algorithm runs a few days out, confirmed jobs represent roughly **60% of final demand**. The remaining 40% arrives closer to the pickup date organically and fills **Express Interest (EI)** slots. This is why the algorithm only targets coverage of *confirmed* jobs — EI handles the rest.

**End-of-month uplift:** In the last weekend and final few days of the month, expect approximately **25% more jobs** than what's currently showing in the data. Factor this in when manually vetting algorithm suggestions for those dates.

### 2. AnyRoute Runs at 11am D-1

At 11am on the day before pickup, **AnyRoute** (a VRP model) generates journeys for tomorrow. It works as follows:

- **Custom and return reservations** → get curated journeys tailored to their specific source/destination and availability window
- **Virtual journeys** → generated from remaining demand:
  - **Local journeys**: start and end within 100km of each other
  - **National journeys**: start and end more than 100km apart

This is why reservations have types: a **national reservation** can serve both local and national journeys, but a **local reservation** can only serve local journeys.

By ~12:30pm all journeys are generated. At **1pm** they go live on the **Express Interest portal**.

### 3. Express Interest (EI) — 1pm to 2pm

EI is a portal where any TP (not just those with reservations) can express interest in doing a specific journey. The window runs from **1pm to 2pm**. After 2pm, the **furniture matcher** code runs to match reserved TPs and EI-interested TPs to journeys and generate the final allocation file.

---

## EI Vetting — Daily Health Check

After the algorithm produces recommendations, a **high-level vetting** should be done to check whether the post-acceptance supply state will result in a healthy number of journeys going to EI.

### The Formula

```
expected_journeys  = total_confirmed_jobs / 5.2
post_acceptance    = already_accepted_TPs + newly_recommended_TPs
journeys_to_EI     = expected_journeys - post_acceptance
```

> **Why 5.2?** The real-world trending Jobs Per Journey (JPJ) is currently ~5.2, which is lower than the algorithm's fixed JPJ values (5.8 for London, 5.3 for dense zones, 5.0 for light zones). The 5.2 figure is used only for vetting — not for the algorithm itself.

### Target Range

| Day | Target EI Journeys |
|---|---|
| Monday – Saturday | 25 – 40 |
| Sunday | 20 – 30 |

Sundays have lower EI participation, so the target range is smaller.

### Interpretation

| Result | Meaning |
|---|---|
| Within range | Supply is well-placed |
| Above upper bound | Under-reserved — not enough TPs accepted, too many journeys going to non-reserved pool |
| Below lower bound | Over-reserved — too few journeys for EI, cost risk |

---

## Key Jobs Per Journey (JPJ) Values

| Context | JPJ | Used For |
|---|---|---|
| London (algorithm) | 5.8 | Target and tightness calculation in algorithm |
| Dense zones — Manchester, Oxford, Birmingham (algorithm) | 5.3 | Target calculation in algorithm |
| Light zones — all others (algorithm) | 5.0 | Target calculation in algorithm |
| Vetting (all zones) | 5.2 | EI journey estimate only — not in the algorithm |

---

## London-Specific Logic

London is the most complex zone. Key rules:

### Tightness vs Oversupply
- If `total_available < 1.2 × base_journeys` → **TIGHT** mode: target = `round(jobs / 5.8)`
- Otherwise → **OVERSUP** mode: target = `round(jobs / 6.0) - buffer`

### D+1 Buffer Rule
The `-5` buffer in OVERSUP mode is **only applied for D+1** (tomorrow's pickup date). For D+2 and beyond, confirmed demand is still early-stage (~60% of final), so the buffer would make the target too conservative. No buffer is applied for those dates.

| Pickup Date | Buffer Applied? | Target Formula |
|---|---|---|
| Tomorrow (D+1) | Yes | `round(jobs / 6.0) - 5` |
| D+2 and beyond | No | `round(jobs / 6.0)` |

### 1-Man Capable TPs
**12-man TPs count as 1-man capable.** They can serve both 1-man and 2-man journeys. When checking the London 1-man shortfall, always count 12-man TPs in the 1-man capable pool.

```
one_man_target  = round(one_man_jobs × 0.66 / 5.5)
one_man_capable = count of TPs where NUMBER_OF_MEN IN (1, 12)
                  from: already accepted + newly recommended
shortfall       = max(0, one_man_target - one_man_capable)
```

Do **not** rely on the algorithm's printed 1-man warning — it fires before new recommendations are counted and does not always credit 12-man TPs correctly. Always recompute from scratch.

### South London Quota
~16% of London's target is reserved for South London TPs (postcodes starting with: SE, SW, KT, CR, TW, BR, SM). This area is underserved and the quota ensures adequate coverage.

---

## Coverage Ratios and TIGHT Zones

Some zones intentionally run below 100% coverage because they have structural supply excess:

| Zone | Normal Coverage | Month-Edge Coverage (days 1–3 and 24–31) |
|---|---|---|
| Birmingham | 50% | 75% |
| Manchester | 75% | 75% |
| Peterborough | 50% | 50% |
| Oxford | 50% | 100% |
| Salisbury | 75% | 100% |
| Sheffield | 75% | 75% |
| All others | 100% | 100% |

If total supply (accepted + pending) can't even reach the 100% target, the zone automatically switches to **100% coverage** and is labelled `[TIGHT]` in the output. This is a signal that supply is thin.

---

## Overflow Credits

If a zone has more accepted TPs than it needs, the integer excess is shared with nearby zones to reduce their gap. Configured overflow routes:

| Source Zone | Overflows To (in order) |
|---|---|
| Birmingham | Oxford → Peterborough |
| Manchester | Sheffield → North Wales → Lake District |
| London | Oxford → Peterborough → Kent → Brighton |
| Brighton | Salisbury → Kent |
| Peterborough | Oxford → Norwich |
| Edinburgh-Glasgow | North Lake District → Newcastle |

If a zone has excess but no downstream zone absorbs it, the credit is **unused** — flagged as informational.

---

## TP Quality Hard Filters

TPs that fail these are **never recommended**:

| Filter | Threshold |
|---|---|
| Minimum rating | 4.4 |
| Minimum van capacity | 8 |

---

## Flags to Watch

When reviewing algorithm output, prioritise these flags (most to least urgent):

1. **Unfilled slots** — zone couldn't fill all gaps algorithmically; human needs to review rejected TPs
2. **London 1-man shortfall** — not enough 1-man/12-man TPs accepted; risk for 1-man journey coverage
3. **High deallocation rate** (> 20%) — TP is likely to cancel after acceptance; consider alternatives
4. **Rating below soft threshold** (4.4–4.5) — allowed but worth human attention
5. **London South quota short** — underserved area not getting enough coverage
6. **TIGHT zone** — supply is thin, switched to 100% coverage
7. **Unused overflow** — informational; excess TPs with nowhere to go

---

## Zone Minimum Jobs Thresholds

Low-volume zones are skipped entirely below these thresholds — demand is too uncertain to commit supply:

| Zone | Minimum Jobs |
|---|---|
| East Yorkshire | 10 |
| Kent | 8 |
| North Wales | 8 |
| North Lake District | 8 |
| Norwich | 8 |

---

## Input Files

The algorithm takes two CSV files as input:

| File | What it contains |
|---|---|
| `final_recommendations_YYYY-MM-DD.csv` | Demand data — confirmed jobs per zone, date, and job type (1-man / 2-man) |
| `recommended_reservations_YYYY-MM-DD.csv` | All reservations — pending, accepted, and return, with TP quality data |

These replace the legacy file names `FURN_Supply_Summary - Detailed view.csv` and `FURN_Supply_Summary - reservations.csv`.

---

## Running the Algorithm

```bash
python3 supply_acceptor.py "final_recommendations_YYYY-MM-DD.csv" "recommended_reservations_YYYY-MM-DD.csv"
```

Output is saved automatically as `supply_acceptor_output_YYYY-MM-DD_HHMM.csv` in the same folder.

To export recommendations for a specific date as a CSV (matching the reservations file format):

```python
import pandas as pd

res = pd.read_csv('recommended_reservations_YYYY-MM-DD.csv')
out = pd.read_csv('supply_acceptor_output_YYYY-MM-DD_HHMM.csv')

ids = out[(pd.to_datetime(out['DATE']).dt.date == pd.Timestamp('YYYY-MM-DD').date()) &
          (out['new_recommendation'] == True)]['ID'].tolist()

res[res['ID'].isin(ids)].to_csv('recommendations_YYYY-MM-DD.csv', index=False)
```
