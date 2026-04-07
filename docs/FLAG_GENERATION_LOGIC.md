# Supply Acceptor — Flag Generation Logic

This document defines the exact rules used to generate the **Flags** section in the daily supply acceptor view. It is intended to be used as a specification for an automated agent.

---

## Input Data Required

| Input | Source | Key Fields |
|---|---|---|
| Zone summary table | Algorithm output | Zone, Jobs, Jrnys, Cov%, Target, Accepted, Excess, OvflCrd, Gap, NewAcc, Unfill |
| TP recommendations table | Algorithm output | Username, NUMBER_OF_MEN, RES_TYPE, rating, Deallo Rate Overall, VAT_STATUS, sourcezone, START_POSTCODE |
| Already-accepted TPs | Reservations CSV (`IRES_STATUS = 'accepted'`) | USERNAME, NUMBER_OF_MEN, sourcezone, DATE |
| Demand file | Demand CSV | sourcezone, pickup_day, number_of_men, realized_lane_level_jobs |

---

## Flag 1 — Unfilled Slots

**Trigger:** `Unfill > 0` for any zone on the target date

**How to compute:** Read directly from the `Unfill` column in the zone summary table.

**Message:**
```
⚠ {zone}: {Unfill} slot(s) unfilled — not enough qualifying pending TPs available after scoring and hard filters
```

**Notes:**
- Hard filters that eliminate candidates: rating < 4.4, van capacity < 8
- These slots cannot be filled algorithmically — a human needs to manually review the rejected pending TPs for that zone and decide whether to accept any borderline candidates
- Show one flag per affected zone

---

## Flag 2 — London 1-Man Shortfall

**Trigger:** London zone only. After all new acceptances, count of 1-man capable TPs is less than `one_man_target`

**How to compute:**

```
# Step 1 — get one_man_jobs for London on the target date
one_man_jobs = sum(realized_lane_level_jobs)
               where sourcezone = 'london'
               and number_of_men = 1
               and pickup_day = target_date
               (from demand file)

# Step 2 — compute target
one_man_target = round(one_man_jobs × 0.66 / 5.5)

# Step 3 — count 1-man capable TPs after all acceptances
one_man_capable = count of TPs where NUMBER_OF_MEN IN (1, 12)
                  from: (already accepted for London on target_date)
                      + (newly recommended for London on target_date)

# Step 4 — compute shortfall
shortfall = max(0, one_man_target - one_man_capable)
```

**Critical rule:** 12-man TPs **count as 1-man capable**. They can serve both 1-man and 2-man journeys. Never exclude them from this count.

**Do NOT use the algorithm's printed warning directly.** The algorithm fires its warning before new recommendations are counted as accepted, and does not always correctly credit 12-man TPs. Always recompute from scratch using the method above.

**Message (only show if shortfall > 0):**
```
⚠ London 1-man shortfall: need {one_man_target}, will have {one_man_capable} after all acceptances (shortfall = {shortfall})
  → 1-man capable breakdown: {x} pure 1-man + {y} 12-man TPs
```

---

## Flag 3 — High Deallocation Rate

**Trigger:** Any recommended TP has `Deallo Rate Overall > 0.20` (i.e. > 20%)

**How to compute:** Check `Deallo Rate Overall` field for every TP in the recommendations table for the target date.

**Message (one per affected TP):**
```
⚠ {username} ({zone}): high deallo rate ({deallo%}) — limited alternatives available
```

**Notes:**
- These TPs are at higher risk of cancelling after acceptance
- The algorithm selects them when the candidate pool is thin — flagging lets the human decide whether to accept or seek alternatives
- Threshold is 20% — do not flag TPs at or below 20%

---

## Flag 4 — Rating Below Soft Threshold

**Trigger:** Any recommended TP has `rating < 4.5` AND `rating >= 4.4`

**How to compute:** Check `rating` field for every TP in the recommendations table.

**Message (one per affected TP):**
```
⚠ {username} ({zone}): rating {rating:.2f} — below 4.5 soft threshold
```

**Notes:**
- Hard floor is 4.4 — TPs below this are never recommended (already filtered out)
- The 4.4–4.5 range is a grey zone: allowed but worth human attention
- Do NOT flag TPs with rating ≥ 4.5

---

## Flag 5 — London South Quota Short

**Trigger:** London only. Fewer South London TPs were recommended than the quota requires.

**How to compute:**

```
south_quota  = round(london_target × 0.16)

south_filled = count of newly recommended London TPs
               where START_POSTCODE begins with:
               SE, SW, KT, CR, TW, BR, or SM

```

**Message (only show if south_filled < south_quota):**
```
⚠ London South quota short: {south_filled} of {south_quota} South London slots filled
```

**Notes:**
- South London postcodes are underserved — the algorithm reserves ~16% of London's target for them
- If the quota can't be filled it means there aren't enough South London pending TPs

---

## Flag 6 — Zone TIGHT

**Trigger:** Any zone (non-London) has `[TIGHT]` label in the zone summary, meaning it switched from its configured reduced coverage ratio to 100% because total supply (accepted + pending) was below the full journey target.

**How to identify:** Zone label contains `[TIGHT]` in the summary table output.

**Message:**
```
⚠ {zone} [TIGHT]: supply thin — switched to 100% coverage (normally configured at {x}%)
```

**Normal coverage rates for reference:**

| Zone | Normal Coverage |
|---|---|
| Birmingham | 50% |
| Manchester | 75% |
| Peterborough | 50% |
| Oxford | 50% |
| Salisbury | 75% |
| Sheffield | 75% |
| All others | 100% |

---

## Flag 7 — Overflow Unused

**Trigger:** A zone has `Excess > 0` but no downstream zone absorbed it (i.e. no other zone shows a corresponding `OvflCrd` on that date).

**How to compute:**
```
For each zone where Excess > 0:
    Check if any other zone has OvflCrd > 0 on the same date
    If no downstream zone absorbed the credit → flag it
```

**Message:**
```
⚠ {zone}: +{excess} excess TPs but no downstream zone to absorb — credit unused
```

**Notes:**
- This is informational, not urgent
- Configured overflow routes (for reference):

| Source Zone | Overflows To (in order) |
|---|---|
| Birmingham | Oxford → Peterborough |
| Manchester | Sheffield → North Wales → Lake District |
| London | Oxford → Peterborough → Kent → Brighton |
| Brighton | Salisbury → Kent |
| Peterborough | Oxford → Norwich |
| Edinburgh-Glasgow | North Lake District → Newcastle |

---

## Priority Order for Displaying Flags

Display flags in this order (most actionable first):

1. **Unfilled slots** — gaps that cannot be algorithmically filled, need human action
2. **London 1-man shortfall** — capacity risk for 1-man journeys
3. **High deallo rate TPs** — reliability risk, human should validate
4. **Rating below soft threshold** — quality concern
5. **London South quota short** — underserved area coverage
6. **TIGHT zones** — supply health signal
7. **Unused overflow** — informational only

---

## What NOT to Flag

| Situation | Reason |
|---|---|
| Gap = 0 and NewAcc = 0 | Zone is fully covered, no action needed |
| Overflow successfully absorbed (e.g. Birmingham +2 → Oxford -2) | Working as intended — show in summary table only, not as a flag |
| Skipped zones (below min jobs threshold) | Expected behaviour — show in summary table only |
| London OVERSUP mode | Normal behaviour when supply is healthy — not a concern |
| VAT registered TPs | Affects scoring but not a flag-worthy issue |

---

## Key Constants (for reference)

| Parameter | Value |
|---|---|
| Hard rating floor | 4.4 |
| Soft rating threshold | 4.5 |
| Hard capacity floor | 8 |
| High deallo threshold (flag) | 20% |
| London 1-man factor | 0.66 |
| London 1-man JPJ | 5.5 |
| London South quota | 16% of target |
| London tightness ratio | 1.2 |
| London JPJ (tightness + target) | 5.8 |
| London oversupply JPJ | 6.0 |
| London oversupply buffer | −5 |
