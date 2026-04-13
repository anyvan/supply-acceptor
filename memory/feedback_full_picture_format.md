---
name: Full Picture output format
description: The exact format to use whenever producing supply acceptor recommendations for any date — both actuals and forecast mode
type: feedback
---

Always produce the Full Picture format when presenting supply acceptor recommendations. This applies to both actuals (D-1, D-2) and forecast (D-3+) runs. The user's manager reads these outputs, so completeness and clarity are critical.

**Why:** A simpler markdown summary was produced initially and the user pointed out it was missing the zone table, 1M/2M splits, per-zone TP tables, flags, and vetting summary that the previous Claude session in supply_acceptor_claude repo produced.

**How to apply:** After running the script, always produce ALL of the following sections:

---

## Section 1 — Header

```
## Full Picture — YYYY-MM-DD (Actuals/Forecast — Weekday) D-N @ HH:MM BST
```

- For forecast runs, add a note: "Mode: FORECAST — Demand figures are forecast totals (vN model). Confirmed columns show jobs booked so far."
- For D-2 actuals runs before full demand is in, add: "Note: At D-2 only X/~Y jobs are confirmed (Z%). Demand will grow as more jobs book today and tomorrow."

---

## Section 2 — Zone Table (ASCII box)

**Actuals mode columns:**
`Zone | 1MJobs | 2MJobs | Rem | Tgt1M | Tgt2M | Target | Acc | A1M | A2M | A12M | Pend | Gap | G1M | G2M | TotRec | NewAcc | Unfill`

**Forecast mode — add confirmed columns:**
`Zone | 1MJobs | 2MJobs | Rem | C1M | C2M | CRem | Tgt1M | Tgt2M | Target | Acc | A1M | A2M | A12M | Pend | Gap | G1M | G2M | TotRec | NewAcc | Unfill`

**Column definitions:**
- TotRec = total new TPs recommended before vetting (primary pass + post-EI fallback, i.e. ACCEPT + HOLD_EI combined)
- NewAcc = post-vetting ACCEPT count only (excludes HOLD_EI)
- Unfill = max(0, Gap − NewAcc) — reflects true supply shortfall after EI quota holds

- Use full ASCII box drawing (┌─┬─┐ etc.)
- Mark zones with unfilled slots with ⚠ in the zone name
- Include a TOTAL row at the bottom
- List skipped zones (below minimum threshold) below the table

---

## Section 3 — EI Vetting Line

```
EI Vetting (Weekday): X jobs ÷ Y.YY (v2 predicted JPJ) = Z.Z expected journeys | Post-acceptance TPs: N | → J.J to EI ✓/⚠ STATUS
```

For D-2 actuals: add a note that EI may read low/high because demand is not fully confirmed and will correct naturally.
For D-3 forecast: add a note that being above range is normal and expected at D-3 — re-run at D-2 will correct.

---

## Section 4 — Vetted Recommendations

State: "X ACCEPT | Y HOLD_EI"

Then for each zone that has recommendations, produce a per-zone mini-table:

**Zone name (N accept/N held)**
```
┌────────┬──────────┬─────┬──────┬────────┬────────┬─────┬────────┐
│   ID   │ Username │ Men │ Type │ Rating │ Deallo │ VAT │ Status │
├────────┼──────────┼─────┼──────┼────────┼────────┼─────┼────────┤
│ NNNNNN │ USERNAME │ XM  │ Loc  │ N.NN   │ N.N%   │ No  │ ACCEPT │
└────────┴──────────┴─────┴──────┴────────┴────────┴─────┴────────┘
```

- Status field: `ACCEPT`, `HOLD_EI (quota)`, `HOLD_EI (EI balancing)`, `ACCEPT ⚠ high deallo`, `ACCEPT ★ new TP`
- Include ALL recommended TPs (both ACCEPT and HOLD_EI) in these tables, grouped by zone
- Note unfilled slots below the zone table if applicable

---

## Section 5 — Flags

Numbered list of issues, in priority order:
1. EI status (below/above range) — explain whether it's a D-2/D-3 artefact or a real concern
2. Unfilled slots — per-zone table with situation description
3. High deallo rate TPs (>20%) — name the TP, zone, rate, and whether it was accepted or held
4. Very high deallo TPs (>35%) — call out explicitly
5. Specific zone issues (shortfalls, pool exhaustion, persistent dedup)
6. Any other notable flags (new TPs, VAT status, capacity concerns)

For EI below range at D-2: always check if the held TPs are in shortage buckets — if so, recommend considering manual accept since the hold is a D-2 artefact.

---

## Section 6 — Vetting Summary Table

```
┌───────────────────┬──────────────────────────────────────────────────┬──────────────────────────────────────────────────────┐
│       Zone        │                     Concern                      │                        Action                        │
├───────────────────┼──────────────────────────────────────────────────┼──────────────────────────────────────────────────────┤
│ Zone name         │ Description of the issue                         │ Recommended action                                    │
└───────────────────┴──────────────────────────────────────────────────┴──────────────────────────────────────────────────────┘
```

One row per zone with an issue + one row for EI status at the end.

---

## Section 7 — Totals footer

```
Totals after this run: N already accepted + N new ACCEPT = N total accepted | N HOLD_EI | N unfilled
```

For forecast runs, add: "Re-run recommended on [date] after 09:00 BST when the tool switches to actuals mode."
