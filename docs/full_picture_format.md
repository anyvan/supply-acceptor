# Full Picture Output Format

Reference document for the exact format to use when presenting supply acceptor recommendations.
If the output format drifts, point Claude to this file.

---

## Section 1 — Header

```
## Full Picture — YYYY-MM-DD (Actuals/Forecast — Weekday) D-N @ HH:MM BST
```

- Actuals: used when running for D-1 or D-2 with real Snowflake demand data
- Forecast: used for D-3+ with forecast demand; add note:
  `Mode: FORECAST — Demand figures are forecast totals (vN model). Confirmed columns show jobs booked so far.`
- D-2 actuals: add note:
  `Note: At D-2 only X/~Y jobs are confirmed (Z%). Demand will grow as more jobs book today and tomorrow.`

---

## Section 2 — Zone Table

Use full ASCII box drawing characters (┌─┬─┐ etc.). No plain markdown tables here.

### Actuals mode columns

```
┌──────────────────────┬─────┬─────┬─────┬───────┬───────┬────────┬─────┬─────┬─────┬──────┬──────┬─────┬─────┬─────┬────────┬────────┬────────┐
│ Zone                 │ 1MJ │ 2MJ │ Rem │ Tgt1M │ Tgt2M │ Target │ Acc │ A1M │ A2M │ A12M │ Pend │ Gap │ G1M │ G2M │ TotRec │ NewAcc │ Unfill │
```

### Forecast mode — add confirmed columns between Rem and Tgt1M

```
│ Zone │ 1MJ │ 2MJ │ Rem │ C1M │ C2M │ CRem │ Tgt1M │ Tgt2M │ Target │ Acc │ A1M │ A2M │ A12M │ Pend │ Gap │ G1M │ G2M │ TotRec │ NewAcc │ Unfill │
```

### Column definitions

| Column | Definition |
|--------|------------|
| TotRec | Post-vetting ACCEPT + HOLD_EI + REMOVE_COVERED count. REMOVE_COVERED TPs count because Layer 0 firing means coverage is sufficient — the zone is not short |
| NewAcc | Post-vetting ACCEPT count only (excludes HOLD_EI and REMOVE_COVERED) |
| Unfill | max(0, Gap − TotRec) — slots with genuine pool exhaustion. If REMOVE_COVERED fired, TotRec already covers the gap so Unfill=0 |

### Zone table rules

- Mark zones with Unfill > 0 with ⚠ in the zone name
- Include a TOTAL row at the bottom
- List skipped zones (below minimum job threshold) in a note below the table
- Do NOT include an EI column in the zone table

---

## Section 3 — EI Vetting Line

```
EI Vetting (Weekday): X jobs ÷ Y.YY (v2 predicted JPJ) = Z.Z expected journeys | Post-acceptance TPs: N (A acc + B new ACCEPT) | → J.J to EI ✓/⚠ STATUS
```

### CRITICAL: correct EI calculation

**EI journeys = total expected journeys − (existing Acc + new ACCEPT only)**

- HOLD_EI TPs are NOT subtracted — they are supply competing for EI journeys, not pre-accepted TPs
- REMOVE_COVERED TPs are NOT subtracted — they are not recommended at all
- The script's own printed line includes HOLD_EI in the pre-accepted count, which under-counts EI journeys and can show a false ⚠ BELOW range. Always recompute manually.

**Example (2026-04-17):**
```
Script says:  76 pre-accepted (55 acc + 21 TotRec) → 95.8 − 76 = 19.8 ⚠  ← WRONG
Correct:      55 acc + 8 new ACCEPT = 63 → 95.8 − 63 = 32.8 ✓
```

EI floor: 25 weekdays, 20 Sundays.

---

## Section 4 — Vetted Recommendations Header line

State before the per-zone tables:

```
X ACCEPT | Y HOLD_EI | Z REMOVE_COVERED
```

---

## Section 5 — Per-zone TP tables

For every zone that has at least one rec (ACCEPT, HOLD_EI, or REMOVE_COVERED), produce a heading and ASCII table.

### Zone heading format

```
**ZONE NAME** — Gap=N, X ACCEPT, Y HOLD_EI (quota/balancing), Unfill=N
```
or for clean zones: `**ZONE NAME** — Gap=N, X ACCEPT`

### Table format

```
┌────────┬──────────┬─────┬──────┬────────┬────────┬─────┬─────────────────────────┐
│   ID   │ Username │ Men │ Type │ Rating │ Deallo │ VAT │         Status          │
├────────┼──────────┼─────┼──────┼────────┼────────┼─────┼─────────────────────────┤
│ NNNNNN │ USERNAME │ XM  │ Loc  │  N.NN  │  N.N%  │ No  │ ACCEPT                  │
│ NNNNNN │ USERNAME │ XM  │ Loc  │  N.NN  │  N.N%  │ No  │ HOLD_EI (quota)         │
│ NNNNNN │ USERNAME │ XM  │ Loc  │  N.NN  │  N.N%  │ No  │ HOLD_EI (EI balancing)  │
│ NNNNNN │ USERNAME │ XM  │ Loc  │  N.NN  │  N.N%  │ No  │ REMOVE_COVERED          │
└────────┴──────────┴─────┴──────┴────────┴────────┴─────┴─────────────────────────┘
```

Add a Note column if needed (e.g. dedup reason, coverage reason).

### Status label reference

| Status | When to use |
|--------|-------------|
| `ACCEPT` | Pre-accepted, no flags |
| `ACCEPT ⚠ high deallo` | Deallo > 20% |
| `ACCEPT ★ new TP` | Rating == 6.0 (brand new TP) |
| `HOLD_EI (quota)` | Held by Layer 0.5 zone EI quota |
| `HOLD_EI (EI balancing)` | Held by Layer 1 EI balance check |
| `REMOVE_COVERED` | Removed by Layer 0 — accepted TPs already cover zone demand |
| `REMOVE_DEDUP` | Removed by dedup layer — duplicate TP, replacement found |

Include ALL TPs in the table (ACCEPT, HOLD_EI, and REMOVE_COVERED), ordered by rank.
Add a brief italics note below the table explaining REMOVE_COVERED if it fired.

---

## Section 6 — Flags

Numbered list in priority order:

1. EI status — ✓ within range or ⚠ below/above; explain if it's a D-2/D-3 artefact or real concern
2. Unfilled slots — per zone with root cause (pool exhaustion / REMOVE_COVERED / bucket mismatch)
3. High deallo (> 20%) TPs that were ACCEPTED — name, zone, rate
4. Very high deallo (> 35%) — call out explicitly regardless of status
5. Low rating (< 4.5) TPs that were ACCEPTED
6. Specific zone issues: London 1M shortfall, Manchester bucket mismatch, persistent dedup (TAONASHE etc.)
7. Other: new TPs (★), VAT concerns, capacity concerns, EI ordering artefacts (good TP held, weaker TP accepted)

For EI below range at D-2: check whether held TPs are in shortage buckets — if so, recommend manual accept since the hold is a D-2 artefact.

---

## Section 7 — Vetting Summary Table

```
┌──────────────────────┬──────────────────────────────────────────────┬───────────────────────────────────────────┐
│ Zone                 │ Concern                                      │ Action                                    │
├──────────────────────┼──────────────────────────────────────────────┼───────────────────────────────────────────┤
│ zone name            │ Description of issue                         │ Recommended action                        │
└──────────────────────┴──────────────────────────────────────────────┴───────────────────────────────────────────┘
```

One row per zone with an issue. Zones that are fully covered with no flags can be omitted.

---

## Section 8 — Totals footer

```
Totals after this run: N already accepted + N new ACCEPT = N total pre-accepted | N HOLD_EI | N REMOVE_COVERED | N unfilled slots (list zones)
```

For forecast runs, add:
`Re-run recommended on YYYY-MM-DD after 09:00 BST when the tool switches to actuals mode.`

---

## Common mistakes to avoid

1. **Wrong EI number** — do not use the script's printed EI line directly; recompute as `journeys − (existing_acc + new_ACCEPT_only)`
2. **EI column in zone table** — do not add an EI hold count column; it is not part of the zone table
3. **Plain markdown tables** — zone table and per-zone TP tables must use ASCII box drawing (┌─┬─┐)
4. **Missing REMOVE_COVERED rows** — always include them in per-zone tables with an explanation
5. **Wrong Unfill** — Unfill = max(0, Gap − TotRec), where TotRec includes REMOVE_COVERED. If REMOVE_COVERED fired, Unfill=0 because coverage is sufficient.
6. **Missing totals footer** — always end with Section 8
7. **HOLD_EI label** — always specify `(quota)` or `(EI balancing)`, never plain `HOLD_EI`
