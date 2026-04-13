---
name: Supply Acceptor V2 — algorithmic decisions and logic context
description: Key business decisions and logic context behind the V2 algorithm that are not obvious from reading the code
type: project
---

## Shortfall fallback (supply_acceptor_v2.py)

If a zone has unfilled slots after the primary selection pass (all rated ≥4.4 TPs exhausted), a second pass runs using TPs that failed only the rating filter (< 4.4) but still meet minimum capacity (≥8 m³). These are accepted and flagged `rating_fallback=True` / `★ shortfall fallback` in output.

**Why:** Better to accept a slightly lower-rated TP than leave a slot genuinely empty. The minimum capacity filter is the hard floor; rating is softened only when no other option exists.

**How to apply:** Flag these clearly in the Full Picture output. Always call out the TP name, zone, and rating so the user can make a manual call if the rating is very low (e.g. 4.0 vs 4.3 are different risks).

---

## Post-EI-quota gap fallback — Layer 0.6 (integrated_supply_acceptor_v2.py)

EI reservation quotas (Layer 0.5) unconditionally hold TPs for EI. If a zone's new recs are entirely held for EI quota (e.g. Sheffield quota=2, only 2 new recs → both held), the supply gap is not filled pre-acceptance. Layer 0.6 runs after quotas are applied and tries to pull additional TPs from the pending pool to fill the remaining ACCEPT gap.

**Why:** The Unfilled Gap metric should reflect post-quota truth (gap − ACCEPT_count), not pre-quota. HOLD_EI TPs are reserved for on-day EI pickup and don't count as pre-accepted supply. Without Layer 0.6, London and Birmingham would regularly show zero additional pre-accepts even though EI only holds a fraction of the gap.

**How to apply:** Zone summary Unfilled Gap is rewritten after Layer 0.6 to reflect true shortfall. TotRec column in the zone table shows all recs (ACCEPT + HOLD_EI); NewAcc shows post-vetting ACCEPT only.

---

## Overflow credits (ZONE_OVERFLOW_TARGETS in legacy/supply_acceptor.py)

When a zone over-accepts TPs relative to its target (excess), that excess is donated as a credit to receiving zones defined in ZONE_OVERFLOW_TARGETS. Credits reduce the receiving zone's gap.

**Example confirmed correct (2026-04-08):** Manchester had 1 excess 2M TP. Sheffield is a Manchester overflow target. Sheffield's G2M was reduced from 1 → 0. Sheffield's gap became G1M=2 only (not G1M=2, G2M=1). The user confirmed this is correct — Manchester's excess covers Sheffield's 2M demand, so Sheffield should not be pre-accepting additional 2M TPs.

**How to apply:** When Sheffield (or any receiving zone) shows OvflCrd > 0 alongside Unfill > 0, the unfilled slots reflect EI-held TPs or genuine 1M shortfalls — NOT a 2M coverage gap. Note this in the flags section to avoid confusion.

---

## EI quota + Unfilled Gap interpretation

- `TotRec` = total new recommendations before vetting (ACCEPT + HOLD_EI)
- `NewAcc` = post-vetting ACCEPT count only (excludes HOLD_EI)
- `Unfill` = max(0, Gap − NewAcc) — true supply shortfall after EI holds

Sheffield Unfill=2 with OvflCrd=1 means: 2 TPs are in EI queue (will be accepted on the day), and the 2M gap is covered by Manchester overflow. Total effective supply = already_accepted + EI-held + overflow = target.

---

## All changes apply in both actuals and forecast mode

The shortfall fallback, Layer 0.6 post-EI fallback, and Unfilled Gap correction all live in code paths shared by both modes. No forecast-specific handling needed.
