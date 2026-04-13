---
name: EI (Express Interest) — correct definition and Layer 1 fix
description: What EI journeys, HOLD_EI TPs, and the EI vetting line actually mean; and the Layer 1 EI balance logic fix
type: project
originSessionId: eb8a5cbd-13a6-4fed-9b55-58c73141c44d
---
EI = Express Interest. It is a journey-matching system, not a TP-holding concept.

**How it works:**
- Journeys (moves) that are NOT pre-assigned to pre-accepted TPs go to the EI pool
- Any available TP in the market (not just reserved TPs) can express interest on EI journeys
- A matcher then allocates journeys to expressions of interest

**HOLD_EI TPs:**
- TPs with reservations that we deliberately hold back from pre-acceptance
- They are freed up to express interest on EI journeys alongside the broader open market
- They do NOT consume EI journeys — they are supply competing for EI journeys

**EI journey count (correct calculation):**
- EI journeys = total expected journeys − pre-accepted TPs (ACCEPT only, not HOLD_EI)
- Example: 450 jobs ÷ 5.41 JPJ = 83.2 journeys. Pre-accepted = 45 existing + 10 new ACCEPT = 55. EI journeys = 83.2 − 55 = 28.2 ✓
- HOLD_EI TPs should NOT be subtracted from total journeys — they are supply, not demand reduction

**Script's "Post-acceptance TPs" figure is misleading:**
- The script prints Post-acceptance TPs = existing Acc + TotRec (ACCEPT + HOLD_EI)
- This under-counts EI journeys and may show a false ⚠ BELOW range flag
- Always verify using the correct formula: total journeys − (existing Acc + new ACCEPT only)
- EI floor: 25 weekdays, 20 Sundays

---

## Layer 1 EI Balance — fixed 2026-04-13

**The bug:** `ei_current` (the trigger for Layer 1 EI balance holds) was computed BEFORE Layer 0.5 quota holds were applied. It counted all new recs as pre-accepted, under-estimated EI journeys, and caused Layer 1 to over-hold TPs unnecessarily.

**Example of the bug (2026-04-14 run before fix):**
- 20 new recs, 45 existing. ei_current = 83.2 − (45+20) = 18.2 < 25 → needed=7
- But Layer 0.5 was about to hold 10 TPs for quota, leaving only 10 new ACCEPT
- Real ei_current after quota = 83.2 − (45+10) = 28.2 ✓ — Layer 1 should NOT have fired
- Result: 7 unnecessary EI balance holds, including shortage-bucket TPs (e.g. Newcastle NABOSH)

**The fix (integrated_supply_acceptor_v2.py):**
- Removed the pre-Layer-0.5 `ei_current` calculation
- After Layer 0.5 runs, recompute `ei_current` using the actual post-quota ACCEPT count:
  ```python
  post_quota_accept = sum(
      1 for i in day_recs_idx
      if recs.at[i, 'vetting_status'] == 'ACCEPT'
  )
  ei_current = total_jobs / ei_jpj - (already_acc + post_quota_accept)
  ```
- Layer 1 now only fires if EI journeys are genuinely short AFTER quota holds

**Effect of fix:**
- Layer 1 EI balance holds = 0 on 2026-04-14 (was 7)
- Total HOLD_EI reduced from 13 → 10 (all quota holds now)
- Brighton, Newcastle, Salisbury correctly pre-accepted instead of held
