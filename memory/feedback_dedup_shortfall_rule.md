---
name: Dedup shortfall rule
description: When dedup finds no replacement TP and the zone has a shortfall, accept the duplicate reservation instead of removing it
type: feedback
originSessionId: eb8a5cbd-13a6-4fed-9b55-58c73141c44d
---
Accept the duplicate when no replacement is available and the zone has unfilled slots. A TP can have multiple vehicles, so two reservations from the same TP is valid supply.

**Why:** User confirmed this explicitly. The current REMOVE_DEDUP behaviour wastes a real supply slot when the pool is exhausted — the duplicate TP reservation represents a genuine vehicle that can take a job.

**How to apply:**
- In the dedup logic (write_vetted_recommendations_csv, integrated_supply_acceptor_v2.py), before setting vetting_status = 'REMOVE_DEDUP' on a "no replacement found" slot, check if the zone still has a gap (Gap > currently ACCEPT count for that zone/date).
- If yes → set vetting_status = 'ACCEPT' with vetting_reason = "Dedup: kept duplicate to fill shortfall — no replacement available".
- If no (zone is already covered) → keep as REMOVE_DEDUP as normal.
- This applies to all zones and all men-types.
