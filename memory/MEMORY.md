# Memory Index

- [Full Picture output format](feedback_full_picture_format.md) — Always use this exact format (zone table, EI line, per-zone TP tables, flags, vetting summary) for all supply acceptor recommendation outputs, both actuals and forecast
- [Algorithm decisions & logic context](project_algorithm_decisions.md) — Shortfall fallback, post-EI-quota Layer 0.6 fallback, overflow credit interpretation, TotRec/NewAcc/Unfill definitions, actuals+forecast parity
- [Dedup shortfall rule](feedback_dedup_shortfall_rule.md) — Accept duplicate TP reservation when no replacement exists and zone has a gap; do not REMOVE_DEDUP in that case
- [EI definition & vetting calculation](project_ei_definition.md) — EI is journey-based not TP-based; HOLD_EI TPs are supply for EI; correct EI journey = total journeys − ACCEPT only; Layer 1 ei_current now computed after Layer 0.5 (fix applied 2026-04-13)
