# UA Context Acquisition Source Capture - 2026-05-23

Purpose: record the source-backed evidence used to move UA Context Acquisition
v1 from blocked readiness to `context_backfill_ready`.

## Source Evidence

| Source | Evidence Used | Repo Usage |
|---|---|---|
| OREE DAM/IDM trading procedure, `https://www.oree.com.ua/index.php/web/13245784` | Section 3.7 states that OREE publishes DAM trading results for each trading zone and settlement period no later than 14:00 Kyiv time on the day before delivery, with delayed gate-closure handling described separately. | `dfl_ua_dam_publication_backfill_frame` uses this as source-backed publication-deadline evidence when row-level publication timestamps are absent. |
| Ukrenergo public Telegram archive, `https://t.me/s/Ukrenergo` | Public operational posts include timestamps and event text. The updated bronze asset paginates the archive with `?before=<post_id>` until the configured archive start date or page limit. | `ukrenergo_grid_events_bronze` now materializes a historical observed archive; `dfl_ua_grid_event_backfill_frame` treats no-event rows as valid only when the source coverage window spans the anchor. |

## Materialized Evidence

- OREE publication readiness: `860 / 860` rows ready through the source-backed
  market-rule deadline.
- Ukrenergo archive materialized posts: `943`.
- Ukrenergo archive coverage: `2024-12-24 16:54:15` UTC through the
  `2026-05-23` materialization timestamp.
- Grid-event readiness: `860 / 860` rows ready.
- Final V11 precondition packet:
  `data/research_runs/week3_dfl_ua_context_acquisition_v1/dfl_ua_context_backfill_readiness_summary.json`.
- Dagster run ids:
  - `e6c8c0d6-f04e-40d1-ba6f-93b2d7888179`: source-repair assets completed,
    but the full selection failed on a transient Open-Meteo `504`.
  - `593aff44-25f1-48a1-ab3b-02a22c20dae0`: downstream readiness gate
    completed using already materialized weather/load/PV rows.

## Claim Boundary

This source repair only unlocks the V11 candidate-generation precondition. It
does not train a selector, does not start DT/LAVA, does not switch dashboard/API
defaults, and does not enable market execution. Frozen calibrated Ukrainian-only
V2+ remains the thesis headline until a later V11/DFL candidate beats it under
the unchanged strict LP/oracle gate.
