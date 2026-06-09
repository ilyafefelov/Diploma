# UA Context Acquisition Source Capture - 2026-05-23

Purpose: record the source-backed evidence used to move UA Context Acquisition
v1 from blocked readiness to `context_backfill_ready`.

## Source Evidence

| Source | Evidence Used | Repo Usage |
|---|---|---|
| OREE DAM/IDM trading procedure, `https://www.oree.com.ua/index.php/web/13245784` | Section 3.7 states that OREE publishes DAM trading results for each trading zone and settlement period no later than 14:00 Kyiv time on the day before delivery, with delayed gate-closure handling described separately. | `dfl_ua_dam_publication_backfill_frame` uses this as source-backed publication-deadline evidence when row-level publication timestamps are absent. |
| Energy Map DAM indexes, `https://energy-map.info/en/datasets/c6218b35-ce7e-45c2-925e-5c8e6f5eb9fb` | Public DAM index and weighted-average price dataset suitable for source traceability checks around Ukrainian DAM history. | Candidate source for cross-checking OREE DAM history coverage; not a substitute for row-level OREE publication receipts. |
| Open-Meteo historical archive, `https://open-meteo.com/en/docs/historical-weather-api` | Public historical weather API covering archive weather variables by timestamp and location. | `tenant_historical_weather_bronze` and V13 source evidence treat weather/load/PV context as source-backed proxy context, not measured tenant telemetry. |
| Ukrenergo public Telegram archive, `https://t.me/s/Ukrenergo` | Public operational posts include timestamps and event text. The updated bronze asset paginates the archive with `?before=<post_id>` until the configured archive start date or page limit. | `ukrenergo_grid_events_bronze` now materializes a historical observed archive; `dfl_ua_grid_event_backfill_frame` treats no-event rows as valid only when the source coverage window spans the anchor. |

## V13 Acquisition Notes

- Explicit DAM publication receipts are still stricter than the market-rule
  deadline. V13 therefore distinguishes `ready_prior_context` OREE timing from
  `partial_context_rule_deadline_without_row_receipts` for the row-level receipt
  target.
- Tenant load/PV is currently accepted only as measured telemetry if such rows
  exist or as a source-backed proxy if the weather/config/proxy provenance is
  present. It must not be described as measured live telemetry by default.
- Grid/outage context is usable only when the archive provides event or
  source-backed no-event coverage for the required anchor window.

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
