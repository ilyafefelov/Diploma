# Evidence Number Audit, 2026-05-13

This note verifies the supervisor-deck numbers against local evidence exports.

## Storage Checked

- `data/research_runs/`
- `.tmp_runtime/official_global_panel_batches/`
- `.tmp_runtime/official_evidence/`
- technical documentation grep checks for AFL/selector figures

Docker Desktop processes were present, but the Docker CLI could not reach either
`npipe:////./pipe/dockerDesktopLinuxEngine` or `npipe:////./pipe/docker_engine`,
and `localhost:5432` was not accepting Postgres connections from this shell.
Therefore, this audit verifies local exported evidence packets, not live
Postgres rows.

## Current Headline Evidence: 365-Anchor Official Global Panel

Source:
`data/research_runs/week3_official_global_panel_365_strategy_promotion/dfl_schedule_value_production_gate_registry.json`

Attempt identity:

- run slug: `week3_official_global_panel_365_strategy_promotion`
- Dagster run id: `official-global-panel-2026-05-11T203000-0000`
- generated/resume timestamp: `2026-05-11T20:30:00+00:00`
- materialization command: `scripts/run-official-global-panel-batches.ps1 -TotalAnchors 365 -BatchSize 20 -GeneratedAtIso 2026-05-11T20:30:00+00:00`
- attempt status from monitor snapshot: `complete`
- planned anchors: `365`
- effective persisted anchors: `365`

Promotion-gate result:

| Source model | Latest validation tenant-anchors | Strict mean/median UAH | Selected mean/median UAH | Improvement vs strict | Rolling strict passes | Offline promotion | Market execution |
|---|---:|---:|---:|---:|---:|---|---|
| `nbeatsx_official_global_panel_horizon_calibrated_v1` | 90 | 310.583 / 198.386 | 206.367 / 96.021 | 33.55% | 4 / 4 | true | false |
| `nbeatsx_official_global_panel_v1` | 90 | 310.583 / 198.386 | 225.437 / 109.692 | 27.41% | 4 / 4 | true | false |

Deck wording should keep this as **Offline Strategy Promotion**, not live market
execution.

## Previous 104-Anchor Compact Schedule/Value Gate

Source:
`data/research_runs/week3_dfl_schedule_value_production_gate/dfl_schedule_value_production_gate_registry.json`

| Source model | Latest validation tenant-anchors | Strict mean/median UAH | Selected mean/median UAH | Improvement vs strict | Rolling strict passes | Offline promotion | Market execution |
|---|---:|---:|---:|---:|---:|---|---|
| `nbeatsx_silver_v0` | 90 | 314.813 / 202.606 | 258.227 / 132.616 | 17.97% | 4 / 4 | true | false |
| `tft_silver_v0` | 90 | 314.813 / 202.606 | 248.488 / 89.891 | 21.07% | 3 / 4 | true | false |

This is useful supporting evidence, but the 365-anchor official global-panel
packet is the stronger current headline.

## Pre-365 Production Gate Blocker

Source:
`data/research_runs/week3_dfl_production_promotion_gate/production_promotion_gate_summary.json`

- asset check passed: `false`
- coverage rows: `5`
- gate rows: `5`
- target anchor count per tenant: `180`
- eligible anchor count: `104`
- missing price hours: `1`
- missing weather hours: `1`
- price observed coverage ratio: `0.9996527777777777`
- weather observed coverage ratio: `0.9996527777777777`
- promotion blocker: `evidence_invalid`

This older blocked result should not be used as the current headline after the
365-anchor official evidence packet.

## Dnipro 90 DFL Vector / Calibration Evidence

Source of truth:
`data/research_runs/week3_dfl_vector_evidence_dnipro_90/dfl_vector_evidence_registry.json`

Vector readiness:

- tenant: `client_003_dnipro_factory`
- anchor count: `90`
- training example rows: `270`
- forecast model count: `3`
- data quality: `thesis_grade`
- observed coverage min: `1.0`
- vector lengths: `24`
- result: pass

Promotion gate:

| Candidate | Mean/median regret UAH | Strict mean/median UAH | Improvement vs strict | Decision |
|---|---:|---:|---:|---|
| `tft_silver_v0` | 2361.957 / 1985.182 | 1384.699 / 999.198 | -70.58% | block |
| `nbeatsx_silver_v0` | 2070.283 / 1805.146 | 1384.699 / 999.198 | -49.51% | block |
| `tft_horizon_regret_weighted_calibrated_v0` | 1727.288 / 1196.848 | 1384.699 / 999.198 | -24.74% | block |
| `nbeatsx_horizon_regret_weighted_calibrated_v0` | 1804.376 / 1471.521 | 1384.699 / 999.198 | -30.31% | block |

Important caveat: several older folders whose names include `dnipro_90`
currently contain CSV summaries with `tenant_count=5` and `anchor_count=104`.
For the Dnipro 90 thesis-grade vector/promotion numbers, use the registry above.

## 104-Anchor Calibration CSVs

Sources include:

- `data/research_runs/week3_calibration_preview_dnipro_90/research_layer_model_summary.csv`
- `data/research_runs/week4_calibration_dnipro_90/research_layer_model_summary.csv`

These CSVs currently report the same 5-tenant / 104-anchor summary:

| Candidate | Rows | Tenants | Anchors | Mean/median regret UAH |
|---|---:|---:|---:|---:|
| `strict_similar_day` | 182 | 5 | 104 | 872.607 / 483.424 |
| `value_aware_ensemble_v0` | 182 | 5 | 104 | 897.640 / 474.683 |
| `tft_silver_v0` | 182 | 5 | 104 | 1527.298 / 941.311 |
| `nbeatsx_silver_v0` | 182 | 5 | 104 | 1581.125 / 1218.638 |

Horizon-calibrated rows in the same 104-anchor CSV family:

| Candidate | Rows | Tenants | Anchors | Mean/median regret UAH |
|---|---:|---:|---:|---:|
| `strict_similar_day` | 182 | 5 | 104 | 872.607 / 483.424 |
| `tft_horizon_regret_weighted_calibrated_v0` | 182 | 5 | 104 | 1175.201 / 823.073 |
| `nbeatsx_horizon_regret_weighted_calibrated_v0` | 182 | 5 | 104 | 1446.936 / 1149.826 |

These are not the same as the Dnipro-only 90-anchor registry numbers.

## Static Classifier Failure Analysis

Source:
`data/research_runs/week3_dfl_classifier_failure_analysis/summary.json`

- asset check passed: `true`
- diagnostic rows expected: `20`
- tenants: `5`
- source models: `2`
- classifier variants: `2`
- conclusion: `blocked_static_action_classifier_not_decision_value_optimized`

Strict projection means:

| Strategy/model | Rows | Final anchors | Mean regret UAH |
|---|---:|---:|---:|
| `dfl_action_classifier_v0_tft_silver_v0` | 90 | 18 | 1157.40 |
| `dfl_action_classifier_v0_nbeatsx_silver_v0` | 90 | 18 | 1186.83 |
| `strict_similar_day` comparison rows | 180 | 18 | 314.81 |
| `dfl_value_aware_action_classifier_v1_tft_silver_v0` | 90 | 18 | 1198.74 |
| `dfl_value_aware_action_classifier_v1_nbeatsx_silver_v0` | 90 | 18 | 1498.95 |

## Source-Specific Research Challenger

Source:
`data/research_runs/week3_source_specific_research_challenger/source_specific_challenger_summary.json`

| Source model | Latest strict mean/median | Latest fallback mean/median | Latest improvement vs strict | Rolling strict passes | Gate label |
|---|---:|---:|---:|---:|---|
| `nbeatsx_silver_v0` | 314.813 / 202.606 | 318.367 / 172.493 | -1.13% | 0 / 4 | `rolling_development_only` |
| `tft_silver_v0` | 314.813 / 202.606 | 258.124 / 136.049 | 18.01% | 0 / 4 | `latest_signal_not_robust` |

This is the older result that motivated the robustness and schedule/value work.

## AFL / Feature-Aware Selector Figures

The exact AFL audit and feature-aware selector figures were found in technical
docs, not in a tracked `data/research_runs` JSON/CSV packet in this pass:

- `docs/technical/DFL_AFL_FORECAST_ERROR_AUDIT.md`
- `docs/technical/DFL_FEATURE_AWARE_STRICT_FAILURE_SELECTOR.md`

Documented figures:

- AFL panel rows covered: `1,560`
- mean LP-value failure rate: `80.23%`
- mean rank/extrema failure rate: `64.83%`
- mean spread-shape failure rate: `55.19%`

Feature-aware selector figures:

| Selector source | Strict mean | Raw mean | Selector mean/median | Improvement vs raw | Improvement vs strict |
|---|---:|---:|---:|---:|---:|
| `nbeatsx_silver_v0` | 314.81 | 813.40 | 299.73 / 182.76 | 63.15% | 4.79% |
| `tft_silver_v0` | 314.81 | 1003.54 | 299.19 / 160.52 | 70.19% | 4.96% |

If these need to be treated as primary evidence in the thesis package, the next
cleanup should export their source asset frames into `data/research_runs/` too.
