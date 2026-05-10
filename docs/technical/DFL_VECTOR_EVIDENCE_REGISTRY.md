# DFL Vector Evidence Registry

Date: 2026-05-07

This registry records the first end-to-end materialization of the sidecar
`dfl_training_example_frame` contract. It is evidence for DFL readiness and
candidate promotion gating only. It is not a full DFL result, not market
execution, and not a deployed Decision Transformer policy.

## Execution

Tracked code runner:

```powershell
.\.venv\Scripts\python.exe scripts\materialize_dfl_vector_evidence_registry.py `
  --run-slug week3_dfl_vector_evidence_dnipro_90
```

Local generated artifacts:

| Artifact | Path |
|---|---|
| Export directory | `data/research_runs/week3_dfl_vector_evidence_dnipro_90` |
| JSON registry | `data/research_runs/week3_dfl_vector_evidence_dnipro_90/dfl_vector_evidence_registry.json` |
| Markdown registry | `data/research_runs/week3_dfl_vector_evidence_dnipro_90/dfl_vector_evidence_registry.md` |

The generated `data/` artifacts remain local evidence outputs. The concise
review values are copied below for supervisor review.

## DFL Vector Evidence

Scope: `client_003_dnipro_factory`, observed OREE DAM prices and tenant
Open-Meteo weather, `2026-01-01` to `2026-04-30`, `max_anchors=90`.

| Check | Result |
|---|---:|
| Latest DFL vector batch | `2026-05-07T12:03:38.940252` |
| Training-example rows | 270 |
| Rolling-origin anchors | 90 |
| Forecast models | 3 |
| Data quality tier | `thesis_grade` |
| Minimum observed coverage | 1.0 |
| Claim scope | `dfl_training_examples_not_full_dfl` |
| `not_full_dfl` | true |
| `not_market_execution` | true |
| Vector lengths | 24 for price, dispatch, and degradation vectors |

Postgres validation for `dfl_training_example_vectors` matched the registry:
270 Dnipro rows, 90 anchors, 3 models, thesis-grade rows only, observed coverage
1.0, and all claim-boundary flags true.

## Promotion Gate Result

Overall decision: `no_candidate_promoted`.

The frozen control remains `strict_similar_day`. Each candidate was evaluated
against the same latest Dnipro batch, with at least 90 anchors and thesis-grade
provenance. All candidates failed the conservative gate because mean regret did
not improve by at least 5 percent and median regret was worse than the control.

| Strategy kind | Candidate | Mean regret UAH | Median regret UAH | Mean improvement vs strict | Gate |
|---|---|---:|---:|---:|---|
| `real_data_rolling_origin_benchmark` | `strict_similar_day` | 1384.70 | 999.20 | control | control |
| `real_data_rolling_origin_benchmark` | `tft_silver_v0` | 2361.96 | 1985.18 | -70.6% | block |
| `real_data_rolling_origin_benchmark` | `nbeatsx_silver_v0` | 2070.28 | 1805.15 | -49.5% | block |
| `horizon_regret_weighted_forecast_calibration_benchmark` | `tft_horizon_regret_weighted_calibrated_v0` | 1727.29 | 1196.85 | -24.7% | block |
| `horizon_regret_weighted_forecast_calibration_benchmark` | `nbeatsx_horizon_regret_weighted_calibrated_v0` | 1804.38 | 1471.52 | -30.3% | block |

This is the correct Week 3/DFL-foundation finding: the vector dataset now exists
and the promotion gate is enforceable, but no forecast/calibration candidate is
promoted over the frozen strict baseline.

## API And Postgres Agreement

Latest-batch Postgres summary for Dnipro:

| Strategy kind | Rows | Anchors | Models | Latest batch |
|---|---:|---:|---:|---|
| `real_data_rolling_origin_benchmark` | 270 | 90 | 3 | `2026-05-07T12:03:38.940252Z` |
| `regret_weighted_forecast_calibration_benchmark` | 450 | 90 | 5 | `2026-05-07T12:03:38.940252Z` |
| `horizon_regret_weighted_forecast_calibration_benchmark` | 450 | 90 | 5 | `2026-05-07T12:03:38.940252Z` |
| `calibrated_value_aware_ensemble_gate` | 90 | 90 | 1 | `2026-05-07T12:03:38.940252Z` |
| `risk_adjusted_value_gate` | 90 | 90 | 1 | `2026-05-07T12:03:38.940252Z` |
| `value_aware_ensemble_gate` | 90 | 90 | 1 | `2026-05-07T12:03:38.940252Z` |

API read-model validation:

| Endpoint | Anchors | Models | Rows | Tier / diagnostics |
|---|---:|---:|---:|---|
| `/dashboard/real-data-benchmark` | 90 | 3 | 270 | `thesis_grade` |
| `/dashboard/calibrated-ensemble-benchmark` | 90 | 1 | 90 | `thesis_grade` |
| `/dashboard/risk-adjusted-value-gate` | 90 | 1 | 90 | `thesis_grade`; 90 rows include forecast diagnostics and 89 include risk candidate scores |
| `/dashboard/forecast-dispatch-sensitivity` | 90 | 5 | 450 | 4 diagnostic buckets |

## Claim Boundary

- Implemented now: DFL vector dataset contract, persisted DFL vector rows,
  latest-batch registry export, and conservative promotion-gate validation.
- Supported by evidence: Dnipro 90-anchor research preview can feed future DFL
  experiments, but strict baseline still wins under the current gate.
- Not claimed: full DFL, live trading, market execution, production promotion,
  full digital twin, or deployed Decision Transformer control.

Research guardrails remain TSFM leakage evaluation for temporal/no-leakage
discipline and the DFL survey for decision-quality evaluation through the
optimizer rather than forecast-only MAE/RMSE.
