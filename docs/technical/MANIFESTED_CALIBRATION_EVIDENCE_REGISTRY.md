# Manifested Calibration Evidence Registry

Date: 2026-05-07

This registry records the fresh manifested calibration export for supervisor review.
The generated `data/` artifacts remain local evidence files; this document tracks only
the concise values needed to audit the claim.

## Claim Boundary

| Evidence lane | Status | Claim |
|---|---:|---|
| Week 3 accepted evidence | Verified | Dnipro 30-anchor real-data benchmark, observed OREE DAM plus historical Open-Meteo, thesis-grade provenance. |
| Dnipro calibration preview | Verified here | Dnipro 90-anchor calibration and selector evidence for the next demo path. |
| Full DFL | Not claimed | The export explicitly sets `not_full_dfl=true`. |
| Market execution | Not claimed | The export explicitly sets `not_market_execution=true`; rows are offline benchmark and selector diagnostics. |

## Export Run

```powershell
.\.venv\Scripts\python.exe scripts\materialize_research_layer_from_store.py `
  --run-slug week3_calibration_preview_manifested_dnipro_90 `
  --calibration-min-prior-anchors 14 `
  --calibration-window-anchors 28
```

| Field | Value |
|---|---|
| Export slug | `week3_calibration_preview_manifested_dnipro_90` |
| Export directory | `data/research_runs/week3_calibration_preview_manifested_dnipro_90` |
| Manifest path | `data/research_runs/week3_calibration_preview_manifested_dnipro_90/research_layer_manifest.json` |
| Manifest generated at | `2026-05-07T01:22:48.951729+00:00` |

Export row counts:

| Artifact | Rows |
|---|---:|
| Raw benchmark summaries | 546 |
| Value-aware ensemble | 182 |
| DFL training frame | 728 |
| Relaxed-LP pilot rows | 1 |
| Regret-weighted calibration | 364 |
| Regret-weighted benchmark | 910 |
| Horizon-regret-weighted calibration | 364 |
| Horizon-regret-weighted benchmark | 910 |
| Calibrated ensemble | 182 |
| Forecast-dispatch sensitivity | 910 |
| Risk-adjusted value gate | 182 |

## Manifest Validation

| Check | Expected | Observed |
|---|---|---|
| `claim_scope` | `calibration_selector_evidence_not_full_dfl` | `calibration_selector_evidence_not_full_dfl` |
| `not_full_dfl` | `true` | `true` |
| `not_market_execution` | `true` | `true` |
| `data_quality_tiers` | `["thesis_grade"]` | `["thesis_grade"]` |
| TSFM leakage source link | Present | `https://huggingface.co/papers/2510.13654` |

Dnipro tenant-specific manifest evidence:

| Strategy kind | Latest generated at | Anchors | Rows | Registry use |
|---|---|---:|---:|---|
| `real_data_rolling_origin_benchmark` | `2026-05-06T22:57:36.014876+00:00` | 90 | 270 | Raw forecast candidates: `strict_similar_day`, `tft_silver_v0`, `nbeatsx_silver_v0`. |
| `regret_weighted_forecast_calibration_benchmark` | `2026-05-06T22:57:36.014876` | 90 | 450 | Calibration evidence, not full DFL. |
| `horizon_regret_weighted_forecast_calibration_benchmark` | `2026-05-06T22:57:36.014876` | 90 | 450 | Horizon-aware calibration evidence, not full DFL. |
| `calibrated_value_aware_ensemble_gate` | `2026-05-06T22:57:36.014876` | 90 | 90 | Selector evidence only. |
| `risk_adjusted_value_gate` | `2026-05-06T22:57:36.014876` | 90 | 90 | Risk selector diagnostics only. |
| `value_aware_ensemble_gate` | `2026-05-06T22:57:36.014876+00:00` | 90 | 90 | Comparator selector context. |

The manifest covers strategy evaluation batches. Forecast-dispatch sensitivity is
validated through the API/read model below because it is a diagnostic export rather
than a `strategy_kind` batch in the manifest.

## API Validation

Commands:

```powershell
Invoke-RestMethod "http://127.0.0.1:8000/dashboard/real-data-benchmark?tenant_id=client_003_dnipro_factory"
Invoke-RestMethod "http://127.0.0.1:8000/dashboard/calibrated-ensemble-benchmark?tenant_id=client_003_dnipro_factory"
Invoke-RestMethod "http://127.0.0.1:8000/dashboard/risk-adjusted-value-gate?tenant_id=client_003_dnipro_factory"
Invoke-RestMethod "http://127.0.0.1:8000/dashboard/forecast-dispatch-sensitivity?tenant_id=client_003_dnipro_factory"
```

| Endpoint | Generated at | Anchors | Models | Rows | Tier | Mean regret UAH | Best model / diagnostic |
|---|---|---:|---:|---:|---|---:|---|
| `/dashboard/real-data-benchmark` | `2026-05-06T22:57:36.014876Z` | 90 | 3 | 270 | `thesis_grade` | 1938.98 | `strict_similar_day` |
| `/dashboard/calibrated-ensemble-benchmark` | `2026-05-06T22:57:36.014876Z` | 90 | 1 | 90 | `thesis_grade` | 1479.65 | `calibrated_value_aware_ensemble_v0` |
| `/dashboard/risk-adjusted-value-gate` | `2026-05-06T22:57:36.014876Z` | 90 | 1 | 90 | `thesis_grade` | 1428.59 | `risk_adjusted_value_gate_v0` |
| `/dashboard/forecast-dispatch-sensitivity` | `2026-05-06T22:57:36.014876Z` | 90 | 5 | 450 | Diagnostic rows carry `thesis_grade` | n/a | Buckets: `forecast_error`, `low_regret`, `lp_dispatch_sensitivity`, `spread_objective_mismatch` |

The API read models agree with the manifest for the Dnipro latest batch:
90 anchors, thesis-grade provenance, and the same latest `generated_at`.

## Postgres Latest-Batch State

Command:

```powershell
docker compose exec -T postgres psql -U smart -d smart_arbitrage -c "
SELECT strategy_kind, tenant_id, COUNT(*) AS rows,
       COUNT(DISTINCT anchor_timestamp) AS anchors,
       MAX(generated_at) AS latest_generated_at
FROM forecast_strategy_evaluations
WHERE tenant_id = 'client_003_dnipro_factory'
GROUP BY strategy_kind, tenant_id
ORDER BY strategy_kind;"
```

| Strategy kind | Persisted rows | Distinct anchors | Latest generated at |
|---|---:|---:|---|
| `calibrated_value_aware_ensemble_gate` | 108 | 108 | `2026-05-06 22:57:36.014876+00` |
| `forecast_driven_lp` | 9 | 2 | `2026-05-06 13:40:34.224105+00` |
| `horizon_regret_weighted_forecast_calibration_benchmark` | 540 | 108 | `2026-05-06 22:57:36.014876+00` |
| `real_data_rolling_origin_benchmark` | 978 | 108 | `2026-05-06 22:57:36.014876+00` |
| `regret_weighted_forecast_calibration_benchmark` | 540 | 108 | `2026-05-06 22:57:36.014876+00` |
| `risk_adjusted_value_gate` | 108 | 108 | `2026-05-06 22:57:36.014876+00` |
| `value_aware_ensemble_gate` | 323 | 108 | `2026-05-06 22:57:36.014876+00` |

These Postgres totals include older persisted Dnipro batches. The supervisor-facing
calibration preview uses the latest Dnipro batch selected by `generated_at`, which is
why the manifest/API evidence reports 90 anchors and 270 raw benchmark rows while
the persisted table contains 108 distinct Dnipro anchors overall.

## Source Guardrails

| Source | Registry role |
|---|---|
| [TSFM leakage evaluation](https://huggingface.co/papers/2510.13654) | Guardrail for no-leakage, temporal evaluation, latest-batch reporting, and explicit source links before claims. |
| [PriceFM](https://huggingface.co/papers/2508.04875) | Future electricity-price foundation-model direction; not implemented in this run. |
| [THieF](https://huggingface.co/papers/2508.11372) | Future day-ahead temporal hierarchy forecasting direction; not implemented in this run. |

## Acceptance Decision

Accepted for supervisor evidence registry use:

- Fresh export folder exists with `research_layer_manifest.json`.
- Manifest flags are conservative and source-linked.
- Dnipro latest raw benchmark has `anchor_count=90`, `row_count=270`, and
  `data_quality_tiers=["thesis_grade"]`.
- API read models agree on Dnipro latest-batch freshness and anchor coverage.
- Generated `data/` artifacts remain local; this registry is the tracked report-ready artifact.
