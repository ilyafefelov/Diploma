# DFL Schedule/Value Production Gate

Date: 2026-05-11

This slice converts Schedule/Value Learner V2 robustness evidence into an
explicit offline promotion/fallback decision. It is intentionally narrower than
live production control: `production_promote=true` here means the source-specific
schedule/value challenger may be used as offline/read-model strategy evidence,
while `strict_similar_day` remains the deterministic fallback and market
execution remains disabled.

Claim boundary: offline strategy evidence only. This is not live trading, not a
market-execution policy, not full Decision-Focused Learning, and not a deployed
Decision Transformer controller.

## Assets

| Artifact | Purpose |
|---|---|
| `smart_arbitrage.dfl.schedule_value_promotion_gate` | Pure helper that combines latest strict LP/oracle evidence with rolling robustness evidence. |
| `dfl_schedule_value_production_gate_frame` | One row per source model with promotion decision, blocker, fallback, allowed challenger, latest-holdout metrics, rolling pass counts, and claim flags. |
| `dfl_schedule_value_production_gate_evidence` | Dagster asset check requiring valid claim boundaries, disabled market execution, and internally consistent promotion decisions. |
| [real_data_dfl_schedule_value_production_gate_week3.yaml](../../configs/real_data_dfl_schedule_value_production_gate_week3.yaml) | Tracked config for the Schedule/Value Learner V2 offline promotion gate. |

The gate consumes:

- `dfl_schedule_value_learner_v2_strict_lp_benchmark_frame`;
- `dfl_schedule_value_learner_v2_robustness_frame`.

## Promotion Semantics

| Gate field | Required condition |
|---|---|
| `latest_source_signal` | Latest holdout improves mean regret by at least 5% versus `strict_similar_day` and median regret is not worse. |
| `rolling_strict_pass_window_count` | At least 3 of 4 rolling validation windows pass strict-control comparison for the same source model. |
| `latest_validation_tenant_anchor_count` | At least 90 validation tenant-anchors per source model. |
| `tenant_count` | Five canonical tenants. |
| `production_promote` | `true` only when latest, rolling, coverage, safety, leakage, and claim-boundary checks all pass. |
| `fallback_strategy` | Always `strict_similar_day_default_fallback`. |
| `market_execution_enabled` | Always `false` in this slice. |

Explicit blockers include `evidence_invalid`, `anchor_coverage_mismatch`,
`tenant_undercoverage`, `validation_undercoverage`, `median_degraded`,
`mean_improvement_below_threshold`, `rolling_undercoverage`,
`rolling_not_robust`, `latest_window_not_robust`,
`robust_challenger_missing`, and `none`.

## Materialization

```powershell
docker compose exec -T dagster-webserver uv run dagster asset materialize -m smart_arbitrage.defs --select dfl_schedule_candidate_library_v2_frame,dfl_schedule_value_learner_v2_frame,dfl_schedule_value_learner_v2_strict_lp_benchmark_frame,dfl_schedule_value_learner_v2_robustness_frame,dfl_schedule_value_production_gate_frame -c configs/real_data_dfl_schedule_value_production_gate_week3.yaml
```

## Materialized Evidence

| Field | Value |
|---|---|
| Dagster run id | `93d0f01c-5140-4958-a64f-74067144df4f` |
| Run status | `SUCCESS` |
| New asset | `dfl_schedule_value_production_gate_frame` materialized |
| New asset check | `dfl_schedule_value_production_gate_evidence` passed |
| Gate rows | 2 source-model rows |
| Production promotions | 2 offline/read-model promotions |
| Market execution | `false` for every row |
| Local registry export | `data/research_runs/week3_dfl_schedule_value_production_gate/` |

Gate result:

| Source model | Latest tenant-anchors | Strict mean | Learner mean | Mean improvement vs strict | Strict median | Learner median | Rolling strict passes | Promotion blocker | Production promote | Allowed challenger |
|---|---:|---:|---:|---:|---:|---:|---:|---|---|---|
| `nbeatsx_silver_v0` | 90 | 314.813 | 258.227 | 17.97% | 202.606 | 132.616 | 4 / 4 | `none` | true | `dfl_schedule_value_learner_v2_nbeatsx_silver_v0` |
| `tft_silver_v0` | 90 | 314.813 | 248.488 | 21.07% | 202.606 | 89.891 | 3 / 4 | `none` | true | `dfl_schedule_value_learner_v2_tft_silver_v0` |

## Decision

Both source-specific Schedule/Value Learner V2 challengers pass the offline
promotion gate for the current accepted 104-anchor Ukrainian panel scope.

This is the strongest DFL evidence in the project so far, but the claim remains
bounded:

- promoted only for offline/read-model strategy evidence;
- no live market execution;
- no dashboard/API default controller change;
- `strict_similar_day` remains the fallback for undercovered,
  out-of-distribution, failed-source, or future market-execution contexts;
- the result is still not a deployed Decision Transformer and not a full
  differentiable end-to-end DFL controller.

The next responsible step is to export a concise promotion registry and decide
whether to expose this offline promotion state in read models. Any read-model or
dashboard change should remain opt-in and continue showing the claim boundary.

## Registry Export

The local ignored registry was generated with:

```powershell
$slug='week3_dfl_schedule_value_production_gate'
$exportDir=Join-Path 'data\research_runs' $slug
New-Item -ItemType Directory -Force -Path $exportDir | Out-Null
$cid=(docker compose ps -q dagster-webserver)
docker cp "${cid}:/opt/dagster/dagster_home/storage/dfl_schedule_value_production_gate_frame" (Join-Path $exportDir 'dfl_schedule_value_production_gate_frame.pkl')
.\.venv\Scripts\python.exe scripts\materialize_schedule_value_production_gate_registry.py --run-slug $slug --gate-frame-pickle (Join-Path $exportDir 'dfl_schedule_value_production_gate_frame.pkl') --dagster-run-id 93d0f01c-5140-4958-a64f-74067144df4f
```

Generated files:

- `data/research_runs/week3_dfl_schedule_value_production_gate/dfl_schedule_value_production_gate_registry.json`;
- `data/research_runs/week3_dfl_schedule_value_production_gate/dfl_schedule_value_production_gate_registry.md`;
- `data/research_runs/week3_dfl_schedule_value_production_gate/dfl_schedule_value_production_gate_frame.pkl`.

The generated `data/` artifacts remain local and ignored. This tracked document
copies the concise report-ready values needed for supervisor review.
