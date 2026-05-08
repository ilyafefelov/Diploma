# DFL Strict-Failure Selector v1

Date: 2026-05-08

This slice follows the strict-challenger diagnostic. The candidate library now
contains non-strict feasible schedules that can beat the frozen
`strict_similar_day` control on many final-holdout anchors, so the next question
is selector learnability:

> Can a prior-only rule identify when to distrust `strict_similar_day`, without
> looking at final-holdout actuals?

The answer is evaluated with the same strict LP/oracle regret protocol. This is
research evidence only, not full DFL, not Decision Transformer control, and not
market execution.

## Assets

| Asset | Purpose |
|---|---|
| `dfl_strict_failure_selector_frame` | Learns a switch threshold from train-selection anchors only, per tenant and source model. |
| `dfl_strict_failure_selector_strict_lp_benchmark_frame` | Emits strict-control, raw-source, best-prior-non-strict, and selector rows for the final holdout. |
| `dfl_strict_failure_selector_evidence` | Dagster asset check for coverage, provenance, and claim flags. |

Config:
[real_data_dfl_strict_failure_selector_week3.yaml](../../configs/real_data_dfl_strict_failure_selector_week3.yaml).

## Selector Rule

For every tenant/source model/anchor:

1. Compute prior mean regret for `strict_similar_day` using only earlier
   `train_selection` anchors.
2. Compute prior mean regret for each non-strict candidate family/model using
   only earlier `train_selection` anchors.
3. Select the best prior non-strict candidate if:
   `strict_prior_mean_regret - best_non_strict_prior_mean_regret >= threshold`.
4. Choose `threshold` from a small grid using train-selection regret only.
5. Score the selected row on final holdout without changing the learned
   threshold.

Final-holdout actuals can change the final score, but they must not change the
selected threshold or the prior candidate metadata.

## Gate

Development evidence may pass when the selector improves over the raw neural
schedule. Production promotion remains blocked unless the selected strategy:

- covers five canonical tenants;
- covers at least 90 final-holdout tenant-anchors per source model;
- uses thesis-grade observed rows only;
- has zero safety violations;
- improves mean regret by at least 5% versus `strict_similar_day`;
- does not worsen median regret versus `strict_similar_day`.

## Expected Evidence

Before materialization, the expected run should report:

- 10 selector rows: five tenants x two source models;
- 180 selector final-holdout rows: five tenants x two source models x 18
  anchors;
- matching strict/raw/selector anchor coverage;
- `not_full_dfl=true`;
- `not_market_execution=true`.

## Latest Materialized Evidence

Run:

- Dagster run id: `568a8a8d-c210-44d0-9842-08300dfe0781`.
- Scope: downstream-only materialization from the checked 104-anchor upstream
  benchmark and strict-challenger schedule library.
- Asset check: `dfl_strict_failure_selector_evidence` passed.
- Selector frame: 10 rows, one per tenant/source model.
- Strict benchmark frame: 720 rows.
- Final-holdout selector coverage: 90 tenant-anchors per source model.
- Claim flags: `not_full_dfl=true`, `not_market_execution=true`.

Strict LP/oracle result:

| Model | Rows | Mean regret UAH | Median regret UAH | Decision |
|---|---:|---:|---:|---|
| `strict_similar_day` | 180 | 314.81 | 202.61 | Frozen Level 1 control. |
| `nbeatsx_silver_v0` | 90 | 813.40 | 520.48 | Raw neural comparator. |
| `dfl_strict_failure_selector_v1_nbeatsx_silver_v0` | 90 | 299.73 | 182.76 | Improves 63.15% vs raw and 4.79% vs strict; below the 5% strict threshold. |
| `tft_silver_v0` | 90 | 1003.54 | 477.99 | Raw neural comparator. |
| `dfl_strict_failure_selector_v1_tft_silver_v0` | 90 | 267.79 | 149.01 | Improves 73.32% vs raw and 14.94% vs strict; passes the per-source strict threshold. |

Selected non-strict reference rows used by the selector:

| Reference candidate | Rows | Mean regret UAH | Median regret UAH |
|---|---:|---:|---:|
| `dfl_schedule_library_v2_blend_raw_0p25_nbeatsx_silver_v0` | 90 | 291.78 | 162.96 |
| `dfl_schedule_library_v2_blend_raw_0p25_tft_silver_v0` | 36 | 198.84 | 79.37 |
| `dfl_schedule_library_v2_blend_raw_0p50_tft_silver_v0` | 54 | 260.11 | 120.85 |

Gate interpretation:

- Development gate: passed, because both selector variants materially improve
  over their raw neural schedules.
- Per-source production evidence: TFT-source selector passes the strict
  `strict_similar_day` gate; NBEATSx-source selector misses the 5% threshold by
  a narrow margin.
- Overall report claim: conservative diagnostic evidence, not promoted control.
  The multi-source gate remains labeled `diagnostic_pass_production_blocked`
  until the promotion policy explicitly accepts a per-source candidate.

## Interpretation

If this selector loses to `strict_similar_day`, that is not a code bug by
itself. It means the available prior features are not sufficient to identify the
anchors where non-strict schedules help. The next work would then be richer
prior features, longer Ukrainian observed coverage, or a more direct
trajectory/value learner, while keeping the same strict LP/oracle gate.

The current result is stronger than that failure case: one source-specific
selector beats the frozen control under strict LP/oracle scoring. The next work
should therefore refine promotion semantics and stress-test this result on a
longer Ukrainian backfill before any production-style claim.

## Materialization

```powershell
docker compose exec -T dagster-webserver uv run dagster asset materialize -m smart_arbitrage.defs --select dfl_strict_failure_selector_frame,dfl_strict_failure_selector_strict_lp_benchmark_frame -c configs/real_data_dfl_strict_failure_selector_week3.yaml
```

If upstream assets are stale, include the strict-challenger upstream chain:

```powershell
docker compose exec -T dagster-webserver uv run dagster asset materialize -m smart_arbitrage.defs --select dfl_schedule_candidate_library_v2_frame,dfl_non_strict_oracle_upper_bound_frame,dfl_strict_baseline_autopsy_frame,dfl_strict_failure_selector_frame,dfl_strict_failure_selector_strict_lp_benchmark_frame -c configs/real_data_dfl_strict_failure_selector_week3.yaml
```
