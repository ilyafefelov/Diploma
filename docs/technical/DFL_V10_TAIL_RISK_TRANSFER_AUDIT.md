# DFL V10 Tail-Risk Transfer Audit

## Status

V10 is negative-but-useful evidence. It mines safe oracle-template schedules
from train/prior anchors, strict-scores the generated schedules, then audits
whether those templates transfer safely to later anchors.

Current frozen comparator remains calibrated Ukrainian-only V2+:

- mean regret: `174.77` UAH;
- median regret: `67.30` UAH;
- rolling robustness: `4 / 4`;
- `market_execution_enabled=false`.

## New Assets

| Asset | Purpose |
|---|---|
| `dfl_v10_tail_risk_transfer_audit_frame` | Classifies every generated V10 candidate as a safe transfer or one failure mode: `template_regime_mismatch`, `forecast_extrema_shift`, `soc_path_transfer_failure`, `throughput_tail_risk`, `missing_prior_context`, or `no_selector_safe_signal`. |
| `dfl_v10_learning_ceiling_decision_frame` | Summarizes whether V10 creates enough prior-safe, non-tail-risk teacher labels to justify DT/LAVA, more candidate generation, context backfill, or stopping model work in the current candidate space. |

The audit consumes only existing V10 candidate/strict-rescore/teacher rows. It
does not train a selector and does not create a market-execution path.

## Materialized Result

Dagster run id: `9e16fa67-566c-41c3-9de4-82a1dfb972a9`.

- transfer-audit rows: `1,204`;
- final-holdout generated V10 candidates: `126`;
- final-holdout non-tail-risk material safe switches: `0`;
- final-holdout tail-risk generated rows: `126`;
- final failure classes: `56` forecast-extrema shifts and `70` missing-prior-context rows;
- learning-ceiling decision: `stop_modeling_current_candidate_space`;
- recommended next branch: `thesis_ml_closure_and_data_acquisition`;
- `dt_lava_ready=false`;
- `market_execution_enabled=false`.

## Interpretation

The important question is no longer "can a larger selector memorize a few
winners?" The question is whether the candidate universe contains enough
prior-supported, non-tail-risk switch labels to learn before the validation
window starts.

This follows the storage DFL literature direction: BESS models should be judged
by downstream value/regret, not only forecast error. It also matches the
Decision Transformer boundary: DT is an offline trajectory model and should not
be promoted without strong transferable teacher trajectories. See
`docs/thesis/sources/dfl-dt-traceability-2026-05-22.md` for the local source
traceability note.

## Gate

`dt_ready` is allowed only if:

- final generated candidates include non-tail-risk material safe switches;
- prior/train anchors contain enough material safe-switch examples;
- the non-tail-risk oracle upper bound can beat V2+ by the configured gate;
- all evidence keeps `market_execution_enabled=false`.

The materialized result emitted `stop_modeling_current_candidate_space`: final
generated safe switches are zero and the non-tail-risk oracle upper bound cannot
beat V2+. That means the next branch is thesis evidence closure plus Ukrainian
data/context acquisition or a lower-tail-risk candidate family, not another
selector or DT variant over the same labels.

## Materialization

```powershell
docker compose exec -T dagster-webserver uv run dagster asset materialize `
  -m smart_arbitrage.defs `
  --select dfl_v10_tail_risk_transfer_audit_frame,dfl_v10_learning_ceiling_decision_frame `
  -c configs/real_data_dfl_ua_context_candidate_v8_week3.yaml
```

Claim boundary:

- Offline Strategy Promotion evidence only;
- not full DFL;
- not deployed Decision Transformer;
- no dashboard/API default switch;
- no live dispatch;
- `market_execution_enabled=false`.
