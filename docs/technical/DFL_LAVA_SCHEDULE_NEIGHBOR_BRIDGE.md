# DFL LAVA Schedule-Neighbor Bridge

This slice starts the DT/LAVA branch without jumping straight to a raw
hourly-action Decision Transformer. It builds a teacher-label and feasible
schedule-neighbor layer anchored to the current frozen Ukrainian-only V2+
baseline.

The frozen comparator remains:

| Metric | Frozen Ukrainian-only calibrated V2+ |
|---|---:|
| Mean regret | `174.77` UAH |
| Median regret | `67.30` UAH |
| Rolling robustness | `4 / 4` windows |
| Market execution | `false` |

Poland remains positive but not promoted. Its token-backed feature lane works,
and the latest holdout showed a useful TFT/Poland signal, but rolling evidence
and the simple Poland ranker were not safe enough to replace V2+.

## Why This Layer Exists

The previous Poland candidate-value ranker selected risky schedules too
aggressively:

| Row | Mean regret, UAH | Median regret, UAH | Interpretation |
|---|---:|---:|---|
| Frozen Ukrainian-only V2+ | `174.77` | `67.30` | current thesis comparator |
| Poland ranker, calibrated NBEATSx | `334.02` | `223.15` | negative evidence |
| Poland ranker, calibrated TFT | `445.75` | `358.19` | negative evidence |

That result says the next model should not imitate the same objective. The next
step is to create explicit teacher labels:

- `v2_plus_best`;
- `poland_safe_win`;
- `poland_tail_risk_loss`;
- `selector_overreach`;
- `oracle_only_train_diagnostic`.

These labels separate useful Poland/TFT opportunities from tail-risk rows and
from rows where the selector overreached. They are evidence for a future
schedule-neighbor or DT model, not a market-execution signal.

## Asset Interface

The implementation is additive and keeps existing V2+/Poland assets unchanged.

| Asset | Purpose |
|---|---|
| `dfl_v2_plus_schedule_neighbor_teacher_label_frame` | Classifies current V2+, Poland V2+, Poland veto, and Poland ranker evidence into teacher-label rows. |
| `dfl_lava_schedule_neighbor_candidate_frame` | Builds feasible schedule-neighbor candidates from V2+, strict fallback, Poland/TFT near-miss schedules, and train-only oracle-neighborhood diagnostics. |
| `dfl_lava_candidate_value_scorer_frame` | Trains a conservative prior-only candidate-level delta scorer and falls back to V2+ when prior evidence is weak. |
| `dfl_lava_candidate_value_strict_lp_benchmark_frame` | Strict-scores `strict_similar_day`, frozen V2+, behavior cloning, and the LAVA scorer under the unchanged LP/oracle evaluator. |
| `dfl_lava_tail_risk_diagnostic_frame` | Uses the failed bridge rows to identify perturbation families that create tail-risk regret. |
| `dfl_lava_tail_risk_aware_target_frame` | Converts the diagnostic into schedule-candidate-index targets and blocks risky families before any DT/LAVA training. |
| `dfl_lava_tail_risk_aware_strict_lp_benchmark_frame` | Strict-scores the redesigned target against frozen V2+ without raw hourly action imitation. |

Tracked config:

- [configs/real_data_dfl_lava_schedule_neighbor_week3.yaml](../../configs/real_data_dfl_lava_schedule_neighbor_week3.yaml)
- [configs/real_data_dfl_lava_tail_risk_target_week3.yaml](../../configs/real_data_dfl_lava_tail_risk_target_week3.yaml)

Core implementation:

- `src/smart_arbitrage/dfl/lava_schedule_neighbor_bridge.py`
- `src/smart_arbitrage/dfl/lava_tail_risk_target.py`

## No-Leakage Rules

- Final-holdout actuals can change labels and strict scores, but not prior
  selector features or selected weights.
- Oracle-neighborhood rows are train-only diagnostics and are never eligible
  for final-holdout selection.
- Poland data remains exogenous feature context only; no European rows become
  Ukrainian target rows.
- `market_execution_enabled=false` is fixed in benchmark rows.

## Gate

The LAVA scorer can become a stronger Offline Strategy Promotion challenger
only if it:

- beats frozen V2+ mean regret;
- does not worsen median regret versus V2+;
- still beats `strict_similar_day` by at least `5%`;
- preserves rolling robustness before headline replacement;
- emits zero market-execution claims.

If the gate fails, the output is still useful negative evidence: it identifies
whether the blocker is weak teacher labels, no safe Poland/TFT candidates, or
over-conservative fallback.

## First Materialized Result

Dagster run:

- `30742a14-2712-4640-9ec8-1aff155f52d1`

Persisted strategy kind:

- `dfl_lava_candidate_value_strict_lp_benchmark`

The bridge materialized successfully, but the scorer did not beat frozen V2+:

| Row | Tenant-anchor rows | Mean regret, UAH | Median regret, UAH | Status |
|---|---:|---:|---:|---|
| Frozen calibrated V2+ | `90` | `174.77` | `67.30` | headline comparator |
| Frozen raw V2+ | `90` | `193.36` | `68.89` | reference |
| `strict_similar_day` | `90` | `310.58` | `198.39` | control |
| Behavior-cloning reference | `90` | `310.58` | `198.39` | required baseline |
| LAVA candidate-value scorer | `90` | `501.25` | `221.77` | negative evidence |

The scorer mostly selected strict-control and strict/raw-blend schedules, but
also selected a small set of rank-extrema perturbation schedules that created
large tail losses. This confirms the main lesson from the Poland ranker: the
current schedule-neighbor feature space is useful for labels and diagnostics,
but not yet strong enough to replace V2+ safely.

## Tail-Risk Target Redesign

The follow-up target uses that negative bridge result directly. Instead of
training DT/LAVA to imitate raw hourly BUY/SELL/HOLD actions, the new target
asks a safer question:

```text
Which feasible schedule candidate or schedule family should be selected, and
when should the system fall back to frozen V2+?
```

The new diagnostic labels candidate rows as `safe_neighbor_candidate`,
`tail_risk_perturbation_loss`, `neutral_or_weak_neighbor`,
`oracle_only_train_diagnostic`, or `v2_plus_default`. The target then blocks
families with prior tail-risk losses and hard-blocks known risky perturbation
families such as `rank_extrema_perturbation_v2_plus`. It emits
`schedule_candidate_index` supervision for future DT/LAVA work. Final-holdout
realized prices may change the strict score, but they do not change the blocked
family list or target selection rules.

Technical spec:
[DFL_LAVA_TAIL_RISK_TARGET.md](DFL_LAVA_TAIL_RISK_TARGET.md).

First tail-risk target result: the redesigned strict benchmark materialized in
Dagster run `60f19630-3469-4d07-9576-14c62c356011`. It hard-blocked risky
perturbation families and fell back to calibrated V2+ for all tenants, matching
the frozen comparator at `174.77` UAH mean regret and `67.30` UAH median regret.
That is a safe diagnostic closure, not a promotion over V2+.

## Materialization

After upstream V2+ and Poland evidence rows are available:

```powershell
docker compose exec -T dagster-webserver uv run dagster asset materialize `
  -m smart_arbitrage.defs `
  --select dfl_v2_plus_schedule_neighbor_teacher_label_frame,dfl_lava_schedule_neighbor_candidate_frame,dfl_lava_candidate_value_scorer_frame,dfl_lava_candidate_value_strict_lp_benchmark_frame `
  -c configs/real_data_dfl_lava_schedule_neighbor_week3.yaml
```

Tail-risk target materialization:

```powershell
docker compose exec -T dagster-webserver uv run dagster asset materialize `
  -m smart_arbitrage.defs `
  --select dfl_v2_plus_schedule_neighbor_teacher_label_frame,dfl_lava_schedule_neighbor_candidate_frame,dfl_lava_candidate_value_scorer_frame,dfl_lava_candidate_value_strict_lp_benchmark_frame,dfl_lava_tail_risk_diagnostic_frame,dfl_lava_tail_risk_aware_target_frame,dfl_lava_tail_risk_aware_strict_lp_benchmark_frame `
  -c configs/real_data_dfl_lava_tail_risk_target_week3.yaml
```

Claim boundary remains unchanged: Offline Strategy Promotion/read-model
evidence only, no live dispatch, no dashboard/API default switch, and no
deployed DT controller.
