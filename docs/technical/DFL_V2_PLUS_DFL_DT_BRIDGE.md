# V2+-Anchored DFL/DT Bridge Evidence

## Purpose

This slice evaluates the existing residual schedule/value and offline Decision
Transformer challengers against the current Ukrainian-only V2+ baseline, not
only against `strict_similar_day`.

The frozen comparator remains:

- calibrated V2+ mean regret: `174.77` UAH;
- improvement vs `strict_similar_day`: `43.73%`;
- rolling robustness: `4 / 4` windows;
- `market_execution_enabled=false`.

This is **Offline Strategy Promotion** evidence only. It is not live market
execution, not a dashboard/API default switch, and not deployed Decision
Transformer control.

## Assets

Compact-path additive asset:

- `dfl_v2_plus_dfl_dt_bridge_strict_lp_benchmark_frame`

Inputs:

- `dfl_residual_dt_fallback_strict_lp_benchmark_frame`;
- `dfl_schedule_value_learner_v2_plus_strict_lp_benchmark_frame`.

The bridge normalizes the following roles into one strict LP/oracle comparison:

| Role | Meaning |
| --- | --- |
| `strict_reference` | Frozen `strict_similar_day` control. |
| `schedule_value_learner_v2_plus_reference` | Current V2+ Ukrainian-only comparator. |
| `residual_dfl_reference` | Prior-only residual schedule/value challenger. |
| `offline_dt_reference` | Tiny return-conditioned offline DT candidate. |
| `filtered_behavior_cloning_reference` | Required non-DT imitation baseline. |
| `residual_dt_fallback_reference` | Strict-default wrapper over residual/DT candidates. |

Asset check:

- `dfl_v2_plus_dfl_dt_bridge_evidence`.

The check validates coverage and claim boundaries without requiring the
challenger to pass. A blocked challenger remains useful evidence if it is
structurally valid and leakage-free.

Official V2+-teacher additive assets:

- `dfl_official_global_panel_v2_plus_trajectory_dataset_frame`;
- `dfl_official_global_panel_v2_plus_residual_schedule_value_model_frame`;
- `dfl_official_global_panel_v2_plus_offline_dt_candidate_frame`;
- `dfl_official_global_panel_v2_plus_dfl_dt_bridge_strict_lp_benchmark_frame`.
- `dfl_official_v2_plus_bridge_failure_audit_frame`.

Inputs:

- `dfl_official_global_panel_schedule_candidate_library_v2_plus_frame`;
- `dfl_official_global_panel_schedule_value_learner_v2_plus_strict_lp_benchmark_frame`.

Asset check:

- `dfl_official_global_panel_v2_plus_dfl_dt_bridge_evidence`.
- `dfl_official_v2_plus_bridge_failure_audit_evidence`.

The official path uses the same role schema, but the source models are
`nbeatsx_official_global_panel_v1` and
`nbeatsx_official_global_panel_horizon_calibrated_v1`. Teacher labels are
selected only from train/prior anchors. Final-holdout rows are scoring-only.
The failure audit is analysis-only: it reads the official bridge benchmark,
compares challenger selections against V2+ selections, and labels why residual
DFL/offline DT lost. It is not training input.

## Gate

A residual/DT challenger can pass only when it:

- beats V2+ mean regret;
- does not worsen median regret versus V2+;
- still beats `strict_similar_day` by at least `5%` mean regret;
- does not worsen median regret versus `strict_similar_day`;
- keeps thesis-grade observed rows, zero safety violations, `not_full_dfl=true`,
  and `not_market_execution=true`.

The offline DT role is additionally compared against filtered behavior cloning,
so the DT result is not overclaimed when simple imitation is equally good or
better.

## Run

Compact bridge materialization:

```powershell
docker compose exec -T dagster-webserver uv run dagster asset materialize `
  -m smart_arbitrage.defs `
  --select dfl_v2_plus_dfl_dt_bridge_strict_lp_benchmark_frame
```

Compact negative-evidence export:

```powershell
.\.venv\Scripts\python.exe scripts\materialize_v2_plus_dfl_dt_bridge_packet.py `
  --bridge-frame-pickle <path-to-pickled-bridge-frame> `
  --run-slug week3_dfl_v2_plus_dfl_dt_bridge_negative_evidence `
  --dagster-run-id <dagster-run-id> `
  --asset-check-status passed
```

Official V2+-teacher bridge materialization:

```powershell
docker compose exec -T dagster-webserver uv run dagster asset materialize `
  -m smart_arbitrage.defs `
  --select dfl_official_global_panel_v2_plus_trajectory_dataset_frame,dfl_official_global_panel_v2_plus_residual_schedule_value_model_frame,dfl_official_global_panel_v2_plus_offline_dt_candidate_frame,dfl_official_global_panel_v2_plus_dfl_dt_bridge_strict_lp_benchmark_frame `
  -c configs/real_data_dfl_official_v2_plus_dfl_dt_bridge_week3.yaml
```

On local Docker Desktop, prefer a serial resume pattern after the trajectory
asset exists. The trajectory frame is large, and running residual DFL plus DT in
parallel can load it twice:

```powershell
docker compose exec -T dagster-webserver uv run dagster asset materialize `
  -m smart_arbitrage.defs `
  --select dfl_official_global_panel_v2_plus_residual_schedule_value_model_frame `
  -c configs/real_data_dfl_official_v2_plus_dfl_dt_bridge_week3.yaml

docker compose exec -T dagster-webserver uv run dagster asset materialize `
  -m smart_arbitrage.defs `
  --select dfl_official_global_panel_v2_plus_offline_dt_candidate_frame `
  -c configs/real_data_dfl_official_v2_plus_dfl_dt_bridge_week3.yaml

docker compose exec -T dagster-webserver uv run dagster asset materialize `
  -m smart_arbitrage.defs `
  --select dfl_official_global_panel_v2_plus_dfl_dt_bridge_strict_lp_benchmark_frame `
  -c configs/real_data_dfl_official_v2_plus_dfl_dt_bridge_week3.yaml
```

Official negative-evidence export:

```powershell
.\.venv\Scripts\python.exe scripts\materialize_v2_plus_dfl_dt_bridge_packet.py `
  --bridge-frame-pickle .tmp_runtime\official_v2_plus_bridge\frame.pkl `
  --run-slug week3_dfl_official_v2_plus_dfl_dt_bridge_negative_evidence `
  --dagster-run-id 53efba76-38cb-4624-9cd8-e15fb8c1c7a9 `
  --asset-check-status passed
```

Official bridge failure audit:

```powershell
docker compose exec -T dagster-webserver uv run dagster asset materialize `
  -m smart_arbitrage.defs `
  --select dfl_official_v2_plus_bridge_failure_audit_frame `
  -c configs/real_data_dfl_official_v2_plus_dfl_dt_bridge_week3.yaml
```

Useful focused tests:

```powershell
.\.venv\Scripts\python.exe -m pytest -p no:cacheprovider `
  tests\dfl\test_v2_plus_dfl_dt_bridge.py `
  tests\dfl\test_v2_plus_dfl_dt_bridge_export.py `
  tests\dfl\test_official_v2_plus_dfl_dt_bridge.py `
  tests\assets\test_dfl_research_assets.py::test_dfl_research_assets_are_registered `
  tests\assets\test_evidence_checks.py::test_dfl_evidence_asset_checks_are_registered
```

## Current Finding

The compact residual DFL / offline DT bridge has been materialized and the
evidence check passed, but the gate blocked promotion: no residual/DT challenger
beat V2+ mean regret without median degradation while also clearing
`strict_similar_day`. This is valid negative evidence, not a failed pipeline.

The official V2+-teacher bridge has also been materialized. The evidence check
passed in Dagster run `53efba76-38cb-4624-9cd8-e15fb8c1c7a9`, but the strict
gate still blocked headline replacement:

| Source model | V2+ mean regret, UAH | Residual/DT mean regret, UAH | Behavior cloning mean regret, UAH | Strict mean regret, UAH | Decision |
| --- | ---: | ---: | ---: | ---: | --- |
| `nbeatsx_official_global_panel_horizon_calibrated_v1` | 174.77 | 367.70 | 580.39 | 310.58 | blocked |
| `nbeatsx_official_global_panel_v1` | 193.36 | 328.51 | 675.84 | 310.58 | blocked |

The official bridge result confirms the same conclusion at the stronger
comparator level: the tiny residual DFL / offline DT candidate is not yet a
better controller than V2+. V2+ remains the thesis headline.

The failure audit was materialized in Dagster run
`5ccff4bd-4628-4595-bb82-f91cb9194180`; the check passed. It produced 720
analysis rows and the local ignored packet
`data/research_runs/week3_dfl_official_v2_plus_dfl_dt_bridge_negative_evidence/`
now includes audit CSV/JSON/Markdown artifacts. The failure-mode distribution is:

| Failure mode | Rows | Share | Mean delta vs V2+, UAH | Interpretation |
| --- | ---: | ---: | ---: | --- |
| `candidate_family_collapse` | 351 | 48.75% | 249.58 | Residual/DT/fallback repeatedly selected the same `strict_raw_blend_v2` family instead of learning when V2+ uses a different schedule family. |
| `dt_imitation_weaker_than_v2_selector` | 142 | 19.72% | 518.69 | Behavior cloning imitates high-value trajectories but remains much weaker than V2+ selection. |
| `weak_trajectory_objective` | 135 | 18.75% | -30.25 | Some challenger rows are locally competitive, but the objective is not robust enough to become a headline replacement. |
| `bad_teacher_target` | 92 | 12.78% | 123.24 | Some anchors show that blindly teaching from V2+ is wrong because strict is already stronger or near-oracle. |

The immediate modeling implication is that the next DFL attempt should not be a
larger tiny DT over the same trajectory contract. It should optimize schedule
value ordering and non-degrading fallback behavior directly. The redesign plan
is tracked in
[DFL_OBJECTIVE_REDESIGN_PLAN.md](DFL_OBJECTIVE_REDESIGN_PLAN.md).

## Boundary

Poland/ENTSO-E features remain blocked by governance and do not enter this
training or evaluation path. V2+ stays the thesis headline until a challenger
beats it under the same strict LP/oracle gate.
