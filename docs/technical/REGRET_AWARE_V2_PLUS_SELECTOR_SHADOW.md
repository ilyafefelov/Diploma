# Regret-Aware V2+ Selector Shadow

Date: 2026-05-26

This note documents the follow-up to the DT vs real V2+ shadow packet. The
purpose is to test the right ML objective after the DT classifier result: a
candidate-level selector trained on regret delta versus V2+, with explicit
abstention back to V2+ when the predicted switch is not strong enough.

The run is research-only. It does not promote DT/LAVA, does not change the
dashboard default, does not emit a market-order payload, and keeps
`market_execution_enabled=false`.

## Command

```powershell
.\.venv\Scripts\python.exe scripts\materialize_regret_aware_v2_plus_selector_packet.py `
  --strict-rows-csv data\research_runs\week3_official_global_panel_schedule_value_v2_plus_comparison\dfl_schedule_value_learner_v2_plus_strict_rows.csv `
  --regret-decomposition-pickle .tmp_runtime\v2_plus_export_inputs\regret_decomposition.pkl `
  --output-dir data\research_runs\week3_regret_aware_v2_plus_selector_current `
  --run-slug week3_regret_aware_v2_plus_selector_current `
  --source-model-name nbeatsx_official_global_panel_horizon_calibrated_v1 `
  --min-predicted-improvement-uah 150 `
  --tail-risk-loss-threshold-uah 150 `
  --max-family-tail-risk-probability 0.5 `
  --ridge-l2 10
```

## Artifacts

Output directory:

```text
data/research_runs/week3_regret_aware_v2_plus_selector_current/
```

Key files:

| File | Purpose |
|---|---|
| `regret_aware_v2_plus_selector_teacher_rows.csv` | V2+ strict rows adapted into candidate-level teacher rows |
| `regret_aware_v2_plus_selector_selected_rows.csv` | Final-holdout selected rows after abstention |
| `regret_aware_v2_plus_selector_summary.json` | Machine-readable summary |
| `regret_aware_v2_plus_selector_summary.md` | Human-readable summary |

## Objective

The selector trains a weighted ridge regressor over point-in-time candidate
features. Target:

```text
regret_delta_vs_v2_plus_uah = candidate_regret_uah - v2_plus_regret_uah
```

Loss:

```text
weighted_ridge_regret_delta_vs_v2_plus
```

Sample weight:

```text
1 + abs(regret_delta_vs_v2_plus_uah) / 100
```

The selector can choose a non-V2+ candidate only when:

- predicted improvement versus V2+ is at least `150` UAH;
- candidate-family tail-risk probability is at most `0.5`;
- the candidate row has no safety violation;
- the row remains research-shadow / non-executable.

Otherwise it abstains to the V2+ comparator.

## Result

| Metric | Value |
|---|---:|
| Selector mean regret | `174.77` UAH |
| V2+ mean regret | `174.77` UAH |
| Selector minus V2+ regret | `0.00` UAH |
| Non-V2+ switches | `0 / 90` |
| V2+ abstentions | `90 / 90` |
| Train weighted RMSE | `668.91` UAH |

Family-level train diagnostics:

| Candidate family | Mean delta vs V2+ | Tail-loss count | Tail-risk probability |
|---|---:|---:|---:|
| `raw_reference` | `+447.48` UAH | `53 / 90` | `0.589` |
| `schedule_value_learner_v2_reference` | `+31.60` UAH | `6 / 90` | `0.067` |
| `strict_reference` | `+135.81` UAH | `31 / 90` | `0.344` |

The conservative selector therefore preserves V2+ exactly. Lower thresholds
caused non-V2+ switches but worsened mean regret; for example, a 1 UAH switch
threshold selected 39 non-V2+ rows and produced `218.87` UAH mean regret.

## Interpretation

This result answers the architecture question more precisely than the earlier
DT classifier smoke:

- the correct target is regret delta/value gap, not candidate-index accuracy;
- the abstention rule is necessary, because the available point-in-time features
  do not identify a robust safe-switch subset in this packet;
- V2+ remains the headline/default because it is already a conservative
  schedule/value selector;
- the selector is useful negative evidence: it prevents a worse learned switch
  from replacing V2+.

This is not a promotion result and not a deployed controller. It is a
research-shadow objective check that shows the next improvement needs richer
prior-safe context, better candidate labels, or more historical safe-switch
examples before DT/LAVA can be evaluated as a replacement.

## Boundary

- `market_execution_enabled=false`
- `dt_lava_ready=false`
- `permits_model_training=false`
- no `ProposedBid`
- no dashboard/API default switch
- no out-of-sample generalization claim
