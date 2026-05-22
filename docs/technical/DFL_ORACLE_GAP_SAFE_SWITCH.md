# Oracle-Gap Safe-Switch Before DT/LAVA

This slice narrows the DT/LAVA problem to the part that is still genuinely
unresolved after the corrected V2+ audit.

Current frozen comparator:

| Metric | Corrected Ukrainian-only calibrated V2+ |
|---|---:|
| Mean regret | `174.77` UAH |
| Median regret | `67.30` UAH |
| Rolling robustness | `4 / 4` windows |
| Market execution | `false` |

The leakage audit fixed the V2+ final-selection tie-breaker and the corrected
result stayed unchanged. The oracle-gap audit then showed the real target:
calibrated V2+ already selected the best available candidate on `71 / 90`
latest tenant-anchor rows, but missed a better candidate on `19 / 90`.

## Method

The safe-switch layer trains on candidate schedules, not raw hourly
BUY/SELL/HOLD actions.

| Asset | Purpose |
|---|---|
| `dfl_oracle_gap_safe_switch_label_frame` | Creates labels for every feasible V2+ candidate: safe switch win, tail-risk loss, best candidate family/index, and regret delta versus corrected V2+. |
| `dfl_oracle_gap_safe_switch_feature_panel_frame` | Publishes prior-only `selector_feature_*` columns: schedule distance, SOC/throughput deltas, spread/risk features, and optional Poland/TFT disagreement indicators. |
| `dfl_oracle_gap_safe_switch_scorer_frame` | Fits a conservative profile-shrunk safe-switch scorer on train/prior anchors only. |
| `dfl_oracle_gap_safe_switch_strict_lp_benchmark_frame` | Scores strict control, corrected V2+, and safe-switch selections under the unchanged strict LP/oracle evaluator. |
| `dfl_oracle_gap_safe_switch_rolling_robustness_frame` | Replays the scorer in latest-first rolling windows with training anchors strictly before each validation window. |

The selector switches away from V2+ only when prior evidence predicts both:

- positive regret improvement versus corrected V2+;
- tail-risk probability below the configured threshold.

Otherwise it falls back to corrected V2+ for that anchor.

Tracked config:

- [configs/real_data_dfl_oracle_gap_safe_switch_week3.yaml](../../configs/real_data_dfl_oracle_gap_safe_switch_week3.yaml)

Core implementation:

- `src/smart_arbitrage/dfl/oracle_gap_safe_switch.py`

## Gate

Promotion requires:

- mean regret beats `174.77` UAH by at least `5%`;
- median regret does not worsen versus `67.30` UAH;
- rolling robustness passes `4 / 4` windows;
- zero safety violations;
- `market_execution_enabled=false`.

Diagnostic success is weaker: any positive mean improvement with no median
degradation and at least `3 / 4` rolling windows. Diagnostic success would
justify a stronger DT/LAVA follow-up, but would not replace V2+ as the thesis
headline.

## Materialization

```powershell
docker compose exec -T dagster-webserver uv run dagster asset materialize `
  -m smart_arbitrage.defs `
  --select dfl_oracle_gap_safe_switch_label_frame,dfl_oracle_gap_safe_switch_feature_panel_frame,dfl_oracle_gap_safe_switch_scorer_frame,dfl_oracle_gap_safe_switch_strict_lp_benchmark_frame,dfl_oracle_gap_safe_switch_rolling_robustness_frame `
  -c configs/real_data_dfl_oracle_gap_safe_switch_week3.yaml
```

Claim boundary remains unchanged: Offline Strategy Promotion/read-model
evidence only, no live dispatch, no dashboard/API default switch, and no market
execution.

## Materialized Result

Dagster run `d9ca0064-8fc9-4da1-880a-47ae0d62958d` materialized the full
safe-switch path. The label frame reproduced the corrected oracle-gap target:

| Final-holdout class | Tenant-anchor rows |
|---|---:|
| V2+ selected best available candidate | `71 / 90` |
| Better candidate existed but was not selected | `19 / 90` |

The conservative scorer did not find a prior-safe switch profile. It fell back
to corrected calibrated V2+ on all `90 / 90` latest holdout rows:

| Row | Mean regret | Median regret | Safety violations |
|---|---:|---:|---:|
| Corrected calibrated V2+ | `174.77` UAH | `67.30` UAH | `0` |
| Oracle-gap safe-switch | `174.77` UAH | `67.30` UAH | `0` |

Rolling robustness also did not promote the challenger: all `4 / 4` windows
used V2+ fallback, so the safe-switch path produced `0 / 4` robust challenger
windows and `0 / 4` diagnostic-success windows. This is valid negative
evidence: the missed-candidate minority exists, but the current prior-only
features cannot identify those switches safely enough to replace V2+.
