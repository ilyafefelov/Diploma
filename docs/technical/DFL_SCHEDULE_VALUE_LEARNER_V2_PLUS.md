# DFL Schedule/Value Learner V2+

Date: 2026-05-15

This slice keeps Schedule/Value Learner V2 frozen as a reproducible baseline and
adds a narrow V2+ research path for attacking the remaining strict LP/oracle
regret. The official global-panel V2+ run is now materialized and passes both
the latest-holdout strict gate and four rolling robustness windows on the
365-anchor Ukrainian panel.

Claim boundary: V2+ is offline/read-model research evidence only. It is not live
market execution, not a deployed Decision Transformer controller, and not a full
end-to-end DFL claim. `strict_similar_day` remains the frozen control and
fallback. V2+ is now the stronger Offline Strategy Promotion headline; V2
remains the frozen fallback comparison inside the evidence packet.

## Why This Slice Exists

The V3 ranker was useful as an additive test, but it did not beat frozen V2 on
the 365-anchor Ukrainian packet. The next useful step is therefore not another
small ranker. It is a decision-regret autopsy plus a richer feasible schedule
library around the failure modes that still create regret.

The design follows the DFL direction captured in the thesis literature review:
storage arbitrage is intertemporal and SOC-path dependent, so the target metric
must remain downstream regret/value under the strict LP/oracle evaluator rather
than raw forecast MAE or hourly action accuracy.

## Assets

| Asset | Purpose |
|---|---|
| `dfl_schedule_value_regret_decomposition_frame` | Decomposes remaining V2 regret by tenant, source model, anchor, selected family, best candidate gap, rank/extrema diagnostics, SOC slack, throughput, and failure mode. |
| `dfl_schedule_candidate_library_v2_plus_frame` | Adds deterministic prior-safe schedule families to the V2 candidate library. |
| `dfl_schedule_value_learner_v2_plus_frame` | Selects among V2 and V2+ candidates using train/prior anchors only, with frozen V2 as the default fallback. |
| `dfl_schedule_value_learner_v2_plus_strict_lp_benchmark_frame` | Emits strict/raw/V2/V2+ rows for unchanged strict LP/oracle scoring. |
| `dfl_schedule_value_learner_v2_plus_evidence` | Dagster asset check for coverage, provenance, and claim-boundary validity. |
| `dfl_official_global_panel_schedule_value_learner_v2_plus_robustness_frame` | Replays V2+ across four rolling 18-anchor validation windows using prior-only selection. |
| `dfl_official_global_panel_schedule_value_learner_v2_plus_robustness_evidence` | Dagster asset check for V2+ rolling-window evidence validity. |
| `dfl_official_global_panel_schedule_*_v2_plus_*` | Official global-panel mirrors for the 365-anchor NBEATSx evidence lane. |
| `entsoe_neighbor_market_aligned_feature_panel_frame` | Poland-first neighbor-market feature alignment lane. Rows remain research/governance evidence until the route approves them for training. |
| `dfl_market_coupling_v2_plus_ablation_frame` | Compares Ukrainian-only V2+ against a future Ukrainian-plus-governed-neighbor V2+ route, or emits `blocked_by_governance` without training B. |

Tracked config:
[real_data_official_global_panel_schedule_value_v2_plus_week3.yaml](../../configs/real_data_official_global_panel_schedule_value_v2_plus_week3.yaml).

Rolling robustness config:
[real_data_official_global_panel_schedule_value_v2_plus_robustness_week3.yaml](../../configs/real_data_official_global_panel_schedule_value_v2_plus_robustness_week3.yaml).

Governed market-coupling ablation config:
[real_data_dfl_market_coupling_ablation_week3.yaml](../../configs/real_data_dfl_market_coupling_ablation_week3.yaml).

## Candidate Library V2+

V2+ adds schedule candidates, not live policies:

- `rank_extrema_perturbation_v2_plus`: emphasizes forecast top/bottom price
  ranks to test whether missed extrema cause value loss.
- `robust_spread_penalty_v2_plus`: shrinks high-spread forecasts toward the mean
  to reduce brittle high-spread schedules.
- `strict_neighborhood_shift_v2_plus`: shifts the strict schedule forecast shape
  by small windows to test timing sensitivity around the similar-day control.
- `temporal_block_reconciled_v2_plus`: smooths forecasts by local temporal
  blocks, inspired by temporal hierarchy/reconciliation ideas.
- `soc_terminal_target_v2_plus`: adjusts late-horizon prices to explore terminal
  SOC value pressure.

Every generated candidate is strict-scored through the existing LP/oracle path.
Final-holdout actuals may affect scoring only; they must not affect candidate
generation parameters, V2 fallback selection, or V2+ selection.

## Selector Rule

V2+ is deliberately conservative:

1. Build the frozen V2 reference only from V2-era families, excluding V2+
   candidates.
2. On train/prior anchors, compare V2 against the best V2+ candidate by prior
   regret evidence.
3. Select V2+ only when train/prior evidence improves over V2 by the configured
   threshold.
4. Otherwise default to V2.
5. Score final holdout with the same strict LP/oracle rows used by V2 and V3.

The promotion gate is stricter than a normal challenger gate:

- V2+ must improve mean regret versus frozen V2.
- V2+ median regret must not degrade versus V2.
- V2+ must still beat `strict_similar_day` by at least 5% mean regret.
- Median regret must not worsen versus `strict_similar_day`.
- Thesis-grade observed coverage, zero safety violations, and no market
  execution claim remain mandatory.

## Materialization

After official global-panel forecast rows and the V2 library exist, materialize:

```powershell
docker compose exec -T dagster-webserver uv run dagster asset materialize -m smart_arbitrage.defs --select dfl_official_global_panel_schedule_candidate_library_frame,dfl_official_global_panel_schedule_candidate_library_v2_frame,dfl_official_global_panel_schedule_candidate_library_v2_plus_frame,dfl_official_global_panel_schedule_value_learner_v2_frame,dfl_official_global_panel_schedule_value_regret_decomposition_frame,dfl_official_global_panel_schedule_value_learner_v2_plus_frame,dfl_official_global_panel_schedule_value_learner_v2_plus_strict_lp_benchmark_frame -c configs/real_data_official_global_panel_schedule_value_v2_plus_week3.yaml
```

If the V2+ check passes, export a separate comparison packet. Do not overwrite
the existing 365-anchor V2 evidence packet unless V2+ passes the unchanged gate
and the result is intentionally promoted as stronger Offline Strategy Promotion
evidence.

The reusable export is:

```powershell
.\.venv\Scripts\python.exe scripts\materialize_schedule_value_v2_plus_comparison.py --strict-frame-pickle .tmp_runtime\v2_plus_export_inputs\strict_lp_benchmark.pkl --learner-frame-pickle .tmp_runtime\v2_plus_export_inputs\learner_v2_plus.pkl --regret-decomposition-pickle .tmp_runtime\v2_plus_export_inputs\regret_decomposition.pkl --run-slug week3_official_global_panel_schedule_value_v2_plus_comparison --dagster-run-id b09194b2-8bf7-42fb-bcc7-1567ca47037c
```

The rolling robustness materialization is:

```powershell
docker compose exec -T dagster-webserver uv run dagster asset materialize -m smart_arbitrage.defs --select dfl_official_global_panel_schedule_value_learner_v2_plus_robustness_frame -c configs/real_data_official_global_panel_schedule_value_v2_plus_robustness_week3.yaml
```

## Current Status

Implementation status: materialized and checked on 2026-05-15.

Latest-holdout comparison packet:
`data/research_runs/week3_official_global_panel_schedule_value_v2_plus_comparison/`.
The packet now includes the attached rolling artifact
`dfl_schedule_value_learner_v2_plus_rolling_robustness.csv` beside the strict
rows, learner trace, role summary, and regret decomposition summary.

Latest strict-gate run:
`b09194b2-8bf7-42fb-bcc7-1567ca47037c`.

Rolling robustness run:
`8832f41e-e605-4107-ab6d-028676faa223`.

Latest-holdout gate result:

| Source | Strict mean regret | Frozen V2 mean regret | V2+ mean regret | V2+ improvement vs strict | V2+ improvement vs V2 |
|---|---:|---:|---:|---:|---:|
| `nbeatsx_official_global_panel_horizon_calibrated_v1` | 310.58 | 206.37 | 174.77 | 43.73% | 15.31% |
| `nbeatsx_official_global_panel_v1` | 310.58 | 225.44 | 193.36 | 37.74% | 14.23% |

Rolling robustness result:

| Source | Rolling windows | Result |
|---|---:|---|
| `nbeatsx_official_global_panel_horizon_calibrated_v1` | 4 / 4 | V2+ beats both `strict_similar_day` and frozen V2 |
| `nbeatsx_official_global_panel_v1` | 4 / 4 | V2+ beats both `strict_similar_day` and frozen V2 |

The calibrated source has the best latest-holdout mean regret, so it is the
preferred thesis-facing V2+ source. Both sources remain offline/read-model
evidence only: `market_execution_enabled=false`, no dashboard/API default switch,
and no live market execution claim.

## Market-Coupling Ablation Baseline

V2+ is now the frozen Ukrainian-only comparator for the next feature experiment.
The market-coupling ablation does not reinterpret the V2+ result as EU-assisted:
the current evidence uses Ukrainian OREE DAM prices, Open-Meteo/weather context,
tenant load/configuration context, and strict LP/oracle scoring only.

The new ablation route may add ENTSO-E Poland or later neighbor-market columns
only after `official_forecast_exogenous_feature_route_frame` marks those columns
`approved_for_official_training=true`. Until then,
`dfl_market_coupling_v2_plus_ablation_frame` must emit
`ablation_status=blocked_by_governance` and `did_train_market_coupled_variant=false`.
The detailed gate is documented in
[DFL_MARKET_COUPLING_ABLATION_V1.md](DFL_MARKET_COUPLING_ABLATION_V1.md).
