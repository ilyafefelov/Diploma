# DT V2+ Promotion Evidence

Date: 2026-05-26

This note documents the offline challenger gate for the residual DT/V2+
selector. The gate is not market execution and not a dashboard/API default
switch.

## Boundary

- V2+ remains the champion/default/fallback.
- The residual DT selector is a challenger that may switch only on rare
  predicted safe-switch cases.
- Strict/oracle regret is used only as frozen final-holdout scoring evidence,
  not as a runtime selector input.
- `promotion_evidence_passed` is an offline evidence flag only.
- `promotion_gate_passed=false`, `market_execution_enabled=false`,
  `dt_lava_ready=false`, `permits_model_training=false`.
- No `ProposedBid` is emitted.

## Command

Selector packet:

```powershell
.\.venv\Scripts\python.exe scripts\materialize_regret_aware_v2_plus_selector_packet.py `
  --teacher-rows-csv data\research_runs\week3_regret_aware_v2_plus_selector_current\regret_aware_v2_plus_selector_teacher_rows.csv `
  --output-dir data\research_runs\week3_dt_v2_plus_safe_switch_selector_current `
  --run-slug week3_dt_v2_plus_safe_switch_selector_current `
  --source-model-name nbeatsx_official_global_panel_horizon_calibrated_v1 `
  --min-predicted-improvement-uah 20 `
  --tail-risk-loss-threshold-uah 150 `
  --max-family-tail-risk-probability 0.5 `
  --ridge-l2 10 `
  --model-kind random_forest `
  --feature-set expanded_prior_context_v1
```

Promotion-evidence gate:

```powershell
.\.venv\Scripts\python.exe scripts\materialize_dt_v2_plus_promotion_evidence_packet.py `
  --selected-rows-csv data\research_runs\week3_dt_v2_plus_safe_switch_selector_current\regret_aware_v2_plus_selector_selected_rows.csv `
  --teacher-rows-csv data\research_runs\week3_dt_v2_plus_safe_switch_selector_current\regret_aware_v2_plus_selector_teacher_rows.csv `
  --output-dir data\research_runs\week3_dt_v2_plus_promotion_evidence_current `
  --run-slug week3_dt_v2_plus_promotion_evidence_current `
  --source-model-name nbeatsx_official_global_panel_horizon_calibrated_v1 `
  --min-final-holdout-anchor-count 90 `
  --min-mean-regret-improvement-ratio-vs-v2-plus 0.03 `
  --max-non-v2-plus-switch-rate 0.25 `
  --tail-risk-loss-threshold-uah 150 `
  --max-tail-risk-loss-count 0
```

## Artifacts

Dagster assets:

| Asset | Purpose |
|---|---|
| `dfl_dt_v2_plus_safe_switch_selector_frame` | Manual residual safe-switch selector with V2+ fallback |
| `dfl_dt_v2_plus_promotion_evidence_frame` | Offline challenger gate over the safe-switch selector |

Output directory:

```text
data/research_runs/week3_dt_v2_plus_safe_switch_selector_current/
data/research_runs/week3_dt_v2_plus_promotion_evidence_current/
```

Key files:

| File | Purpose |
|---|---|
| `dt_v2_plus_promotion_evidence_gate_rows.csv` | One-row gate decision |
| `dt_v2_plus_promotion_evidence_selected_rows.csv` | Selector final-holdout rows used by the gate |
| `dt_v2_plus_promotion_evidence_safe_switch_opportunities.csv` | Frozen teacher/oracle safe-switch opportunities |
| `dt_v2_plus_promotion_evidence_summary.json` | Machine-readable summary |
| `dt_v2_plus_promotion_evidence_summary.md` | Human-readable summary |

## Current Result

| Metric | Value |
|---|---:|
| Promotion evidence passed | `true` |
| Promotion blocker | `none` |
| Final-holdout anchors | `90` |
| Selector mean regret | `168.16` UAH |
| V2+ mean regret | `174.77` UAH |
| Selector minus V2+ mean regret | `-6.61` UAH |
| Mean regret improvement vs V2+ | `3.78%` |
| Non-V2+ switches | `4` |
| Observed safe-switch opportunities | `15` |
| Recovered safe-switch opportunities | `3` |
| Safe-switch wins/losses/ties | `3 / 0 / 1` |
| Tail-risk losses | `0` |
| Strict reference mean regret | `310.58` UAH |

## Interpretation

The gate confirms the next research target and shows a useful residual
safe-switch signal. The frozen teacher/oracle evidence contains rare safe-switch
opportunities; the expanded prior-context random-forest selector recovers `3`
winning opportunities and makes `4 / 90` non-V2+ switches with no configured
tail-risk losses.

This is still not a production promotion. The evidence threshold used here is
`3%` mean-regret improvement versus V2+ for a research challenger screen, while
full production promotion remains a separate explicit gate. V2+ remains the
operator default/fallback until a later promotion gate is intentionally added
and passed.
