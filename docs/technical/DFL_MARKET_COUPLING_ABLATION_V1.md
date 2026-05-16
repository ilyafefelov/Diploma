# DFL Market-Coupling Ablation V1

Date: 2026-05-16

This slice adds a governed market-coupling ablation around the current thesis
baseline: Ukrainian-only official global-panel NBEATSx Schedule/Value Learner
V2+. The baseline stays frozen as the comparator:

- calibrated V2+ mean regret: `174.77` UAH;
- improvement versus `strict_similar_day`: `43.73%`;
- rolling robustness: `4 / 4` windows;
- `market_execution_enabled=false`.

The ablation asks one narrow question: can point-in-time approved neighbor-market
features improve V2+ without weakening the strict LP/oracle gate? If governance
is incomplete, the correct output is a blocked evidence row, not a trained
market-coupled variant.

## Boundary

ENTSO-E/neighbor data is never added as European training rows. The only allowed
future path is an approved exogenous feature column joined to Ukrainian
timestamps before official global-panel training. Approval remains centralized
in `official_forecast_exogenous_feature_route_frame`.

The current expected state is blocked:

- source-backed sample alone is insufficient;
- missing publication timestamp blocks approval;
- missing prior-known EUR/UAH FX normalization blocks approval;
- timezone/DST, licensing, market-rule mapping, and domain-shift checks must
  all pass before `approved_for_official_training=true`;
- no dashboard/API default switch is made.

## Assets

| Asset | Purpose |
|---|---|
| `entsoe_neighbor_market_aligned_feature_panel_frame` | Aligns Poland-first ENTSO-E feature candidates to tenant benchmark timestamps while keeping `training_use_allowed=false`. |
| `dfl_market_coupling_v2_plus_ablation_frame` | Compares Ukrainian-only V2+ against Ukrainian plus approved neighbor-market features, or emits `ablation_status=blocked_by_governance`. |
| `dfl_market_coupling_v2_plus_ablation_evidence` | Dagster asset check for thesis-grade claim boundaries, blocked-training behavior, and strict comparison semantics. |

Tracked config:
[real_data_dfl_market_coupling_ablation_week3.yaml](../../configs/real_data_dfl_market_coupling_ablation_week3.yaml).

## Gate Semantics

The ablation has three valid states:

| Status | Meaning |
|---|---|
| `blocked_by_governance` | No external feature column is approved, so B is not trained. |
| `approved_route_pending_materialization` | Governance approved at least one feature, but market-coupled strict evidence is missing. |
| `comparison_complete` | A governed market-coupled B variant exists and is scored against Ukrainian-only V2+. |

If B is materialized, it passes only when:

- B improves mean regret over Ukrainian-only V2+;
- B does not worsen median regret versus Ukrainian-only V2+;
- rolling robustness is preserved;
- thesis-grade observed coverage, zero safety violations, `not_full_dfl=true`,
  `not_market_execution=true`, and `market_execution_enabled=false` hold.

## Materialization

After the Ukrainian-only V2+ strict and robustness assets exist:

```powershell
docker compose exec -T dagster-webserver uv run dagster asset materialize -m smart_arbitrage.defs --select forecast_afe_feature_catalog_frame,market_coupling_temporal_availability_frame,entsoe_neighbor_market_query_spec_frame,entsoe_neighbor_market_feature_candidate_frame,entsoe_neighbor_market_aligned_feature_panel_frame,official_forecast_exogenous_governance_frame,official_forecast_exogenous_feature_route_frame,dfl_official_global_panel_schedule_value_learner_v2_plus_strict_lp_benchmark_frame,dfl_official_global_panel_schedule_value_learner_v2_plus_robustness_frame,dfl_market_coupling_v2_plus_ablation_frame -c configs/real_data_dfl_market_coupling_ablation_week3.yaml
```

Validate definitions locally, then run the asset check from Dagster UI or the
Dagster asset-check surface after materialization:

```powershell
uv run dg check defs
```

## Current Interpretation

Until all governance columns pass, ENTSO-E Poland is a readiness/audit lane, not
a training feature. This preserves the current thesis result as Ukrainian-only:
OREE DAM prices, Open-Meteo/weather context, tenant load/config context, and
strict LP/oracle scoring.

The follow-on path is:

1. complete source-backed point-in-time governance for one neighbor feature lane;
2. rerun official global-panel training with the approved feature column only;
3. compare Ukrainian-only V2+ and Ukrainian-plus-governed-neighbor V2+ under the
   unchanged strict LP/oracle gate;
4. revisit official global-panel TFT only if the feature lane is approved or
   clearly blocked.
