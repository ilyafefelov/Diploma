# DFL ENTSO-E Poland Governance Ablation

Date: 2026-05-17

This slice closes the first concrete external-feature lane around the current
headline comparator: Ukrainian-only Schedule/Value Learner V2+.

The lane is deliberately narrow:

- source: ENTSO-E Poland day-ahead price context;
- role: point-in-time exogenous feature column only;
- non-role: no European training rows, no dashboard/API switch, no live market
  execution;
- comparator: Ukrainian-only calibrated V2+ mean regret `174.77` UAH with
  `4 / 4` rolling robustness windows.

## Governance Gate

`entsoe_poland_feature_governance_frame` consumes
`entsoe_neighbor_market_feature_candidate_frame` and emits one Poland route row.
It can approve only one official-training column:
`entsoe_pl_day_ahead_price_uah_mwh`.

Approval requires all controls to pass:

- local `ENTSOE_TOKEN`, `ENTSOE_SECURITY_TOKEN`, or `ENTSO_E_SECURITY_TOKEN`;
- source-backed Poland day-ahead sample;
- publication timestamp earlier than the Ukrainian decision anchor;
- timezone/DST mapping marked ready;
- prior-known EUR/UAH FX rate, timestamped earlier than the Ukrainian anchor;
- FX source recorded;
- licensing approval;
- DAM market-rule mapping;
- domain-shift validation.

If any control is missing, the route stays blocked with
`training_use_allowed=false`, `feature_use_allowed=false`, and
`approved_for_official_training=false`.

## Existing Route Reuse

The implementation extends the existing route rather than adding a parallel
path:

- `entsoe_neighbor_market_feature_candidate_frame` remains the source-backed
  candidate adapter;
- `entsoe_neighbor_market_aligned_feature_panel_frame` keeps timestamp-aligned
  neighbor rows as research evidence;
- `official_forecast_exogenous_feature_route_frame` remains the only route into
  official global-panel training;
- `dfl_market_coupling_v2_plus_ablation_frame` decides whether B can train.

If the Poland route is blocked, the ablation exports
`ablation_status=blocked_by_governance` and does not train the
Ukrainian-plus-neighbor variant.

## Tracked Config

[real_data_dfl_entsoe_poland_feature_ablation_week3.yaml](../../configs/real_data_dfl_entsoe_poland_feature_ablation_week3.yaml)
is the default evidence config for this slice. It keeps fetch disabled and
governance false by default, so the expected status without explicit source,
publication, FX, licensing, market-rule, and domain-shift evidence is blocked.

## Materialization

```powershell
docker compose exec -T dagster-webserver uv run dagster asset materialize -m smart_arbitrage.defs --select forecast_afe_feature_catalog_frame,market_coupling_temporal_availability_frame,entsoe_neighbor_market_query_spec_frame,entsoe_neighbor_market_feature_candidate_frame,entsoe_poland_feature_governance_frame,entsoe_neighbor_market_aligned_feature_panel_frame,official_forecast_exogenous_governance_frame,official_forecast_exogenous_feature_route_frame,dfl_official_global_panel_schedule_value_learner_v2_plus_strict_lp_benchmark_frame,dfl_official_global_panel_schedule_value_learner_v2_plus_robustness_frame,dfl_market_coupling_v2_plus_ablation_frame -c configs/real_data_dfl_entsoe_poland_feature_ablation_week3.yaml
```

The local packet slug for this lane is:
`week3_dfl_entsoe_poland_feature_ablation_v1`.

## Interpretation

A blocked packet is a valid success state. It proves that a source-backed
neighbor-market sample is not enough to enter Ukrainian training. The feature
route must be point-in-time governed before any strict LP/oracle comparison can
claim Ukrainian-plus-neighbor evidence.

## Materialized Evidence

The 2026-05-17 run closed with the expected blocked governance state:

- Dagster run id: `65c87210-36f3-4491-add7-995fa0214d86`;
- local packet:
  `data/research_runs/week3_dfl_entsoe_poland_feature_ablation_v1/`;
- ablation rows: `2`;
- status counts: `blocked_by_governance=2`;
- approved feature columns: none;
- blocked Poland feature column: `entsoe_pl_day_ahead_price_uah_mwh`;
- market-coupled B training runs: `0`;
- evidence check: `dfl_market_coupling_v2_plus_ablation_evidence` passed;
- `market_execution_enabled=false`.

Training blockers in the exported packet:

- `entsoe_token`;
- `source_backed_sample`;
- `publication_time`;
- `prior_eur_uah_fx_rate`;
- `currency`;
- `timezone`;
- `licensing`;
- `market_rules`;
- `domain_shift`;
- `temporal_availability`.

Interpretation: the route is now more precise than the earlier generic
market-coupling block. It identifies exactly which Poland governance controls
still need evidence before the Ukrainian-plus-neighbor B variant can be trained.
