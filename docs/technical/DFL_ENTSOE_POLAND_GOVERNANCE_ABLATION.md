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

- local `ENTSOE_TOKEN`, `ENTSOE_SECURITY_TOKEN`, `ENTSO_E_SECURITY_TOKEN`,
  or lowercase `.env` aliases such as `entsoe_token`;
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

### Two-Stage Approval Boundary

The governance frame now separates two states that used to be collapsed into a
single flag:

| State | Meaning | Can train headline official models? |
| --- | --- | --- |
| `experimental_ablation_use_allowed=true` | Source access, publication-time, timezone/DST, FX, licensing, market-rule, and temporal-availability controls are ready; only domain-shift validation remains. | No. It may only trigger a controlled Ukrainian-plus-Poland ablation packet. |
| `approved_for_official_training=true` | The controlled ablation has also validated domain shift and the route has no blockers. | Yes, as an official exogenous feature route, still with `market_execution_enabled=false`. |

This distinction avoids a circular blocker. Domain shift cannot be validated
before the project runs a controlled ablation, but the ablation itself must not
be confused with thesis headline approval. Therefore the route may emit
`approved_for_experimental_ablation=true` while keeping
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

[real_data_dfl_entsoe_poland_feature_ablation_token_week3.yaml](../../configs/real_data_dfl_entsoe_poland_feature_ablation_token_week3.yaml)
is the token-backed source-smoke config. It enables a Poland ENTSO-E API sample
using the local token, but keeps publication-time, FX, timezone/DST, licensing,
market-rule, and domain-shift controls blocked unless those controls are
explicitly provided. The token value is never written to receipts, packets, or
logs; only safe metadata such as `entsoe_token_available=true` is recorded.

## Materialization

Preferred repo-local wrapper:

```powershell
.\scripts\run-entsoe-poland-governance-ablation.ps1 -RunSlug week3_dfl_entsoe_poland_feature_ablation_v1
```

Use dry-run mode before a token-backed run:

```powershell
.\scripts\run-entsoe-poland-governance-ablation.ps1 -DryRun
```

The wrapper records an `entsoe-poland-governance-run-receipt.json`, runs the
Dagster materialization, copies the materialized ablation frame from Dagster
storage, and exports the local evidence packet with
[materialize_market_coupling_ablation_packet.py](../../scripts/materialize_market_coupling_ablation_packet.py).

Manual equivalent:

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

## Token-Backed Source Smoke Evidence

The 2026-05-20 local rerun used the lowercase `.env` key `entsoe_token` as a
safe alias for `ENTSOE_TOKEN`, rebuilt the Dagster/API containers, and ran:

```powershell
.\scripts\run-entsoe-poland-governance-ablation.ps1 -ConfigPath configs\real_data_dfl_entsoe_poland_feature_ablation_token_week3.yaml -RunSlug week3_dfl_entsoe_poland_token_source_governance_v3
```

Token/source status:

- File Library token smoke returned `token_available=true`, `token_type=Bearer`,
  `expires_in=900`, with no token value written to disk.
- ENTSO-E API Poland day-ahead source fetch produced `186` source-backed
  feature-candidate rows.
- Candidate fetch status:
  `source_backed_feature_sample_fetched_not_training`.
- The source-backed candidate frame still had `training_use_allowed=false`.

The exported ablation packet is:
`data/research_runs/week3_dfl_entsoe_poland_token_source_governance_v3/`.

Materialized evidence:

- Dagster run id: `2a1983fd-3b54-4020-9d76-a8fc6c36ef90`;
- status counts: `blocked_by_governance=2`;
- approved feature columns: none;
- blocked Poland feature column: `entsoe_pl_day_ahead_price_uah_mwh`;
- market-coupled B training runs: `0`;
- evidence check: `dfl_market_coupling_v2_plus_ablation_evidence` passed;
- `market_execution_enabled=false`.

The token and source-backed-sample blockers are now cleared for this smoke run.
The remaining blockers are:

- `publication_time`;
- `prior_eur_uah_fx_rate`;
- `currency`;
- `timezone`;
- `licensing`;
- `market_rules`;
- `domain_shift`;
- `temporal_availability`.

Interpretation: the token solved access/source evidence, but it did not approve
the feature for official training. The correct next market-coupling work is to
attach point-in-time publication metadata, prior-known EUR/UAH FX evidence,
licensing/rule documentation, timezone/DST mapping, and domain-shift validation.
Only after those pass may the Ukrainian-plus-Poland B variant be trained and
compared against frozen Ukrainian-only V2+.

## Prior-Safe Lagged Feature Path

The first admissible Poland feature is now implemented as a guarded lagged
market-regime candidate, not as a same-delivery-day Polish DAM future input.
The same-delivery-day value remains too close to the Ukrainian decision cutoff
unless a source proves it was published before the Ukrainian anchor for every
evaluated timestamp.

The implemented lane is:

- `entsoe_pl_lag24_day_ahead_price_uah_mwh`;
- source: ENTSO-E Poland day-ahead prices;
- alignment: Ukrainian timestamp `t` receives the Poland value from `t - 24h`;
- asset: `entsoe_poland_lagged_feature_candidate_frame`;
- publication proof: the row is treated as prior-safe only when the lagged
  Poland source-delivery timestamp is before the Ukrainian anchor and the
  coverage status is `full_lagged_feature_coverage`;
- FX: EUR/MWh is converted to UAH/MWh only when an NBU EUR/UAH rate, timestamp,
  and source label are supplied before the Ukrainian anchor;
- role: exogenous context only, not European training rows.

The asset checks full timestamp coverage against the benchmark timestamps. If
any required timestamp lacks a lagged Poland source row, the feature remains
blocked with `partial_lagged_feature_coverage`; this prevents a partially
observed external series from silently entering the comparison. If coverage,
prior NBU FX metadata, timezone/DST, licensing, and market-rule controls pass,
the feature may become `experimental_ablation_use_allowed=true` while still
keeping `approved_for_official_training=false` until domain-shift validation
passes.

This lane is the practical way to clear `publication_time` and
`temporal_availability` without weakening leakage rules. The ablation can
materialize B evidence only through the existing
`official_forecast_exogenous_feature_route_frame` and
`dfl_market_coupling_v2_plus_ablation_frame`. It still cannot become the thesis
headline unless the B result beats Ukrainian-only V2+ and validates domain shift
under the same strict LP/oracle gate.

## Lag-24 Attempt, 2026-05-20

The first materialized lag-24 attempt is stored locally as
`data/research_runs/week3_dfl_entsoe_poland_lag24_governance_attempt/`.

- Dagster run id: `e004a33f-8851-4451-9da5-83ddf8b43154`;
- evidence check: `dfl_market_coupling_v2_plus_ablation_evidence` passed;
- ablation rows: `2`;
- ablation status: `blocked_by_governance` for both official NBEATSx source
  rows;
- approved external feature columns: none;
- blocked external feature columns include
  `entsoe_pl_lag24_day_ahead_price_uah_mwh`;
- remaining blockers:
  `currency,domain_shift,licensing,market_rules,prior_eur_uah_fx_rate,publication_time,temporal_availability,timezone`;
- market-coupled B training runs: `0`;
- claim boundary: `market_execution_enabled=false`, no European rows in
  Ukrainian training.

Interpretation: the route and asset checks now work end to end, but the current
config does not yet provide the NBU EUR/UAH FX metadata and full governance
coverage needed to run a Ukrainian-plus-Poland B comparison. This is a valid
blocked evidence state, not a failed model result.

## Lag-24 + NBU Attempt, 2026-05-20

The token-backed route was rerun as
`data/research_runs/week3_dfl_entsoe_poland_lag24_nbu_approved_route/`.

- Dagster run id: `5c62678e-d310-4e86-90fc-d0bea701d3aa`;
- evidence check: `dfl_market_coupling_v2_plus_ablation_evidence` passed;
- ENTSO-E source window: `2024-12-31T00:00Z` through
  `2026-04-30T00:00Z`;
- NBU EUR/UAH metadata: `485` source-backed effective dates from
  `2024-12-31` to `2026-04-29`;
- lagged benchmark coverage: `11,638 / 11,638` timestamps;
- ENTSO-E gap handling: `141` small source gaps were filled by deterministic
  source-backed interpolation from adjacent ENTSO-E prices, not by Ukrainian
  target actuals;
- approved experimental feature column:
  `entsoe_pl_lag24_day_ahead_price_uah_mwh`;
- route status: `approved_for_experimental_ablation=true`;
- official training status: still `approved_for_official_training=false`
  because `domain_shift` is not validated;
- ablation status: `approved_route_pending_materialization` for both official
  global-panel NBEATSx source rows;
- market-coupled B training runs: `0`;
- claim boundary: `market_execution_enabled=false`, no European rows in
  Ukrainian training.

Interpretation: token/source access, full timestamp coverage, publication-time
mechanics for the lagged feature, timezone/DST mapping, licensing/rule toggles,
and prior-known NBU EUR/UAH normalization are now sufficient for a controlled
experimental ablation route. This is not yet a model-result improvement. The
next materialization must train/evaluate the Ukrainian-plus-Poland B variant and
can only replace Ukrainian-only V2+ if it beats the frozen V2+ strict LP/oracle
gate without median or rolling-robustness degradation.

## Lag-24 B-Variant Comparison, 2026-05-20

The controlled Ukrainian-plus-Poland B variant was then materialized as
`data/research_runs/week3_dfl_entsoe_poland_lag24_b_variant_comparison/`.

- Dagster run id: `a32de660-a3be-4e04-b907-fbdf96a9b45b`;
- evidence check: `dfl_market_coupling_v2_plus_ablation_evidence` passed;
- approved experimental feature column:
  `entsoe_pl_lag24_day_ahead_price_uah_mwh`;
- B variant: `dfl_market_coupled_schedule_value_learner_v2_plus_*`;
- ablation status: `comparison_complete` for both official global-panel
  NBEATSx source rows;
- calibrated source result: Ukrainian-only V2+ mean regret `174.77` UAH,
  Ukrainian-plus-Poland B mean regret `174.77` UAH;
- raw source result: Ukrainian-only V2+ mean regret `193.36` UAH,
  Ukrainian-plus-Poland B mean regret `193.36` UAH;
- rolling robustness: B preserved `4 / 4` windows because it fell back to
  Ukrainian-only V2+;
- ablation passed: `false`;
- blocker: `mean_not_improved`;
- claim boundary: `market_execution_enabled=false`, no European rows in
  Ukrainian training.

Interpretation: the Poland lag-24 feature route is now executable as a
controlled exogenous ablation, but the current prior-only market-coupled
selector did not find a safe improvement over frozen Ukrainian-only V2+. The
result should be treated as useful negative evidence: source/governance access
is no longer the blocker for this lane, but the lagged Poland context did not
yet add decision value under the unchanged strict LP/oracle gate.

## Rich Prior-Safe Poland Regime Features, 2026-05-20

The next Poland ablation expanded the lagged feature panel beyond a single
price level. The upstream `entsoe_poland_lagged_feature_candidate_frame` now
emits prior-safe regime diagnostics derived only from the lagged ENTSO-E Poland
source series and prior-known NBU EUR/UAH metadata:

- `entsoe_pl_lag24_day_ahead_price_uah_mwh`;
- `entsoe_pl_lag24_delta_1h_uah_mwh`;
- `entsoe_pl_lag24_delta_24h_uah_mwh`;
- `entsoe_pl_lag24_daily_spread_uah_mwh`;
- `entsoe_pl_lag24_daily_price_rank`;
- `entsoe_pl_lag24_daily_peak_hour_utc`;
- `entsoe_pl_lag24_daily_trough_hour_utc`.

These fields are not European training rows. They are deterministic
point-in-time transformations of the same lagged Poland feature lane. The
experimental selector may use them only inside the controlled B ablation, while
official training remains blocked until domain-shift validation passes.

The richer B comparison is stored locally as
`data/research_runs/week3_dfl_entsoe_poland_rich_prior_safe_b_variant_comparison/`.

- Dagster run id: `3fe654b3-43e3-471d-9b36-2be5baf16477`;
- evidence check: `dfl_market_coupling_v2_plus_ablation_evidence` passed;
- lagged frame shape: `11,638` rows and `49` columns;
- primary lagged coverage: `full_lagged_feature_coverage`, `source_backed=true`;
- approved experimental feature column:
  `entsoe_pl_lag24_day_ahead_price_uah_mwh`;
- richer selector profiles tested on prior/train anchors:
  `lag24_level`, `lag24_daily_spread`, `lag24_delta_1h`,
  `lag24_peak_timing`, and `lag24_level_spread`;
- B selector fallback rows: `10 / 10` tenant/source rows;
- calibrated source result: Ukrainian-only V2+ mean regret `174.77` UAH,
  Ukrainian-plus-Poland B mean regret `174.77` UAH;
- raw source result: Ukrainian-only V2+ mean regret `193.36` UAH,
  Ukrainian-plus-Poland B mean regret `193.36` UAH;
- rolling robustness: B preserved `4 / 4` windows by falling back to
  Ukrainian-only V2+;
- ablation passed: `false`;
- blocker: `mean_not_improved`;
- claim boundary: `market_execution_enabled=false`, no European rows in
  Ukrainian training.

Interpretation: richer Poland spreads, deltas, ranks, and peak/trough timing
features are now available as prior-safe experimental context. However, the
train/prior evidence showed that the market-coupled candidate choices were
worse than frozen Ukrainian-only V2+ for every tenant/source row. The fallback
therefore behaved correctly. This result narrows the next market-coupling
problem: the route is no longer blocked by source access or timestamp mechanics,
but this Poland-only lagged regime signal is not yet strong enough to improve
strict LP/oracle regret.

Source capture:
[entsoe-poland-lag24-nbu-source-capture-2026-05-20.md](../sources/entsoe-poland-lag24-nbu-source-capture-2026-05-20.md).

## Experimental NBEATSx/TFT Training Route, 2026-05-20

The richer Poland feature lane can now be used by official global-panel
NBEATSx and TFT as experimental known-future covariates. This does not change
the thesis headline route. The new assets are additive and explicitly labelled
as experimental ablation evidence:

- `official_global_panel_poland_lag24_experimental_training_frame`;
- `nbeatsx_official_global_panel_poland_lag24_experimental_price_forecast`;
- `tft_official_global_panel_poland_lag24_experimental_price_forecast`.

The training frame carries the seven prior-safe Poland columns in
`known_future_feature_columns_csv`:

- `entsoe_pl_lag24_day_ahead_price_uah_mwh`;
- `entsoe_pl_lag24_delta_1h_uah_mwh`;
- `entsoe_pl_lag24_delta_24h_uah_mwh`;
- `entsoe_pl_lag24_daily_spread_uah_mwh`;
- `entsoe_pl_lag24_daily_price_rank`;
- `entsoe_pl_lag24_daily_peak_hour_utc`;
- `entsoe_pl_lag24_daily_trough_hour_utc`.

NBEATSx receives these columns through the existing NeuralForecast
`futr_exog_list` path. TFT receives the same columns through the existing
PyTorch Forecasting `time_varying_known_reals` path. In both cases the model
outputs are renamed to separate research candidates:
`nbeatsx_official_global_panel_poland_lag24_experimental_v1` and
`tft_official_global_panel_poland_lag24_experimental_v1`.

Tracked screen config:
[real_data_official_global_panel_poland_lag24_experimental_forecast_week3.yaml](../../configs/real_data_official_global_panel_poland_lag24_experimental_forecast_week3.yaml).

Manual materialization:

```powershell
docker compose exec -T dagster-webserver uv run dagster asset materialize -m smart_arbitrage.defs --select observed_market_price_history_bronze,tenant_historical_weather_bronze,real_data_benchmark_silver_feature_frame,forecast_afe_feature_catalog_frame,market_coupling_temporal_availability_frame,entsoe_neighbor_market_query_spec_frame,entsoe_neighbor_market_feature_candidate_frame,nbu_eur_uah_fx_metadata_frame,poland_neighbor_market_snapshot_bronze,poland_neighbor_market_snapshot_feature_candidate_frame,entsoe_poland_lagged_feature_candidate_frame,entsoe_poland_feature_governance_frame,official_forecast_exogenous_governance_frame,official_forecast_exogenous_feature_route_frame,official_global_panel_poland_lag24_experimental_training_frame,nbeatsx_official_global_panel_poland_lag24_experimental_price_forecast,tft_official_global_panel_poland_lag24_experimental_price_forecast -c configs/real_data_official_global_panel_poland_lag24_experimental_forecast_week3.yaml
```

Interpretation boundary: this route tests whether the lagged Poland regime
signal can improve the calibrated NBEATSx/TFT predictors before schedule/value
selection. It is not official headline training because domain-shift validation
is still pending. Any resulting schedules must still pass the frozen
Ukrainian-only V2+ comparator and the strict LP/oracle gate before they can be
used as thesis headline evidence.

Smoke materialization evidence:

- Dagster run id: `9ca621e7-9959-4b65-99fe-68ff4a2d7a15`;
- status: `RUN_SUCCESS`;
- scope: feature route, experimental global-panel training frame, NBEATSx
  experimental forecast, and TFT experimental forecast;
- runtime note: local host CUDA was available and used by the TFT screen;
- interpretation: adapter/training wiring is functional. This is not yet a
  strict LP/oracle schedule-value result and does not alter the V2+ headline.

Downstream strict LP/oracle schedule-value evidence is now captured in local
packet
`data/research_runs/week3_poland_lag24_experimental_schedule_value_near_miss/`.
The packet uses persisted rows from generated timestamp
`2026-05-20T12:10:06.716775+00:00` and compares the experimental forecast model
names against frozen Ukrainian-only calibrated V2+:

| Schedule/value row | Mean regret, UAH | Median regret, UAH | Interpretation |
|---|---:|---:|---|
| Frozen Ukrainian-only V2+ | `174.77` | `67.30` | headline comparator |
| Poland lag-24 NBEATSx V2+ | `184.66` | `65.16` | near miss; mean is `9.89` UAH worse |
| Poland lag-24 TFT V2+ | `218.12` | `105.50` | worse than headline comparator |

Both experimental schedule/value rows still beat `strict_similar_day`, but the
unchanged acceptance rule is stricter: a new route must beat frozen V2+ without
weakening the LP/oracle gate. The blocker is therefore
`mean_not_improved_vs_frozen_v2_plus`. This keeps the Poland lag-24 route as
source-backed experimental evidence, not as promoted thesis headline evidence.

The follow-up feature-representation branch keeps the same governance boundary
but adds cross-market prior-safe features to the Poland lag-24 panel:

- `entsoe_pl_lag24_ua_spread_uah_mwh` - Poland lag-24 price minus observed
  Ukrainian DAM price at the same lagged timestamp;
- `entsoe_pl_lag24_ua_spread_delta_24h_uah_mwh` - change in that cross-market
  spread versus the prior lagged day;
- `entsoe_pl_lag24_ua_spread_ratio` - spread scaled by the lagged Ukrainian
  DAM price.

These features are closer to the market-coupling hypothesis than Poland price
level alone because they describe relative pressure between the neighbor market
and the Ukrainian observed market state. They are still computed only from
timestamp `t - 24h` and earlier data, so they remain prior-safe for Ukrainian
timestamp `t`.

The follow-up materialization completed as Dagster run
`65d86cdd-435e-46d1-86ac-80f1ce960245` and is exported locally as
`data/research_runs/week3_poland_lag24_cross_market_experimental_schedule_value/`.
It did not pass the frozen V2+ replacement gate:

| Schedule/value row | Mean regret, UAH | Median regret, UAH | Interpretation |
|---|---:|---:|---|
| Frozen Ukrainian-only V2+ | `174.77` | `67.30` | headline comparator |
| Poland cross-market TFT V2+ | `188.26` | `71.06` | improved over lag-only TFT, but still worse than V2+ |
| Poland cross-market NBEATSx V2+ | `253.68` | `137.35` | worse than V2+ and worse than lag-only NBEATSx |

The best cross-market experimental result is therefore `13.49` UAH worse than
frozen Ukrainian-only V2+ (`-7.72%` improvement ratio versus the comparator).
This is useful negative evidence: relative Poland-vs-UA pressure features are
mechanically routed through official NBEATSx/TFT and help the TFT branch versus
the earlier lag-only TFT screen, but they still do not provide enough decision
value to replace Ukrainian-only V2+ under the unchanged strict LP/oracle gate.

The calibrated follow-up tested whether the same Poland-enhanced forecast rows
help after prior-only forecast calibration, before V2+ schedule/value
selection. The additive calibrated model names are
`nbeatsx_official_global_panel_poland_lag24_horizon_calibrated_v1` and
`tft_official_global_panel_poland_lag24_horizon_quantile_calibrated_v1`.
NBEATSx uses horizon-aware residual calibration. TFT uses the same
horizon/quantile-compatible calibration contract for the p50 Poland-enhanced
route; true p10/p90 Poland quantile routes remain future work. Calibration rows
are built from train/prior anchors only.

The screen materialized as Dagster run
`25ac4101-b557-42b0-8950-3613dc77ad4e` and is exported locally as
`data/research_runs/week3_poland_lag24_calibrated_experimental_schedule_value/`.
It is still a near miss, not a promoted replacement:

| Schedule/value row | Mean regret, UAH | Median regret, UAH | Interpretation |
|---|---:|---:|---|
| Frozen Ukrainian-only calibrated V2+ | `174.77` | `67.30` | headline comparator |
| Poland calibrated TFT V2+ | `181.93` | `44.29` | best Poland result; median improves, mean still worse |
| Poland raw TFT V2+ | `188.26` | `71.06` | cross-market raw TFT near miss |
| Poland calibrated NBEATSx V2+ | `233.37` | `126.81` | calibration helps raw NBEATSx but remains weak |
| Poland raw NBEATSx V2+ | `253.68` | `137.35` | weakest experimental row |

The best Poland-enhanced result is now calibrated TFT. It improves the previous
cross-market TFT screen (`188.26` -> `181.93` UAH) and materially improves
median regret (`44.29` UAH), but the unchanged promotion rule is based on mean
regret versus frozen V2+. The remaining blocker is
`mean_not_improved_vs_frozen_v2_plus`, with a `+7.16` UAH mean-regret gap.

Reusable export:

```powershell
.\.venv\Scripts\python.exe scripts\materialize_poland_lag24_experimental_schedule_value_packet.py `
  --comparison-frame-pickle .tmp_runtime\poland_lag24_experimental_schedule_value_export\comparison.pkl `
  --raw-strict-frame-pickle .tmp_runtime\poland_lag24_experimental_schedule_value_export\raw_strict.pkl `
  --run-slug week3_poland_lag24_experimental_schedule_value_near_miss
```

Cross-market export:

```powershell
.\.venv\Scripts\python.exe scripts\materialize_poland_lag24_experimental_schedule_value_packet.py `
  --comparison-frame-pickle .tmp_runtime\poland_lag24_cross_market_export\comparison.pkl `
  --raw-strict-frame-pickle .tmp_runtime\poland_lag24_cross_market_export\raw_strict.pkl `
  --run-slug week3_poland_lag24_cross_market_experimental_schedule_value `
  --dagster-run-id 65d86cdd-435e-46d1-86ac-80f1ce960245
```

Calibrated export:

```powershell
.\.venv\Scripts\python.exe scripts\materialize_poland_lag24_experimental_schedule_value_packet.py `
  --comparison-frame-pickle .tmp_runtime\poland_lag24_calibrated_export\dfl_poland_lag24_calibrated_vs_v2_plus_comparison_frame.pkl `
  --raw-strict-frame-pickle .tmp_runtime\poland_lag24_calibrated_export\official_global_panel_poland_lag24_experimental_horizon_calibrated_strict_lp_benchmark_frame.pkl `
  --run-slug week3_poland_lag24_calibrated_experimental_schedule_value `
  --dagster-run-id 25ac4101-b557-42b0-8950-3613dc77ad4e
```
