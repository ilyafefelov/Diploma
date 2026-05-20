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
