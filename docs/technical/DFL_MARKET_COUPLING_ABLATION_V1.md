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

As of 2026-05-17, the ENTSO-E File Library credentialed smoke is source-backed:
`2026_01_EnergyPrices_12.1.D_r3.csv` was downloaded and normalized to `2,976`
Poland price rows. This improves source evidence, but it does not by itself
approve training use. The ablation remains blocked until the route also has
license approval, point-in-time EUR/UAH FX, timezone/DST, market-rule mapping,
and domain-shift evidence.

The follow-up governance-closure slice hourly-aligns those Poland rows before
approval. It adds `poland_neighbor_market_hourly_feature_frame` and
`entsoe_poland_governance_closure_frame`. The expected current result is still
blocked: the downloaded FMS file update timestamp is later than the Ukrainian
decision-anchor boundary, and prior-known EUR/UAH FX plus licensing,
timezone/DST, market-rule, and domain-shift evidence are incomplete. Therefore
no Ukrainian-plus-Poland B training run is admitted yet.

## Assets

| Asset | Purpose |
|---|---|
| `entsoe_neighbor_market_aligned_feature_panel_frame` | Aligns Poland-first ENTSO-E feature candidates to tenant benchmark timestamps while keeping `training_use_allowed=false`. |
| `entsoe_poland_feature_governance_frame` | Checks the Poland route for source-backed rows, publication time, timezone/DST, prior-known EUR/UAH FX, licensing, market-rule mapping, and domain-shift validation before any official training approval. |
| `poland_neighbor_market_snapshot_bronze` | Parses a no-token local/public Poland CSV export with source URL, retrieval timestamp, publication timestamp, license status, and checksum. |
| `poland_neighbor_market_snapshot_feature_candidate_frame` | Converts no-token Poland snapshots into the same feature-candidate contract used by the ENTSO-E route. |
| `poland_neighbor_market_hourly_feature_frame` | Aggregates source-backed Poland rows to hourly feature evidence without approving training use. |
| `entsoe_poland_governance_closure_frame` | Records the Poland-specific governance closure result and exact blockers. |
| `dfl_market_coupling_v2_plus_ablation_frame` | Compares Ukrainian-only V2+ against Ukrainian plus approved neighbor-market features, or emits `ablation_status=blocked_by_governance`. |
| `dfl_market_coupling_v2_plus_ablation_evidence` | Dagster asset check for thesis-grade claim boundaries, blocked-training behavior, and strict comparison semantics. |

Tracked config:
[real_data_dfl_market_coupling_ablation_week3.yaml](../../configs/real_data_dfl_market_coupling_ablation_week3.yaml).

Poland governance-completion config:
[real_data_dfl_entsoe_poland_feature_ablation_week3.yaml](../../configs/real_data_dfl_entsoe_poland_feature_ablation_week3.yaml).

Token-backed Poland governance config:
[real_data_dfl_entsoe_poland_feature_ablation_token_week3.yaml](../../configs/real_data_dfl_entsoe_poland_feature_ablation_token_week3.yaml).

No-token Poland snapshot config:
[real_data_dfl_poland_snapshot_ablation_week3.yaml](../../configs/real_data_dfl_poland_snapshot_ablation_week3.yaml).

Poland hourly governance-closure config:
[real_data_dfl_entsoe_poland_governance_closure_week3.yaml](../../configs/real_data_dfl_entsoe_poland_governance_closure_week3.yaml).

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
docker compose exec -T dagster-webserver uv run dagster asset materialize -m smart_arbitrage.defs --select forecast_afe_feature_catalog_frame,market_coupling_temporal_availability_frame,entsoe_neighbor_market_query_spec_frame,entsoe_neighbor_market_feature_candidate_frame,entsoe_poland_feature_governance_frame,entsoe_neighbor_market_aligned_feature_panel_frame,official_forecast_exogenous_governance_frame,official_forecast_exogenous_feature_route_frame,dfl_official_global_panel_schedule_value_learner_v2_plus_strict_lp_benchmark_frame,dfl_official_global_panel_schedule_value_learner_v2_plus_robustness_frame,dfl_market_coupling_v2_plus_ablation_frame -c configs/real_data_dfl_entsoe_poland_feature_ablation_week3.yaml
```

Validate definitions locally, then run the asset check from Dagster UI or the
Dagster asset-check surface after materialization:

```powershell
uv run dg check defs
```

## Local Evidence Packet Export

After materialization, export the ablation frame from Dagster storage and build
the local evidence packet:

```powershell
$RunSlug = "week3_dfl_market_coupling_ablation_v1"
$ExportDir = Join-Path "data\research_runs" $RunSlug
New-Item -ItemType Directory -Force -Path $ExportDir | Out-Null
$ContainerId = docker compose ps -q dagster-webserver
docker cp "${ContainerId}:/opt/dagster/dagster_home/storage/dfl_market_coupling_v2_plus_ablation_frame" (Join-Path $ExportDir "dfl_market_coupling_v2_plus_ablation_frame.pkl")
.\.venv\Scripts\python.exe scripts\materialize_market_coupling_ablation_packet.py --ablation-frame-pickle (Join-Path $ExportDir "dfl_market_coupling_v2_plus_ablation_frame.pkl") --run-slug $RunSlug
```

The exporter writes:

- `dfl_market_coupling_v2_plus_ablation_summary.json`;
- `dfl_market_coupling_v2_plus_ablation_summary.md`;
- `dfl_market_coupling_v2_plus_ablation_rows.csv`.

The export is allowed when the ablation evidence check passes. A
`blocked_by_governance` row is valid exportable evidence because it proves that
no market-coupled B variant was trained without a fully approved exogenous
feature route. Failed evidence checks are refused.

## Materialized Evidence

The 2026-05-16 materialization closed the slice with the expected governance
block:

- Dagster run id: `b1026e47-249f-463d-a60d-b4f01b3897cd`;
- local packet:
  `data/research_runs/week3_dfl_market_coupling_ablation_v1/`;
- ablation rows: `2`;
- status counts: `blocked_by_governance=2`;
- approved external feature columns: none;
- market-coupled B training runs: `0`;
- passing ablation rows: `0`;
- evidence check:
  `dfl_market_coupling_v2_plus_ablation_evidence` passed;
- `market_execution_enabled=false`.

Blocked feature columns:

- `entsoe_neighbor_day_ahead_price_context`;
- `europe_generation_mix_context_placeholder`;
- `european_power_system_time_series_placeholder`;
- `nord_pool_price_context_placeholder`;
- `pricefm_european_price_context`;
- `thief_temporal_hierarchy_context`.

Training blockers reported by the packet:

- `currency`;
- `domain_shift`;
- `licensing`;
- `market_rules`;
- `temporal_availability`;
- `timezone`.

This means the ablation did not test whether neighbor-market features improve
regret yet. It tested and passed the governance behavior: incomplete external
feature governance blocks Ukrainian-plus-neighbor training instead of silently
mixing EU-derived signals into the thesis result.

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

## Poland Governance Completion Evidence

The 2026-05-17 Poland-specific rerun used
[real_data_dfl_entsoe_poland_feature_ablation_week3.yaml](../../configs/real_data_dfl_entsoe_poland_feature_ablation_week3.yaml)
and the new `entsoe_poland_feature_governance_frame`:

- Dagster run id: `65c87210-36f3-4491-add7-995fa0214d86`;
- local packet:
  `data/research_runs/week3_dfl_entsoe_poland_feature_ablation_v1/`;
- status counts: `blocked_by_governance=2`;
- approved feature columns: none;
- blocked Poland feature column: `entsoe_pl_day_ahead_price_uah_mwh`;
- market-coupled B training runs: `0`;
- evidence check passed;
- `market_execution_enabled=false`.

The new packet reports more precise blockers:

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

Therefore the next external-feature action is operational governance, not model
selection: after token/source access is available, attach publication-time
evidence, attach prior-known EUR/UAH FX, and resolve licensing,
market-rule/timezone, and domain-shift checks before rerunning any approved
ablation.

For future reruns, use the repo-local wrapper instead of manual log/copy steps:

```powershell
.\scripts\run-entsoe-poland-governance-ablation.ps1 -DryRun
.\scripts\run-entsoe-poland-governance-ablation.ps1 -RunSlug week3_dfl_entsoe_poland_feature_ablation_v1
```

The wrapper writes a receipt, runs the exact asset selection, copies the
materialized ablation frame from Dagster storage, and exports the packet.

## Token-Backed ENTSO-E Source Evidence

The 2026-05-20 local rerun used a real local ENTSO-E token through the lowercase
`.env` key `entsoe_token`, accepted as a safe alias for `ENTSOE_TOKEN`. The
wrapper passes only the variable name into Docker (`-e ENTSOE_TOKEN`) and writes
only `entsoe_token_available=true` to the receipt.

Run:

```powershell
.\scripts\run-entsoe-poland-governance-ablation.ps1 -ConfigPath configs\real_data_dfl_entsoe_poland_feature_ablation_token_week3.yaml -RunSlug week3_dfl_entsoe_poland_token_source_governance_v3
```

Evidence:

- Dagster run id: `2a1983fd-3b54-4020-9d76-a8fc6c36ef90`;
- local packet:
  `data/research_runs/week3_dfl_entsoe_poland_token_source_governance_v3/`;
- ENTSO-E API source-backed Poland candidate rows: `186`;
- token/source-backed-sample blockers cleared;
- status counts: `blocked_by_governance=2`;
- approved feature columns: none;
- market-coupled B training runs: `0`;
- evidence check passed;
- `market_execution_enabled=false`.

Remaining blockers:

- `publication_time`;
- `prior_eur_uah_fx_rate`;
- `currency`;
- `timezone`;
- `licensing`;
- `market_rules`;
- `domain_shift`;
- `temporal_availability`.

This is a stronger blocked packet than the 2026-05-17 run. It proves that source
access is no longer the blocker, but the feature is still not allowed into
training because point-in-time economic/governance evidence is incomplete.

## Experimental-Ablation Route

The feature route now distinguishes controlled ablation readiness from official
training approval:

- `approved_for_experimental_ablation=true` means the point-in-time mechanics
  are ready, but `domain_shift` is still pending. This may produce
  `ablation_status=approved_route_pending_materialization`.
- `approved_for_official_training=true` is stricter. It requires the same
  mechanics plus a passing Ukrainian holdout/domain-shift validation result.
- `training_use_allowed` and `feature_use_allowed` remain false until official
  approval. This keeps accidental NBEATSx/TFT/DFL training from consuming a
  not-yet-validated external feature.

The next market-coupling implementation target has now been added as
`entsoe_poland_lagged_feature_candidate_frame`. It emits the prior-safe
candidate column `entsoe_pl_lag24_day_ahead_price_uah_mwh`, where Ukrainian
timestamp `t` receives the Poland ENTSO-E price from `t - 24h`. The row is only
eligible for controlled ablation when the benchmark has full timestamp coverage,
the NBU EUR/UAH FX rate is source-labelled and timestamped before the Ukrainian
anchor, and the remaining timezone/DST, licensing, and market-rule controls are
ready. This clears the mechanics for a leak-safe lagged feature without
assuming that same-delivery Polish DAM results are always published before the
Ukrainian decision anchor.

The first lag-24 materialization ran on 2026-05-20 as
`week3_dfl_entsoe_poland_lag24_governance_attempt` with Dagster run
`e004a33f-8851-4451-9da5-83ddf8b43154`. The ablation evidence check passed and
exported a valid packet, but the state remained `blocked_by_governance` for both
official NBEATSx source rows. No market-coupled B model was trained. The blocker
list is now precise:
`currency,domain_shift,licensing,market_rules,prior_eur_uah_fx_rate,publication_time,temporal_availability,timezone`.

## No-Token Poland Snapshot Route

If an ENTSO-E token is unavailable, the project now has a source-neutral local
snapshot route:
[POLAND_NEIGHBOR_MARKET_SNAPSHOT.md](POLAND_NEIGHBOR_MARKET_SNAPSHOT.md).
It supports manual/exported CSV snapshots from ENTSO-E File Library, PSE public
exports, or Instrat Polish DAM context, provided the source URL, retrieval time,
publication timestamp, license status, and raw file checksum are recorded.

This route does not scrape protected pages and does not weaken governance:
manual/public snapshot rows set `security_token_required=false`, but they still
feed the same `entsoe_poland_feature_governance_frame` and remain blocked until
publication-time, prior-known EUR/UAH FX, timezone/DST, licensing, market-rule,
and domain-shift gates pass.

The hourly governance-closure layer is the stricter source-backed audit before
any ablation rerun. It can close evidence as `blocked_by_governance`, but it
does not route Poland features into official training unless all point-in-time
and governance checks pass.

The 2026-05-17 closure materialization produced `744` hourly Poland feature
rows from `2,976` source-backed 15-minute File Library rows. The closure check
passed with `approved_for_official_training=false`,
`market_execution_enabled=false`, and blockers:

- `publication_time`;
- `timezone_dst_mapping`;
- `prior_eur_uah_fx`;
- `licensing`;
- `market_rule_mapping`;
- `domain_shift`;
- `temporal_availability`.

The closure packet is exported with:

```powershell
.\.venv\Scripts\python.exe scripts\materialize_entsoe_poland_governance_closure_packet.py `
  --snapshot-frame-pickle .tmp_dagster_home_entsoe_poland_governance_closure\storage\poland_neighbor_market_snapshot_bronze `
  --hourly-feature-frame-pickle .tmp_dagster_home_entsoe_poland_governance_closure\storage\poland_neighbor_market_hourly_feature_frame `
  --governance-closure-frame-pickle .tmp_dagster_home_entsoe_poland_governance_closure\storage\entsoe_poland_governance_closure_frame `
  --run-slug week3_entsoe_poland_governance_closure `
  --dagster-run-id 7fb842f0-1aa2-4f5c-afd4-48d055e9bda0
```

Local packet:
`data/research_runs/week3_entsoe_poland_governance_closure/`.
