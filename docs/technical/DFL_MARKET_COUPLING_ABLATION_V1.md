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

## Assets

| Asset | Purpose |
|---|---|
| `entsoe_neighbor_market_aligned_feature_panel_frame` | Aligns Poland-first ENTSO-E feature candidates to tenant benchmark timestamps while keeping `training_use_allowed=false`. |
| `entsoe_poland_feature_governance_frame` | Checks the Poland route for source-backed rows, publication time, timezone/DST, prior-known EUR/UAH FX, licensing, market-rule mapping, and domain-shift validation before any official training approval. |
| `poland_neighbor_market_snapshot_bronze` | Parses a no-token local/public Poland CSV export with source URL, retrieval timestamp, publication timestamp, license status, and checksum. |
| `poland_neighbor_market_snapshot_feature_candidate_frame` | Converts no-token Poland snapshots into the same feature-candidate contract used by the ENTSO-E route. |
| `dfl_market_coupling_v2_plus_ablation_frame` | Compares Ukrainian-only V2+ against Ukrainian plus approved neighbor-market features, or emits `ablation_status=blocked_by_governance`. |
| `dfl_market_coupling_v2_plus_ablation_evidence` | Dagster asset check for thesis-grade claim boundaries, blocked-training behavior, and strict comparison semantics. |

Tracked config:
[real_data_dfl_market_coupling_ablation_week3.yaml](../../configs/real_data_dfl_market_coupling_ablation_week3.yaml).

Poland governance-completion config:
[real_data_dfl_entsoe_poland_feature_ablation_week3.yaml](../../configs/real_data_dfl_entsoe_poland_feature_ablation_week3.yaml).

No-token Poland snapshot config:
[real_data_dfl_poland_snapshot_ablation_week3.yaml](../../configs/real_data_dfl_poland_snapshot_ablation_week3.yaml).

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
selection: provide a local ENTSO-E token, fetch a source-backed Poland sample,
attach publication-time evidence, attach prior-known EUR/UAH FX, and resolve
licensing/market-rule/domain-shift checks before rerunning the ablation.

For future reruns, use the repo-local wrapper instead of manual log/copy steps:

```powershell
.\scripts\run-entsoe-poland-governance-ablation.ps1 -DryRun
.\scripts\run-entsoe-poland-governance-ablation.ps1 -RunSlug week3_dfl_entsoe_poland_feature_ablation_v1
```

The wrapper writes a receipt, runs the exact asset selection, copies the
materialized ablation frame from Dagster storage, and exports the packet.

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
