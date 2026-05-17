# Poland Neighbor-Market Snapshot Route

Date: 2026-05-17

This slice adds a no-token/manual route and a credentialed ENTSO-E File Library
smoke route for Poland neighbor-market evidence without weakening the
market-coupling governance gate. It is not a scraper that bypasses ENTSO-E
access. It accepts local/source-backed CSV snapshots, parses them into the
existing Poland feature-candidate contract, and then lets the existing
`entsoe_poland_feature_governance_frame` decide whether the feature can be
routed into official global-panel training.

## Claim Boundary

- Source rows are external exogenous feature candidates only.
- No European rows enter Ukrainian target training.
- `training_use_allowed=false` and `feature_use_allowed=false` at snapshot and
  candidate level.
- `market_execution_enabled=false`.
- The current V2+ Offline Strategy Promotion result remains Ukrainian-only until
  the route passes governance and an ablation beats V2+.

## Supported Source Methods

The parser is intentionally source-neutral. The raw input is a local CSV with at
least:

- `delivery_timestamp_utc` or compatible timestamp column;
- `price_eur_mwh` or compatible price column.

The source metadata must record:

- source URL;
- access method, for example `manual_export_csv`;
- retrieval timestamp;
- source publication timestamp, if known;
- license status;
- raw file SHA-256 checksum.

Practical source candidates:

| Source | Use in this route | Notes |
|---|---|---|
| ENTSO-E File Library | Manual/exported CSV snapshot or future FMS API token route. | The official guide states the File Library requires login, supports CSV extracts, has a bearer-token API, and includes an export log for update timestamps. |
| PSE public report/export pages | Public Polish system/market context snapshot if the needed series is exportable. | Still requires source terms, timestamp, and unit mapping. |
| Instrat Polish DAM page | Research/non-commercial Polish DAM context snapshot if the license boundary is acceptable. | Still blocked unless license, publication-time, FX, and market-rule checks pass. |

## Credentialed ENTSO-E FMS Smoke

The repository includes a secret-safe helper for source-backed File Library
smoke tests:

```powershell
.\.venv\Scripts\python.exe scripts\fetch_entsoe_file_library_energy_prices.py `
  --env-file .env `
  --month 2026-01 `
  --output-dir data\external_sources\poland\entsoe_fms `
  --config-output .tmp_runtime\poland_snapshot_entsoe_fms_smoke.yaml
```

The helper reads `entsoe_email` / `entsoe_password` from `.env`, requests the
ENTSO-E Keycloak bearer token in memory, lists files through FMS
`listFileMetadata`, downloads one `EnergyPrices_12.1.D_r3` monthly file, and
normalizes Poland rows to:

- `delivery_timestamp_utc`;
- `price_eur_mwh`.

The token and password are never written to receipts, configs, logs, or evidence
packets. The generated CSV and receipt live under ignored local evidence paths.

The 2026-05-17 smoke fetched:

- file: `2026_01_EnergyPrices_12.1.D_r3.csv`;
- normalized Poland rows: `2,976`;
- selected file update timestamp: `2026-05-02T18:55:16.465Z`;
- local receipt:
  `data/external_sources/poland/entsoe_fms/entsoe-fms-smoke-receipt.json`;
- local evidence packet:
  `data/research_runs/week3_poland_neighbor_market_snapshot_entsoe_fms_smoke/`.

The snapshot and feature-candidate asset checks passed, but the feature remains
blocked from training until licensing, publication-time, FX, timezone/DST,
market-rule, and domain-shift governance is completed.

## Hourly Feature And Governance Closure

The source-backed File Library rows are 15-minute records, while the Ukrainian
evidence panel and strict LP/oracle gate are hourly. The governance-closure
slice therefore adds a narrow hourly evidence layer before any training route:

| Asset / check | Purpose |
|---|---|
| `poland_neighbor_market_hourly_feature_frame` | Aggregates source-backed Poland price rows to hourly `entsoe_pl_day_ahead_price_eur_mwh_hourly` evidence. |
| `poland_neighbor_market_hourly_feature_evidence` | Confirms hourly rows are source-backed, research-only, and still have `training_use_allowed=false`. |
| `entsoe_poland_governance_closure_frame` | Emits one readiness row with publication-time, prior EUR/UAH FX, timezone/DST, licensing, market-rule, and domain-shift blockers. |
| `entsoe_poland_governance_closure_evidence` | Allows a blocked governance row to pass as evidence, but rejects inconsistent approval flags. |

The 2026-05-17 FMS smoke can be hourly-aligned, but the selected file update
timestamp (`2026-05-02T18:55:16.465Z`) is later than the current Ukrainian
decision-anchor boundary (`2025-12-31T12:00:00Z`). Without separate
publication-rule evidence proving the day-ahead value was available before each
Ukrainian anchor, the route must stay blocked. Prior-known EUR/UAH FX,
licensing, timezone/DST, market-rule, and domain-shift evidence are also still
required.

## Assets And Checks

| Asset / check | Purpose |
|---|---|
| `poland_neighbor_market_snapshot_bronze` | Parses a local/public CSV snapshot into source-backed Bronze rows. |
| `poland_neighbor_market_snapshot_evidence` | Validates source rows are source-only, no-token, no-training evidence. |
| `poland_neighbor_market_snapshot_feature_candidate_frame` | Converts snapshot rows into the existing `entsoe_neighbor_day_ahead_price_context` candidate schema. |
| `poland_neighbor_market_snapshot_feature_candidate_evidence` | Reuses the feature-candidate evidence check to keep rows out of training before governance passes. |
| `poland_neighbor_market_hourly_feature_frame` | Aggregates source-backed Poland rows to hourly feature evidence for the Ukrainian hourly panel. |
| `entsoe_poland_governance_closure_frame` | Records the stronger Poland-specific blocker set before the route can be approved. |
| `entsoe_poland_feature_governance_frame` | Now accepts both token/API candidates and no-token snapshot candidates, then applies the same publication-time, FX, timezone, licensing, market-rule, and domain-shift gates. |
| `official_forecast_exogenous_feature_route_frame` | Remains the only approved/blocked interface into official global-panel training. |

## Why The Token Block Is Not Weakened

The existing ENTSO-E API path still requires a token for source-backed API rows.
The new snapshot path is separate: it records `security_token_required=false`
only for manually exported or public-source CSV snapshots. Those rows still do
not train until governance passes.

This means there are two valid source-backed routes:

1. ENTSO-E API/File Library route with credentials handled through `.env` or
   environment variables and no token written to evidence artifacts.
2. Manual/public snapshot route with raw local file checksum and source metadata.

Both routes converge into the same governance and ablation gate.

## Config

Tracked config:
[real_data_dfl_poland_snapshot_ablation_week3.yaml](../../configs/real_data_dfl_poland_snapshot_ablation_week3.yaml).

Poland hourly governance-closure config:
[real_data_dfl_entsoe_poland_governance_closure_week3.yaml](../../configs/real_data_dfl_entsoe_poland_governance_closure_week3.yaml).

Example source section:

```yaml
ops:
  poland_neighbor_market_snapshot_bronze:
    config:
      snapshot_csv_path: "data/external_sources/poland/poland-dam-price-export.csv"
      source_url: "https://transparencyplatform.zendesk.com/hc/en-us/articles/35960137882129-File-Library-Guide"
      source_access_method: "manual_export_csv"
      source_retrieved_at_utc: "2026-05-17T10:00:00+00:00"
      source_publication_timestamp_utc: "2025-12-31T11:00:00+00:00"
      source_license_status: "research_non_commercial_review_required"
```

## Evidence Packet Export

After materialization, copy the two Dagster-stored frames and export:

```powershell
.\.venv\Scripts\python.exe scripts\materialize_poland_neighbor_market_snapshot_packet.py `
  --snapshot-frame-pickle data\research_runs\week3_poland_neighbor_market_snapshot_no_token\poland_neighbor_market_snapshot_bronze.pkl `
  --feature-candidate-frame-pickle data\research_runs\week3_poland_neighbor_market_snapshot_no_token\poland_neighbor_market_snapshot_feature_candidate_frame.pkl `
  --run-slug week3_poland_neighbor_market_snapshot_no_token
```

Artifacts:

- `poland_neighbor_market_snapshot_summary.json`;
- `poland_neighbor_market_snapshot_summary.md`;
- `poland_neighbor_market_snapshot_rows.csv`;
- `poland_neighbor_market_feature_candidate_rows.csv`.

The packet is allowed only when both evidence checks pass. A blocked governance
state is valid; a failed evidence check is not.

For the 2026-05-17 credentialed FMS smoke, the host materialization command was:

```powershell
$env:DAGSTER_HOME = (Resolve-Path .).Path + '\.tmp_dagster_home_entsoe_fms_smoke'
.\.venv\Scripts\dagster.exe asset materialize -m smart_arbitrage.defs `
  --select poland_neighbor_market_snapshot_bronze,poland_neighbor_market_snapshot_feature_candidate_frame `
  -c .tmp_runtime\poland_snapshot_entsoe_fms_smoke.yaml
```

This produced `2,976` source-backed snapshot rows and `2,976` feature-candidate
rows. Both attached asset checks passed, and `market_execution_enabled=false`
remained unchanged.

For the hourly governance closure, run:

```powershell
$env:DAGSTER_HOME = (Resolve-Path .).Path + '\.tmp_dagster_home_entsoe_poland_governance_closure'
.\.venv\Scripts\dagster.exe asset materialize -m smart_arbitrage.defs `
  --select poland_neighbor_market_snapshot_bronze,poland_neighbor_market_hourly_feature_frame,entsoe_poland_governance_closure_frame `
  -c configs\real_data_dfl_entsoe_poland_governance_closure_week3.yaml
```

The expected current status is a passing evidence check with
`approved_for_official_training=false` and blockers including publication time,
prior EUR/UAH FX, licensing, timezone/DST, market-rule mapping, domain shift,
and temporal availability. That is evidence closure, not feature admission.

The 2026-05-17 governance-closure materialization produced:

- Dagster run id: `7fb842f0-1aa2-4f5c-afd4-48d055e9bda0`;
- hourly Poland feature rows: `744`;
- source intervals represented: `2,976`;
- `approved_for_official_training=false`;
- `market_execution_enabled=false`;
- checks passed:
  `poland_neighbor_market_snapshot_evidence`,
  `poland_neighbor_market_hourly_feature_evidence`,
  `entsoe_poland_governance_closure_evidence`;
- blockers:
  `publication_time`, `timezone_dst_mapping`, `prior_eur_uah_fx`,
  `licensing`, `market_rule_mapping`, `domain_shift`,
  `temporal_availability`.

## Next Step

If a real Poland CSV export is available, materialize this route with its
source metadata and then rerun:

1. `entsoe_poland_feature_governance_frame`;
2. `official_forecast_exogenous_feature_route_frame`;
3. `dfl_market_coupling_v2_plus_ablation_frame`.

If the route approves, the next experiment is a Ukrainian-only V2+ versus
Ukrainian-plus-governed-Poland V2+ ablation under the unchanged strict LP/oracle
gate. If it remains blocked, the packet should state the exact blocker rather
than training a B variant.
