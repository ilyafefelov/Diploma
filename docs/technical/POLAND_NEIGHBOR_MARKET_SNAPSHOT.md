# Poland Neighbor-Market Snapshot Route

Date: 2026-05-17

This slice adds a no-token route for Poland neighbor-market evidence without
weakening the market-coupling governance gate. It is not a scraper that bypasses
ENTSO-E access. It accepts local/source-backed CSV snapshots, parses them into
the existing Poland feature-candidate contract, and then lets the existing
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

## Assets And Checks

| Asset / check | Purpose |
|---|---|
| `poland_neighbor_market_snapshot_bronze` | Parses a local/public CSV snapshot into source-backed Bronze rows. |
| `poland_neighbor_market_snapshot_evidence` | Validates source rows are source-only, no-token, no-training evidence. |
| `poland_neighbor_market_snapshot_feature_candidate_frame` | Converts snapshot rows into the existing `entsoe_neighbor_day_ahead_price_context` candidate schema. |
| `poland_neighbor_market_snapshot_feature_candidate_evidence` | Reuses the feature-candidate evidence check to keep rows out of training before governance passes. |
| `entsoe_poland_feature_governance_frame` | Now accepts both token/API candidates and no-token snapshot candidates, then applies the same publication-time, FX, timezone, licensing, market-rule, and domain-shift gates. |
| `official_forecast_exogenous_feature_route_frame` | Remains the only approved/blocked interface into official global-panel training. |

## Why The Token Block Is Not Weakened

The existing ENTSO-E API path still requires a token for source-backed API rows.
The new snapshot path is separate: it records `security_token_required=false`
only for manually exported or public-source CSV snapshots. Those rows still do
not train until governance passes.

This means there are two valid source-backed routes:

1. ENTSO-E API/File Library route with credentials handled through environment
   variables and no token written to evidence artifacts.
2. Manual/public snapshot route with raw local file checksum and source metadata.

Both routes converge into the same governance and ablation gate.

## Config

Tracked config:
[real_data_dfl_poland_snapshot_ablation_week3.yaml](../../configs/real_data_dfl_poland_snapshot_ablation_week3.yaml).

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
