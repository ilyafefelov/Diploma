# Poland Neighbor-Market Snapshot Source Capture

Date captured: 2026-05-17

Purpose: support the no-token Poland neighbor-market snapshot route used as
governed exogenous feature evidence for future Ukrainian V2+ ablations.

## Sources

1. ENTSO-E File Library Guide
   URL: <https://transparencyplatform.zendesk.com/hc/en-us/articles/35960137882129-File-Library-Guide>
   Captured facts: File Library data is available as CSV extracts; website access
   requires login; API access retrieves a bearer token through the Keycloak token
   endpoint using Transparency Platform account credentials; file-library APIs
   use bearer authorization; download limits are documented; files are
   tab-delimited UTF-8 CSV; the export log records recent creation/update times.

2. ENTSO-E File Library Python download example
   URL: <https://transparencyplatform.zendesk.com/hc/en-us/articles/36523801579025-How-to-download-file-from-FMS-via-Python>
   Captured facts: the official example uses `client_id=tp-fms-public`,
   `grant_type=password`, Keycloak bearer-token authentication, and FMS
   `downloadFileContent`.

3. ENTSO-E File Library Python list example
   URL: <https://transparencyplatform.zendesk.com/hc/en-us/articles/38943763721105-How-to-list-folder-content-from-FMS-via-Python>
   Captured facts: the official example uses FMS `listFolder`; in live smoke,
   `listFileMetadata` returned the monthly `EnergyPrices_12.1.D_r3` items needed
   for source-backed exports.

4. ENTSO-E Transparency Platform
   URL: <https://www.entsoe.eu/data/transparency-platform/>
   Captured role: official transparency-platform entrypoint and source registry
   for future token/API or manual export evidence.

5. PSE public report/export page
   URL: <https://www.pse.pl/web/pse-eng/data/polish-power-system-operation/day-ahead-basic-data>
   Captured role: possible public Polish source snapshot lane. Before training
   use, a concrete exported series still needs license, timestamp, unit, and
   market-rule mapping.

6. Instrat Polish DAM price page
   URL: <https://energy.instrat.pl/en/prices/electricity-dam/>
   Captured role: possible research/non-commercial Polish DAM context snapshot.
   Before training use, the route must preserve license status, publication
   timestamp, EUR/UAH prior-known FX, timezone/DST mapping, and domain-shift
   evidence.

## 2026-05-17 Live Smoke Result

The credentialed File Library smoke succeeded without writing secrets to disk:

- token-only smoke: passed with redacted metadata;
- metadata listing: `139` `EnergyPrices_12.1.D_r3` monthly files found;
- selected file: `2026_01_EnergyPrices_12.1.D_r3.csv`;
- selected file update timestamp: `2026-05-02T18:55:16.465Z`;
- normalized Poland rows: `2,976`;
- local receipt:
  `data/external_sources/poland/entsoe_fms/entsoe-fms-smoke-receipt.json`;
- local evidence packet:
  `data/research_runs/week3_poland_neighbor_market_snapshot_entsoe_fms_smoke/`;
- `training_use_allowed=false`;
- `feature_use_allowed=false`;
- `market_execution_enabled=false`.

The live smoke proves source-backed ENTSO-E File Library access is available.
It does not approve the feature for official training, because licensing,
prior-known EUR/UAH FX, and broader governance remain separate blockers.

## Governance Interpretation

The implementation must not scrape protected ENTSO-E pages to bypass tokens.
Instead, it supports two source-backed routes:

- credentials/API route, with secrets provided only through environment
  variables and never written to artifacts;
- manual/public CSV snapshot route, with source URL, retrieval time, publication
  time, license status, and raw file checksum recorded.

Both routes feed the same `entsoe_poland_feature_governance_frame` and
`official_forecast_exogenous_feature_route_frame`. No external feature enters
official training until every governance blocker is resolved.
