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

2. ENTSO-E Transparency Platform
   URL: <https://www.entsoe.eu/data/transparency-platform/>
   Captured role: official transparency-platform entrypoint and source registry
   for future token/API or manual export evidence.

3. PSE public report/export page
   URL: <https://www.pse.pl/web/pse-eng/data/polish-power-system-operation/day-ahead-basic-data>
   Captured role: possible public Polish source snapshot lane. Before training
   use, a concrete exported series still needs license, timestamp, unit, and
   market-rule mapping.

4. Instrat Polish DAM price page
   URL: <https://energy.instrat.pl/en/prices/electricity-dam/>
   Captured role: possible research/non-commercial Polish DAM context snapshot.
   Before training use, the route must preserve license status, publication
   timestamp, EUR/UAH prior-known FX, timezone/DST mapping, and domain-shift
   evidence.

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
