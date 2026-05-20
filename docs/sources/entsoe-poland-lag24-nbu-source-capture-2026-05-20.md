# ENTSO-E Poland Lag-24 + NBU EUR/UAH Source Capture

Date: 2026-05-20

Scope: source basis for the governed Poland lag-24 exogenous feature route
`entsoe_pl_lag24_day_ahead_price_uah_mwh`.

## Sources

- ENTSO-E Transparency Platform API/File Library remains the source for Poland
  day-ahead price observations. The project uses the token-backed ENTSO-E API
  only as a source of exogenous feature candidates; the token is not written to
  evidence artifacts, logs, or docs.
- National Bank of Ukraine Developer API documents official exchange-rate API
  access, including date-range requests by currency code and JSON output:
  `https://bank.gov.ua/en/open-data/api-dev`.
- NBU exchange-rate service instruction documents the endpoint shape used in
  the code:
  `https://bank.gov.ua/NBU_Exchange/exchange_site?start=yyyymmdd&end=yyyymmdd&valcode=usd&sort=exchangedate&order=desc&json`.

## Implementation Capture

- Code asset: `nbu_eur_uah_fx_metadata_frame`.
- Code helper: `build_nbu_eur_uah_fx_metadata_frame`.
- NBU endpoint used by the asset:
  `https://bank.gov.ua/NBU_Exchange/exchange_site?start=<YYYYMMDD>&end=<YYYYMMDD>&valcode=eur&sort=exchangedate&order=asc&json`.
- FX timestamp policy: the NBU `calcdate` is represented as 15:30
  Europe/Kyiv converted to UTC. This keeps the FX metadata prior-known for the
  lag-24 Poland feature route.
- ENTSO-E parsing policy: the XML `resolution` field is respected, so `PT15M`
  data no longer drifts into hourly offsets. Small source gaps are filled only
  from adjacent ENTSO-E source prices; Ukrainian target actuals are not used.

## Evidence Packet

Local packet:
`data/research_runs/week3_dfl_entsoe_poland_lag24_nbu_approved_route/`.

Materialized state:

- ENTSO-E lagged coverage: `11,638 / 11,638` Ukrainian benchmark timestamps.
- NBU EUR/UAH metadata: `485 / 485` source-backed effective dates.
- Approved experimental feature:
  `entsoe_pl_lag24_day_ahead_price_uah_mwh`.
- Remaining blocker for official training: `domain_shift`.
- Ablation state: `approved_route_pending_materialization`.
- Claim boundary: Offline Strategy Promotion evidence only, no live execution,
  no European training rows, and `market_execution_enabled=false`.
