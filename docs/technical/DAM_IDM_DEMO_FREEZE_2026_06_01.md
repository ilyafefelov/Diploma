# DAM/IDM Demo Freeze - 2026-06-01

This note freezes the current local demo state for the operator-facing
`DAM/IDM hourly recommendation preview`.

## Boundary

- The surface is a read-model/operator preview only.
- Published windows use official/source-backed OREE rows first.
- Unpublished windows use NBEATSx/TFT forecast-store rows only when the target
  window has complete hourly forecast coverage and point-in-time metadata.
- Missing source rows or incomplete forecast horizons remain blocker states.
- The demo does not emit `ProposedBid`, live IDM bids, market submissions,
  settlement payloads, or executable dispatch commands.
- `market_execution_enabled=false` remains part of the claim boundary.

## Runtime

Use the local helper:

```powershell
.\scripts\start-local-project.ps1 -ApiPort 8000 -DashboardPort 64163 -SkipCompose
```

The helper sets these local Postgres DSNs when they are not already present:

```powershell
SMART_ARBITRAGE_MARKET_DATA_DSN=postgresql://smart:arbitrage@localhost:5432/smart_arbitrage
SMART_ARBITRAGE_FORECAST_DSN=postgresql://smart:arbitrage@localhost:5432/smart_arbitrage
```

Local URLs:

- Dashboard: `http://127.0.0.1:64163/operator`
- API: `http://127.0.0.1:8000`

Run dashboard unit tests through `npm -C dashboard run test:unit` so Vitest's
working directory stays inside the dashboard package and does not collect
legacy MJS contract files from the repository root.

## Data Readiness

Observed OREE DAM/IDM rows were materialized for `2026-05-01` through
`2026-05-31`.

```powershell
$env:SMART_ARBITRAGE_MARKET_DATA_DSN='postgresql://smart:arbitrage@localhost:5432/smart_arbitrage'
@'
from datetime import date
import json
from smart_arbitrage.assets.bronze.market_weather import build_observed_market_price_history
from smart_arbitrage.resources.market_data_store import get_market_data_store, market_price_observations_from_frame

store = get_market_data_store()
summary = []
for venue in ("DAM", "IDM"):
    frame = build_observed_market_price_history(
        start_date=date.fromisoformat("2026-05-01"),
        end_date=date.fromisoformat("2026-05-31"),
        market_venue=venue,
    )
    observations = market_price_observations_from_frame(frame)
    store.upsert_market_prices(observations)
    summary.append(
        {
            "market_venue": venue,
            "rows": frame.height,
            "first": frame.select("timestamp").to_series().item(0).isoformat(),
            "last": frame.select("timestamp").to_series().item(-1).isoformat(),
            "source_kinds": sorted(set(frame.select("source_kind").to_series().to_list())),
            "sources": sorted(set(frame.select("source").to_series().to_list()))[:5],
        }
    )
print(json.dumps(summary, indent=2))
'@ | .\.venv\Scripts\python.exe -
```

Observed materialization summary:

- DAM: `744` rows, `2026-05-01T00:00:00` to `2026-05-31T23:00:00`,
  `source_kind=observed`, `source=OREE_DATA_VIEW`.
- IDM: `744` rows, `2026-05-01T00:00:00` to `2026-05-31T23:00:00`,
  `source_kind=observed`, `source=OREE_DATA_VIEW`.

Forecast-store rows were materialized for a 72-hour horizon starting
`2026-06-01`.

```powershell
$env:SMART_ARBITRAGE_MARKET_DATA_DSN='postgresql://smart:arbitrage@localhost:5432/smart_arbitrage'
$env:SMART_ARBITRAGE_FORECAST_DSN='postgresql://smart:arbitrage@localhost:5432/smart_arbitrage'
.\.venv\Scripts\python.exe scripts\materialize_operator_preview_forecast_store.py --market-venue DAM --forecast-start 2026-06-01 --horizon-hours 72 --nbeatsx-max-steps 1 --tft-max-epochs 1
.\.venv\Scripts\python.exe scripts\materialize_operator_preview_forecast_store.py --market-venue IDM --forecast-start 2026-06-01 --horizon-hours 72 --nbeatsx-max-steps 1 --tft-max-epochs 1
```

The materializer can take several minutes for both venues; allow the process to
finish before running smoke checks.

## Smoke Evidence

FastAPI and the Nuxt proxy were checked against the same cases:

| Case | Result |
| --- | --- |
| `market_venue=DAM` | `200`, `target_delivery_date=2026-06-01`, `price_context_status=official_published`, `rows=24` |
| `market_venue=IDM` | `200`, `target_delivery_date=2026-05-31`, `price_context_status=official_published`, `rows=24` |
| `market_venue=DAM&target_delivery_date=2026-06-03` | `200`, `price_context_status=pre_publication_forecast`, `source=nbeatsx_official_v0`, `rows=24` |
| `market_venue=IDM&target_delivery_date=2026-06-03` | `200`, `price_context_status=pre_publication_forecast`, `source=nbeatsx_official_idm_v0`, `rows=24` |
| `market_venue=IDM&target_delivery_date=2030-01-01` | `503` blocker; no substitute prices rendered |

Rendered QA screenshots were captured under:

`C:\Users\ilyaf\AppData\Local\Temp\codex-dashboard-hardening-qa-20260601-final`

Files:

- `final-01-dam-latest.png`
- `final-02-dam-dayplus2.png`
- `final-03-idm-dayplus2.png`
- `final-04-idm-blocker.png`
- `final-05-mobile-dam-dayplus2.png`

Rendered states checked:

- DAM latest shows selected period `01 Jun, 00:00 -> 02 Jun, 00:00`,
  official/source price, and `24/24` rows.
- DAM `2026-06-03` shows `ML forecast: nbeatsx_official_v0`, selected
  period, generated timestamp, and `24/24` rows.
- IDM `2026-06-03` shows `ML forecast: nbeatsx_official_idm_v0`, selected
  period, generated timestamp, and `24/24` rows.
- IDM far future shows a source-backed preview blocker and no stale
  BUY/SELL/HOLD advice.
- Mobile DAM day+2 layout was checked for readable controls and chart labels.

## Verification Commands

```powershell
.\.venv\Scripts\python.exe -m pytest tests/api/test_main.py tests/assets/test_market_weather_sources.py tests/resources/test_forecast_store.py tests/research/test_operator_preview_forecast.py
$env:NODE_OPTIONS='--max-old-space-size=8192'; npm -C dashboard run typecheck
$env:NODE_OPTIONS='--max-old-space-size=8192'; npm -C dashboard run test:unit
git diff --check
```

Last recorded results:

- Backend focused tests: `90 passed`.
- Dashboard typecheck: passed.
- Dashboard Vitest: `60 passed / 209 tests`.
- Current-facing stale-copy audit: no hits.
- `git diff --check`: no errors; only existing LF-to-CRLF warnings.
