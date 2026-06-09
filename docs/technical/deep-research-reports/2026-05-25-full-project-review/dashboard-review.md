# Dashboard and API Review

## Runtime Setup Reviewed

- FastAPI dev server started on port `8010`.
- Nuxt dashboard dev server started on port `64163`.
- `/health` returned status ok.
- `/operator` returned HTTP 200.
- `/defense` rendered successfully.

Screenshots:

- [Operator desktop after data load](assets/operator_desktop_after_8s.png)
- [Operator mobile](assets/operator_mobile.png)
- [Defense desktop](assets/defense_desktop.png)
- [Defense mobile](assets/defense_mobile.png)
- [Operator dock C2 desktop](assets/operator-dock-playwright-desktop-1440x1100-after.png)
- [Operator dock C2 mobile](assets/operator-dock-playwright-mobile-390x844-after.png)

## Positive Findings

- The pages render without console errors in the captured browser pass.
- `/operator` eventually loads concrete values instead of placeholders:
  - net plan value: `1265 UAH`
  - energy arbitrage: `1826 UAH`
  - weather uplift: `+167.9 UAH/MWh`
  - cycle preview: `0.67 EFC`
  - read-model health: `98.7%`
- `/defense` is visually dense enough for a project defense and keeps the no-execution framing.
- Dashboard typecheck and tests pass: `npm run typecheck`; `npm exec -- vitest run` -> 12 files, 66 tests.

## Critical UI Issue - Closed

The original schedule dock was fixed to the bottom and too tall for the page padding. At a 1440 x 1100 viewport, it measured about 431 px tall while the page shell reserved only `12rem` of bottom padding.

Relevant CSS:

- `dashboard/app/assets/css/operator-hud.css:2-8`
- `dashboard/app/assets/css/operator-hud.css:1826-1835`
- `dashboard/app/assets/css/operator-hud.css:2215-2219`

Impact:

- Main content can be hidden behind the dock.
- Mobile risk is higher because the mobile padding is fixed, while dock content can grow.
- This directly affects demo quality.

Implemented fix:

1. `.schedule-dock` now participates in normal document flow with `position: relative`.
2. The artificial fixed-overlay bottom padding was removed from `.operator-shell`.
3. `dashboard/app/utils/operatorHudCss.test.ts` now asserts the no-overlay CSS contract.
4. Post-fix Browser/Playwright evidence reports `noDockOcclusion=true`, `noHorizontalOverflow=true`, zero console/page errors, and no forbidden market-execution wording on desktop and mobile.

## API Review

The API behavior reviewed here is read-model only. That matches the thesis boundary.

Closure status:

- The original review found `api/main.py` Mypy failures in shadow schedule helpers. Current closure rerun no longer shows `api/main.py` errors, and full Mypy now passes across 247 source files.

Risk:

- New shadow preview logic is exactly the kind of path that can grow into product-facing evidence. Keep it typed and read-model-only before using it in defense claims.
