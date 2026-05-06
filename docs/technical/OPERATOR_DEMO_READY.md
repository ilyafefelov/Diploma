# Operator Demo Readiness

## Призначення

Цей документ фіксує, що поточні Slice 1 і Slice 2 готові до supervisor demo та weekly-report packaging без розширення domain scope.

## Що вже demo-ready

### Slice 1: weather control surface

Підтверджені операторські кроки:
- tenant selection через dashboard
- weather run-config preview
- weather materialization outcome
- backend-owned latest operator status for `weather_control`

Основні технічні опори:
- [api/main.py](d:/School/GoIT/Courses/Diploma/api/main.py)
- [src/smart_arbitrage/resources/operator_status_store.py](d:/School/GoIT/Courses/Diploma/src/smart_arbitrage/resources/operator_status_store.py)
- [dashboard/app/composables/useWeatherControls.ts](d:/School/GoIT/Courses/Diploma/dashboard/app/composables/useWeatherControls.ts)
- [dashboard/server/api/control-plane/dashboard/operator-status.get.ts](d:/School/GoIT/Courses/Diploma/dashboard/server/api/control-plane/dashboard/operator-status.get.ts)

### Slice 2: baseline recommendation preview surface

Підтверджені операторські кроки:
- baseline LP preview read model
- hourly forecast surface
- feasible signed MW recommendation surface
- projected SOC trace
- UAH economics with separate degradation component
- current battery economics framed as a public-source capex-throughput proxy, not a folklore cycle-cost constant

Основні технічні опори:
- [api/main.py](d:/School/GoIT/Courses/Diploma/api/main.py)
- [src/smart_arbitrage/optimization/projected_battery_state.py](d:/School/GoIT/Courses/Diploma/src/smart_arbitrage/optimization/projected_battery_state.py)
- [dashboard/app/components/dashboard/HudBaselinePreview.vue](d:/School/GoIT/Courses/Diploma/dashboard/app/components/dashboard/HudBaselinePreview.vue)
- [dashboard/server/api/control-plane/dashboard/baseline-lp-preview.get.ts](d:/School/GoIT/Courses/Diploma/dashboard/server/api/control-plane/dashboard/baseline-lp-preview.get.ts)

### Slice 3: future-stack evidence surface

Підтверджені операторські кроки:
- operator recommendation read model with manual strategy switch and selected-policy explanation
- NBEATSx/TFT forecast-stack graph from FastAPI read models
- DT policy-preview value-gap/action graph when policy rows exist
- daily value against hold/no-arbitrage baseline
- explicit policy boundary: preview/read model, not market execution

Основні технічні опори:
- [api/main.py](d:/School/GoIT/Courses/Diploma/api/main.py)
- [src/smart_arbitrage/decision_transformer/policy_training.py](d:/School/GoIT/Courses/Diploma/src/smart_arbitrage/decision_transformer/policy_training.py)
- [src/smart_arbitrage/assets/gold/simulated_trades.py](d:/School/GoIT/Courses/Diploma/src/smart_arbitrage/assets/gold/simulated_trades.py)
- [dashboard/app/components/dashboard/operator/OperatorFutureStackPanel.vue](d:/School/GoIT/Courses/Diploma/dashboard/app/components/dashboard/operator/OperatorFutureStackPanel.vue)
- [dashboard/app/composables/useDefenseDashboard.ts](d:/School/GoIT/Courses/Diploma/dashboard/app/composables/useDefenseDashboard.ts)

## Scope boundary

Поточна demo-ready поверхня не повинна описуватися як:
- `Proposed Bid`
- `Cleared Trade`
- `Dispatch Command`
- market execution engine
- full digital twin battery physics
- live DT/M3DT market execution
- full SOTA NeuralForecast/PyTorch-Forecasting study unless official-adapter runs are materialized

Поточна demo-ready поверхня повинна описуватися як:
- operator-facing read model
- recommendation preview
- tenant-aware control surface
- constrained baseline LP analytical surface
- projected battery state preview
- forecast-stack and DT-policy evidence surface with deterministic safety boundary

## Weekly-report-ready artifact links

- Dashboard UI: `http://localhost:64163/`
- FastAPI docs: `http://127.0.0.1:8010/docs`
- Dagster UI: `http://127.0.0.1:3000/`
- MLflow UI: `http://127.0.0.1:5000/`
- Vercel public entry: `https://dashboard-gilt-one-97.vercel.app/` (redirects to the Week 1 report)
- Vercel operator dashboard: `https://dashboard-gilt-one-97.vercel.app/operator`
- Vercel Week 1 report: `https://dashboard-gilt-one-97.vercel.app/week1/interactive_report1`
- Week 1 report: [docs/thesis/weekly-reports/week1/report.md](d:/School/GoIT/Courses/Diploma/docs/thesis/weekly-reports/week1/report.md)
- Week 1 short summary: [docs/thesis/weekly-reports/week1/supervisor-summary.md](d:/School/GoIT/Courses/Diploma/docs/thesis/weekly-reports/week1/supervisor-summary.md)
- API contracts: [docs/technical/API_ENDPOINTS.md](d:/School/GoIT/Courses/Diploma/docs/technical/API_ENDPOINTS.md)
- PRD and issue trace: [docs/technical/PRD-operator-mvp-slices.md](d:/School/GoIT/Courses/Diploma/docs/technical/PRD-operator-mvp-slices.md), [docs/technical/issues](d:/School/GoIT/Courses/Diploma/docs/technical/issues)
- Week 1 presentation script: [docs/thesis/weekly-reports/week1/presentation-script.md](d:/School/GoIT/Courses/Diploma/docs/thesis/weekly-reports/week1/presentation-script.md)
- Week 1 screenshots: [docs/thesis/weekly-reports/week1/assets/dagster-ui.png](d:/School/GoIT/Courses/Diploma/docs/thesis/weekly-reports/week1/assets/dagster-ui.png), [docs/thesis/weekly-reports/week1/assets/mlflow-ui.png](d:/School/GoIT/Courses/Diploma/docs/thesis/weekly-reports/week1/assets/mlflow-ui.png)

## Verified implementation commits

- `86f44c5` — Persist operator flow status read models
- `cdb01b6` — Sync dashboard weather state with operator status read model
- `25ab328` — Add projected battery state simulator preview
- `13e482b` — Add baseline LP preview read model
- `f8792f0` — Surface baseline LP preview in dashboard
- `f72a366` — Add DT policy preview backend read models
- `21b2bba` — Add future stack dashboard graphs

## Validation snapshot

- Focused API tests are green in [tests/api/test_main.py](d:/School/GoIT/Courses/Diploma/tests/api/test_main.py)
- Dashboard production build succeeds in [dashboard](d:/School/GoIT/Courses/Diploma/dashboard)
- Future-stack dashboard lint/typecheck/build and the touched defense dataset Vitest file pass.
- Browser smoke-check confirms the visible Slice 2 slab `Baseline LP recommendation surface`

## Recommended demo framing

Найкраще представляти систему як поетапно зібраний operator MVP:
- спочатку tenant-aware weather control slice
- потім baseline recommendation preview slice
- далі future-stack evidence slice: NBEATSx/TFT graph, DT policy-preview graph, policy readiness, and value-vs-hold economics
- окремо підкреслювати, що execution semantics і повний DFL contour є наступним етапом, а не поточним deliverable
