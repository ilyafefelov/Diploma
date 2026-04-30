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

Основні технічні опори:
- [api/main.py](d:/School/GoIT/Courses/Diploma/api/main.py)
- [src/smart_arbitrage/optimization/projected_battery_state.py](d:/School/GoIT/Courses/Diploma/src/smart_arbitrage/optimization/projected_battery_state.py)
- [dashboard/app/components/dashboard/HudBaselinePreview.vue](d:/School/GoIT/Courses/Diploma/dashboard/app/components/dashboard/HudBaselinePreview.vue)
- [dashboard/server/api/control-plane/dashboard/baseline-lp-preview.get.ts](d:/School/GoIT/Courses/Diploma/dashboard/server/api/control-plane/dashboard/baseline-lp-preview.get.ts)

## Scope boundary

Поточна demo-ready поверхня не повинна описуватися як:
- `Proposed Bid`
- `Cleared Trade`
- `Dispatch Command`
- market execution engine
- full digital twin battery physics

Поточна demo-ready поверхня повинна описуватися як:
- operator-facing read model
- recommendation preview
- tenant-aware control surface
- constrained baseline LP analytical surface
- projected battery state preview

## Weekly-report-ready artifact links

- Dashboard UI: `http://localhost:3611/`
- FastAPI docs: `http://127.0.0.1:8010/docs`
- Dagster UI: `http://127.0.0.1:3000/`
- MLflow UI: `http://127.0.0.1:5000/`
- API contracts: [docs/technical/API_ENDPOINTS.md](d:/School/GoIT/Courses/Diploma/docs/technical/API_ENDPOINTS.md)
- PRD and issue trace: [docs/technical/PRD-operator-mvp-slices.md](d:/School/GoIT/Courses/Diploma/docs/technical/PRD-operator-mvp-slices.md), [docs/technical/issues](d:/School/GoIT/Courses/Diploma/docs/technical/issues)
- Week 2 demo script: [docs/thesis/weekly-reports/week2/demo-script.md](d:/School/GoIT/Courses/Diploma/docs/thesis/weekly-reports/week2/demo-script.md)
- Week 2 report: [docs/thesis/weekly-reports/week2/report.md](d:/School/GoIT/Courses/Diploma/docs/thesis/weekly-reports/week2/report.md)

## Verified implementation commits

- `86f44c5` — Persist operator flow status read models
- `cdb01b6` — Sync dashboard weather state with operator status read model
- `25ab328` — Add projected battery state simulator preview
- `13e482b` — Add baseline LP preview read model
- `f8792f0` — Surface baseline LP preview in dashboard

## Validation snapshot

- Focused API tests are green in [tests/api/test_main.py](d:/School/GoIT/Courses/Diploma/tests/api/test_main.py)
- Dashboard production build succeeds in [dashboard](d:/School/GoIT/Courses/Diploma/dashboard)
- Browser smoke-check confirms the visible Slice 2 slab `Baseline LP recommendation surface`

## Recommended demo framing

Найкраще представляти систему як поетапно зібраний operator MVP:
- спочатку tenant-aware weather control slice
- потім baseline recommendation preview slice
- окремо підкреслювати, що execution semantics і повний DFL contour є наступним етапом, а не поточним deliverable
