---
marp: true
theme: default
paginate: true
size: 16:9
style: |
  section {
    font-family: 'Aptos', 'Segoe UI', sans-serif;
    background: linear-gradient(180deg, #fbfdff 0%, #e7f3ff 100%);
    color: #183a59;
    padding: 42px 58px;
  }
  h1, h2 {
    color: #075985;
  }
  strong {
    color: #0f3f64;
  }
  code {
    background: rgba(7, 89, 133, 0.08);
    color: #0f3f64;
    padding: 0.12em 0.32em;
    border-radius: 0.35em;
  }
  blockquote {
    border-left: 5px solid #38bdf8;
    padding-left: 18px;
    color: #29516f;
  }
  section::after {
    content: 'codebase onboarding / week2';
    position: absolute;
    right: 58px;
    bottom: 22px;
    font-size: 12px;
    letter-spacing: 0.12em;
    text-transform: uppercase;
    color: rgba(15, 63, 100, 0.55);
  }
---

# Codebase Onboarding
## Smart Energy Arbitrage for BESS

**Goal:** help a newcomer understand what the repository does, where the important files live, and what to learn next.

- Autonomous energy arbitrage research-and-demo system
- Ukraine-focused BESS / DAM baseline MVP
- Backend + orchestration + API + operator dashboard + thesis artifacts

<!-- Speaker note: Start by saying this is not only code; it is both an engineering MVP and a thesis evidence repository. -->

---

# 1. What this project is

The repository builds a controlled path from **data** to **operator-facing decision preview**:

1. Resolve a tenant and location
2. Ingest or synthesize market, weather, and telemetry data
3. Build forecasts and battery-state read models
4. Solve a deterministic baseline battery arbitrage plan
5. Expose results through FastAPI
6. Visualize them in a Nuxt operator dashboard

> Current output = **recommendation preview**, not live market execution.

---

# 2. The high-level architecture

```text
Tenant registry
  -> Bronze weather / market assets
  -> Silver features and forecast candidates
  -> Gold baseline LP and simulated training assets
  -> Resource stores
  -> FastAPI dashboard read models
  -> Nuxt operator dashboard
```

**Key principle:** keep market intent, market clearing, and physical dispatch as separate concepts.

---

# 3. Main repository map

| Path | What lives there |
|---|---|
| `src/smart_arbitrage/` | Python package: assets, solver, schemas, stores, telemetry, training |
| `api/main.py` | FastAPI control-plane and dashboard read-model endpoints |
| `dashboard/` | Nuxt/Vue operator UI and same-origin proxy routes |
| `simulations/tenants.yml` | Canonical tenant registry and battery/location defaults |
| `docs/technical/` | API, architecture, issue, and demo-readiness docs |
| `docs/thesis/` | Thesis chapters, sources, reports, and presentation artifacts |
| `docker-compose.yaml` | Full local stack: Postgres, MQTT, MLflow, API, Dagster |

---

# 4. Domain language to learn first

Before changing code, read `CONTEXT.md`.

Important terms:

- **Proposed Bid** — market bid before clearing
- **Bid Gatekeeper** — safety layer that blocks infeasible bids
- **Cleared Trade** — post-clearing allocation
- **Dispatch Command** — physical action after final checks
- **Baseline Strategy** — deterministic LP MVP and control group
- **Target Strategy** — future DFL strategy, not current dispatch logic
- **EFC / degradation penalty** — battery wear made visible in economics

---

# 5. Backend package structure

`src/smart_arbitrage/` is organized by responsibility:

- `assets/bronze/` — weather and market ingestion
- `assets/silver/` — model-ready features and forecast candidates
- `assets/gold/` — LP baseline and simulated trade-training assets
- `assets/telemetry/` — raw telemetry and hourly battery snapshots
- `forecasting/` — NBEATSx-style and TFT-style helpers
- `optimization/` — projected battery-state simulation
- `gatekeeper/` — strict Pydantic contracts
- `resources/` — Postgres/null/in-memory persistence adapters
- `training/` — regret-aware simulated trajectories
- `defs/` — Dagster `Definitions` entrypoint

---

# 6. Current MVP data flow

1. **Tenant registry** gives location, timezone, and battery defaults
2. **Bronze asset** fetches Open-Meteo weather or uses synthetic fallback
3. **Market history** is enriched with source/provenance metadata
4. **Strict similar-day forecast** creates the Level 1 forecast input
5. **LP baseline** optimizes charge/discharge over an hourly horizon
6. **Projected battery simulator** checks SOC, power limits, throughput, and degradation
7. **API + dashboard** show operator-readable preview state

Scope boundary: **hourly DAM baseline**, not full multi-market bidding.

---

# 7. The baseline solver is the core MVP

The Level 1 baseline solver:

- Works on **hourly DAM** intervals
- Builds a **strict similar-day forecast**
- Solves an LP with charge, discharge, and SOC variables
- Enforces SOC min/max and max-power constraints
- Subtracts degradation penalty from gross market value
- Commits only the first step as a rolling-horizon preview

Why it matters:

> It is the reliable baseline that future DFL / learned policies must beat.

---

# 8. Battery feasibility and degradation

The projected battery simulator turns a signed MW schedule into a feasible trace:

- Validates contiguous hourly intervals
- Clips power to battery max power
- Enforces SOC floor and ceiling
- Applies simplified round-trip efficiency
- Computes throughput in MWh
- Computes degradation penalty in UAH

This is best described as a **feasibility-and-economics preview model**, not a full electrochemical digital twin.

---

# 9. FastAPI control plane

The API provides the dashboard and demo with backend-owned contracts:

- `GET /health`
- `GET /tenants`
- `POST /weather/run-config`
- `POST /weather/materialize`
- `GET /dashboard/signal-preview`
- `GET /dashboard/operator-status`
- `POST /dashboard/projected-battery-state`
- `GET /dashboard/battery-state`
- `GET /dashboard/baseline-lp-preview`

Debug tip: start with `/docs`, then trace into `api/main.py`.

---

# 10. Dashboard flow

The Nuxt dashboard is an operator-facing surface:

- `dashboard/app/pages/operator.vue` wires the page together
- Composables load tenants, signal preview, baseline preview, and weather controls
- Browser requests stay same-origin
- Nuxt server routes proxy to the FastAPI API base
- UI components render market signals, baseline schedule, SOC, gatekeeper state, metrics, and right-rail controls

Debug tip: if a request fails, check both **Nuxt proxy route** and **FastAPI endpoint**.

---

# 11. Persistence and local stack

The system can run in lightweight or full-stack modes.

**Without DSNs:**
- Stores often become null/no-op adapters
- Good for local demos and isolated tests

**With Docker Compose:**
- Postgres stores market, telemetry, forecast, simulated trade, and operator status data
- MQTT supports telemetry ingestion/publishing
- MLflow tracks experiments
- Dagster webserver/daemon runs asset orchestration
- FastAPI serves the control plane

---

# 12. Current MVP vs future research

## Implemented / demo-ready
- Tenant-aware weather control flow
- Baseline LP recommendation preview
- Projected SOC and degradation-aware UAH economics
- Operator dashboard surface
- Dagster + API + persistence hooks

## Planned research direction
- Stronger NBEATSx / TFT forecast comparison
- Decision-Focused Learning / predict-then-bid
- Differentiable or surrogate clearing
- Learned strategy layer
- Deeper battery digital twin
- Wider market scope

Do not present planned DFL as already implemented.

---

# 13. Recommended reading order

1. `CONTEXT.md` — vocabulary and scope boundaries
2. `pyproject.toml` — Python stack and Dagster config
3. `simulations/tenants.yml` — tenant and battery defaults
4. `src/smart_arbitrage/defs/__init__.py` — Dagster entrypoint
5. `src/smart_arbitrage/assets/bronze/market_weather.py` — Bronze ingestion
6. `src/smart_arbitrage/assets/gold/baseline_solver.py` — LP baseline
7. `src/smart_arbitrage/optimization/projected_battery_state.py` — feasibility simulation
8. `api/main.py` — API contracts
9. `dashboard/app/pages/operator.vue` — dashboard composition

---

# 14. What to learn next

For engineering work:

- Polars DataFrames
- Pydantic v2 validation
- Dagster assets and materialization
- FastAPI endpoint contracts
- Nuxt composables and server routes

For research work:

- Battery arbitrage optimization
- SOC / SOH / throughput / EFC
- CVXPY linear programming
- Forecasting with exogenous features
- Regret-aware evaluation
- Decision-Focused Learning

---

# 15. Closing message

The repository already has a **working baseline contour**:

- data ingestion and provenance
- tenant-aware control plane
- deterministic LP baseline
- battery feasibility and degradation economics
- operator-facing dashboard
- thesis-ready documentation trail

The next major step is not to replace the baseline too early, but to use it as the measurable reference point for stronger forecasts and future decision-focused strategies.

---

# Appendix: presenter checklist

Before presenting:

- Open `CONTEXT.md` for terminology
- Open FastAPI docs at `/docs`
- Open dashboard operator route
- Keep `docs/technical/API_ENDPOINTS.md` nearby
- Be explicit: current system is a **recommendation preview**, not live bidding
- Be explicit: DFL is the **planned target architecture**, not current production logic
