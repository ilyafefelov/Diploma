---
title: "Smart Energy Arbitrage 2026: Architecture Review"
subtitle: "Dagster, ML evidence, Docker runtime, data quality, and DFL roadmap"
date: "2026-05-11"
---

# 1. Verdict

The project direction is coherent and thesis-defensible.

The current system is not a trading bot. It is a reproducible evidence platform:

- observed market/weather data
- Dagster medallion assets
- strict LP baseline
- forecast candidate scoring
- offline DFL/DT research lanes
- Pydantic safety boundary
- FastAPI/Nuxt/MLflow/Postgres observability

---

# 2. Current Architecture

![ML architecture review infographic](assets/ml-architecture-review-infographic.png)

Key flow:

```text
Sources -> Dagster Bronze/Silver/Gold -> LP/Forecast/DFL/DT
  -> Pydantic Gatekeeper -> Postgres/MLflow/FastAPI/Nuxt
```

---

# 3. Dagster Evidence

| Item | Count |
|---|---:|
| Assets | 109 |
| Asset checks | 25 |
| Jobs | 2 |
| Schedules | 2 |
| Sensors | 0 |

Strength:

- lineage is visible
- claim boundaries are encoded
- research artifacts are reproducible

Risk:

- daemon heartbeat warnings need runtime hardening

---

# 4. Docker Runtime

Current services:

- API: healthy
- Dagster webserver: running
- Dagster daemon: running with heartbeat warnings
- MLflow: running
- Postgres: healthy
- MQTT/telemetry services: defined, not in current `compose ps` snapshot

Compose config passes.

---

# 5. Data Findings

| Source | Status | Risk |
|---|---|---|
| OREE DAM prices | Observed, cap-clean | Good baseline |
| Open-Meteo weather | Observed | Uneven tenant coverage |
| Battery telemetry | Synthetic | Future timestamps |
| Grid events | Observed | Small dataset |

Highest priority: fix future synthetic telemetry before "live SOC" demo language.

---

# 6. Strategy Findings

| Lane | Status |
|---|---|
| strict similar-day + LP | Current fallback and strongest control |
| compact NBEATSx/TFT | Research candidates |
| official NBEATSx | Adapter executes, output invalid/out-of-cap |
| official TFT | Cleaner smoke rows, still needs value promotion |
| Schedule/Value Learner V2 | Offline/read-model promotion only |
| Decision Transformer | Preview-only |
| Full DFL | Blocked |

---

# 7. Scientific Fit

Supported by literature:

- decision-focused ESS arbitrage
- predict-then-bid storage bidding
- SPO+/decision-aware optimization
- NBEATSx and TFT for exogenous multi-horizon forecasting
- degradation-aware BESS optimization
- differentiable optimization layers
- Decision Transformer policy scaffolding

Main message:

The research direction is right, but the current evidence supports disciplined offline promotion gates, not live autonomous control.

---

# 8. Regulatory Fit

Key anchors:

- Ukraine NEURC DAM/IDM caps: 10..15,000 UAH/MWh
- OREE 2026 transaction tariff: 6.88 UAH/MWh excluding VAT
- EU electricity market reform: flexibility, market coupling, transparency
- Ukraine NECP: energy/climate alignment and EU integration
- AI Act: risk-based governance and human oversight

Architecture implication:

Pydantic Gatekeeper, logging, data quality gates, and market-execution-disabled flags are core safety features.

---

# 9. Main Risks

1. Future synthetic telemetry contaminates live SOC.
2. Official NBEATSx smoke forecasts are absurdly out of cap.
3. Dagster daemon heartbeat warnings reduce runtime confidence.
4. Weather panel coverage differs across tenants.
5. Forecast store key may collide across models sharing run/timestamp.
6. DFL/DT claims must stay offline until evidence improves.

---

# 10. Roadmap

Immediate:

- fix telemetry timestamp sanity
- quarantine out-of-cap forecasts
- investigate daemon heartbeat
- add latest-common-panel gate

Next:

- consolidate evidence registry
- review forecast-store schema
- add market-coupling source gate with ENTSO-E token workflow
- run official forecasts only after sanity/value gates

Later:

- full DFL objective
- robust DT offline evaluation
- net settlement economics
- deeper degradation-aware digital twin

---

# 11. Defense Language

Use:

- strict LP baseline
- forecast evidence surface
- offline DFL research lane
- operator preview
- market execution disabled

Avoid:

- live trading bot
- deployed Decision Transformer
- full DFL
- production BESS controller
- full electrochemical digital twin
