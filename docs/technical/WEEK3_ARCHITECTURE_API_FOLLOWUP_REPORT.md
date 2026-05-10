# Week 3 Architecture/API Follow-Up Report

Date: 2026-05-07

## Scope

This follow-up completed two requested slices after the Week 3 evidence package:

1. Architecture/evidence reconciliation.
2. Postgres/API evidence freshness and read-model performance.

No Pydantic schemas, API response models, Dagster asset keys, partitions,
resources, IO managers, dashboard contracts, or dependencies were changed.

## Commit Summary

| Commit | Scope | Result |
|---|---|---|
| `8de60d3` | `docs: align architecture evidence narrative` | Added the architecture/data-flow entry point, indexed the infographic, and separated Week 3 accepted evidence from the 90-anchor calibration preview. |
| `dfaa24b` | `api: index research read models` | Added a Postgres latest-batch read index, a focused schema test, API docs, and measured read-model performance. |

## Architecture/Evidence Reconciliation

Updated artifacts:

- [ARCHITECTURE_AND_DATA_FLOW.md](ARCHITECTURE_AND_DATA_FLOW.md)
- [README.md](../../README.md)
- [docs/README.md](../README.md)
- [autonomous-energy-arbitrage-architecture-data-flow.png](assets/autonomous-energy-arbitrage-architecture-data-flow.png)
- [week3-architecture-api-reconciliation-infographic.svg](assets/week3-architecture-api-reconciliation-infographic.svg)

Key clarification:

- Week 3 accepted evidence remains the Dnipro 30-anchor thesis-grade
  rolling-origin benchmark.
- The Dnipro 90-anchor calibration path is prepared-ahead calibration/selector
  evidence, not the Week 3 headline.
- The canonical vocabulary is `Proposed Bid` and `Bid Gatekeeper`; older visual
  shorthand such as `ProposedTrade` is treated as non-contractual diagram text.

## Postgres/API Freshness And Performance

Updated artifacts:

- [API_READ_MODEL_FRESHNESS_AND_PERFORMANCE.md](API_READ_MODEL_FRESHNESS_AND_PERFORMANCE.md)
- [API_ENDPOINTS.md](API_ENDPOINTS.md)
- [strategy_evaluation_store.py](../../src/smart_arbitrage/resources/strategy_evaluation_store.py)
- [test_strategy_evaluation_store.py](../../tests/resources/test_strategy_evaluation_store.py)

The backing table `forecast_strategy_evaluations` now creates:

```sql
CREATE INDEX IF NOT EXISTS forecast_strategy_evaluations_latest_read_idx
ON forecast_strategy_evaluations (
    tenant_id,
    strategy_kind,
    generated_at DESC,
    anchor_timestamp,
    rank_by_regret,
    forecast_model_name
);
```

Measured effect on local Compose Postgres:

| Query | Before | After |
|---|---:|---:|
| Dnipro real-data benchmark latest batch | seq scans, `19.302 ms`, `1219` buffer hits | indexed latest lookup, `1.183 ms`, `60` buffer hits |
| Dnipro risk-adjusted gate latest batch | seq scans, `13.815 ms`, `1219` buffer hits | indexed latest lookup, `1.572 ms`, `24` buffer hits |

API freshness check:

| Endpoint | Latest `generated_at` | Coverage |
|---|---|---|
| `/dashboard/real-data-benchmark` | `2026-05-06T22:57:36.014876Z` | 90 anchors, 3 models, thesis grade |
| `/dashboard/calibrated-ensemble-benchmark` | `2026-05-06T22:57:36.014876Z` | 90 selector rows |
| `/dashboard/risk-adjusted-value-gate` | `2026-05-06T22:57:36.014876Z` | 90 selector rows |
| `/dashboard/forecast-dispatch-sensitivity` | `2026-05-06T22:57:36.014876Z` | 450 diagnostic rows |

## Verification

Commands run:

```powershell
.\.venv\Scripts\python.exe -m pytest -p no:cacheprovider tests\resources\test_strategy_evaluation_store.py
.\.venv\Scripts\python.exe -m pytest -p no:cacheprovider tests\resources\test_strategy_evaluation_store.py tests\api\test_main.py
git diff --check
& .\.venv\Scripts\Activate.ps1; .\scripts\verify.ps1
uv run dg list defs --json
uv run dg check defs
docker compose config --quiet
```

Results:

- Focused resource/API tests: `34 passed`.
- Full verification: `151 passed`.
- Ruff: passed.
- Mypy: passed.
- Dagster definitions: loaded successfully.
- Compose config: valid.

## Residual Risk

Postgres lookup is no longer the bottleneck for the measured Dnipro read models.
Warm HTTP timings still include Polars conversion, sensitivity reconstruction,
Pydantic serialization, and local Windows/Compose overhead. If endpoint latency
becomes a dashboard issue, the next performance slice should profile API
serialization before adding more database indexes.
