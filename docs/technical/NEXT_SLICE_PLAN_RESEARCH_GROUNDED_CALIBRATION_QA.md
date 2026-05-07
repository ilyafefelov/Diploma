# Next Slice Plan: Research-Grounded Calibration QA

Date: 2026-05-07

## Recommendation

The next slice should be **Calibration Evidence QA Manifest + No-Leakage
Guardrails**.

Do not start full DFL training yet. The latest source scan reinforces the
current project stance: foundation and neural forecasters are only useful for
the thesis after no-leakage rolling-origin evaluation and downstream
decision-value checks.

## Source Scan

| Status | Source | Planning implication |
|---|---|---|
| include | [PriceFM: Foundation Model for Probabilistic Electricity Price Forecasting](https://huggingface.co/papers/2508.04875) | Supports the future electricity-price foundation-model direction and European cross-region/topology context, but current implementation should remain benchmark-first. |
| include/watch | [THieF: Predicting Day-ahead Electricity Prices with Temporal Hierarchy Forecasting](https://huggingface.co/papers/2508.11372) | Supports later reconciliation across hourly, block, and baseload products. Keep as future forecast-structure work, not a Week 3 implementation. |
| include | [Rethinking Evaluation in the Era of Time Series Foundation Models](https://huggingface.co/papers/2510.13654) | Strong guardrail for this project: prevent train/test overlap and temporal leakage; report truly out-of-sample rolling-origin evidence. |
| watch | [TFMAdapter](https://huggingface.co/papers/2509.13906) | Relevant to Open-Meteo and future market-coupling covariates, but not needed until the benchmark/export protocol is unambiguous. |
| watch | [Reverso](https://huggingface.co/papers/2602.17634) | Efficient zero-shot TSFM direction; evaluate later only through the same strict LP/oracle protocol. |
| watch | [Distributional RL Energy Arbitrage](https://huggingface.co/papers/2401.00015) | Useful for later risk-sensitive/multi-venue strategy discussion; not a DAM-only Week 3/4 target. |

## Implementation Scope

Add a report-ready calibration QA manifest to the existing research export
path. The manifest should make it impossible to confuse tenant-specific Dnipro
evidence with all-tenant/latest-persisted aggregate exports.

Proposed files:

- `tests/research/test_research_layer_manifest.py`
- `src/smart_arbitrage/research/real_data_research_layer.py`
- `scripts/materialize_research_layer_from_store.py`
- `docs/technical/RESEARCH_INTEGRATION_PLAN.md`
- `docs/thesis/weekly-reports/week3/report.md`

Proposed manifest fields:

- `run_slug`
- `generated_at_utc`
- `tenant_ids`
- `strategy_kinds`
- `latest_generated_at_by_tenant_strategy`
- `anchor_count_by_tenant_strategy`
- `row_count_by_tenant_strategy`
- `data_quality_tiers`
- `claim_scope`
- `not_full_dfl`
- `not_market_execution`
- `source_links`

## TDD Plan

1. RED: add a research-layer manifest test using mixed old/new generated batches
   and multiple tenants.
2. GREEN: add the smallest manifest builder/exporter change.
3. REFACTOR: keep the manifest generation behind one small function; do not
   change existing CSV/JSON export names.
4. VERIFY:

```powershell
.\.venv\Scripts\python.exe -m pytest -p no:cacheprovider tests\research\test_real_data_research_layer.py tests\research\test_research_layer_manifest.py
.\scripts\verify.ps1
uv run dg list defs --json
uv run dg check defs
docker compose config --quiet
git diff --check
```

## Acceptance Criteria

- Exported research runs include a manifest that separates latest Dnipro
  tenant evidence from aggregate persisted rows.
- Manifest states `not_full_dfl=true` and `not_market_execution=true`.
- Week 3 report remains anchored on the accepted 30-anchor Dnipro result.
- 90-anchor calibration stays framed as preview/calibration evidence.
- No public API, Pydantic, Dagster key, dashboard, dependency, or legacy-folder
  changes.

## Commit Plan

- `research: add calibration evidence manifest`
- `docs: document calibration qa plan`
