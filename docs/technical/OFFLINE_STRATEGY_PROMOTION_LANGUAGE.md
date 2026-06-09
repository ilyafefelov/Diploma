# Offline Strategy Promotion Language

Date: 2026-05-12

This note freezes the thesis-facing language for the promotion gates without
renaming stable internal contracts.

## Decision

Human-facing thesis and demo material should use **Offline Strategy Promotion**.
The internal code, database, asset, and API fields may still use
`production_promote`, `production_gate`, and related names for compatibility.

The phrase means:

> The system has enough strict LP/oracle evidence to allow a challenger strategy
> in offline/read-model strategy evidence, while `strict_similar_day` remains the
> fallback and market execution remains disabled.

It does **not** mean live bidding, market execution, a dashboard default switch,
or a deployed Decision Transformer controller.

## Compatibility Boundary

The following names are intentionally unchanged:

| Surface | Stable name |
|---|---|
| Asset | `dfl_schedule_value_production_gate_frame` |
| API endpoint | `/dashboard/dfl-schedule-value-production-gate` |
| API fields | `production_promote`, `production_promote_count` |
| Postgres/read-model rows | `dfl_schedule_value_production_gate_rows` |
| Claim flag | `market_execution_enabled=false` |

Changing these names now would create avoidable read-model and documentation
churn. Instead, the API mapper uses
`smart_arbitrage.dfl.offline_strategy_promotion` to translate stored/internal
gate wording into the thesis-safe `academic_scope` and claim-boundary language.

## Read-Model Rule

Read models may report internal `production_promote=true` only when the gate has
passed its strict LP/oracle, rolling robustness, coverage, safety, and no-leakage
conditions. Even then, every response must keep:

- `claim_boundary=offline_read_model_strategy_evidence_only_not_market_execution`;
- `market_execution_enabled=false`;
- `strict_similar_day` as fallback.

## Current Scope

The current promoted evidence remains Ukrainian-only:

- observed OREE DAM prices;
- Open-Meteo/weather and tenant load/configuration context;
- strict LP/oracle scoring;
- no EU market-coupling training rows;
- no live market execution.

Market-coupling and external dataset features are still routed through the
governance layer before they can affect official NBEATSx/TFT or DFL training.
