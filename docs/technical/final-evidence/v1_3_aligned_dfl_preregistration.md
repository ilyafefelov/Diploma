# v1.3 Aligned Differentiable DFL Preregistration

Status: frozen before construction of the aligned full-context panel and before
training either loss condition.

## Research question

Does a hybrid forecast-plus-decision loss improve strict decision quality over
the **same** contextual transformer trained with forecast loss alone? This is
the primary DFL comparison. Any comparison with released V2+ is a separate
system-level gate and cannot convert this experiment into a market controller.

## Required point-in-time feature panel

Every anchor must contain all of these horizon-aligned, prior-safe vectors:

1. forecast p50;
2. Ukrainian price lag-24;
3. historical/known weather temperature;
4. calendar hour sine and cosine;
5. forecast p10 and p90;
6. governed Poland lag-24 context.

The implementation rejects a panel missing any field. Actual prices, oracle
values, realized regret, realized value, and return-to-go may be labels or
evaluation quantities only, never transformer inputs.

## Split and training

- Use the same full-history temporal split: at least 293 earlier anchors,
  28 validation anchors, then an untouched future test block.
- Train one contextual transformer architecture under two objectives:
  `forecast_loss` and `hybrid_forecast_decision_loss`.
- Warm-start the hybrid condition from the forecast-loss checkpoint.
- Select hybrid weight, smoothing, epoch/checkpoint, and any regularization on
  validation only; freeze them before future-test evaluation.
- Record seed, code commit, input artifact hashes, selected checkpoint, and
  all solver statuses.

## Storage contract

The differentiable relaxed dispatch layer and strict evaluator must use the
same per-tenant capacity, power, SOC bounds, round-trip efficiency, degradation
cost, starting SOC, and terminal-SOC policy. The strict evaluator currently has
no terminal equality constraint; aligned training therefore uses
`enforce_terminal_soc_equality=false`. The legacy v1.2 experiment's equality
constraint remains historical evidence only.

## Present source gate

The current 365-day V2+ candidate library does not contain this complete
feature panel. It is therefore unsuitable for this aligned experiment and is
rejected by the contract. Before training, materialize a source-backed joined
panel with feature provenance and availability timestamps. This is a data
readiness condition, not permission to impute outcome information or silently
drop feature families.

## Boundaries

All outputs stay offline research evidence with
`market_execution_enabled=false`. A true temporal Decision Transformer remains
deferred until an independently validated trajectory corpus exists.
