# UA Context-Aware Safe-Switch Before DT/LAVA

## Status

This slice is an additive Offline Strategy Promotion experiment. It does not
replace the frozen Ukrainian-only V2+ baseline unless it beats the same strict
LP/oracle gate.

Frozen comparator:

- calibrated V2+ mean regret: `174.77` UAH;
- calibrated V2+ median regret: `67.30` UAH;
- rolling robustness: `4 / 4` windows;
- `market_execution_enabled=false`.

## Why This Exists

The corrected V2+ leakage audit showed that the baseline is not artificially
strong: mutating final-holdout regret labels no longer changes candidate
selection, and the headline result stayed at `174.77` UAH. The next question is
narrower: V2+ already chooses the best available final candidate on most rows,
but the oracle-gap audit still found missed rows where another feasible schedule
would have reduced regret after scoring.

This experiment asks whether those missed wins are distinguishable **before**
the scoring window using Ukrainian context only.

## Source-Backed Context Lanes

The gate adds three context lanes, each timestamped before the anchor:

- `dfl_ua_calendar_publication_context_frame`: anchor hour, day of week,
  weekend/holiday and morning/evening block features, plus prior-known DAM
  publication metadata.
- `dfl_ua_weather_load_context_frame`: Open-Meteo historical weather, tenant
  load/config proxy, PV/net-load aggregates.
- `dfl_ua_grid_event_context_frame`: Ukrenergo/grid-event context from the
  existing grid-event signal assets, with prior-availability flags.

The merged feature panel is
`dfl_ua_context_oracle_gap_feature_panel_frame`. Selector features are prefixed
with `selector_feature_*`; realized outcomes remain `label_*` or
`diagnostic_*`. Poland, ENTSO-E, and other EU market features are not inputs to
this slice.

## Safe-Switch Models

The safe-switch layer trains two bounded scorers over the same feature panel:

- `sklearn`: ridge/logistic-style expected regret delta and tail-risk heads.
- `torch`: a small deterministic MLP with CUDA support when available and CPU
  fallback otherwise.

Both models predict candidate-level regret delta versus corrected V2+ and a
tail-risk probability. They select a non-V2+ schedule only when prior evidence
predicts a strong improvement and low tail risk. Otherwise the selected row is
the corrected V2+ fallback.

## Gate

Strict benchmark asset:

`dfl_ua_context_safe_switch_strict_lp_benchmark_frame`

Rolling robustness asset:

`dfl_ua_context_safe_switch_rolling_robustness_frame`

Promotion requires:

- mean regret beats corrected V2+ by at least `5%`;
- median regret is not worse than `67.30` UAH;
- rolling robustness passes `4 / 4` windows;
- zero safety violations;
- `market_execution_enabled=false`.

Diagnostic success is weaker: any positive mean improvement without median harm
and at least `3 / 4` rolling windows. That would justify a DT/LAVA follow-up,
but it still would not replace V2+ as the thesis headline.

## Run

```powershell
docker compose exec -T dagster-webserver uv run dagster asset materialize `
  -m smart_arbitrage.defs `
  --select dfl_ua_calendar_publication_context_frame,dfl_ua_weather_load_context_frame,dfl_ua_grid_event_context_frame,dfl_ua_context_oracle_gap_feature_panel_frame,dfl_ua_context_safe_switch_separability_audit_frame,dfl_ua_context_safe_switch_scorer_frame,dfl_ua_context_safe_switch_strict_lp_benchmark_frame,dfl_ua_context_safe_switch_rolling_robustness_frame `
  -c configs/real_data_dfl_ua_context_safe_switch_week3.yaml
```

If upstream oracle-gap/V2+ assets are not already available in the active
Dagster IO store, materialize their documented upstream selections first.

## Materialized Latest-90 Result

The full UA context path materialized in Dagster run
`79132fcb-e9dd-40c0-92b8-bb71b0d86087`. A corrected strict/rolling replay then
materialized in run `1573267c-9a00-49f7-8947-113ffc7b0c85` after aligning the
strict reference rows to the calibrated source used by the UA context feature
panel.

The separability audit found candidate-level upside, but also much larger
tail-risk exposure:

| Audit field | Value |
|---|---:|
| Train safe-switch win candidates | `1,641` |
| Train tail-risk loss candidates | `15,765` |
| Final missed safe-switch candidate opportunities | `82` |
| Pre-anchor distinguishability flag | `true` |

This means the richer Ukrainian context can describe some missed opportunities,
but the safe-win cases are still swamped by many more risky candidate switches.

Latest-holdout strict LP/oracle result:

| Selection role | Rows | Mean regret | Median regret | Safety violations |
|---|---:|---:|---:|---:|
| Corrected calibrated V2+ | `90` | `174.77` UAH | `67.30` UAH | `0` |
| `ua_context_safe_switch_sklearn` | `90` | `174.77` UAH | `67.30` UAH | `0` |
| `ua_context_safe_switch_torch` | `90` | `174.77` UAH | `67.30` UAH | `0` |

Both scorers activated full V2+ fallback for all five tenant/source scopes.
Rolling robustness also stayed diagnostic-only: sklearn and Torch each produced
`0 / 4` robust challenger windows and `0 / 4` diagnostic-success windows.

Interpretation: the context repair is useful evidence, but it does not promote a
new strategy. It shows that current Ukrainian context still cannot identify the
missed candidate wins safely enough to move away from V2+ under the unchanged
strict gate. The next DT/LAVA target should therefore use this label/audit layer
as training signal for tail-risk-aware candidate-index or schedule-family
selection, not raw hourly action imitation.

The follow-up DT/LAVA target is documented in
[DFL_UA_CONTEXT_LAVA_DT.md](DFL_UA_CONTEXT_LAVA_DT.md). It consumes this
UA-context feature/audit layer and predicts a feasible candidate index or
schedule family with corrected V2+ fallback, rather than emitting raw hourly
actions.

## Claim Boundary

This remains offline/read-model evidence only. It is not full DFL, not deployed
Decision Transformer control, not live market execution, and not a dashboard/API
default strategy switch.
