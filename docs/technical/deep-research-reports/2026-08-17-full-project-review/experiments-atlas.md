# Experiments atlas

| Lane | Implemented evidence | Promotion boundary |
|---|---|---|
| Strict LP baseline | Feasible hourly schedules, degradation accounting, comparator value | Current baseline/read model |
| Forecast diagnostics | Naive, official NBEATSx/TFT adapters, rolling and calibration tests | Adapter/forecast evidence only |
| DFL research | Decision loss, regret surrogates, schedule-value learners, strict promotion gates | Offline research; not full differentiable controller |
| DT/LAVA research | Trajectories, residual challenger, bridge/readiness and margin-smoke contracts | Blocked without V13 inputs; no deployed DT |
| V13 acquisition | OREE receipt/source audits, safe-switch deficits, SCMO probes, tracked readiness contracts | Acquisition gate, not modeling slice |
| Public forecast challenge | Point-in-time forecast JSON and post-realization scoreboard | Public read model; no execution |

## Quality interpretation

Passing unit tests proves contract behavior and deterministic transformations.
It does not prove economic superiority. Numeric value/regret claims require a
specific materialized packet under a frozen comparator and must retain negative
results. The clean release checkout intentionally does not manufacture missing
V13 or DT/LAVA artifacts.
