# Week 3 Research Source Map

Date indexed: 2026-05-07

This source map connects the Week 3 deep-research reports to the current
repository. It is intentionally conservative: reports can motivate future work,
but implementation claims must be backed by code, tests, materialized evidence,
or tracked documentation.

## Thesis Question

Can decision-aware forecasting reduce regret in Ukrainian BESS day-ahead
arbitrage compared with a frozen strict-similar-day LP baseline, while preserving
SOC feasibility, degradation-aware economics, and operator-verifiable safety
constraints?

The current project answer is partial: the benchmark and calibration substrate
exists, but full differentiable DFL is not yet a positive result.

## Implemented Now

| Research claim | Repository evidence |
|---|---|
| Level 1 scope is DAM-only, hourly, and UAH-native. | `CONTEXT.md`, `BaselineSolverConfig`, `LEVEL1_MARKET_VENUE="DAM"`, and `LEVEL1_INTERVAL_MINUTES=60`. |
| Strict similar-day is the control forecast. | `HourlyDamBaselineSolver.build_forecast` copies `t-24h` or `t-168h` slots without smoothing. |
| Forecast candidates are routed through the same LP/oracle scorer. | `evaluate_forecast_candidates_against_oracle` and `real_data_rolling_origin_benchmark_frame`. |
| Evidence uses rolling-origin temporal discipline. | `run_real_data_rolling_origin_benchmark` and Dagster check `dnipro_thesis_grade_90_anchor_evidence`. |
| DFL readiness is gated before stronger claims. | `DFL_READINESS_GATE.md`, `smart_arbitrage.evidence.quality_checks`, and registered asset checks. |
| First offline DFL experiment exists but is negative. | `offline_dfl_experiment_frame` and `OFFLINE_DFL_EXPERIMENT.md`. |

## Supported By Benchmark Evidence

| Evidence lane | Current interpretation |
|---|---|
| Week 3 Dnipro 30-anchor benchmark | Accepted Week 3 thesis-grade real-data evidence. |
| Dnipro 90-anchor calibration preview | Prepared-ahead calibration/selector evidence; useful for next demo path, not the Week 3 headline. |
| All-tenant diagnostic tables in reports | Useful aggregate context only; must be labeled separately from Dnipro latest-batch values. |
| Horizon-aware calibration | Improves neural candidate diagnostics in some runs, but does not replace strict similar-day as frozen control. |
| Risk-adjusted selector | Selector evidence only; not full DFL and not market execution. |
| Offline relaxed-LP DFL v0 | Proves the training loop can run on gated data, but held-out regret worsens and must not be promoted. |

## Planned Research

| Research direction | Why it matters | Current boundary |
|---|---|---|
| DFL training-example vectors | Needed for SPO+, differentiable optimization, and sequence-model datasets. | Implement as a sidecar v2 contract; do not mutate `dfl_training_frame`. |
| Conservative promotion gate | Prevents weak ML/DFL candidates from being presented as improved control. | Pure Python helper first; no API/dashboard contract. |
| DFL-lite v2 | Next experiment after negative offline v0: validation-safe checkpointing, strict-vs-relaxed regret comparison, small covariate adapter. | Not in the foundation slice. |
| Decision Transformer / M3DT-inspired strategy | Useful offline research primitive once DFL data and gates are mature. | Not live, not deployed, not next implementation slice. |
| Probabilistic/diffusion forecasting | Useful for uncertainty and CRPS/pinball methodology. | Literature/watch track only. |
| ProbTS/TFB/Monash benchmarks | Useful external reproducibility reference. | Separate environment/spec only; no root dependency change. |

## Out Of Scope / Not Claimed

- Live trading or market execution.
- Proposed Bid or Cleared Trade semantics for the current benchmark rows.
- Full multi-market DAM/IDM/balancing optimization.
- Full electrochemical SOH or path-dependent degradation model.
- Full differentiable DFL success.
- Decision Transformer deployment.
- SOTA deep-learning superiority.
- Diffusion/foundation-model implementation in the current stack.

## Report Mapping

| Report | Integrated takeaway | Repo action |
|---|---|---|
| `Research Summary Integration.md` | Keep the thesis logic as `Forecast -> Optimize -> Validate -> Compare regret -> Promote only if decision value improves`. | Use as narrative basis for baseline freeze and roadmap docs. |
| `Decision-Focused Learning and Decision-Aware Forecasting.md` | DFL should start with data/evaluation substrate, not a large policy model. | Add vector training examples and promotion gate before any DFL v2 experiment. |
| `Foucsed TFT and NbeatsX Reading.md` | NBEATSx/TFT are forecast candidates that need decision-value evaluation. | Preserve current candidate framing in literature review and source map. |
| `NeuralForecast as a diffusion bridge` | Official NBEATSx/TFT implementations support reproducibility, but dependencies must stay optional. | Defer root dependency changes; keep current adapters as the implementation surface. |
| `Thesis DOI stack for forecasting.md` | Reproducibility needs commands, splits, configs, metrics, and artifact manifests. | Reuse evidence registry and manifests; add baseline freeze documentation. |
| `Diffusion-Based Generative Modeling...` | Diffusion is relevant when calibrated trajectory uncertainty is required. | Watch track only; no implementation in this slice. |
| `Project specification for integrating ProbTS, TFB, and Monash datasets into.md` | External benchmarks need isolated environments and careful metric consistency. | Future adapter plan only; no root `pyproject.toml` changes. |

## Source Guardrails

- [TSFM leakage evaluation](https://huggingface.co/papers/2510.13654) supports latest-batch, temporal/no-leakage evaluation and strict source separation.
- [Decision-Focused Learning survey](https://huggingface.co/papers/2307.13565) supports decision-quality learning through constrained optimization, while also justifying cautious benchmark-first sequencing.
- [PriceFM](https://huggingface.co/papers/2508.04875) supports future electricity-price foundation-model work with European cross-region context.
- [THieF](https://huggingface.co/papers/2508.11372) supports later temporal hierarchy forecasting after the current evaluation protocol is stable.
