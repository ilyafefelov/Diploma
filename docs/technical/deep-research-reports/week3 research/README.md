# Week 3 Deep Research Reports

Date indexed: 2026-05-07

This folder stores the Week 3 deep-research intake used to move the thesis from
demo evidence toward a Decision-Focused Learning foundation. The reports are
source material, not implementation truth. The canonical implementation truth
remains the code, `CONTEXT.md`, the Dagster asset graph, and the tracked evidence
registry.

## Report Index

| Report | Status | Project use |
|---|---|---|
| [Research Summary Integration.md](<Research Summary Integration.md>) | Include | Canonical narrative bridge: Level 1 DAM baseline, real-data benchmark, strict similar-day control, and DFL claim boundaries. |
| [Decision-Focused Learning and Decision-Aware Forecasting.md](<Decision-Focused Learning and Decision-Aware Forecasting.md>) | Include | DFL roadmap input: decision-value metrics, DFL-lite sequence, promotion rules, and Decision Transformer as offline primitive. |
| [Foucsed TFT and NbeatsX Reading.md](<Foucsed TFT and NbeatsX Reading.md>) | Include | Forecasting bibliography input for NBEATSx/TFT as research candidates, not operational dispatch policies. |
| [NeuralForecast as a diffusion bridge](<NeuralForecast as a diffusion bridge>) | Include/watch | Implementation-source map for NBEATSx/TFT; diffusion bridge remains future context. |
| [Thesis DOI stack for forecasting.md](<Thesis DOI stack for forecasting.md>) | Include | Reproducibility checklist for thesis-grade forecast experiments and artifact reporting. |
| [Diffusion-Based Generative Modeling and Sequence Framing for Multivariate Time-Series Forecasting.md](<Diffusion-Based Generative Modeling and Sequence Framing for Multivariate Time-Series Forecasting.md>) | Watch | Future uncertainty/probabilistic forecasting context; no current dependency or model expansion. |
| [Project specification for integrating ProbTS, TFB, and Monash datasets into.md](<Project specification for integrating ProbTS, TFB, and Monash datasets into.md>) | Watch | External benchmark plan only; root project dependencies remain unchanged. |

## Current Repository Alignment

The reports assume several tasks are still future work. In this repository, some
of that foundation already exists:

- Real-data rolling-origin benchmark assets and thesis-grade Dnipro evidence are
  materialized and documented.
- Regret-weighted and horizon-regret-weighted calibration assets already exist.
- `dfl_training_frame` already provides a summary DFL-ready table.
- Dagster evidence checks and the DFL readiness gate are registered.
- `offline_dfl_experiment_frame` already runs a bounded relaxed-LP experiment.
  Its first held-out result is negative and remains diagnostic only.

The next accepted implementation slice therefore does not start DFL from
scratch. It adds a source map, freezes the control comparator, and adds a richer
sidecar DFL training-example contract plus a promotion gate.

## Claim Categories

| Category | Meaning |
|---|---|
| Implemented now | Present in code/docs and allowed as current thesis or engineering evidence. |
| Supported by benchmark evidence | Present in materialized Dnipro/all-tenant evidence, but still scoped to benchmark/calibration/selector results. |
| Planned research | Useful source-supported direction, but not a current implementation claim. |
| Out of scope / not claimed | Must not be presented as a current result. |

## Guardrails

- `strict_similar_day` remains the frozen Level 1 control comparator.
- Forecast candidates are evaluated by LP/oracle regret and net UAH value, not
  MAE/RMSE alone.
- The current battery layer is a feasibility-and-economics preview model, not a
  full electrochemical digital twin.
- Calibration and offline DFL assets are research evidence only. They are not
  full DFL, not a Decision Transformer policy, and not market execution.
- Diffusion models, ProbTS/TFB/Monash integration, and foundation-model
  forecasting are future tracks unless a later slice explicitly approves them.

See [source-map.md](source-map.md) for the detailed report-to-code mapping.
