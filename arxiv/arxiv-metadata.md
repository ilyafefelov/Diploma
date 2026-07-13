# arXiv submission metadata

## Required fields

- **Title:** Decision-Value Evaluation of BESS Arbitrage Recommendations: A Reproducible Retrospective Study of Ukraine's Day-Ahead Market
- **Authors:** Illya Fefelov
- **Primary category:** eess.SY (Systems and Control)
- **Cross-list:** none for version 1; reconsider cs.LG only after genuinely temporally separated learned-selector evidence
- **Are you an author of this paper?** Yes

## Abstract

Battery energy storage arbitrage is often treated as a price-forecasting problem, although forecast error is only indirectly related to charge-discharge value. This retrospective study evaluates Ukraine day-ahead recommendations by converting forecast and heuristic candidates into feasible schedules and scoring them with one LP/realized-price oracle contour. The main window crosses 18 dates with five configured BESS profiles. With calibrated NBEATSx, Schedule/Value Learner V2+ had mean regret 174.77 UAH, versus 310.58 UAH for a strict comparator and 206.37 UAH for V2; with raw NBEATSx, V2+ had 193.36 UAH, versus 310.58 and 225.44 UAH. V2+ was below both comparators in four adjacent windows for both sources. A lineage audit found that the later artifact historically named dt_v2_plus is a random forest trained on exact timestamp-shifted copies of evaluation candidates. A time-separated RF suite has no improvement. The previously reported 36-run candidate-list suite is reclassified in the v1.3 evidence supplement as invalid/non-causal diagnostic evidence, not a return-conditioned Decision Transformer result. The preregistered v1.2 differentiable forecast-to-storage suite remains negative evidence only for its small-data residual-corrector implementation with mismatched terminal-SOC contracts. A v1.3 full-history candidate-set encoder is also negative relative to its compatible fallback. Separately, an experimental aligned hybrid DFL transformer reduces strict regret by 17.90 UAH relative to the same forecast-loss transformer on 18 untouched market dates; it does not beat or replace V2+. The contribution is evidentiary rather than a new ML method. Intraday support remains a read-model capability and market execution is disabled.

## Optional fields

- **Comments:** 18-page v1.2 archival manuscript plus v1.3 corrective evidence supplement. Based on a Master's thesis defended in 2026. Code and evidence artifacts: https://github.com/ilyafefelov/Diploma/tree/v2026-arxiv-v1.3
- **Report number:** leave blank
- **Journal reference:** leave blank
- **DOI:** leave blank
- **ACM classification:** leave blank
- **MSC classification:** leave blank
- **License:** author selection required; arXiv states that the choice is irrevocable

## Submission files

- Source archive: `submission/arxiv-bess-paper-source-v1.2.zip` (archival manuscript package)
- Locally verified PDF: `build/main.pdf`
- v1.3 evidence supplement: tracked correction and lineage JSON under `evidence/`
- Ancillary evidence: `anc/evidence/` inside the source archive, including the
  post-defense RF suite, the narrow differentiable-suite paired rows, OREE
  source audit, and acceptance checks
