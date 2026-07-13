# arXiv submission metadata

## Required fields

- **Title:** Decision-Value Evaluation of BESS Arbitrage Recommendations: A Reproducible Retrospective Study of Ukraine's Day-Ahead Market
- **Authors:** Illya Fefelov
- **Primary category:** eess.SY (Systems and Control)
- **Cross-list:** none for version 1; reconsider cs.LG only after genuinely temporally separated learned-selector evidence
- **Are you an author of this paper?** Yes

## Abstract

Battery energy storage arbitrage is often treated as a price-forecasting problem, although forecast error is only indirectly related to charge-discharge value. This retrospective study evaluates Ukraine day-ahead recommendations by converting forecast and heuristic candidates into feasible schedules and scoring them with one LP/realized-price oracle contour. The main window crosses 18 dates with five configured BESS profiles. With calibrated NBEATSx, Schedule/Value Learner V2+ had mean regret 174.77 UAH, versus 310.58 UAH for a strict comparator and 206.37 UAH for V2; with raw NBEATSx, V2+ had 193.36 UAH, versus 310.58 and 225.44 UAH. V2+ was below both comparators in four adjacent windows for both sources. A lineage audit found that the later artifact historically named dt_v2_plus is a random forest trained on exact timestamp-shifted copies of evaluation candidates. A time-separated RF suite has no improvement. A separate 36-run return-conditioned Decision Transformer suite has zero train/evaluation content overlap: 33 runs tie V2+ and three are harmful. A preregistered 72-run differentiable forecast-to-storage suite backpropagates through profile-aware cvxpylayers problems and then uses strict LP/oracle rescoring. No run beats V2+. However, transformer correction has lower strict regret than matched MLP in 28/36 comparisons, and the forecast-loss transformer improves the raw schedule in 15/18 runs. This is positive architecture evidence and negative promotion evidence. The contribution is evidentiary rather than a new ML method. Intraday support remains a read-model capability and market execution is disabled.

## Optional fields

- **Comments:** 18 pages, 4 figures, 11 tables. Version 1.2. Based on a Master's thesis defended in 2026. Code and evidence artifacts: https://github.com/ilyafefelov/Diploma/tree/v2026-arxiv-v1.2
- **Report number:** leave blank
- **Journal reference:** leave blank
- **DOI:** leave blank
- **ACM classification:** leave blank
- **MSC classification:** leave blank
- **License:** author selection required; arXiv states that the choice is irrevocable

## Submission files

- Source archive: `submission/arxiv-bess-paper-source-v1.2.zip`
- Locally verified PDF: `build/main.pdf`
- Ancillary evidence: `anc/evidence/` inside the source archive, including the
  post-defense RF/DT temporal suites, differentiable-suite paired rows, OREE
  source audit, and acceptance checks
