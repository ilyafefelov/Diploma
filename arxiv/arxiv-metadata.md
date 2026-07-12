# arXiv submission metadata

## Required fields

- **Title:** Decision-Value Evaluation of BESS Arbitrage Recommendations: A Reproducible Retrospective Study of Ukraine's Day-Ahead Market
- **Authors:** Illya Fefelov
- **Primary category:** eess.SY (Systems and Control)
- **Cross-list:** none for version 1; reconsider cs.LG only after genuinely temporally separated learned-selector evidence
- **Are you an author of this paper?** Yes

## Abstract

Battery energy storage system (BESS) arbitrage is often treated as a price-forecasting problem, although forecast error is only indirectly related to charge-discharge value. This retrospective study evaluates recommendations for Ukraine's day-ahead market by converting forecast and heuristic candidates into feasible schedules and scoring them in one linear-programming and realized-price oracle contour. The main window crosses 18 dates with five configured BESS profiles, yielding 90 profile-date rows that share one market-price process. Raw and horizon-calibrated NBEATSx are co-primary sensitivities. With calibrated NBEATSx, Schedule/Value Learner V2+ had mean regret 174.77 UAH, versus 310.58 UAH for a strict comparator and 206.37 UAH for V2; with raw NBEATSx, V2+ had 193.36 UAH, versus 310.58 and 225.44 UAH. V2+ was below both comparators in four adjacent windows for both sources. Capacity normalization preserves the ordering, and paired date-cluster bootstrap conclusions remain positive for block lengths 2, 3, 4, and 6. These are descriptive estimates, not confirmatory holdout results. A lineage audit found that an artifact historically named dt_v2_plus is a random forest trained on exact timestamp-shifted copies of the evaluation packet; all four nonfallback profile-row changes occur on one date. Separate Hugging Face transformer results use the mirrored packet or report read-model readiness without realized regret. They are engineering diagnostics, not model-performance claims. The contribution is evidentiary: reproducible separation of forecast quality, schedule value, model lineage, source governance, and execution readiness. Published rows are used for published targets; forecasts provide unpublished-horizon scenarios. Intraday support is a read-model capability, not evaluated bidding performance, and market execution remains disabled.

## Optional fields

- **Comments:** 16 pages, 4 figures, 9 tables. Based on a Master's thesis defended in 2026. Code and evidence artifacts: https://github.com/ilyafefelov/Diploma/tree/v2026-arxiv-v1
- **Report number:** leave blank
- **Journal reference:** leave blank
- **DOI:** leave blank
- **ACM classification:** leave blank
- **MSC classification:** leave blank
- **License:** author selection required; arXiv states that the choice is irrevocable

## Submission files

- Source archive: `submission/arxiv-bess-paper-source-v2.zip`
- Locally verified PDF: `build/main.pdf`
- Ancillary evidence: `anc/evidence/` inside the source archive
