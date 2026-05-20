# Literature Review Academic Review - 2026-05-20

Scope: Chapter 2 / literature review in `docs/thesis/chapters/02-literature-review.md`
and the matching Google Doc tab `t.wox4yywng724`.

## Review Result

Chapter 2 is academically usable after this pass, with one important boundary:
it should be presented as a literature-supported and repo-grounded review of an
offline/read-model BESS arbitrage research system, not as evidence of live market
execution, a deployed Decision Transformer controller, a complete DFL controller,
or a full electrochemical digital twin.

The chapter now supports its main claims with inline author-year and numbered
source callouts. The bibliography grew from the original 50 checked entries to
60 entries because the citation-completeness and DOI/URL enrichment passes found
real gaps where the text used a concept but the source list did not yet contain
the supporting reference.

## Citation Gaps Found And Fixed

| Area | Problem found | Fix applied |
|---|---|---|
| LP ESS scheduling | The text referred to Park et al. as a scheduling/LP source, but the bibliography did not contain the paper. | Added Park et al. (2017), DOI `10.3390/en10020207`, as source #51 and cited it in Sections 2.2-2.3. |
| Battery cost/performance assumptions | The text cited NREL cost/performance assumptions, but no explicit NREL source was listed in Section 2.13. | Added Augustine and Blair (2021) NREL Storage Futures as #52 and NREL ATB 2023 as #53. |
| Optimization layers | The text used differentiable optimization-layer framing but only had cvxpylayers/Agrawal et al.; OptNet was missing. | Added Amos and Kolter (2017) as #54 and cited it alongside Agrawal et al. |
| MLOps/evidence pipeline | Dagster, MLflow, FastAPI, Pydantic, and Medallion architecture claims were present without primary documentation sources. | Added official documentation references #55-#59 and cited them in Section 2.8. |
| Broad synthesis claims | Several synthesis paragraphs had no citation even though they summarized PTO/DFL, benchmark, DT, or digital-twin boundaries. | Added compact source callouts without overloading every sentence. |
| Probabilistic/quantile forecasting context | Methodology cited the Jin source but Chapter 2 did not yet list its verified DOI/URL. | Added Jin and Blanco-Encomienda (2026), DOI `10.1002/for.70065`, as source #60. |

## Link And Metadata Review

The earlier source audit verified the original 50 references. This pass extended
that audit to sources #51-#59. Current status:

- bibliography count: 60 sources;
- known bad NBEATSx arXiv URL `2201.12886`: removed from Chapter 2 and technical paper index;
- correct NBEATSx arXiv URL: `2104.05522`;
- corrected Yi et al. title: `A Decision-Focused Predict-then-Bid Framework for Strategic Energy Storage`;
- corrected Persak and Anjos title: `Decision-Focused Forecasting: A Differentiable Multistage Optimisation Architecture`;
- copied URLs no longer carry terminal punctuation in Chapter 2.

The detailed link-by-link table is maintained in
`docs/thesis/sources/literature-review-source-audit-2026-05-20.md`.

## Academic Correctness Review

The chapter is strongest where it separates three different layers:

1. Current Level 1 implementation: observed-data DAM benchmark, `strict_similar_day`,
   LP dispatch, degradation proxy, feasibility checks, read models, and evidence
   tracking.
2. Research challenger layer: NBEATSx/TFT, calibration, selectors, AFL/AFE,
   trajectory/value learning, relaxed/perturbed DFL, and offline DT preparation.
3. Future operational target: predict-then-bid, richer DFL/DT strategy layer,
   venue-aware or multi-venue markets, deeper battery model, and regulated
   execution semantics.

That separation is academically important. It prevents the literature review
from turning future architecture into a claim about current implementation.

The following claims are now defensible:

- EPF models such as NBEATSx and TFT are relevant forecast candidates, but must be
  judged through downstream LP value/regret, not MAE/RMSE alone.
- `strict_similar_day` is a legitimate strong baseline, especially under a small
  decision-evaluation panel.
- Negative or blocked neural/DFL results are not a failure; they are valid
  evidence that decision-value improvement is harder than forecast-family
  selection.
- The current battery layer is a degradation-aware economic proxy, not a full
  P2D/SEI/thermal digital twin.
- Market-coupling and neighboring-market features are plausible future
  exogenous covariates, but they require publication-time, licensing, timezone,
  currency, market-rule, and domain-shift checks before training use.
- EU AI Act and governance sources support deterministic validation, logging,
  human/operator review, provenance, and `market_execution_enabled=false`.

The following claims should still be avoided:

- "The system performs live trading."
- "The Decision Transformer is deployed as the controller."
- "DFL is fully implemented and promoted as the production strategy."
- "The current battery model is a full electrochemical digital twin."
- "Ukrainian DAM is already fully equivalent to SDAC/SIDC market coupling for all
  relevant delivery dates."
- "European market rows can be mixed directly into Ukrainian DAM training data."

## Plagiarism And Attribution Risk

This was a citation and source-target review, not a formal plagiarism/similarity
scan. From the text reviewed, the main plagiarism risk was not long copied prose;
it was under-attribution: concepts appeared in the narrative without a visible
source callout. The pass reduced that risk by adding compact inline citations and
missing bibliography entries.

Before final submission, a formal similarity check should still be run if the
university requires it. That check should verify similarity against published
papers, web documentation, and any generated intermediate drafts.

## Remaining Risks Before Final Submission

- Policy and regulatory sources are time-sensitive. Re-check NEURC, OREE,
  European Commission, ACER, and AI Act pages close to submission.
- arXiv-only sources should remain labelled as preprints unless a final journal
  version or DOI is added.
- The 2026 review/preprint sources are current but may change metadata, venue, or
  claims. Re-run metadata checks before final formatting.
- Google Docs rendered formatting cannot be pixel-verified from connector text
  alone; connector readback can verify text, tab identity, and inserted content.
- The bibliography format is consistent enough for a working thesis draft, but a
  final pass should convert it to the exact style required by the program if the
  syllabus mandates a specific citation format.
