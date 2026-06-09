# Methodology Traceability Pass - 2026-05-20

Scope: `docs/thesis/chapters/02-literature-review.md`,
`docs/thesis/chapters/03-Methodology.md`, and the thesis source index.

## Purpose

This pass follows the deeper similarity/citation audit finding that the
literature review is mostly well supported, while Methodology needed stronger
traceability for implementation and evidence claims. The goal was not to add
paper citations mechanically to every sentence. The goal was to distinguish:

- academic method claims, which need paper/DOI/arXiv support;
- implementation claims, which need repository artifact support;
- evidence/result claims, which need configs, scripts, technical docs, or
  evidence-packet references.

## DOI And URL Enrichment

Chapter 2 bibliography entries that had a DOI or arXiv identifier without a
canonical URL were enriched with DOI/arXiv URLs. A local check confirms there
are no remaining Chapter 2 bibliography entries with `DOI:` or `arXiv:` but no
URL.

The Jin probabilistic-forecasting source used by Methodology was also corrected
and made explicit:

- corrected citation target: Jin and Blanco-Encomienda (2026), `Seasonal
  Decomposition-Enhanced Deep Learning Architecture for Probabilistic
  Forecasting`;
- DOI: `10.1002/for.70065`;
- canonical URL: <https://doi.org/10.1002/for.70065>;
- note: the local PDF filename contains `2025`, but Crossref metadata verifies
  the journal citation as 2026.

The Chapter 2 bibliography now has 60 sources.

## Methodology Changes

Methodology now includes explicit support markers for the highest-risk claim
families:

- `forecast -> optimize -> validate -> compare regret -> promote only if
  decision value improves` is tied to LP scheduling, EPF benchmark discipline,
  SPO/DFL, and storage-specific DFL sources.
- `strict_similar_day` fallback and Offline Strategy Promotion are tied to
  local promotion-boundary technical docs.
- observed Ukrainian evidence, synthetic-row limits, and external market
  coupling are tied to data-source docs, tracked configs, and governance docs.
- rolling-origin evaluation and oracle LP are tied to strict benchmark docs and
  `src/smart_arbitrage/strategy/forecast_strategy_evaluation.py`.
- NBEATSx, TFT, calibration, DFL, and Decision Transformer roles are tied both
  to paper sources and to the repo artifacts that implement or bound them.
- operator-facing read model claims are tied to the API appendix,
  `docs/technical/API_ENDPOINTS.md`, and `api/main.py`.
- run/evidence reproducibility is tied to attempt-manifest and registry-export
  scripts.

Section `3.13` now contains DOI/arXiv/URL metadata for the main methodology
sources and a traceability matrix that maps implementation claims to repository
artifacts.

## Validation

Completed checks:

- local Markdown link existence check for Chapter 2, Methodology, and the thesis
  source index: pass;
- Chapter 2 DOI/arXiv entries without URL: none found;
- old Jin-year / old source-count wording in thesis docs: none found;
- `git diff --check` for the edited thesis/source files: no whitespace errors
  reported, only existing LF/CRLF warnings.
- after the traceability pass, a local claim-marker heuristic on Methodology
  reports 11 remaining claim-like paragraphs without an obvious marker; these
  are mostly equations, metric lists, or explanatory formula definitions already
  bracketed by supported paragraphs rather than unsupported source claims.

## Remaining Work

This pass focused on Methodology. The next traceability pass should handle:

- Chapter 1 implementation/status claims that still read as project assertions
  without an explicit artifact reference;
- Chapter 4 self-similarity candidate around Candidate-Value DFL v3, either by
  citing the technical artifact or rewriting the paragraph more independently;
- final Google Docs sync after the local Markdown version is stable.
