# arXiv paper package

This directory contains the English article derived from the defended 2026
Master's thesis, its verified bibliography, compact evidence subset, LaTeX
sources, figures, and submission metadata.

- `build/main.pdf`: locally compiled 18-page v1.2 manuscript, retained as an
  archival paper artifact.
- `main.tex`, `sections/`, `figures/`, `references.bib`: publication sources.
- `evidence/`: self-contained reconstruction inputs, script, test, and hashes.
- `workflow/`: internal drafting and review records; not included in the arXiv upload.
- `submission/arxiv-bess-paper-source-v1.2.zip`: locally verified v1.2
  upload candidate, including the evidence subset under arXiv's required
  `anc/` directory.

The v1.3 evidence release corrects the legacy temporal-DT claim and adds the
full-history HF and aligned-DFL follow-up records without silently changing the
v1.2 manuscript or upload archive. Cite `v2026-arxiv-v1.3` for the corrected
evidence package; `v2026-arxiv-v1.2`, `v2026-arxiv-v1.1`, the earlier
`v2026-arxiv-v1` tag, and the defended thesis PDF remain preserved as archival
records.

The paper is retrospective/descriptive. It does not claim market execution,
live bidding, autonomous dispatch, full predict-then-bid, or a deployed
Decision Transformer. The legacy v1.2 candidate-list suite is invalid/non-causal
diagnostic evidence, not temporal-DT evidence. The small-data v1.2
differentiable suite is a narrow negative result; the v1.3 aligned DFL result
is experimental within-architecture evidence only. All releases keep
`market_execution_enabled=false`.
