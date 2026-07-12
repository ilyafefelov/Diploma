# arXiv paper package

This directory contains the English article derived from the defended 2026
Master's thesis, its verified bibliography, compact evidence subset, LaTeX
sources, figures, and submission metadata.

- `build/main.pdf`: locally compiled 17-page v1.1 corrective manuscript.
- `main.tex`, `sections/`, `figures/`, `references.bib`: publication sources.
- `evidence/`: self-contained reconstruction inputs, script, test, and hashes.
- `workflow/`: internal drafting and review records; not included in the arXiv upload.
- `submission/arxiv-bess-paper-source-v1.1.zip`: locally verified corrective
  upload candidate, including the evidence subset under arXiv's required
  `anc/` directory.

The immutable repository citation for this version is
`v2026-arxiv-v1.1`. The earlier `v2026-arxiv-v1` tag and the defended thesis
PDF remain preserved as archival records.

The paper is retrospective/descriptive. It does not claim market execution,
live bidding, autonomous dispatch, full differentiable DFL, or a deployed
Decision Transformer. The reported operator surface keeps
`market_execution_enabled=false`.
