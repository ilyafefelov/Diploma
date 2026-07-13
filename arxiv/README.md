# arXiv paper package

This directory contains the English article derived from the defended 2026
Master's thesis, its verified bibliography, compact evidence subset, LaTeX
sources, figures, and submission metadata.

- `build/main.pdf`: locally compiled 18-page v1.2 manuscript.
- `main.tex`, `sections/`, `figures/`, `references.bib`: publication sources.
- `evidence/`: self-contained reconstruction inputs, script, test, and hashes.
- `workflow/`: internal drafting and review records; not included in the arXiv upload.
- `submission/arxiv-bess-paper-source-v1.2.zip`: locally verified v1.2
  upload candidate, including the evidence subset under arXiv's required
  `anc/` directory.

The immutable repository citation for this version will be
`v2026-arxiv-v1.2`. The `v2026-arxiv-v1.1` correction, earlier
`v2026-arxiv-v1` tag, and defended thesis PDF remain preserved as archival
records.

The paper is retrospective/descriptive. It does not claim market execution,
live bidding, autonomous dispatch, full predict-then-bid, or a deployed
Decision Transformer. It reports a differentiable forecast-to-storage research
shadow and keeps `market_execution_enabled=false`.
