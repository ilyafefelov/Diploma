# Correction release v1.1

Date: 2026-07-12

This release follows the immutable `v2026-arxiv-v1` paper/evidence release.
It does not replace the defended thesis PDF or rewrite the earlier tag.

## Scientific corrections

1. The historical artifact identifier `dt_v2_plus` is identified as a
   `RandomForestRegressor`, not a Decision Transformer.
2. Its 168.1566 UAH result is classified as an exact-mirror in-packet
   diagnostic. The stored p-value and seed repetition are not used as
   inferential evidence.
3. A post-defense temporal suite evaluates 14 protocol rows with zero
   candidate-content overlap between training and evaluation.
4. The suite contains no beneficial RF protocol: 11 rows tie V2+ through full
   abstention and three earlier-window rows increase mean regret by
   65.18--123.08 UAH at primary seed 42. Harm remains across seeds 42, 2026,
   and 7 at the frozen 20 UAH threshold.
5. The RF selector is therefore not promoted or reused as a production-facing
   model. `market_execution_enabled=false` remains unchanged.

## Verification gates

- focused regression, lineage, reconstruction, replay, and suite tests;
- Ruff and Mypy on all affected Python surfaces;
- isolated LaTeX build from the source archive;
- ancillary hash audit and model-lineage audit;
- visual inspection of the revised 17-page PDF.
