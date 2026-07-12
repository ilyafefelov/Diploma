# arXiv pre-submission audit (2026-07-12)

Status: **major revision required before upload**.

This packet records the repository, defended-thesis, manuscript, model-lineage,
reproducibility, and arXiv-process audit performed before the first submission.
The audit used the current local repository and frozen evidence, reran the
headline reconstruction and the canonical safe-switch materializer, and checked
current arXiv guidance.

## Bottom line

- The main V2+ retrospective result reproduces exactly and is suitable as the
  paper's central descriptive result.
- The canonical artifact historically named `dt_v2_plus` is a
  `RandomForestRegressor`, not a Decision Transformer.
- Its training rows are exact timestamp-shifted copies of the evaluation packet.
  All four nonfallback profile-row changes occur on one delivery date.
- The separate Hugging Face lineage does instantiate
  `DecisionTransformerModel`, but its frozen 158.71 UAH diagnostic uses the same
  mirrored packet and selects its threshold on that packet. It is not an
  independent holdout estimate.
- The defended PDF and several repository summaries conflate the RF artifact
  with DT. A public erratum and terminology correction are required.
- The arXiv source tree is currently local/untracked. The availability statement
  becomes true only after the exact paper and evidence package are published in
  a new immutable tag or release.

See `review.md` for the editorial decision, `model-lineage.md` for the forensic
mapping, and `arxiv-guidance.md` for the submission checklist.
