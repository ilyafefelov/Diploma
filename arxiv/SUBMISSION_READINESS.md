# arXiv submission readiness

Status: **v1.1 corrective sources, PDF, evidence, and source archive are
release-ready. The defended thesis PDF and immutable v1 tag remain unchanged.
An arXiv upload is still blocked by the author's license choice, possible
endorsement, and inspection of arXiv's server-generated preview.**

## Candidate files

- PDF: `build/main.pdf` -- 17 pages, 4 figures, 10 tables; SHA-256
  `FFED0736BCADDA424D4BE01273FE61070B9E631FB9406DEEDFEBA163622334F1`.
- Source archive: `submission/arxiv-bess-paper-source-v1.1.zip` -- 473,560 bytes,
  SHA-256
  `A444B90EA5F6071B4FF795DABE5EA6010F73CD57A4C475DEA48C1A7F213F0BCB`.
- Metadata: `arxiv-metadata.md` -- 1,656-character ASCII abstract.
- Primary category: `eess.SY`.
- Cross-list: none for version 1.
- Provenance comment: `Based on a Master's thesis defended in 2026.`

## Local acceptance results

- Tectonic source build: passed.
- Isolated build from extracted v1.1 source archive: passed (17 pages).
- PDF visual inspection: revised pages 1 and 8--17 inspected; unchanged pages
  2--7 retain the previously inspected layout. Tables and references remain
  readable.
- Headline reconstruction test: passed.
- Model-lineage audit test: passed.
- Declared ancillary SHA-256 hashes: 16/16 matched, including the temporal
  replay, temporal-suite JSON/CSV, and regenerated combined lineage audit.
- Citation keys: 31 used, 31 bibliography entries, no missing/uncited/duplicates.
- Metadata abstract: 1,656 characters, below arXiv's 1,920-character limit.
- Package scan: 37 allowlisted entries; no absolute local paths, secrets,
  runtime caches, build logs, or compiled PDF.

## Claim boundary

The central result is the retrospective V2+ comparison over 18 distinct DAM
dates and five configured profiles. The historical `dt_v2_plus` artifact is a
random forest trained on exact mirrored representations, with four profile-row
switches on one date. The separate HF transformer-backbone result is also a
mirrored-packet diagnostic; the 32-day read-model audit has no realized-regret
outcome. None is OOS learned-model evidence or market execution.

The post-defense RF temporal suite contains 14 protocol rows spanning two
source models, three evaluation windows, latest-window threshold sensitivity,
and three-seed checks at the frozen 20 UAH threshold. All rows have zero
candidate-content overlap. No row improves V2+: 11 tie through full abstention,
while three earlier-window protocols increase mean regret by 65.18--123.08 UAH
at primary seed 42 and remain harmful across all three seeds. This is negative
retrospective evidence, not prospective confirmation and not part of the
defended thesis.
