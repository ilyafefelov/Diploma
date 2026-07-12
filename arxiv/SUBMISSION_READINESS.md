# arXiv submission readiness

Status: **scientific/source package revised; final upload blocked by immutable
release, license choice, possible endorsement, and server preview.**

## Candidate files

- PDF: `build/main.pdf` -- 16 pages, 4 figures, 9 tables.
- Source archive: `submission/arxiv-bess-paper-source-v2.zip`.
- Metadata: `arxiv-metadata.md` -- 1,873-character ASCII abstract.
- Primary category: `eess.SY`.
- Cross-list: none for version 1.
- Provenance comment: `Based on a Master's thesis defended in 2026.`

## Local acceptance results

- Tectonic source build: passed.
- Isolated build from extracted source archive: passed.
- PDF visual inspection: all 16 pages inspected; tables and references readable.
- Headline reconstruction test: passed.
- Model-lineage audit test: passed.
- Declared ancillary SHA-256 hashes: 13/13 matched.
- Citation keys: 31 used, 31 bibliography entries, no missing/uncited/duplicates.
- Metadata abstract: 1,873 characters, below arXiv's 1,920-character limit.
- Package scan: no absolute local paths, secrets, build logs, compiled PDF, or
  unused RF/DT comparison chart.

## Claim boundary

The central result is the retrospective V2+ comparison over 18 distinct DAM
dates and five configured profiles. The historical `dt_v2_plus` artifact is a
random forest trained on exact mirrored representations, with four profile-row
switches on one date. The separate HF transformer-backbone result is also a
mirrored-packet diagnostic; the 32-day read-model audit has no realized-regret
outcome. None is OOS learned-model evidence or market execution.
