# arXiv submission readiness

Status: **v1.2 sources, PDF, evidence, and isolated source archive are
release-ready. The defended thesis, v1, and v1.1 remain unchanged. An arXiv
upload still requires the author's irreversible license choice and inspection
of arXiv's server-generated preview.**

## Candidate files

- PDF: `build/main.pdf` -- 18 pages, 4 figures, 11 tables; 427,721 bytes;
  SHA-256 `8F2F8B52D176B30C475B94CC86EBE8F5E3706975E203E830421DF6B2071CD8A0`.
- Source archive: `submission/arxiv-bess-paper-source-v1.2.zip` -- 798,541
  bytes; SHA-256
  `AD960DD28746E2A32DBF741FEFD5749D8E52EF2CD3B8A05E3336D804F1F747BA`.
- Metadata abstract: 1,616 ASCII characters, below arXiv's 1,920-character
  limit.
- Primary category: `eess.SY`; `cs.LG` cross-list remains optional.
- Provenance comment: `Version 1.2. Based on a Master's thesis defended in
  2026.`

## Local acceptance results

- Tectonic source build: passed (18 pages).
- Isolated build from extracted v1.2 source archive: passed (18 pages).
- PDF visual inspection: pages 1 and 10--18 inspected; the new DT/DFL table is
  readable and no clipping or overlap was observed.
- Package evidence tests: 5/5 passed using stock Python.
- Focused DFL/relaxed-layer tests: 10/10 passed.
- New and touched source checks: Ruff passed; targeted Mypy passed; 29 focused
  DFL/assets/DT Pytest tests and 5 stock-Python evidence tests passed after
  restoring the legacy private-layer default.
- Repository-wide Pytest initially reported 1,084 passed and 11 failed. Five
  DFL failures were a v1.2 compatibility regression and now pass. The six
  remaining rerun failures are environment/worktree blockers: two expect a
  local V13 acquisition packet, two invoke a fresh `uv` build that lacks a
  Windows C++/NMake toolchain for `diffcp`, and two expect untracked local
  `AGENTS.md`/pulse files absent from this worktree.
- Repository-wide Mypy reports seven existing errors in untouched publication
  modules; targeted Mypy for every v1.2 Python surface passes.
- Dagster definitions validation: passed for all code locations.
- Package scan: 44 files; no cache, bytecode, log, auxiliary, or compiled-PDF
  files.
- Old `arxiv-bess-paper-source-v1.1.zip` remains present and unchanged.

## Claim boundary

V2+ remains the primary result. The genuine time-separated Decision Transformer
suite has 0 beneficial, 33 tie, and 3 harmful runs versus V2+. The
preregistered differentiable forecast-to-storage suite has 0 beneficial and 72
harmful runs versus V2+, while transformer correction beats matched MLP in
28/36 comparisons and the forecast-loss transformer improves raw schedules in
15/18 runs. This is positive architecture evidence and negative promotion
evidence.

The public OREE probe retrieved a complete 24-row DAM day but no explicit
source publication timestamp. V13 therefore remains externally blocked with
`promotable_v13_permitted_training_rows=0` and
`market_execution_enabled=false`.
