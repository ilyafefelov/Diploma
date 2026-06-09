# Final Deep Thesis Similarity And Citation/Source Audit

Date: 2026-06-08

## Scope

- Primary text: current Google Doc raw text export, document id `1jjja9ng99O-xCisijMUbPrEM-3UJi_hilwnFJY8nups`.
- Body-only scope: factual thesis chapters `РОЗДІЛ 1` through `РОЗДІЛ 5`, excluding front matter, contents, bibliography, and appendices.
- Source corpus: `docs/thesis/sources/`, `docs/sources/`, plus freshly downloaded bibliography URL snapshots under `downloaded_web_sources/`.
- Internal/self corpus: `docs/technical`, thesis weekly/demo materials, and deep-research report folders.

## Methods

- Direct-copy check: exact word shingles.
- Near-copy check: character and word TF-IDF.
- Cross-language semantic check: `sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2`.
- Independent larger-model cross-check: `sentence-transformers/paraphrase-multilingual-mpnet-base-v2`.
- Citation numbering check: Google Doc body and local markdown first-mention order.
- Citation/source-support check: 90 citation-bearing contexts compared only against the cited source candidates, using multilingual semantic retrieval plus manual file mapping for non-obvious local PDF names.

## Similarity Results

- Body-only MiniLM: 260 thesis chunks, 5163 external chunks, 79 semantic/topic candidates, high-priority candidates = 0, extraction errors = 0.
- Body-only MPNet: 113 thesis chunks, 5163 external chunks, 42 semantic/topic candidates, high-priority candidates = 0, extraction errors = 0.
- Body-only with freshly downloaded web sources: 133 source files, 6463 external chunks, 40 candidates, high-priority candidates = 0, extraction errors = 0.
- Full-text MiniLM control: 163 chunks, 85 external candidates, 4 internal candidates. Full-text hits are expected to include bibliography/appendix/source-title overlap and are not treated as body plagiarism risk.

Interpretation: all body-only hits were semantic topical proximity to cited/related literature or prior internal audit notes, not direct-copy or close-paraphrase evidence. The exact-shingle and TF-IDF layers did not produce body-only high-priority external candidates.

## Multilingual Coverage

The audit did account for Ukrainian-vs-English language mismatch. Both MiniLM and MPNet are multilingual sentence-embedding models; Ukrainian thesis chunks and English source chunks were embedded in the same vector space. This is materially stronger than a lexical-only plagiarism check, which would miss translated paraphrase risk.

## Citation Numbering

- Google Doc body citation mentions: 90; unique citations: 60; first-mention order `1..60`: True.
- Local markdown citation mentions: 90; unique citations: 60; first-mention order `1..60`: True; visible-anchor mismatches: 0.
- Google Doc bibliography entries: 60; bibliography sequence `1..60`: True.

## Source Fetch Coverage

- Bibliography entries parsed: 60.
- Visible bibliography URLs fetched: 61.
- Saved URL snapshots/files: 59.
- Successful HTTP responses: 54; HTTP errors: 5; network/fetch errors: 2.

Fetch limitations / blocked landing pages:
- source [12] status=403, error=n/a, url=https://doi.org/10.1287/mnsc.2020.3922
- source [17] status=403, error=n/a, url=https://doi.org/10.3390/en10020207
- source [18] status=403, error=n/a, url=https://doi.org/10.3390/en12060999
- source [26] status=n/a, error=ConnectionError: HTTPSConnectionPool(host='www.nrel.gov', port=443): Max retries exceeded with url: /docs/fy21osti/78694.pdf (Caused by NameResolutionError("HTTPSConnection(host='www.nrel.gov', port=443): Failed to resolve 'www.nrel.gov' ([Errno 11001] getaddrinfo failed)")), url=https://www.nrel.gov/docs/fy21osti/78694.pdf
- source [27] status=n/a, error=ConnectionError: HTTPSConnectionPool(host='atb.nrel.gov', port=443): Max retries exceeded with url: /electricity/2023/residential_battery_storage/utility-scale_pv-plus-battery (Caused by NameResolutionError("HTTPSConnection(host='atb.nrel.gov', port=443): Failed to resolve 'atb.nrel.gov' ([Errno 11001] getaddrinfo failed)")), url=https://atb.nrel.gov/electricity/2023/residential_battery_storage/utility-scale_pv-plus-battery
- source [28] status=403, error=n/a, url=https://doi.org/10.1002/for.3084
- source [31] status=403, error=n/a, url=https://doi.org/10.1002/for.70065

## Citation/Source-Support Results

- Citation contexts audited: 90; unique cited sources: 60.
- Source candidate coverage: all 60 sources had at least one candidate after local/web/manual mapping: True.
- Automated verdict counts: {'needs-human': 8, 'partially-supported': 53, 'supported': 29}.
- Manual follow-up resolution: 8 reviewed, 7 supported, 1 supported after a scoped text edit, 0 `full-text-needed`.
- Manual resolution artifacts:
  - `claim_source_support/needs_human_resolution.md`
  - `claim_source_support/needs_human_resolution.csv`
  - `claim_source_support/needs_human_resolution_summary.json`

Manual finding: none of the 8 automated `needs-human` rows became confirmed `wrong-source`, `contradicted`, or `unsupported`. One broad Table 2.1 cell was corrected so that source [4] supports only local DAM/IDM/operator context, while source [8] supports the market-coupling/source-readiness boundary.

## Verdict

- No body-only direct-copy plagiarism candidates were found.
- No body-only close-paraphrase/high-priority external-source candidates were found under MiniLM, MPNet, exact-shingle, or TF-IDF layers.
- The previous concern about English sources versus Ukrainian thesis text is addressed: this run used multilingual semantic models, not only same-language lexical matching.
- Citation numbering and bibliography order are consistent in the current Google Doc and local markdown.
- No confirmed `wrong-source`, `contradicted`, or `unsupported` citation was identified automatically or in the manual follow-up pass.
- The 8 automated `needs-human` rows were manually resolved; none remains `full-text-needed`.

## Limitations

- This is not Turnitin/iThenticate and cannot compare against closed student-paper databases.
- Some DOI/publisher pages returned 403 or landing-page text rather than article full text; local PDFs covered the important academic sources where available.
- Semantic similarity can find translated topical overlap but cannot prove absence of plagiarism with mathematical certainty.
- Broad paragraphs citing several governance/data sources produce lower per-source scores because one paragraph is intentionally supported by a source group rather than a single article.
