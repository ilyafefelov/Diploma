# Thesis Plagiarism, Semantic Similarity, And Citation Audit

Date: 2026-05-26

## Scope

- Primary thesis surface: Google Doc `Draft.Thesis.2.goit.energy_arbitrage.Fefelov`, document id `1jjja9ng99O-xCisijMUbPrEM-3UJi_hilwnFJY8nups`.
- Fresh Google Doc text snapshot: `analysis_outputs/thesis_similarity/google_doc_export_2026-05-26-current.txt`.
- Local thesis mirror checked: `docs/thesis/chapters/*.md`.
- Local source corpus checked: `docs/thesis/sources/` with 56 readable source files.
- Internal self-similarity corpus checked: `docs/technical`, `docs/thesis/weekly-reports`, and `docs/thesis/demo-day-2`.

This is a local evidence audit, not a Turnitin/iThenticate certificate. It cannot compare against closed university or commercial student-paper databases.

## Method

- Exact 10-word shingles for direct-copy risk.
- TF-IDF character and word n-gram similarity for near-copy risk.
- Multilingual semantic embeddings with `sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2` for Ukrainian-English semantic proximity.
- Body-only rerun excluding the Chapter 2 bibliography/source list, because bibliography entries are expected to be very similar to source reference sections.
- Citation checks for source anchors, source numbering, clickable Markdown URLs, HTTP reachability, DOI/Crossref or arXiv title consistency, and Google Doc citation/source number coverage.

## Results

### Full-Text Run

Output: `analysis_outputs/thesis_similarity/current_semantic/`

- Thesis chunks: 255.
- External source chunks: 5149.
- Internal source chunks: 1741.
- External candidates: 36.
- Internal candidates: 20.
- Citation issues: 0.
- Extraction errors: 0.

The high-scoring full-text candidates are bibliography/reference-list matches. The target chunks point to the Chapter 2 source list in the Google Doc or local Markdown, not to prose paragraphs in the literature review body.

### Body-Only Run

Output: `analysis_outputs/thesis_similarity/current_semantic_body/`

- Thesis chunks: 227.
- Source/internal chunks scanned: 6890.
- External similarity candidates: 0.
- Internal self-similarity candidates: 0.
- Citation issues: 0.
- Extraction errors: 0.

No main-body plagiarism-risk candidates crossed the configured exact, TF-IDF, or multilingual semantic thresholds.

### Citation And Source Metadata

Outputs:

- `analysis_outputs/thesis_similarity/current_citations/chapter2_source_metadata_audit.csv`
- `analysis_outputs/thesis_similarity/current_citations/chapter2_source_metadata_summary.json`
- `analysis_outputs/thesis_similarity/current_citations/google_doc_citation_number_audit.json`

Findings:

- Local Chapter 2 source entries checked: 60.
- Source sequence: 1-60, OK.
- Source metadata issue rows: 0.
- Google Doc citation numbers found: 1-60.
- Google Doc source list numbers found: 1-60.
- Citations without source entry: none.
- Source entries not cited: none.

PapersFlow verified priority citations for Yi et al. 2025, Olivares et al. 2023, Lim et al. arXiv/TFT, Lago et al. 2021, Elmachtoub and Grigas 2022, Mandi et al. 2024, and Sang et al. 2022. SciSpace searches also returned the expected decision-focused ESS arbitrage and NBEATSx/EPF source families. Hugging Face paper search/pages were checked for the active time-series/EPF context, including Decision-Focused Learning, PriceFM, and THieF-style day-ahead electricity-price forecasting context.

## Conclusion

Based on the local source corpus, current Google Doc export, local Markdown thesis files, and the multilingual embedding rerun, the thesis main body does not show plagiarism-risk matches above the configured thresholds. The only full-text matches are expected bibliography/reference-list overlaps.

The citation structure is currently consistent: all 60 cited source numbers exist, all 60 source entries are cited at least once, and the Chapter 2 source list has clickable URLs and verified metadata in the local Markdown source of truth.

## Remaining Limitations

- This audit does not access closed plagiarism databases.
- Google Docs plain-text export does not prove every visual hyperlink object in the rendered Google Doc; it proves citation/source numbering and source URL text coverage. The local Markdown source list hyperlink syntax is verified.
- HTTP and metadata checks are time-sensitive and should be rerun immediately before final submission if the source list changes.
