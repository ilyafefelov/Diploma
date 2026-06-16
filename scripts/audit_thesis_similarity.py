"""Local thesis similarity, plagiarism-risk, and citation consistency audit.

This is not a Turnitin/iThenticate replacement. It compares thesis drafts
against the local source corpus and optional internal repo documents using:

* exact word shingles for direct-copy risk;
* TF-IDF character/word vectors for near-copy and light paraphrase risk;
* optional sentence-transformers embeddings for multilingual semantic search;
* citation-anchor checks for Chapter 2 style sources.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import re
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Iterable, cast


WORD_RE = re.compile(r"[A-Za-zА-Яа-яІіЇїЄєҐґ0-9][A-Za-zА-Яа-яІіЇїЄєҐґ0-9_+'’-]*")
CITATION_RE = re.compile(r"\[\[(\d+)\]\]\(#source-(\d+)\)")
SOURCE_ANCHOR_RE = re.compile(r"^\s*(\d+)\.\s*<a\s+id=\"source-(\d+)\"></a>(.*)$")
MARKDOWN_LINK_RE = re.compile(r"\[[^\]]+\]\((https?://[^)]+)\)")
BARE_URL_RE = re.compile(r"(?<!\]\()https?://[^\s)]+")


@dataclass(frozen=True)
class TextDoc:
    doc_id: str
    path: str
    kind: str
    text: str


@dataclass(frozen=True)
class Chunk:
    chunk_id: str
    doc_id: str
    path: str
    kind: str
    start_line: int
    text: str
    normalized: str
    token_count: int


@dataclass(frozen=True)
class SimilarityHit:
    target_id: str
    target_path: str
    target_line: int
    source_id: str
    source_path: str
    source_line: int
    method: str
    score: float
    exact_shared_shingles: int
    target_preview: str
    source_preview: str


@dataclass(frozen=True)
class CitationIssue:
    path: str
    line: int
    issue: str
    detail: str


def stable_id(value: str, length: int = 16) -> str:
    return hashlib.sha1(value.encode("utf-8", errors="ignore")).hexdigest()[:length]


def normalize_text(text: str) -> str:
    text = text.replace("\ufeff", " ")
    text = text.replace("’", "'").replace("`", "'")
    text = re.sub(r"https?://\S+", " ", text)
    text = re.sub(r"\[\[?\d+\]?\]\(#source-\d+\)", " ", text)
    text = re.sub(r"<[^>]+>", " ", text)
    text = text.lower()
    text = re.sub(r"[^0-9a-zа-яіїєґ_+' -]+", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def tokens(text: str) -> list[str]:
    return [match.group(0).lower() for match in WORD_RE.finditer(text)]


def shingles(words: list[str], size: int) -> set[str]:
    if len(words) < size:
        return set()
    return {" ".join(words[i : i + size]) for i in range(0, len(words) - size + 1)}


def line_number_for_offset(text: str, offset: int) -> int:
    return text.count("\n", 0, offset) + 1


def split_paragraph_chunks(doc: TextDoc, *, min_tokens: int = 35, max_tokens: int = 150) -> list[Chunk]:
    chunks: list[Chunk] = []
    for match in re.finditer(r"(?ms)\S.*?(?=\n\s*\n|\Z)", doc.text):
        raw = match.group(0).strip()
        norm = normalize_text(raw)
        word_list = tokens(norm)
        if len(word_list) < min_tokens:
            continue
        start_line = line_number_for_offset(doc.text, match.start())
        if len(word_list) <= max_tokens:
            chunk_texts = [raw]
        else:
            chunk_texts = token_windows(raw, max_tokens=max_tokens, overlap=30)
        for index, chunk_text in enumerate(chunk_texts):
            chunk_norm = normalize_text(chunk_text)
            chunk_words = tokens(chunk_norm)
            if len(chunk_words) < min_tokens:
                continue
            base = f"{doc.path}:{start_line}:{index}:{chunk_norm[:80]}"
            chunks.append(
                Chunk(
                    chunk_id=stable_id(base),
                    doc_id=doc.doc_id,
                    path=doc.path,
                    kind=doc.kind,
                    start_line=start_line,
                    text=chunk_text,
                    normalized=chunk_norm,
                    token_count=len(chunk_words),
                )
            )
    return chunks


def token_windows(text: str, *, max_tokens: int, overlap: int) -> list[str]:
    word_matches = list(WORD_RE.finditer(text))
    if len(word_matches) <= max_tokens:
        return [text]
    windows: list[str] = []
    step = max(1, max_tokens - overlap)
    for start in range(0, len(word_matches), step):
        end = min(len(word_matches), start + max_tokens)
        if end - start < max_tokens // 2:
            break
        windows.append(text[word_matches[start].start() : word_matches[end - 1].end()])
    return windows


def split_source_chunks(doc: TextDoc, *, window_tokens: int = 180, overlap: int = 60) -> list[Chunk]:
    norm = normalize_text(doc.text)
    word_list = tokens(norm)
    chunks: list[Chunk] = []
    if len(word_list) < 50:
        return chunks
    step = max(1, window_tokens - overlap)
    for index, start in enumerate(range(0, len(word_list), step)):
        end = min(len(word_list), start + window_tokens)
        if end - start < 50:
            break
        chunk_norm = " ".join(word_list[start:end])
        base = f"{doc.path}:{index}:{chunk_norm[:80]}"
        chunks.append(
            Chunk(
                chunk_id=stable_id(base),
                doc_id=doc.doc_id,
                path=doc.path,
                kind=doc.kind,
                start_line=1,
                text=chunk_norm,
                normalized=chunk_norm,
                token_count=end - start,
            )
        )
    return chunks


def read_text_file(path: Path) -> str:
    data = path.read_bytes()
    for encoding in ("utf-8-sig", "utf-8", "cp1251", "latin-1"):
        try:
            return data.decode(encoding)
        except UnicodeDecodeError:
            continue
    return data.decode("utf-8", errors="replace")


def extract_pdf_text(path: Path) -> str:
    errors: list[str] = []
    try:
        from pypdf import PdfReader as PypdfPdfReader

        reader = PypdfPdfReader(str(path))
        return "\n\n".join(page.extract_text() or "" for page in reader.pages)
    except Exception as exc:  # pragma: no cover - fallback depends on local PDFs
        errors.append(f"pypdf: {exc}")
    try:
        from PyPDF2 import PdfReader as PyPdf2PdfReader

        reader = PyPdf2PdfReader(str(path))
        return "\n\n".join(page.extract_text() or "" for page in reader.pages)
    except Exception as exc:  # pragma: no cover - fallback depends on local PDFs
        errors.append(f"PyPDF2: {exc}")
    try:
        from pdfminer.high_level import extract_text

        return extract_text(str(path))
    except Exception as exc:  # pragma: no cover - fallback depends on local PDFs
        errors.append(f"pdfminer: {exc}")
    raise RuntimeError("; ".join(errors))


def read_doc(path: Path, kind: str, cache_dir: Path | None = None) -> TextDoc:
    suffix = path.suffix.lower()
    if suffix == ".pdf":
        text = cached_pdf_text(path, cache_dir) if cache_dir else extract_pdf_text(path)
    else:
        text = read_text_file(path)
    return TextDoc(doc_id=stable_id(str(path.resolve())), path=str(path), kind=kind, text=text)


def cached_pdf_text(path: Path, cache_dir: Path | None) -> str:
    assert cache_dir is not None
    cache_dir.mkdir(parents=True, exist_ok=True)
    stat = path.stat()
    key = stable_id(f"{path.resolve()}:{stat.st_mtime_ns}:{stat.st_size}", length=24)
    cache_path = cache_dir / f"{key}.txt"
    if cache_path.exists():
        return read_text_file(cache_path)
    text = extract_pdf_text(path)
    cache_path.write_text(text, encoding="utf-8")
    return text


def iter_files(paths: Iterable[Path], suffixes: set[str]) -> list[Path]:
    out: list[Path] = []
    for path in paths:
        if path.is_file() and path.suffix.lower() in suffixes:
            out.append(path)
        elif path.is_dir():
            out.extend(p for p in path.rglob("*") if p.is_file() and p.suffix.lower() in suffixes)
    return sorted(out)


def exact_shingle_hits(
    targets: list[Chunk],
    sources: list[Chunk],
    *,
    shingle_size: int,
    min_shared: int,
    min_ratio: float,
) -> list[SimilarityHit]:
    source_index: dict[str, list[int]] = {}
    source_shingles: list[set[str]] = []
    for source_pos, source in enumerate(sources):
        source_set = shingles(tokens(source.normalized), shingle_size)
        source_shingles.append(source_set)
        for shingle in source_set:
            source_index.setdefault(shingle, []).append(source_pos)

    hits: list[SimilarityHit] = []
    for target in targets:
        target_set = shingles(tokens(target.normalized), shingle_size)
        if not target_set:
            continue
        counts: dict[int, int] = {}
        for shingle in target_set:
            for source_pos in source_index.get(shingle, []):
                counts[source_pos] = counts.get(source_pos, 0) + 1
        for source_pos, shared in counts.items():
            source_set = source_shingles[source_pos]
            ratio = shared / max(1, min(len(target_set), len(source_set)))
            if shared >= min_shared and ratio >= min_ratio:
                hits.append(make_hit(target, sources[source_pos], "exact_shingle", ratio, shared))
    return sorted(hits, key=lambda item: item.score, reverse=True)


def tfidf_hits(
    targets: list[Chunk],
    sources: list[Chunk],
    *,
    threshold: float,
    top_k: int,
    analyzer: str,
) -> list[SimilarityHit]:
    try:
        from sklearn.feature_extraction.text import TfidfVectorizer
        from sklearn.neighbors import NearestNeighbors
    except ImportError as exc:
        raise RuntimeError("scikit-learn is required for TF-IDF audit") from exc

    source_texts = [chunk.normalized for chunk in sources]
    target_texts = [chunk.normalized for chunk in targets]
    if analyzer == "char":
        vectorizer = TfidfVectorizer(analyzer="char_wb", ngram_range=(4, 7), min_df=2, max_features=150_000)
    elif analyzer == "word":
        vectorizer = TfidfVectorizer(analyzer="word", ngram_range=(1, 2), min_df=2, max_features=120_000)
    else:
        raise ValueError(f"unknown analyzer: {analyzer}")

    matrix = vectorizer.fit_transform(source_texts + target_texts)
    source_matrix = matrix[: len(source_texts)]
    target_matrix = matrix[len(source_texts) :]

    neighbors = NearestNeighbors(n_neighbors=min(top_k, len(sources)), metric="cosine", algorithm="brute")
    neighbors.fit(source_matrix)
    distances, indices = neighbors.kneighbors(target_matrix)

    hits: list[SimilarityHit] = []
    for target_pos, (row_distances, row_indices) in enumerate(zip(distances, indices, strict=True)):
        for distance, source_pos in zip(row_distances, row_indices, strict=True):
            score = 1.0 - float(distance)
            if score >= threshold and not math.isnan(score):
                hits.append(make_hit(targets[target_pos], sources[int(source_pos)], f"tfidf_{analyzer}", score, 0))
    return sorted(hits, key=lambda item: item.score, reverse=True)


def semantic_hits(
    targets: list[Chunk],
    sources: list[Chunk],
    *,
    threshold: float,
    top_k: int,
    model_name: str,
) -> tuple[list[SimilarityHit], str]:
    try:
        import numpy as np
        from sentence_transformers import SentenceTransformer
    except ImportError:
        return [], "sentence-transformers unavailable; semantic layer skipped"

    model = SentenceTransformer(model_name)
    source_embeddings = model.encode(
        [chunk.text for chunk in sources],
        batch_size=32,
        convert_to_numpy=True,
        normalize_embeddings=True,
        show_progress_bar=True,
    )
    target_embeddings = model.encode(
        [chunk.text for chunk in targets],
        batch_size=32,
        convert_to_numpy=True,
        normalize_embeddings=True,
        show_progress_bar=True,
    )
    scores = target_embeddings @ source_embeddings.T
    hits: list[SimilarityHit] = []
    for target_pos, row in enumerate(scores):
        if len(row) == 0:
            continue
        top_indices = np.argpartition(row, -min(top_k, len(row)))[-min(top_k, len(row)) :]
        for source_pos in top_indices:
            score = float(row[source_pos])
            if score >= threshold:
                hits.append(make_hit(targets[target_pos], sources[int(source_pos)], "semantic_embedding", score, 0))
    return sorted(hits, key=lambda item: item.score, reverse=True), f"semantic model: {model_name}"


def make_hit(target: Chunk, source: Chunk, method: str, score: float, exact_shared: int) -> SimilarityHit:
    return SimilarityHit(
        target_id=target.chunk_id,
        target_path=target.path,
        target_line=target.start_line,
        source_id=source.chunk_id,
        source_path=source.path,
        source_line=source.start_line,
        method=method,
        score=round(score, 4),
        exact_shared_shingles=exact_shared,
        target_preview=preview(target.text),
        source_preview=preview(source.text),
    )


def preview(text: str, limit: int = 280) -> str:
    return re.sub(r"\s+", " ", text).strip()[:limit]


def dedupe_hits(hits: Iterable[SimilarityHit], *, limit: int) -> list[SimilarityHit]:
    best: dict[tuple[str, str, str], SimilarityHit] = {}
    for hit in hits:
        key = (hit.target_id, hit.source_id, hit.method)
        if key not in best or hit.score > best[key].score:
            best[key] = hit
    return sorted(best.values(), key=lambda item: item.score, reverse=True)[:limit]


def citation_issues(paths: list[Path]) -> list[CitationIssue]:
    issues: list[CitationIssue] = []
    for path in paths:
        text = read_text_file(path)
        cited_ids: set[int] = set()
        bad_citation_pairs: list[tuple[int, int, int]] = []
        for line_no, line in enumerate(text.splitlines(), start=1):
            for citation in CITATION_RE.finditer(line):
                visible = int(citation.group(1))
                anchor = int(citation.group(2))
                cited_ids.add(anchor)
                if visible != anchor:
                    bad_citation_pairs.append((line_no, visible, anchor))

        source_ids: set[int] = set()
        clickable_ids: set[int] = set()
        bare_url_ids: set[int] = set()
        for line_no, line in enumerate(text.splitlines(), start=1):
            match = SOURCE_ANCHOR_RE.match(line)
            if not match:
                continue
            visible = int(match.group(1))
            anchor = int(match.group(2))
            source_ids.add(anchor)
            if visible != anchor:
                issues.append(
                    CitationIssue(str(path), line_no, "source-number-anchor-mismatch", f"visible {visible}, anchor {anchor}")
                )
            body = match.group(3)
            if MARKDOWN_LINK_RE.search(body):
                clickable_ids.add(anchor)
            if BARE_URL_RE.search(body) and not MARKDOWN_LINK_RE.search(body):
                bare_url_ids.add(anchor)

        for line_no, visible, anchor in bad_citation_pairs:
            issues.append(
                CitationIssue(str(path), line_no, "citation-visible-anchor-mismatch", f"visible [[{visible}]], target source-{anchor}")
            )
        for source_id in sorted(cited_ids - source_ids):
            issues.append(CitationIssue(str(path), 1, "missing-source-anchor", f"source-{source_id} is cited but absent"))
        for source_id in sorted(source_ids - cited_ids):
            issues.append(CitationIssue(str(path), 1, "unused-source-anchor", f"source-{source_id} exists but is not cited"))
        for source_id in sorted(source_ids - clickable_ids):
            issues.append(CitationIssue(str(path), 1, "source-without-clickable-url", f"source-{source_id} has no markdown URL"))
        for source_id in sorted(bare_url_ids):
            issues.append(CitationIssue(str(path), 1, "bare-url-in-source-entry", f"source-{source_id} should use markdown link"))

        if source_ids:
            missing = sorted(set(range(1, max(source_ids) + 1)) - source_ids)
            for source_id in missing:
                issues.append(CitationIssue(str(path), 1, "non-sequential-source-list", f"source-{source_id} missing"))
    return issues


def write_csv(path: Path, rows: Iterable[object]) -> None:
    rows = list(rows)
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(_dataclass_row_dict(rows[0]).keys()))
        writer.writeheader()
        for row in rows:
            writer.writerow(_dataclass_row_dict(row))


def _dataclass_row_dict(row: object) -> dict[str, Any]:
    payload = asdict(cast(Any, row))
    if not isinstance(payload, dict):
        raise TypeError(f"Expected dataclass row to convert to dict, got {type(row).__name__}")
    return payload


def write_report(
    path: Path,
    *,
    targets: list[Chunk],
    sources: list[Chunk],
    external_hits: list[SimilarityHit],
    internal_hits: list[SimilarityHit],
    citations: list[CitationIssue],
    semantic_note: str,
    extraction_errors: list[str],
) -> None:
    high_external = [hit for hit in external_hits if hit.score >= 0.82 or hit.exact_shared_shingles >= 3]
    report = [
        "# Thesis Similarity And Citation Audit",
        "",
        "## Scope",
        "",
        f"- Thesis chunks checked: {len(targets)}",
        f"- Source/internal chunks scanned: {len(sources)}",
        f"- External similarity candidates: {len(external_hits)}",
        f"- High-priority external candidates: {len(high_external)}",
        f"- Internal self-similarity candidates: {len(internal_hits)}",
        f"- Citation issues: {len(citations)}",
        f"- Semantic layer: {semantic_note}",
        "",
        "This local audit is not a formal university plagiarism certificate. It cannot compare against closed student-paper databases.",
        "",
        "## High-Priority External Candidates",
        "",
    ]
    if high_external:
        for hit in high_external[:25]:
            report.extend(format_hit(hit))
    else:
        report.append("No high-priority external-source candidates crossed the configured thresholds.")
    report.extend(["", "## Top External Similarity Candidates", ""])
    for hit in external_hits[:30]:
        report.extend(format_hit(hit))
    if not external_hits:
        report.append("No external candidates crossed the configured thresholds.")
    report.extend(["", "## Top Internal Self-Similarity Candidates", ""])
    for hit in internal_hits[:30]:
        report.extend(format_hit(hit))
    if not internal_hits:
        report.append("No internal self-similarity candidates crossed the configured thresholds.")
    report.extend(["", "## Citation Consistency Issues", ""])
    if citations:
        for issue in citations[:120]:
            report.append(f"- `{issue.path}:{issue.line}` {issue.issue}: {issue.detail}")
    else:
        report.append("No citation-anchor or clickable-URL issues found in audited citation files.")
    if extraction_errors:
        report.extend(["", "## Extraction Errors", ""])
        for error in extraction_errors:
            report.append(f"- {error}")
    path.write_text("\n".join(report) + "\n", encoding="utf-8")


def format_hit(hit: SimilarityHit) -> list[str]:
    return [
        f"- `{hit.target_path}:{hit.target_line}` vs `{hit.source_path}:{hit.source_line}` "
        f"method={hit.method} score={hit.score} exact_shared={hit.exact_shared_shingles}",
        f"  - Thesis: {hit.target_preview}",
        f"  - Source: {hit.source_preview}",
    ]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--thesis", nargs="+", type=Path, required=True, help="Thesis text/markdown files or directories.")
    parser.add_argument("--sources", nargs="+", type=Path, required=True, help="Source corpus files or directories.")
    parser.add_argument("--internal", nargs="*", type=Path, default=[], help="Optional internal repo docs for self-similarity.")
    parser.add_argument("--citation-files", nargs="*", type=Path, default=[], help="Markdown files with source anchors to audit.")
    parser.add_argument("--output-dir", type=Path, default=Path("analysis_outputs/thesis_similarity"))
    parser.add_argument("--semantic-model", default="", help="Optional sentence-transformers model name.")
    parser.add_argument("--tfidf-threshold", type=float, default=0.58)
    parser.add_argument("--word-tfidf-threshold", type=float, default=0.50)
    parser.add_argument("--semantic-threshold", type=float, default=0.82)
    parser.add_argument("--top-k", type=int, default=3)
    parser.add_argument("--limit", type=int, default=250)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    cache_dir = args.output_dir / "cache"
    extraction_errors: list[str] = []

    thesis_files = iter_files(args.thesis, {".md", ".txt"})
    source_files = iter_files(args.sources, {".md", ".txt", ".pdf"})
    internal_files = iter_files(args.internal, {".md", ".txt"}) if args.internal else []

    if not thesis_files:
        print("No thesis files found", file=sys.stderr)
        return 2
    if not source_files:
        print("No source files found", file=sys.stderr)
        return 2

    thesis_docs = [read_doc(path, "thesis", cache_dir) for path in thesis_files]
    source_docs: list[TextDoc] = []
    for path in source_files:
        try:
            source_docs.append(read_doc(path, "external_source", cache_dir))
        except Exception as exc:
            extraction_errors.append(f"{path}: {exc}")
    internal_docs = [read_doc(path, "internal_repo", cache_dir) for path in internal_files]

    target_chunks = [chunk for doc in thesis_docs for chunk in split_paragraph_chunks(doc)]
    external_chunks = [chunk for doc in source_docs for chunk in split_source_chunks(doc)]
    internal_chunks = [chunk for doc in internal_docs for chunk in split_source_chunks(doc)]

    external_hits: list[SimilarityHit] = []
    external_hits.extend(
        exact_shingle_hits(target_chunks, external_chunks, shingle_size=10, min_shared=3, min_ratio=0.12)
    )
    external_hits.extend(tfidf_hits(target_chunks, external_chunks, threshold=args.tfidf_threshold, top_k=args.top_k, analyzer="char"))
    external_hits.extend(
        tfidf_hits(target_chunks, external_chunks, threshold=args.word_tfidf_threshold, top_k=args.top_k, analyzer="word")
    )

    semantic_note = "not requested"
    if args.semantic_model:
        semantic_results, semantic_note = semantic_hits(
            target_chunks,
            external_chunks,
            threshold=args.semantic_threshold,
            top_k=args.top_k,
            model_name=args.semantic_model,
        )
        external_hits.extend(semantic_results)

    internal_hits: list[SimilarityHit] = []
    if internal_chunks:
        internal_hits.extend(tfidf_hits(target_chunks, internal_chunks, threshold=0.60, top_k=args.top_k, analyzer="char"))
        internal_hits.extend(exact_shingle_hits(target_chunks, internal_chunks, shingle_size=10, min_shared=3, min_ratio=0.12))

    external_hits = dedupe_hits(external_hits, limit=args.limit)
    internal_hits = dedupe_hits(internal_hits, limit=args.limit)
    citation_paths = [path for path in args.citation_files if path.exists()]
    citation_findings = citation_issues(citation_paths)

    args.output_dir.mkdir(parents=True, exist_ok=True)
    write_csv(args.output_dir / "external_similarity_hits.csv", external_hits)
    write_csv(args.output_dir / "internal_similarity_hits.csv", internal_hits)
    write_csv(args.output_dir / "citation_issues.csv", citation_findings)
    write_report(
        args.output_dir / "similarity_audit_report.md",
        targets=target_chunks,
        sources=external_chunks + internal_chunks,
        external_hits=external_hits,
        internal_hits=internal_hits,
        citations=citation_findings,
        semantic_note=semantic_note,
        extraction_errors=extraction_errors,
    )
    summary = {
        "thesis_files": [str(path) for path in thesis_files],
        "source_files": len(source_files),
        "internal_files": len(internal_files),
        "thesis_chunks": len(target_chunks),
        "external_source_chunks": len(external_chunks),
        "internal_source_chunks": len(internal_chunks),
        "external_hits": len(external_hits),
        "internal_hits": len(internal_hits),
        "citation_issues": len(citation_findings),
        "semantic_note": semantic_note,
        "extraction_errors": extraction_errors,
    }
    (args.output_dir / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
