from __future__ import annotations

import importlib.util
import sys
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "audit_thesis_similarity.py"
SPEC = importlib.util.spec_from_file_location("audit_thesis_similarity", SCRIPT)
assert SPEC and SPEC.loader
audit = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = audit
SPEC.loader.exec_module(audit)


def test_normalize_and_shingles_ignore_source_links() -> None:
    text = "Lago et al. [[14]](#source-14) show Forecasting, https://example.com."
    normalized = audit.normalize_text(text)
    assert "source-14" not in normalized
    assert "https" not in normalized
    words = audit.tokens(normalized)
    assert "lago" in words
    assert audit.shingles(["a", "b", "c", "d"], 2) == {"a b", "b c", "c d"}


def test_citation_issues_detect_anchor_and_clickable_url_problems(tmp_path: Path) -> None:
    chapter = tmp_path / "chapter.md"
    chapter.write_text(
        "Text [[1]](#source-2) and [[3]](#source-3).\n\n"
        "1. <a id=\"source-1\"></a>Author. Title. https://example.com\n"
        "2. <a id=\"source-2\"></a>Other. [https://doi.org/x](https://doi.org/x)\n",
        encoding="utf-8",
    )
    issues = audit.citation_issues([chapter])
    issue_names = {issue.issue for issue in issues}
    assert "citation-visible-anchor-mismatch" in issue_names
    assert "missing-source-anchor" in issue_names
    assert "source-without-clickable-url" in issue_names
    assert "bare-url-in-source-entry" in issue_names
