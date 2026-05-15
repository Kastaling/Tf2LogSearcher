"""Tests for ``/results`` HTML embed meta block replacement and fallback strip."""

import pytest

from app.routes import (
    _RESULTS_EMBED_META_BLOCK_RE,
    _merge_results_embed_meta_regex_miss,
    _strip_tf2ls_embed_meta_block,
)


def test_embed_meta_regex_matches_crlf_after_end_marker() -> None:
    html = (
        "  <!-- tf2ls:embed-meta-replace-start -->\n"
        "  <meta name=\"description\" content=\"x\">\n"
        "  <!-- tf2ls:embed-meta-replace-end -->\r\n"
        "  <link>\n"
    )
    out, n = _RESULTS_EMBED_META_BLOCK_RE.subn("REPLACED\n", html, count=1)
    assert n == 1
    assert "REPLACED" in out
    assert "description" not in out


def test_strip_embed_meta_block_when_regex_would_miss_trailing_newline() -> None:
    """If the strict pattern cannot match (e.g. no newline after the end marker), strip still clears the region."""
    html = (
        "<head><title>TF2 Log Searcher</title>\n"
        "  <!-- tf2ls:embed-meta-replace-start -->\n"
        "  <meta name=\"description\" content=\"placeholder\">\n"
        "  <!-- tf2ls:embed-meta-replace-end -->"
        "<link rel=\"icon\">\n"
        "</head>"
    )
    assert _RESULTS_EMBED_META_BLOCK_RE.search(html) is None
    stripped = _strip_tf2ls_embed_meta_block(html)
    assert stripped is not None
    assert "placeholder" not in stripped
    assert "tf2ls:embed-meta" not in stripped
    assert "<link rel=\"icon\">" in stripped


@pytest.mark.parametrize(
    "trail",
    ["\n", "\r\n", ""],
)
def test_strip_embed_meta_block_consumes_optional_newline(trail: str) -> None:
    html = (
        "<title></title>\n"
        "  <!-- tf2ls:embed-meta-replace-start -->\n"
        "  <meta>\n"
        "  <!-- tf2ls:embed-meta-replace-end -->"
        f"{trail}next"
    )
    stripped = _strip_tf2ls_embed_meta_block(html)
    assert stripped is not None
    assert "<meta>" not in stripped
    assert stripped.endswith("next")


def test_regex_miss_merge_injects_meta_only_after_successful_strip() -> None:
    """Strip clears the placeholder; server meta is inserted once (after title)."""
    html = (
        "<head><title>TF2 Log Searcher</title>\n"
        "  <!-- tf2ls:embed-meta-replace-start -->\n"
        "  <meta name=\"description\" content=\"old\">\n"
        "  <!-- tf2ls:embed-meta-replace-end -->"
        "<link>\n</head>"
    )
    assert _RESULTS_EMBED_META_BLOCK_RE.search(html) is None
    out = _merge_results_embed_meta_regex_miss(html, "\n  <meta name=\"NEW\" content=\"1\">")
    assert "old" not in out
    assert "NEW" in out
    assert out.count("NEW") == 1
    assert "</title>\n  <meta name=\"NEW\"" in out


def test_regex_miss_merge_skips_inject_when_markers_absent() -> None:
    """If tf2ls markers are missing, do not append meta (would duplicate existing head tags)."""
    html = (
        "<head><title>TF2 Log Searcher</title>"
        '<meta name="description" content="static">'
        "</head>"
    )
    assert _strip_tf2ls_embed_meta_block(html) is None
    out = _merge_results_embed_meta_regex_miss(html, "\n  <meta INJECT>")
    assert "INJECT" not in out
    assert "static" in out
