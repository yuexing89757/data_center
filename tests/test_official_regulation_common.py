from __future__ import annotations

from datetime import UTC, datetime
from io import BytesIO
from typing import get_type_hints

import pytest

from market_data_center.domain.regulation import RegulationEventRecord
from market_data_center.providers.contracts import (
    ProviderBatch,
    ProviderError,
    RegulationEventProvider,
)
from market_data_center.providers.official_regulation_common import (
    MAX_DOCUMENT_BYTES,
    extract_official_text,
    validate_official_url,
)


def test_html_text_extraction_discards_script_and_normalizes_whitespace() -> None:
    text = extract_official_text(
        "text/html; charset=utf-8",
        (
            "<html><script>bad()</script><body>属于\u3000股票交易异常波动\n +20\uff05</body></html>"
        ).encode(),
    )

    assert text == "属于 股票交易异常波动 +20%"


@pytest.mark.parametrize(
    "url",
    (
        "http://www.sse.com.cn/a.pdf",
        "https://user:password@www.sse.com.cn/a.pdf",
        "https://www.sse.com.cn:444/a.pdf",
        "https://example.com/a.pdf",
    ),
)
def test_url_validator_rejects_nonofficial_or_unsafe_urls(url: str) -> None:
    with pytest.raises(ProviderError, match="official URL is not allowed"):
        validate_official_url(url, frozenset({"www.sse.com.cn"}))


def test_url_validator_accepts_allowlisted_https_url() -> None:
    validate_official_url(
        "https://www.sse.com.cn/disclosure/listedinfo/announcement/a.pdf",
        frozenset({"www.sse.com.cn"}),
    )


def test_text_extraction_rejects_oversized_documents() -> None:
    with pytest.raises(ProviderError, match="document exceeds bounded size"):
        extract_official_text("text/html", b"x" * (MAX_DOCUMENT_BYTES + 1))


def test_text_extraction_rejects_unknown_or_blank_documents() -> None:
    with pytest.raises(ProviderError, match="unsupported official document content type"):
        extract_official_text("application/octet-stream", b"content")
    with pytest.raises(ProviderError, match="official document contains no text"):
        extract_official_text("text/html", b"<html><body>  </body></html>")


def test_pdf_extraction_rejects_more_than_fifty_pages() -> None:
    from pypdf import PdfWriter

    writer = PdfWriter()
    for _ in range(51):
        writer.add_blank_page(width=72, height=72)
    output = BytesIO()
    writer.write(output)

    with pytest.raises(ProviderError, match="official PDF exceeds page limit"):
        extract_official_text("application/pdf", output.getvalue())


def test_regulation_provider_contract_uses_aware_interval_and_event_batch() -> None:
    hints = get_type_hints(RegulationEventProvider.fetch_events)

    assert hints["observed_from"] is datetime
    assert hints["observed_to"] is datetime
    assert hints["return"] == ProviderBatch[RegulationEventRecord]
    assert datetime(2026, 9, 21, tzinfo=UTC).utcoffset() is not None
