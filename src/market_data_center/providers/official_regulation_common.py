"""Bounded parsing helpers shared by official regulation-event adapters."""

from __future__ import annotations

import re
import unicodedata
from io import BytesIO
from urllib.parse import urlsplit

from bs4 import BeautifulSoup
from pypdf import PdfReader

from market_data_center.providers.contracts import ProviderError

MAX_DOCUMENT_BYTES = 5 * 1024 * 1024
MAX_PDF_PAGES = 50
MAX_TEXT_CHARS = 500_000

_WHITESPACE = re.compile(r"\s+")


def validate_official_url(url: str, allowed_hosts: frozenset[str]) -> None:
    """Reject URLs that could escape a provider's reviewed official-host boundary."""

    try:
        parsed = urlsplit(url)
        port = parsed.port
    except ValueError as error:
        raise ProviderError("official URL is not allowed") from error
    host = parsed.hostname.lower() if parsed.hostname is not None else None
    if (
        parsed.scheme.lower() != "https"
        or host not in allowed_hosts
        or parsed.username is not None
        or parsed.password is not None
        or port not in (None, 443)
    ):
        raise ProviderError("official URL is not allowed")


def extract_official_text(content_type: str, body: bytes) -> str:
    """Extract bounded normalized text without changing signed numeric evidence."""

    if len(body) > MAX_DOCUMENT_BYTES:
        raise ProviderError("document exceeds bounded size")
    media_type = content_type.partition(";")[0].strip().lower()
    if media_type in {"text/html", "application/xhtml+xml"}:
        soup = BeautifulSoup(body, "html.parser")
        for element in soup(("script", "style", "noscript")):
            element.decompose()
        text = soup.get_text(" ")
    elif media_type == "application/pdf":
        text = _extract_pdf_text(body)
    elif media_type == "text/plain":
        text = body.decode("utf-8-sig", errors="strict")
    else:
        raise ProviderError("unsupported official document content type")
    normalized = _normalize_text(text)
    if not normalized:
        raise ProviderError("official document contains no text")
    if len(normalized) > MAX_TEXT_CHARS:
        raise ProviderError("official document text exceeds bounded size")
    return normalized


def _extract_pdf_text(body: bytes) -> str:
    try:
        reader = PdfReader(BytesIO(body))
        if reader.is_encrypted:
            raise ProviderError("encrypted official PDF is not supported")
        if len(reader.pages) > MAX_PDF_PAGES:
            raise ProviderError("official PDF exceeds page limit")
        return " ".join(page.extract_text() or "" for page in reader.pages)
    except ProviderError:
        raise
    except Exception as error:
        raise ProviderError("official PDF could not be parsed") from error


def _normalize_text(value: str) -> str:
    return _WHITESPACE.sub(" ", unicodedata.normalize("NFKC", value)).strip()
