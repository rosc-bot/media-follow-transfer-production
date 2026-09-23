"""ResourceLinkExtractor — unified supported-share-URL recognition.

The backfill tool, the resource pipeline and Scout all share one definition
of "a message carries a usable resource link".  Only URLs for cloud providers
the transfer adapters actually support (guangya first) are accepted; a plain
chat message with a random link must NOT become a transfer candidate.

This module deliberately reuses ``app.ingest.url_extractor`` for raw URL
discovery and only adds the provider whitelist + purpose-built helpers.
"""

from __future__ import annotations

from app.ingest.url_extractor import CLOUD_DRIVE_PROVIDERS, identify_cloud_drive

#: Providers the current transfer adapters can actually restore from.  Keep in
#: sync with ``app/transfer/orchestrator.py`` adapters map and any provider a
#: Scouter is allowed to feed.
SUPPORTED_RESOURCE_PROVIDERS: frozenset[str] = frozenset(
    {'guangya', 'quark', 'baidu', 'aliyun', '115', 'xunlei', 'tianyi'}
)


class ResourceLinkExtractor:
    """Extracts and classifies resource links from raw text or URL lists."""

    @staticmethod
    def has_supported_share_url(urls: list[str]) -> bool:
        """True when *urls* contains at least one supported cloud share link."""
        for url in urls or []:
            provider = identify_cloud_drive(url)
            if provider in SUPPORTED_RESOURCE_PROVIDERS:
                return True
        return False

    @staticmethod
    def first_supported_share_url(urls: list[str]) -> str | None:
        """Return the first supported share URL (prefer guangya)."""
        supported = [
            url for url in urls or []
            if identify_cloud_drive(url) in SUPPORTED_RESOURCE_PROVIDERS
        ]
        if not supported:
            return None
        supported.sort(key=lambda u: 0 if identify_cloud_drive(u) == 'guangya' else 1)
        return supported[0]

    @staticmethod
    def classify_urls(urls: list[str]) -> dict:
        """One-shot classification used by dry-run reporting."""
        classified: dict[str, int] = {}
        for url in urls or []:
            provider = identify_cloud_drive(url)
            key = provider if provider in SUPPORTED_RESOURCE_PROVIDERS else 'unsupported'
            classified[key] = classified.get(key, 0) + 1
        return classified

    @staticmethod
    def supported_providers() -> tuple[str, ...]:
        return tuple(str(p) for p in SUPPORTED_RESOURCE_PROVIDERS)


def extract_share_urls_from_text(text: str) -> list[str]:
    """Best-effort URL extraction from plain text rows (backfill source)."""
    urls = []
    for block in (text or '').split():
        cleaned = block.strip('"\'<>[]()*_`~')
        if cleaned.startswith(('http://', 'https://')):
            urls.append(cleaned)
    return urls


def provider_domains() -> dict[str, list[str]]:
    return dict(CLOUD_DRIVE_PROVIDERS)
