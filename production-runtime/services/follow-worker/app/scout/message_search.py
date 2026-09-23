import json
import re
import sqlite3
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class ResourceMessage:
    chat_id: str
    message_id: int
    text: str
    urls: list[str]
    chat_title: str | None = None
    source_link: str | None = None
    content_hash: str | None = None
    updated_at: str | None = None

    @property
    def url(self) -> str | None:
        """Best candidate share URL (guangya first)."""
        for url in self.urls:
            if 'guangyapan' in url or 'gypan' in url:
                return url
        return self.urls[0] if self.urls else None


class MessageSearch:
    """Read-only search over the resource message store.

    Query surface: title (fuzzy), year, season, episode and common Chinese
    episode forms (SxxExx / Exx / 第N集).  The index is deliberately NOT the
    full tg_messages.db — only rows backfilled from whitelisted RESOURCE /
    MANUAL_INGEST channels (see tools.backfill_resource_messages).
    """

    def __init__(self, path: str):
        self.path = Path(path)

    # -- helpers --------------------------------------------------------- #

    @staticmethod
    def _episode_forms(episode_key: str) -> list[str]:
        """Expand one canonical key like S01E02 into searchable variants."""
        matched = re.fullmatch(r'S(\d{1,3})E(\d{1,4})', str(episode_key or '').upper())
        if not matched:
            return [str(episode_key or '').upper()]
        season, episode = int(matched.group(1)), int(matched.group(2))
        forms = {
            f'S{season:02d}E{episode:02d}',
            f'S{season}E{episode}',
            f'S{season:02d}E{episode}',
            f'第{season}季 第{episode}集',
            f'第{episode}集',
            f'E{episode:02d}',
            f'EP{episode:02d}',
        }
        return sorted(forms)

    @staticmethod
    def _title_variants(title: str) -> list[str]:
        """Return case-insensitive title tokens (raw + whitespace-stripped)."""
        value = (title or '').strip()
        if not value:
            return []
        return [value, re.sub(r'\s+', '', value)]

    @staticmethod
    def _row_urls(raw: str) -> list[str]:
        try:
            return [str(u) for u in json.loads(raw or '[]') if str(u).startswith('http')]
        except json.JSONDecodeError:
            return []

    # -- search ---------------------------------------------------------- #

    def search(self, title: str, episode_key: str) -> list[ResourceMessage]:
        """Find messages containing the title + one episode key form."""
        if not self.path.exists():
            return []
        titles = self._title_variants(title)
        if not titles:
            return []
        episode_forms = self._episode_forms(episode_key)
        title_patterns = [self._safe_like_patterns(value) for value in titles]
        candidates: list[ResourceMessage] = []
        seen: set[tuple[str, int]] = set()
        uri = self.path.resolve().as_uri() + '?mode=ro'
        with sqlite3.connect(uri, uri=True) as db:
            columns = {row[1] for row in db.execute('PRAGMA table_info(messages)').fetchall()}
            selected_columns = ['chat_id', 'chat_title', 'message_id', 'text']
            has_caption = 'caption' in columns
            if has_caption:
                selected_columns.append('caption')
            selected_columns.append('urls')
            selected_columns.extend(column for column in ('source_link', 'content_hash', 'updated_at') if column in columns)
            if has_caption:
                title_predicate = ' OR '.join(
                    "(instr(lower(COALESCE(text, '')), ?) > 0 OR instr(lower(COALESCE(caption, '')), ?) > 0)"
                    for _ in title_patterns
                )
                query_patterns = [pattern for value in title_patterns for pattern in (value, value)]
            else:
                title_predicate = ' OR '.join("instr(lower(COALESCE(text, '')), ?) > 0" for _ in title_patterns)
                query_patterns = title_patterns
            order_by = 'COALESCE(updated_at, date)' if 'updated_at' in columns else 'date'
            rows = db.execute(
                f'SELECT {", ".join(selected_columns)} FROM messages WHERE ({title_predicate}) '
                f'ORDER BY {order_by} DESC, message_id DESC LIMIT 200',
                query_patterns,
            ).fetchall()
        for row in rows:
            values = dict(zip(selected_columns, row, strict=True))
            chat_id = values['chat_id']
            chat_title = values['chat_title']
            message_id = values['message_id']
            row_text = '\n'.join(
                part for part in (str(values.get('text') or ''), str(values.get('caption') or '')) if part
            ).strip()
            raw_urls = values['urls']
            source_link = values.get('source_link')
            lowered = row_text.lower()
            title_ok = any(pat in lowered for pat in title_patterns)
            if not title_ok:
                continue
            episode_ok = any(form.lower() in lowered for form in episode_forms)
            if not episode_ok:
                continue
            key = (str(chat_id), int(message_id))
            if key in seen:
                continue
            seen.add(key)
            urls = self._row_urls(raw_urls)
            candidates.append(ResourceMessage(
                chat_id=str(chat_id),
                message_id=int(message_id),
                text=row_text,
                urls=urls,
                chat_title=chat_title,
                source_link=source_link,
                content_hash=values.get('content_hash'),
                updated_at=values.get('updated_at'),
            ))
        return candidates

    @staticmethod
    def _safe_like_patterns(value: str) -> str:
        """Lower-cased pattern safe for substring matching."""
        return value.lower()

    # -- stats (Phase 2B explicability) ---------------------------------- #

    def channel_stats(self) -> dict:
        """Per-channel message counts + URL-bearing counts (read-only)."""
        if not self.path.exists():
            return {'channels': [], 'total': 0, 'total_with_urls': 0}
        from app.ingest.resource_link_extractor import ResourceLinkExtractor

        counts: dict[str, dict] = {}
        total = 0
        total_with = 0
        uri = self.path.resolve().as_uri() + '?mode=ro'
        with sqlite3.connect(uri, uri=True) as db:
            rows = db.execute(
                'SELECT chat_id, chat_title, urls FROM messages'
            ).fetchall()
        for chat_id, chat_title, raw_urls in rows:
            total += 1
            key = str(chat_id)
            entry = counts.setdefault(key, {'chat_id': key, 'chat_title': chat_title or '', 'messages': 0, 'with_urls': 0})
            entry['messages'] += 1
            if ResourceLinkExtractor.has_supported_share_url(self._row_urls(raw_urls)):
                entry['with_urls'] += 1
                total_with += 1
        return {
            'channels': sorted(counts.values(), key=lambda item: item['messages'], reverse=True),
            'total': total,
            'total_with_urls': total_with,
        }
