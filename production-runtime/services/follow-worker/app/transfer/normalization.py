import hashlib
import re
from urllib.parse import urlsplit, urlunsplit

from app.follow.episode_keys import canonical_episode_key


def normalize_share_url(url: str) -> str:
    value = (url or '').strip()
    if not value:
        raise ValueError('share URL is empty')
    parts = urlsplit(value if '://' in value else f'https://{value}')
    host = parts.netloc.lower()
    path = re.sub(r'/+', '/', parts.path).rstrip('/') or '/'
    return urlunsplit(('https', host, path, parts.query, ''))


def share_hash(url: str) -> str:
    return hashlib.sha256(normalize_share_url(url).encode()).hexdigest()


def build_idempotency_key(resource_id: int, provider: str, episode_keys: list[str] | None = None) -> str:
    normalized = {
        canonical_episode_key(None, value) or str(value).strip()
        for value in (episode_keys or [])
        if str(value).strip()
    }
    keys = ','.join(sorted(normalized))
    return f'{resource_id}:{provider.strip().lower()}:{keys}'


def canonical_file_name(name: str) -> str:
    return re.sub(r'\\s+', ' ', (name or '').strip()).replace('#追新转存', '').strip()
