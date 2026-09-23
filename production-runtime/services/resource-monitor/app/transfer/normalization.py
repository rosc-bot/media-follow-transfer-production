import hashlib
import re
from urllib.parse import urlsplit, urlunsplit


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
    keys = ','.join(sorted(set(episode_keys or [])))
    return f'{resource_id}:{provider.strip().lower()}:{keys}'


def canonical_file_name(name: str) -> str:
    return re.sub(r'\\s+', ' ', (name or '').strip()).replace('#追新转存', '').strip()
