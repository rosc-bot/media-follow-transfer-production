import re


def clean_title(value: str) -> str:
    value = re.sub(r'https?://\S+', ' ', value or '')
    value = re.sub(r'\[[^]]+\]|\([^)]*\)', ' ', value)
    value = re.sub(r'(?i)\bS\d{1,3}E\d{1,4}\b', ' ', value)
    return re.sub(r'\s+', ' ', re.sub(r'[._-]+', ' ', value)).strip()


def build_identity_key(*, tmdb_id: int | None, season: int | None, episodes: list[str], share_hash: str, version_key: str = '') -> str:
    return '|'.join([str(tmdb_id or 'unknown'), str(season or 0), ','.join(sorted(episodes)), version_key, share_hash])
