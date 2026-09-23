import hashlib


def canonical_episode_keys(values): return tuple(sorted(set(values)))

def title_scoped_text(title: str, text: str) -> bool: return title.strip().lower() in (text or '').lower()

def batch_fingerprint(title: str, season: int, episodes: list[str]) -> str:
    raw=f'{title}|{season}|{",".join(canonical_episode_keys(episodes))}'
    return hashlib.sha256(raw.encode()).hexdigest()
