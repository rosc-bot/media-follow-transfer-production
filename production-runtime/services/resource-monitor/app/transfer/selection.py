from collections.abc import Iterable


def choose_provider(url: str, preferred: str | None = None) -> str:
    if preferred:
        return preferred.strip().lower()
    lowered = (url or '').lower()
    for name, domains in {
        'guangya': ('guangyapan', 'gypan', 'guangya.com'),
        'alist': ('alist',),
        'mobile': ('mobile',),
    }.items():
        if any(domain in lowered for domain in domains):
            return name
    return 'guangya'


def choose_episode_keys(detected: Iterable[str], requested: Iterable[str] | None = None) -> list[str]:
    values = set(requested or detected)
    return sorted(value for value in values if value and value.upper().startswith('S'))
