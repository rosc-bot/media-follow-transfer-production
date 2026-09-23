import re

_EPISODE = re.compile(r'(?i)(?:S(?P<s>\d{1,3})[ ._-]*E(?P<e>\d{1,4})|第(?P<cn_s>\d{1,3})季[^0-9]{0,8}(?:第)?(?P<cn_e>\d{1,4})集)')


def parse_episode_keys(text: str) -> list[str]:
    found=set()
    for m in _EPISODE.finditer(text or ''):
        season=m.group('s') or m.group('cn_s')
        episode=m.group('e') or m.group('cn_e')
        found.add(f'S{int(season):02d}E{int(episode):02d}')
    return sorted(found)


def parse_season_episode(text: str) -> tuple[int | None, int | None]:
    keys=parse_episode_keys(text)
    if not keys: return None, None
    s,e=keys[0].replace('S','').split('E')
    return int(s), int(e)
