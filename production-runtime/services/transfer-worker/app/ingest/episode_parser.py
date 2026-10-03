import re

_EPISODE = re.compile(
    r"(?i)(?:S(?P<s>\d{1,3})[ ._\-]*E(?:P)?(?P<e>\d{1,4})|第(?P<cn_s>\d{1,3})季[^0-9]{0,8}(?:第)?(?P<cn_e>\d{1,4})[集话期])"
)
_EP_RANGE = re.compile(
    r"(?i)S(?P<s>\d{1,3})[ ._\-]*E(?P<e1>\d{1,4})[ ._\-]*(?:[-~至]|to)[ ._\-]*(?:S\d{1,3}[ ._\-]*)?(?:E|EP)?(?P<e2>\d{1,4})"
)
_CN_UPDATE = re.compile(
    r"(?i)(?:(?:更新至|更至|已更新|更|至)\s*(?:第)?(?:EP|E)?(?P<e>\d{1,4})\s*(?:集|话|期)?|(?P<e2>\d{1,4})\s*(?:集|话|期)(?:全|完结))"
)
_CN_RANGE = re.compile(
    r"(?i)(?:第)?(?:EP|E)?(?P<e1>\d{1,4})\s*[-~至]\s*(?:第)?(?:EP|E)?(?P<e2>\d{1,4})\s*(?:集|话|期)?"
)
_LABEL_EPS = re.compile(r"(?:剧集|集数)[:：\s]*(?:第)?([0-9\s,，\-~至、]+)[集话期]")
_STANDALONE_E = re.compile(r"(?i)(?:\b|[ ._\-]|更至|更新至)(?:EP|E)(?P<e>\d{1,4})(?:\b|[ ._\-]|$)")


def parse_episode_keys(text: str) -> list[str]:
    found = set()
    raw = text or ""
    season_m = re.search(r"(?i)(?:\bS(\d{1,2})\b|第(\d{1,2})季)", raw)
    s_num = int(season_m.group(1) or season_m.group(2)) if season_m else 1

    # 1. 显式范围如 S01E01-E40, S01E33-E40
    for m in _EP_RANGE.finditer(raw):
        s = int(m.group("s"))
        e1 = int(m.group("e1"))
        e2 = int(m.group("e2"))
        if 0 < e1 <= e2 and (e2 - e1) <= 300:
            for e in range(e1, e2 + 1):
                found.add(f"S{s:02d}E{e:02d}")

    # 2. 单集如 S01E01, S01EP11
    for m in _EPISODE.finditer(raw):
        season = m.group("s") or m.group("cn_s")
        episode = m.group("e") or m.group("cn_e")
        found.add(f"S{int(season):02d}E{int(episode):02d}")

    # 3. 标签式集数，如 "📺 剧集：第1-6，13集" 或 "📺 剧集：第3集"
    for m in _LABEL_EPS.finditer(raw):
        parts = re.split(r"[,，、\s]+", m.group(1).strip())
        for p in parts:
            if not p:
                continue
            rm = re.match(r"^(\d+)[-~至](\d+)$", p)
            if rm:
                e1, e2 = int(rm.group(1)), int(rm.group(2))
                if 0 < e1 <= e2 and (e2 - e1) <= 300:
                    for e in range(e1, e2 + 1):
                        found.add(f"S{s_num:02d}E{e:02d}")
            elif p.isdigit():
                e = int(p)
                if 0 < e <= 2000:
                    found.add(f"S{s_num:02d}E{e:02d}")

    # 4. 中文更新至与范围，如 "更至EP11", "更至11集", "更新至102集"
    if not found:
        for m in _CN_UPDATE.finditer(raw):
            e_max = int(m.group("e") or m.group("e2"))
            if 0 < e_max <= 300:
                for e in range(1, e_max + 1):
                    found.add(f"S{s_num:02d}E{e:02d}")

    if not found:
        for m in _CN_RANGE.finditer(raw):
            e1, e2 = int(m.group("e1")), int(m.group("e2"))
            if 0 < e1 <= e2 and (e2 - e1) <= 300:
                for e in range(e1, e2 + 1):
                    found.add(f"S{s_num:02d}E{e:02d}")

    if not found:
        for m in re.finditer(r"第(?P<e>\d{1,4})\s*[集话期]", raw):
            e = int(m.group("e"))
            if 0 < e <= 2000:
                found.add(f"S{s_num:02d}E{e:02d}")

    if not found:
        for m in _STANDALONE_E.finditer(raw):
            e = int(m.group("e"))
            if 0 < e <= 2000 and e not in (264, 265):
                found.add(f"S{s_num:02d}E{e:02d}")

    return sorted(found)


def parse_season_episode(text: str) -> tuple[int | None, int | None]:
    keys = parse_episode_keys(text)
    if not keys:
        season_m = re.search(r"(?i)(?:\bS(\d{1,2})\b|第(\d{1,2})季)", text or "")
        if season_m:
            return int(season_m.group(1) or season_m.group(2)), None
        return None, None
    s, e = keys[0].replace("S", "").split("E")
    return int(s), int(e)
