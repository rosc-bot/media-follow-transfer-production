import hashlib
import re

SPAM_TITLE_KEYWORDS = frozenset({
    "api", "商城", "防失联", "官方群", "交流群", "点击加入", "出租", "广告",
    "合作", "招租", "代充", "发卡", "主页", "官网", "进群", "备用",
    "专线", "客服", "赞助", "代挂", "影视库", "资源群", "网盘群", "进行了检查",
    "观看中", "上传的",
})


def is_spam_text(text: str) -> bool:
    return is_spam_or_ad_text(text)

def is_spam_or_ad_text(text: str) -> bool:
    if not text:
        return False
    raw = text.lower()
    if "进行了检查" in raw and "观看中" in raw:
        return True
    for kw in SPAM_TITLE_KEYWORDS:
        if kw in raw:
            if re.search(rf"(?:片名|剧名|名称|标题)[:：\s*]*[^\n\r]*{re.escape(kw)}", text, re.IGNORECASE):
                return True
    return False


def is_spam_title(title: str) -> bool:
    if not title:
        return True
    t_lower = title.lower()
    for kw in SPAM_TITLE_KEYWORDS:
        if kw in t_lower:
            return True
    if re.search(r"https?://|\.com|\.cn|\.net|\.cc|\.org", t_lower):
        return True
    return False


def extract_explicit_tmdb_id(text: str) -> int | None:
    if not text:
        return None
    patterns = [
        r"(?i)themoviedb\.org/(?:movie|tv)/(\d{2,10})",
        r"(?i)\btmdb(?:[-_\s]*id)?\s*[:：\-\s*`]*(\d{2,10})\b",
    ]
    for pat in patterns:
        m = re.search(pat, text)
        if m:
            return int(m.group(1))
    return None


def extract_explicit_media_type(text: str) -> str | None:
    if not text:
        return None
    if re.search(r"themoviedb\.org/movie/", text, re.IGNORECASE):
        return "movie"
    if re.search(r"themoviedb\.org/tv/", text, re.IGNORECASE):
        return "tv"
    if re.search(
        r"(?:\[(?:电视剧|剧集|动漫|日番|国漫|综艺|纪录片)\]|类型[:：]\s*\*{0,2}\s*(?:剧集|电视剧|动漫|短剧|连载|综艺)|国产剧|华语剧|美剧|韩剧|日剧|英剧|泰剧|港剧|台剧|短剧|网剧|番剧|新番|集全|总集数|\d+\s*集|第\s*\d+\s*季|\bS\d{1,2}\b|更至|更新至)",
        text,
        re.IGNORECASE,
    ):
        return "tv"
    if re.search(r"(?:\[(?:电影|动画电影|纪录电影)\]|类型[:：]\s*\*{0,2}\s*(?:[^\n\r]*?(?:电影|动画电影|纪录电影)))", text, re.IGNORECASE):
        return "movie"
    return None


def extract_year(text: str) -> int | None:
    if not text:
        return None
    m = re.search(r"[（(]((?:19|20)\d{2})[）)]", text)
    if m:
        return int(m.group(1))
    m2 = re.search(r"(?:上映|首播|年份)[:：\s*]*((?:19|20)\d{2})", text)
    if m2:
        return int(m2.group(1))
    return None


def _clean_candidate_title(val: str) -> str:
    val = re.sub(r"[\(（]\s*(?:19|20)\d{2}\s*[\)）]", " ", val)
    val = re.sub(r"[\(（][A-Za-z0-9\s._\-–—]+[\)）]", " ", val)
    val = re.sub(r"[\(（][^\)）]*[\)）]", " ", val)
    val = re.sub(r"^[📺🎬⭐🍿🖥️📦💾👤📖🏷🎞🆔*\s`]+", "", val)
    val = re.sub(r"^\[\s*(?:电视剧|电影|剧集|动漫|纪录片|综艺)(?:[·•]\S+)?\s*\]\s*", "", val)
    val = re.sub(r"[\s._\-–—]+已更新\b", "", val)
    val = re.sub(
        r"\b(?:4K|2160[pP]|1080[pP]|WEB-DL|SDR|HDR|HEVC|H\.?26[45]|Remux)\b.*$",
        "",
        val,
        flags=re.IGNORECASE,
    )
    val = re.sub(r"\s+", " ", val).strip(" -_:：*`")
    return val


def clean_title(value: str) -> str:
    raw = value or ""
    if is_spam_or_ad_text(raw):
        return ""
    raw_clean = re.sub(r"\[\s*(?:电视剧|电影|剧集|动漫|纪录片|综艺)(?:[·•][^\]]*)?\s*\]", " ", raw)
    m_label = re.search(r"(?:名称|片名|剧名|标题)[:：]\s*\*{0,2}([^\n\r(（\[*]+)", raw_clean)
    if m_label:
        candidate = m_label.group(1).strip()
        candidate = re.split(
            r"\s+(?:🏷|📺|⭐|🍿|🖥|📦|💾|👤|🎞|🆔|4K|2160[pP]|1080[pP]|WEB-DL|WEB\s+DL|BluRay|S\d{1,2}|更至|更新至)",
            candidate,
        )[0].strip()
        candidate = _clean_candidate_title(candidate)
        if is_spam_title(candidate):
            return ""
        if candidate:
            return candidate

    m_cinema = re.search(r"(?:🎬|📺)\s*\*{0,2}([^\n\r(（*]+)", raw_clean)
    if m_cinema:
        candidate = m_cinema.group(1).strip()
        candidate = re.sub(r"^(?:名称|片名|剧名)[:：]\s*", "", candidate).strip()
        candidate = re.split(
            r"\s+(?:🏷|📺|⭐|🍿|🖥|📦|💾|👤|🎞|🆔|4K|2160[pP]|1080[pP]|WEB-DL|WEB\s+DL|BluRay|S\d{1,2}|更至|更新至)",
            candidate,
        )[0].strip()
        candidate = _clean_candidate_title(candidate)
        if is_spam_title(candidate):
            return ""
        if candidate:
            return candidate

    first_line = next((line.strip() for line in raw_clean.splitlines() if line.strip()), "")
    val = re.sub(r"https?://\S+", " ", first_line or raw_clean)
    val = re.sub(r"(?i)\bS\d{1,3}(?:\s*E(?:P)?\d{1,4})?(?:[-~]E?P?\d{1,4})?\b", " ", val)
    val = re.sub(r"(?i)(?:更至|更新至)\s*(?:EP|E)?\d{1,4}\s*(?:集|话|期)?", " ", val)
    val = _clean_candidate_title(val)
    if is_spam_title(val):
        return ""
    return val


def build_identity_key(
    *,
    tmdb_id: int | None,
    season: int | None,
    episodes: list[str],
    share_hash: str,
    version_key: str = "",
) -> str:
    eps_str = ",".join(sorted(episodes))
    if len(eps_str) > 200:
        eps_str = f"h_{hashlib.md5(eps_str.encode()).hexdigest()}"
    return "|".join([str(tmdb_id or "unknown"), str(season or 0), eps_str, version_key, share_hash])
