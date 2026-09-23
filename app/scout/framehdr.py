import asyncio
import json
import logging
import re
import urllib.parse
from pathlib import Path
from typing import Any

import aiohttp
from yarl import URL

from app.core.config import get_settings

logger = logging.getLogger(__name__)

# ------------------------------------------------------------------------- #
# Season normalization & card matcher — SINGLE SOURCE OF TRUTH shared by the
# production search_series() and tools/diagnose_framehdr.py (Phase 2C §二).
# Never maintain a second copy of these rules in the diagnostic tool.
# ------------------------------------------------------------------------- #

#: Chinese digit names for seasons 1..20 (enough for every current follow).
_CN_DIGITS = {
    1: '一', 2: '二', 3: '三', 4: '四', 5: '五', 6: '六', 7: '七', 8: '八', 9: '九',
    10: '十', 11: '十一', 12: '十二', 13: '十三', 14: '十四', 15: '十五', 16: '十六',
    17: '十七', 18: '十八', 19: '十九', 20: '二十',
}


def season_tokens(season: int) -> list[str]:
    """All season spellings the matcher must understand (Phase 2C §二).

    Supports: S01, S1, Season 1, Season 01, 第1季, 第一季, 第01季 plus the
    '期' alias used by FrameHDR for some series.
    """
    tokens = {
        f'S{season:02d}',
        f'S{season}',
        f'Season {season}',
        f'Season {season:02d}',
        f'第{season}季',
        f'第{season:02d}季',
        f'第{_CN_DIGITS.get(season, season)}季',
        f'第{season}期',
        f'{season}期',
    }
    return sorted(token for token in tokens if token)


def _later_season_tokens(season: int) -> list[str]:
    """Tokens for every season strictly AFTER ``season`` (blacklist helper)."""
    tokens: set[str] = set()
    for later in range(season + 1, season + 20):
        tokens.update(season_tokens(later))
    return sorted(tokens)


def card_matches_season(card_title: str, season: int) -> bool:
    """Whether a FrameHDR card title belongs to the requested season.

    Production semantics (kept identical to search_series):

    * season == 1: blacklist later seasons (第二季/第三季/… are excluded, so
      the main-S1 card '怪奇物语 第一季' and unmarked year-episodes match).
    * season > 1: whitelist the exact season tokens; cards without the season
      marker are NOT considered season N.
    """
    title = str(card_title or '')
    if season == 1:
        return not any(token in title for token in _later_season_tokens(1))
    return any(token in title for token in season_tokens(season))


def card_title_normalized(title: str) -> str:
    """Whitespace/punctuation-stripped title used by the title matcher."""
    return re.sub(r'[^\w一-龥]', '', str(title or ''))


class FrameHdrService:
    _session: aiohttp.ClientSession | None = None
    _lock: asyncio.Lock = asyncio.Lock()

    @classmethod
    async def get_session(cls) -> aiohttp.ClientSession:
        async with cls._lock:
            if cls._session is None or cls._session.closed:
                headers = {
                    'User-Agent': (
                        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                        'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36'
                    ),
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                    'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
                }
                cookie_jar = aiohttp.CookieJar(unsafe=True)
                cls._session = aiohttp.ClientSession(headers=headers, cookie_jar=cookie_jar)
                cls._load_cookies()
            return cls._session

    @classmethod
    def _load_cookies(cls) -> None:
        settings = get_settings()
        cookie_path = Path(settings.framehdr_cookie_file)
        if cookie_path.exists() and cls._session:
            try:
                with open(cookie_path, encoding='utf-8') as f:
                    cookies = json.load(f)
                url_obj = URL(settings.framehdr_base_url)
                for c in cookies:
                    cls._session.cookie_jar.update_cookies({c['name']: c['value']}, url_obj)
                logger.info('Loaded %d cookies for FrameHdr from %s', len(cookies), cookie_path)
            except (OSError, json.JSONDecodeError) as e:
                logger.warning('Failed to load FrameHdr cookies: %s', e)

    @classmethod
    def _save_cookies(cls) -> None:
        settings = get_settings()
        cookie_path = Path(settings.framehdr_cookie_file)
        if not cls._session:
            return
        try:
            cookie_path.parent.mkdir(parents=True, exist_ok=True)
            cookies_to_save = []
            for cookie in cls._session.cookie_jar:
                cookies_to_save.append({
                    'name': cookie.key,
                    'value': cookie.value,
                    'domain': cookie['domain'],
                    'path': cookie['path'],
                })
            with open(cookie_path, 'w', encoding='utf-8') as f:
                json.dump(cookies_to_save, f, ensure_ascii=False, indent=2)
            logger.info('Saved %d cookies for FrameHdr to %s', len(cookies_to_save), cookie_path)
        except (OSError, TypeError) as e:
            logger.warning('Failed to save FrameHdr cookies: %s', e)

    @classmethod
    async def login(cls) -> bool:
        settings = get_settings()
        if not settings.framehdr_enabled or not settings.framehdr_username or not settings.framehdr_password:
            logger.warning('FrameHdr is disabled or credentials not set.')
            return False

        session = await cls.get_session()
        login_url = f"{settings.framehdr_base_url.rstrip('/')}/login.php"
        logger.info('Executing FrameHdr login for user: %s', settings.framehdr_username)

        try:
            async with session.get(login_url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                html = await resp.text()

            form_match = re.search(r'<form[^>]*id=[\x27\x22]loginForm[\x27\x22][^>]*>(.*?)</form>', html, re.DOTALL)
            data: dict[str, str] = {
                'username': settings.framehdr_username,
                'password': settings.framehdr_password,
                'login_mode': 'password',
            }
            if form_match:
                for inp in re.finditer(r'<input[^>]*>', form_match.group(1)):
                    tag = inp.group(0)
                    n = re.search(r'name=[\x27\x22]([^\x27\x22]+)[\x27\x22]', tag)
                    v = re.search(r'value=[\x27\x22]([^\x27\x22]*)[\x27\x22]', tag)
                    if n and n.group(1) not in data:
                        data[n.group(1)] = v.group(1) if v else ''

            post_headers = {
                'Content-Type': 'application/x-www-form-urlencoded',
                'Referer': login_url,
            }
            async with session.post(
                login_url,
                data=data,
                headers=post_headers,
                allow_redirects=True,
                timeout=aiohttp.ClientTimeout(total=15),
            ) as post_resp:
                await post_resp.text()

            has_session = any(c.key == 'wangpan_session' for c in session.cookie_jar)
            if has_session:
                logger.info('FrameHdr login successful! Session cookie obtained.')
                cls._save_cookies()
                return True
            else:
                logger.error('FrameHdr login failed: wangpan_session not found in cookies.')
                return False
        except (TimeoutError, aiohttp.ClientError, OSError):
            logger.exception('FrameHdr login encountered error')
            return False

    @classmethod
    async def ensure_logged_in(cls) -> bool:
        session = await cls.get_session()
        has_session = any(c.key == 'wangpan_session' for c in session.cookie_jar)
        if not has_session:
            return await cls.login()
        return True

    @classmethod
    def extract_episodes_from_text(cls, text: str) -> set[int]:
        eps: set[int] = set()
        if not text:
            return eps

        for m in re.finditer(r'(?:更新至|更至|更新到|更新|全)\s*(?:第)?\s*0*(\d{1,4})\s*(?:集|话)?', text):
            val = int(m.group(1))
            if 1 <= val <= 2500 and val not in (1080, 2160, 720, 2023, 2024, 2025, 2026):
                eps.update(range(1, val + 1))

        for m in re.finditer(r'(?:[Ee]|EP|ep)?\s*0*(\d{1,4})\s*(?:-|~|到|至)\s*(?:[Ee]|EP|ep)?\s*0*(\d{1,4})\s*(?:集|话)?', text):
            s_ep, e_ep = int(m.group(1)), int(m.group(2))
            if 1 <= s_ep <= e_ep <= 2500 and (e_ep - s_ep) <= 150:
                eps.update(range(s_ep, e_ep + 1))

        for m in re.finditer(r'(?:[Ee]|EP|ep)\s*0*(\d{1,4})\b', text):
            val = int(m.group(1))
            if 1 <= val <= 2500 and val not in (1080, 2160, 720, 2023, 2024, 2025, 2026):
                eps.add(val)
        for m in re.finditer(r'第\s*0*(\d{1,4})\s*(?:集|话)', text):
            val = int(m.group(1))
            if 1 <= val <= 2500 and val not in (1080, 2160, 720, 2023, 2024, 2025, 2026):
                eps.add(val)

        return eps

    @classmethod
    async def search_series(
        cls,
        title: str,
        season: int = 1,
        episodes: list[int] | None = None,
        tmdb_id: int | None = None,
        limit: int = 5,
    ) -> list[dict[str, Any]]:
        settings = get_settings()
        if not settings.framehdr_enabled:
            return []

        clean_title = re.sub(r'[^\w\u4e00-\u9fa5]', '', title)
        if not clean_title:
            return []

        await cls.ensure_logged_in()
        session = await cls.get_session()

        import difflib
        aliases: list[str] = []
        if tmdb_id and settings.tmdb_api_key:
            try:
                a_url = f'https://api.themoviedb.org/3/tv/{tmdb_id}/alternative_titles?api_key={settings.tmdb_api_key}'
                async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=5)) as _s, _s.get(a_url) as _r:
                    if _r.status == 200:
                        _d = await _r.json()
                        for _it in _d.get('results', []):
                            _t = str(_it.get('title') or '').strip()
                            if _it.get('iso_3166_1') in ('CN', 'TW', 'HK') and re.search(r'[一-龥]', _t) and _t not in aliases:
                                aliases.append(_t)
            except (TimeoutError, aiohttp.ClientError, OSError) as exc:
                logger.debug('Alternative titles lookup skipped: %s', exc)

        queries = [title] + [a for a in aliases if a != title]
        target_cleans = [clean_title] + [
            re.sub(r'[^\w一-龥]', '', a) for a in aliases
            if re.sub(r'[^\w一-龥]', '', a)
        ]

        def title_matches_card(c_title: str) -> bool:
            c_clean = re.sub(r'[^\w一-龥]', '', c_title)
            for tc in target_cleans:
                if tc in c_clean or c_clean in tc:
                    return True
                if len(tc) >= 4 and difflib.SequenceMatcher(None, tc, c_clean[:len(tc)+5]).ratio() >= 0.75:
                    return True
            return False

        matched_cards: list[dict[str, str]] = []
        for q in queries:
            search_url = f"{settings.framehdr_base_url.rstrip('/')}/search.php?q={urllib.parse.quote(q)}"
            try:
                async with session.get(search_url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                    html = await resp.text()
            except (TimeoutError, aiohttp.ClientError, OSError) as e:
                logger.warning("FrameHdr search request failed for '%s': %s", q, e)
                continue

            card_matches = re.finditer(
                r'detail\.php\?id=(\d+).*?<h3[^>]*class=[^>]*card-title[^>]*>([^<]+)</h3>',
                html,
                re.DOTALL,
            )
            cards = [{'id': m.group(1), 'title': m.group(2).strip()} for m in card_matches]
            for c in cards:
                if not title_matches_card(c['title']):
                    continue
                # Single source of truth for season matching (Phase 2C §二) —
                # shared with tools/diagnose_framehdr.py so the diagnostic can
                # never disagree with production.
                if card_matches_season(c['title'], season):
                    matched_cards.append(c)
            if matched_cards:
                break

        if not matched_cards:
            logger.info("FrameHdr found no matched cards for '%s' Season %d across %d queries", title, season, len(queries))
            return []

        target_set = set(episodes or [])
        results: list[dict[str, Any]] = []

        for mc in matched_cards[:2]:
            detail_url = f"{settings.framehdr_base_url.rstrip('/')}/detail.php?id={mc['id']}"
            try:
                async with session.get(detail_url, timeout=aiohttp.ClientTimeout(total=15)) as d_resp:
                    d_html = await d_resp.text()

                if '_isLoggedIn = false' in d_html:
                    logger.info('FrameHdr detail page detected logged-out state. Re-authenticating...')
                    await cls.login()
                    async with session.get(detail_url, timeout=aiohttp.ClientTimeout(total=15)) as retry_resp:
                        d_html = await retry_resp.text()

                links = re.findall(
                    r"copyToClipboard\(\x27([^\x27]+)\x27\s*,\s*\x27([^\x27]*)\x27\s*,\s*(\d+)\s*,\s*\x27([^\x27]*)\x27\)",
                    d_html,
                )
                descriptions = re.findall(
                    r'<div class=[\x27\x22]link-description[\x27\x22]>([^<]*)</div>',
                    d_html,
                )
                publish_times = re.findall(
                    r'<span class=[\x27\x22]link-publish-time[\x27\x22]>发布时间：([^<]+)</span>',
                    d_html,
                )
                publishers = re.findall(
                    r'<span class=[\x27\x22]link-publisher-name[\x27\x22]>([^<]+)</span>',
                    d_html,
                )

                for idx, (url, raw_code, link_id, disk_name) in enumerate(links):
                    provider = (
                        'guangya'
                        if ('光鸭' in disk_name or 'guangyapan' in url)
                        else ('115' if '115' in disk_name else 'unknown')
                    )

                    clean_code = str(raw_code or '').strip(' -')
                    full_url = url
                    if clean_code and len(clean_code) >= 4 and 'guangyapan' in url and 'code=' not in url:
                        full_url = f'{url}?code={clean_code}'

                    desc = descriptions[idx].strip() if idx < len(descriptions) else ''
                    pub_time = publish_times[idx].strip() if idx < len(publish_times) else ''
                    pub_name = publishers[idx].strip() if idx < len(publishers) else ''

                    extracted_eps = cls.extract_episodes_from_text(f"{mc['title']} {desc}")
                    matched_eps = sorted(target_set & extracted_eps) if target_set else sorted(extracted_eps)

                    results.append({
                        'msg_id': 800000000 + int(link_id),
                        'chat_title': f"帧影·{pub_name or '分享'}",
                        'title': title,
                        'season': season,
                        'provider': provider,
                        'url': full_url,
                        'matched_episodes': matched_eps,
                        'date_cst': pub_time,
                        'snippet': f"[帧影分享] {mc['title']} | {desc}",
                        'source': 'framehdr',
                        'tmdb_id': tmdb_id,
                    })
                    if len(results) >= limit:
                        break
            except (TimeoutError, aiohttp.ClientError, OSError) as d_err:
                logger.warning('Failed to fetch detail for FrameHdr ID %s: %s', mc['id'], d_err)

        return results[:limit]

    @classmethod
    async def close(cls) -> None:
        async with cls._lock:
            if cls._session and not cls._session.closed:
                await cls._session.close()
                cls._session = None
