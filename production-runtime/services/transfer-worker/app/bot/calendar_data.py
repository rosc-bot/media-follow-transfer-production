import asyncio
import html
import logging
import re
import socket
import time
from datetime import date, datetime, timedelta, timezone
from typing import Any

import aiohttp

from app.core.config import get_settings

logger = logging.getLogger(__name__)
BEIJING_TZ = timezone(timedelta(hours=8))

CATEGORIES: dict[str, dict[str, str]] = {
    "my_following": {
        "name": "⭐ 我的在追 (连载监控)",
        "kind": "following",
        "icon": "⭐",
    },
    "domestic": {
        "name": "📺 国产剧集",
        "path": "/calendar/domestic",
        "kind": "calendar",
        "icon": "📺",
    },
    "anime": {
        "name": "🎌 动漫番剧 (Bangumi官方)",
        "path": "/calendar/anime",
        "kind": "bangumi",
        "icon": "🎌",
    },
    "western": {
        "name": "🎬 欧美剧集 (流媒体大作)",
        "path": "/calendar/western",
        "kind": "tmdb_western",
        "icon": "🎬",
    },
    "jp-kr": {
        "name": "🌸 日韩剧集",
        "path": "/calendar/jp-kr",
        "kind": "tmdb_jpkr",
        "icon": "🌸",
    },
    "movie": {
        "name": "🎥 院线电影",
        "path": "/calendar/movie",
        "kind": "calendar",
        "icon": "🎥",
    },
    "reality": {
        "name": "🎪 热门综艺",
        "path": "/calendar/reality",
        "kind": "calendar",
        "icon": "🎪",
    },
    "documentary": {
        "name": "🌍 纪录大片",
        "path": "/calendar/documentary",
        "kind": "calendar",
        "icon": "🌍",
    },
}

CACHE_TTL = 900  # 15 minutes cache
_PAGE_CACHE: dict[str, dict[str, Any]] = {}
_BGM_CACHE: dict[str, tuple[list[dict[str, Any]], float]] = {}
_TMDB_CACHE: dict[str, tuple[list[dict[str, Any]], float]] = {}


class CalendarService:
    CATEGORIES = CATEGORIES
    BASE_URL = "https://www.sztv.net"

    @classmethod
    def _clean_text(cls, text: str) -> str:
        if not text:
            return ""
        s = html.unescape(text)
        s = re.sub(r'[\r\n\t]+', ' ', s)
        return s.strip()

    @staticmethod
    def parse_show_title_and_season(raw_title: str, default_season: int = 1) -> tuple[str, int]:
        s = html.unescape(raw_title.strip())
        m_s = re.search(r'第\s*([0-9一二三四五六七八九十]+)\s*季', s)
        if m_s:
            val = m_s.group(1)
            cn_map = {"一": 1, "二": 2, "三": 3, "四": 4, "五": 5, "六": 6, "七": 7, "八": 8, "九": 9, "十": 10}
            season = int(val) if val.isdigit() else cn_map.get(val, default_season)
            clean_t = re.sub(r'第\s*[0-9一二三四五六七八九十]+\s*季', '', s).strip()
            return clean_t, season
        m_en = re.search(r'Season\s*(\d+)', s, re.IGNORECASE)
        if m_en:
            season = int(m_en.group(1))
            clean_t = re.sub(r'Season\s*\d+', '', s, flags=re.IGNORECASE).strip()
            return clean_t, season
        return s, default_season

    @staticmethod
    def parse_episode_tag(tag_text: str) -> dict[str, Any]:
        raw = html.unescape(tag_text.strip()) if tag_text else ""
        if not raw:
            return {
                "season": 1,
                "start_ep": 1,
                "end_ep": 1,
                "episodes": [],
                "display": "全集",
                "invalid_episode_tag": True,
            }
        m_range = re.search(r'S(\d+)\s*E(\d+)\s*[-~到至]\s*E?(\d+)', raw, re.IGNORECASE)
        if m_range:
            season = int(m_range.group(1))
            if season >= 1900:
                season = 1
            start_ep = int(m_range.group(2))
            end_ep = int(m_range.group(3))
            return {
                "season": season,
                "start_ep": start_ep,
                "end_ep": end_ep,
                "episodes": list(range(start_ep, end_ep + 1)),
                "display": f"S{season:02d}E{start_ep:02d}-{end_ep:02d}",
            }
        m_single = re.search(r'S(\d+)\s*E(\d+)', raw, re.IGNORECASE)
        if m_single:
            season = int(m_single.group(1))
            if season >= 1900:
                season = 1
            ep = int(m_single.group(2))
            return {
                "season": season,
                "start_ep": ep,
                "end_ep": ep,
                "episodes": [ep],
                "display": f"S{season:02d}E{ep:02d}",
            }
        m_s_only = re.search(r'S(\d+)', raw, re.IGNORECASE)
        if m_s_only:
            season = int(m_s_only.group(1))
            if season >= 1900:
                season = 1
            return {
                "season": season,
                "start_ep": 1,
                "end_ep": 1,
                "episodes": [1],
                "display": f"S{season:02d}全季",
            }
        m_ep_only = re.search(r'E?(\d+)\s*[-~到至]\s*E?(\d+)', raw, re.IGNORECASE)
        if m_ep_only:
            s_ep = int(m_ep_only.group(1))
            e_ep = int(m_ep_only.group(2))
            return {
                "season": 1,
                "start_ep": s_ep,
                "end_ep": e_ep,
                "episodes": list(range(s_ep, e_ep + 1)),
                "display": f"E{s_ep:02d}-{e_ep:02d}",
            }
        m_ep_s = re.search(r'E?(\d+)', raw, re.IGNORECASE)
        if m_ep_s:
            ep = int(m_ep_s.group(1))
            return {
                "season": 1,
                "start_ep": ep,
                "end_ep": ep,
                "episodes": [ep],
                "display": f"E{ep:02d}",
            }
        return {
            "season": 1,
            "start_ep": 1,
            "end_ep": 1,
            "episodes": [1],
            "display": raw or "全集",
        }

    # ==========================================
    # Source 1: My Following Shows
    # ==========================================
    @classmethod
    async def get_my_following_schedule(cls, target_date: date) -> dict[str, Any]:
        # This module is used by the new unified project; read its PostgreSQL
        # watchlist directly instead of importing the retired SQLite service.
        from sqlalchemy import select

        from app.core.database import AsyncSessionLocal
        from app.models.watchlist import SeriesWatchlist

        async with AsyncSessionLocal() as db:
            subs = list((await db.scalars(
                select(SeriesWatchlist)
                .where(SeriesWatchlist.status == "FOLLOWING")
                .order_by(SeriesWatchlist.updated_at.desc())
            )).all())
        shows = []
        for s in subs:
            title = cls._clean_text(s.title or "")
            season = int(s.season or 1)
            total = s.total_episodes
            raw_coll = s.collected_episodes or []
            collected_eps: set[int] = set()
            for item in raw_coll:
                match = re.search(r"(?:S\d{1,2})?E?(\d{1,4})$", str(item), re.IGNORECASE)
                if match:
                    collected_eps.add(int(match.group(1)))
            max_c = max(collected_eps) if collected_eps else 0
            missing_count = (int(total) - len(collected_eps)) if (total and int(total) > len(collected_eps)) else 0
            ep_disp = f"已收录至E{max_c:02d}" + (f" (共{total}集/待补{missing_count}集)" if total else " (连载跟更中)")
            shows.append({
                "title": title,
                "season": season,
                "ep_display": ep_disp,
                "episodes": [max_c + 1] if max_c > 0 else [1],
                "is_premiere": False,
                "poster": s.poster_path or "",
                "tmdb_id": s.tmdb_id,
                "total_episodes": total,
                "collected_count": len(collected_eps),
                "is_following": True,
            })

        return {
            "cat_key": "my_following",
            "cat_name": CATEGORIES["my_following"]["name"],
            "day_text": f"{target_date.month}月{target_date.day}日 · 在追连载总览 ({len(shows)}部)",
            "shows": shows,
            "target_date": target_date,
            "kind": "grid",
        }

    # ==========================================
    # Source 2: Bangumi Anime Broadcast Calendar
    # ==========================================
    @classmethod
    async def get_bangumi_anime_schedule(cls, target_date: date) -> dict[str, Any]:
        now = time.time()
        bgm_key = "bangumi_calendar"
        calendar_data = None
        if bgm_key in _BGM_CACHE:
            data, exp = _BGM_CACHE[bgm_key]
            if now < exp:
                calendar_data = data

        if not calendar_data:
            headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) ZhuiXinBot/2.0"}
            try:
                connector = aiohttp.TCPConnector(family=socket.AF_INET)
                async with (
                    aiohttp.ClientSession(connector=connector, headers=headers) as session,
                    session.get("https://api.bgm.tv/calendar", timeout=aiohttp.ClientTimeout(total=8)) as resp,
                ):
                    if resp.status == 200:
                        calendar_data = await resp.json()
                        _BGM_CACHE[bgm_key] = (calendar_data, now + 1800)  # 30m cache
            except Exception as exc:  # noqa: BLE001
                logger.warning("Failed to fetch Bangumi calendar: %s", exc)

        shows: list[dict[str, Any]] = []
        bgm_weekday_id = target_date.weekday() + 1  # 1=Mon .. 7=Sun
        weekdays_cn = ["周一", "周二", "周三", "周四", "周五", "周六", "周日"]
        weekday_name = weekdays_cn[target_date.weekday()]

        if calendar_data and isinstance(calendar_data, list):
            for day_group in calendar_data:
                w_info = day_group.get("weekday", {})
                if w_info.get("id") == bgm_weekday_id:
                    items = day_group.get("items", [])
                    for it in items:
                        cn_name = cls._clean_text(it.get("name_cn") or it.get("name") or "")
                        if not cn_name:
                            continue
                        clean_t, sea = cls.parse_show_title_and_season(cn_name, 1)
                        images = it.get("images") or {}
                        poster = images.get("large") or images.get("common") or ""
                        shows.append({
                            "title": clean_t,
                            "raw_full": cn_name,
                            "season": sea,
                            "episodes": [],
                            "ep_display": f"第{sea}季 · 每周{weekday_name}放送",
                            "poster": poster,
                            "is_premiere": False,
                            "rating": (it.get("rating") or {}).get("score"),
                        })
                    break

        # If Bangumi returned items, use it! Otherwise fallback to SZTV
        if not shows:
            return await cls._fetch_sztv_grid_fallback("anime", target_date)

        return {
            "cat_key": "anime",
            "cat_name": CATEGORIES["anime"]["name"],
            "day_text": f"{target_date.month}月{target_date.day}日 {weekday_name} (Bangumi同步)",
            "shows": shows,
            "target_date": target_date,
            "kind": "grid",
        }

    # ==========================================
    # Source 3: TMDB Western & JP-KR Integration
    # ==========================================
    @classmethod
    async def get_tmdb_curated_schedule(cls, cat_key: str, target_date: date) -> dict[str, Any]:
        if not get_settings().tmdb_api_key:
            return await cls._fetch_sztv_calendar_fallback(cat_key, target_date)

        is_western = (cat_key == "western")
        cache_key = f"tmdb_curated_{cat_key}"
        now = time.time()
        cached_shows = None
        if cache_key in _TMDB_CACHE:
            data, exp = _TMDB_CACHE[cache_key]
            if now < exp:
                cached_shows = data

        if cached_shows is None:
            url_airing = f"https://api.themoviedb.org/3/tv/airing_today?api_key={get_settings().tmdb_api_key}&language=zh-CN&timezone=Asia/Shanghai"
            url_trend = f"https://api.themoviedb.org/3/trending/tv/day?api_key={get_settings().tmdb_api_key}&language=zh-CN"
            try:
                connector = aiohttp.TCPConnector(family=socket.AF_INET)
                async with aiohttp.ClientSession(connector=connector) as session:
                    res_airing, res_trend = await asyncio.gather(
                        session.get(url_airing, timeout=aiohttp.ClientTimeout(total=8)),
                        session.get(url_trend, timeout=aiohttp.ClientTimeout(total=8)),
                        return_exceptions=True
                    )
                    items_raw = []
                    if not isinstance(res_airing, Exception) and res_airing.status == 200:
                        d_air = await res_airing.json()
                        items_raw.extend(d_air.get("results") or [])
                    if not isinstance(res_trend, Exception) and res_trend.status == 200:
                        d_trend = await res_trend.json()
                        items_raw.extend(d_trend.get("results") or [])

                target_countries = {"US", "GB", "CA", "AU", "FR", "DE", "ES", "IT"} if is_western else {"JP", "KR"}
                shows = []
                seen_ids = set()
                for item in items_raw:
                    tid = item.get("id")
                    if tid in seen_ids:
                        continue
                    countries = set(item.get("origin_country") or [])
                    if not (countries & target_countries):
                        continue

                    seen_ids.add(tid)
                    cn_title = cls._clean_text(item.get("name") or item.get("original_name") or "")
                    # Special check: If title is English/Pinyin but has known CN alias
                    # TMDBService handled
                    poster_path = item.get("poster_path")
                    poster_url = f"https://image.tmdb.org/t/p/w500{poster_path}" if poster_path else ""

                    shows.append({
                        "title": cn_title,
                        "raw_full": cn_title,
                        "season": 1,
                        "episodes": [],
                        "ep_display": "第1季 · 今日热播流媒体更新" if is_western else "第1季 · 今日放送更新",
                        "poster": poster_url,
                        "is_premiere": False,
                        "popularity": item.get("popularity", 0),
                        "tmdb_id": tid,
                    })

                shows.sort(key=lambda x: -x.get("popularity", 0))
                # Merge with SZTV shows
                sztv_data = await cls.fetch_category_data(cat_key)
                sztv_shows = []
                if sztv_data.get("kind") == "calendar":
                    target_str = f"{target_date.month}月{target_date.day}日"
                    for d in sztv_data.get("days", []):
                        if target_str in d.get("day_text", ""):
                            sztv_shows = d.get("shows", [])
                            break
                    if not sztv_shows and sztv_data.get("days"):
                        sztv_shows = sztv_data["days"][0].get("shows", [])
                else:
                    sztv_shows = sztv_data.get("shows", [])

                existing_titles = {s["title"].lower() for s in shows}
                for ss in sztv_shows:
                    st_clean = cls._clean_text(ss.get("title") or "")
                    if st_clean and st_clean.lower() not in existing_titles:
                        shows.append(ss)
                        existing_titles.add(st_clean.lower())

                _TMDB_CACHE[cache_key] = (shows, now + 1800)
                cached_shows = shows
            except Exception as exc:  # noqa: BLE001
                logger.warning("TMDB curated schedule error for %s: %s", cat_key, exc)
                return await cls._fetch_sztv_calendar_fallback(cat_key, target_date)

        cfg = CATEGORIES[cat_key]
        weekdays_cn = ["周一", "周二", "周三", "周四", "周五", "周六", "周日"]
        weekday_name = weekdays_cn[target_date.weekday()]
        return {
            "cat_key": cat_key,
            "cat_name": cfg["name"],
            "day_text": f"{target_date.month}月{target_date.day}日 {weekday_name} (TMDB全网同步)",
            "shows": cached_shows or [],
            "target_date": target_date,
            "kind": "grid",
        }

    # ==========================================
    # Fallback Parsers using SZTV
    # ==========================================
    @classmethod
    async def _fetch_sztv_grid_fallback(cls, cat_key: str, target_date: date) -> dict[str, Any]:
        data = await cls.fetch_category_data(cat_key)
        cfg = CATEGORIES.get(cat_key, CATEGORIES["domestic"])
        return {
            "cat_key": cat_key,
            "cat_name": cfg["name"],
            "day_text": f"{target_date.month}月{target_date.day}日 (最新热播)",
            "shows": data.get("shows", []),
            "target_date": target_date,
            "kind": "grid",
        }

    @classmethod
    async def _fetch_sztv_calendar_fallback(cls, cat_key: str, target_date: date) -> dict[str, Any]:
        data = await cls.fetch_category_data(cat_key)
        cfg = CATEGORIES.get(cat_key, CATEGORIES["domestic"])
        target_str = f"{target_date.month}月{target_date.day}日"
        days = data.get("days", [])
        for d in days:
            if target_str in d.get("day_text", ""):
                return {
                    "cat_key": cat_key,
                    "cat_name": data.get("cat_name", cfg["name"]),
                    "day_text": d.get("day_text"),
                    "shows": d.get("shows", []),
                    "target_date": target_date,
                    "kind": "calendar",
                }
        first_day = days[0] if days else {}
        return {
            "cat_key": cat_key,
            "cat_name": data.get("cat_name", cfg["name"]),
            "day_text": first_day.get("day_text", target_str),
            "shows": first_day.get("shows", []),
            "target_date": target_date,
            "kind": "calendar",
        }

    # ==========================================
    # SZTV Regex Parser (No external bs4 dependency)
    # ==========================================
    @classmethod
    async def fetch_category_data(cls, cat_key: str) -> dict[str, Any]:
        cfg = CATEGORIES.get(cat_key)
        if not cfg or not cfg.get("path"):
            cat_key = "domestic"
            cfg = CATEGORIES["domestic"]

        now = time.time()
        if cat_key in _PAGE_CACHE:
            cached = _PAGE_CACHE[cat_key]
            if now - cached.get("timestamp", 0) < CACHE_TTL:
                return cached.get("data", {})

        url = f"https://www.sztv.net{cfg['path']}"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        }
        html_doc = None
        last_error = None
        stale_data = (_PAGE_CACHE.get(cat_key) or {}).get("data")
        for attempt in range(2):
            try:
                connector = aiohttp.TCPConnector(family=socket.AF_INET)
                async with (
                    aiohttp.ClientSession(connector=connector, timeout=aiohttp.ClientTimeout(total=25)) as session,
                    session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=12)) as resp,
                ):
                    if resp.status != 200:
                        last_error = f"HTTP {resp.status}"
                    else:
                        html_doc = await resp.text()
                        break
            except Exception as e:  # noqa: BLE001
                last_error = f"{type(e).__name__}: {e}"
            if attempt == 0:
                await asyncio.sleep(0.4)

        if html_doc is None:
            logger.warning("sztv request error for %s after retry: %s", cat_key, last_error or "unknown")
            if stale_data:
                return stale_data
            return {}

        parsed: dict[str, Any] = {"cat_key": cat_key, "cat_name": cfg["name"]}

        if '<div class="tv-calendar-cell' in html_doc or "<div class='tv-calendar-cell" in html_doc:
            cells = re.split(r"<div class=['\"]tv-calendar-cell", html_doc)
            days = []
            for cell in cells[1:]:
                day_m = re.search(r"<span class=['\"]tv-day-long['\"][^>]*>(?:<a[^>]*>)?([^<]+)", cell)
                day_text = cls._clean_text(day_m.group(1)) if day_m else ""
                shows = []
                matches = re.finditer(r"<a\s+([^>]*class=['\"][^'\"]*tv-calendar-item[^'\"]*['\"][^>]*)>", cell)
                for m in matches:
                    tag_attrs = m.group(1)
                    title_m = re.search(r"title=['\"]([^'\"]+)['\"]", tag_attrs)
                    poster_m = re.search(r"data-poster=['\"]([^'\"]*)['\"]", tag_attrs)
                    raw_full = cls._clean_text(title_m.group(1)) if title_m else ""
                    poster = poster_m.group(1).strip() if poster_m else ""
                    is_premiere = "premiere" in tag_attrs.lower()

                    if "·" in raw_full:
                        t, ep_raw = raw_full.split("·", 1)
                    else:
                        t, ep_raw = raw_full, ""
                    ep_info = cls.parse_episode_tag(ep_raw)
                    clean_title, final_season = cls.parse_show_title_and_season(t.strip(), ep_info["season"])
                    shows.append({
                        "title": clean_title,
                        "raw_full": raw_full,
                        "season": final_season,
                        "episodes": ep_info["episodes"],
                        "ep_display": f"第{final_season}季 " + ep_info["display"],
                        "poster": poster,
                        "is_premiere": is_premiere,
                    })
                if day_text and shows:
                    dm = re.search(r'(\d+)月(\d+)日', day_text)
                    month = int(dm.group(1)) if dm else None
                    day = int(dm.group(2)) if dm else None
                    days.append({
                        "day_text": day_text,
                        "month": month,
                        "day": day,
                        "shows": shows,
                    })
            parsed["kind"] = "calendar"
            parsed["days"] = days
        else:
            pattern = re.compile(
                r"<div class=['\"]media-card['\"]><a class=['\"]media-item['\"][^>]*href=['\"]([^'\"]+)['\"]>(?:<img[^>]+src=['\"]([^'\"]+)['\"])?.*?<div class=['\"]title['\"]>([^<]+)</div>(?:<div class=['\"]rate['\"]>([^<]*)</div>)?",
                re.DOTALL
            )
            matches = pattern.findall(html_doc)
            shows = []
            for href, poster, title, rate in matches:
                ep_info = cls.parse_episode_tag(rate.strip())
                clean_title, final_season = cls.parse_show_title_and_season(title.strip(), ep_info["season"])
                shows.append({
                    "title": clean_title,
                    "raw_full": f"{clean_title} · {rate.strip()}" if rate else clean_title,
                    "season": final_season,
                    "episodes": ep_info["episodes"],
                    "ep_display": f"第{final_season}季 " + ep_info["display"],
                    "poster": poster.strip(),
                    "is_premiere": False,
                })
            parsed["kind"] = "grid"
            parsed["shows"] = shows

        _PAGE_CACHE[cat_key] = {"timestamp": now, "data": parsed}
        return parsed

    # ==========================================
    # Unified Router
    # ==========================================
    @classmethod
    async def get_category_schedule(cls, cat_key: str, day_offset: int = 0) -> dict[str, Any]:
        return await cls.get_shows_for_category_and_date(cat_key, date_offset=day_offset)

    @classmethod
    async def get_shows_for_category_and_date(
        cls,
        cat_key: str,
        target_date: date | None = None,
        date_offset: int = 0
    ) -> dict[str, Any]:
        req_date = target_date or (datetime.now(BEIJING_TZ).date() + timedelta(days=date_offset))
        
        if cat_key == "my_following":
            return await cls.get_my_following_schedule(req_date)
        elif cat_key == "anime":
            return await cls.get_bangumi_anime_schedule(req_date)
        elif cat_key == "western" or cat_key == "jp-kr":
            return await cls.get_tmdb_curated_schedule(cat_key, req_date)
        else:
            return await cls._fetch_sztv_calendar_fallback(cat_key, req_date)

    @classmethod
    async def get_today_shows(cls, category: str = "domestic") -> dict[str, Any]:
        return await cls.get_shows_for_category_and_date(category, datetime.now(BEIJING_TZ).date())

    @staticmethod
    async def search_tmdb(query: str) -> list[dict[str, Any]]:
        if not get_settings().tmdb_api_key:
            return []
        url = "https://api.themoviedb.org/3/search/tv"
        params = {
            "api_key": get_settings().tmdb_api_key,
            "query": query,
            "language": "zh-CN",
        }
        try:
            connector = aiohttp.TCPConnector(family=socket.AF_INET)
            async with (
                aiohttp.ClientSession(connector=connector, timeout=aiohttp.ClientTimeout(total=25)) as session,
                session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as resp,
            ):
                if resp.status == 200:
                    data = await resp.json()
                    results = []
                    for r in (data.get("results") or [])[:5]:
                        results.append({
                            "id": r.get("id"),
                            "title": r.get("name") or r.get("original_name"),
                            "year": (r.get("first_air_date") or "")[:4],
                            "overview": (r.get("overview") or "")[:120],
                            "poster_path": f"https://image.tmdb.org/t/p/w500{r.get('poster_path')}" if r.get("poster_path") else None,
                        })
                    return results
        except Exception as e:  # noqa: BLE001
            logger.warning("TMDB search failed: %s", e)
        return []
