from datetime import UTC, datetime
import asyncio
import re
import json
import socket
import logging
import time
from typing import Any, Dict, List, Optional, Set, Tuple
import aiohttp
from sqlalchemy import select, text
from app.core.database import AsyncSessionLocal
from app.models.cloud import CloudConfig
from app.models.watchlist import SeriesWatchlist

logger = logging.getLogger(__name__)

VIDEO_EXTS = (".mkv", ".mp4", ".ts", ".avi", ".mov", ".wmv", ".flv", ".webm", ".iso", ".strm")


def parse_season_episode_from_filename(filename: str, default_season: int = 1) -> Tuple[int, Optional[int]]:
    fn = filename.strip()
    if re.search(r"(?<![A-Za-z0-9])S\d{4}\s*E\d{1,4}(?!\d)", fn, re.I):
        return default_season, None
    season = default_season
    m_se = re.search(r"S(\d{1,2})\s*E(\d{1,4})", fn, re.I)
    ep = None
    if m_se:
        season = int(m_se.group(1))
        ep = int(m_se.group(2))
    else:
        m_cn_s = re.search(r"第\s*([一二三四五六七八九十0-9]+)\s*季", fn)
        if m_cn_s:
            s_raw = m_cn_s.group(1)
            cn_nums = {"一": 1, "二": 2, "三": 3, "四": 4, "五": 5, "六": 6, "七": 7, "八": 8, "九": 9, "十": 10}
            season = int(s_raw) if s_raw.isdigit() else cn_nums.get(s_raw, default_season)

        m_ep = re.search(r"(?:[Ee]|EP|ep)\s*0*(\d{1,4})\b", fn)
        if m_ep:
            ep = int(m_ep.group(1))
        else:
            m_cn_ep = re.search(r"第\s*0*(\d{1,4})\s*(?:集|话)", fn)
            if m_cn_ep:
                ep = int(m_cn_ep.group(1))
            else:
                m_simple_ep = re.search(r"(?:[._\-\s])(\d{2,4})(?:[._\-\s]|\.[a-zA-Z0-9]+$)", fn)
                if m_simple_ep:
                    val = int(m_simple_ep.group(1))
                    if 1 <= val <= 2500 and not (1900 <= val <= 2099) and val not in (1080, 2160, 720, 480, 264, 265):
                        ep = val
    return season, ep


class MasterInventoryScanner:
    @classmethod
    async def scan_and_sync(cls, *, include_completed: bool = False) -> dict:
        """Physical cloud scan using IPv4 aiohttp connector directly to Guangya API."""
        t0 = time.monotonic()
        async with AsyncSessionLocal() as db:
            cfg = await db.scalar(select(CloudConfig).where(CloudConfig.name == "guangya"))
            if not cfg or not cfg.enabled or not cfg.auth_ref:
                logger.warning("MasterInventoryScanner: Guangya cloud provider not configured or disabled.")
                return {"success": False, "error": "Guangya provider disabled"}
            ongoing_root = cfg.ongoing_target_folder_id
            completed_root = cfg.target_folder_id
            auth_token_raw = cfg.auth_ref

            watchlists_all = list((await db.scalars(select(SeriesWatchlist))).all())
            title_to_tmdb: dict[str, int] = {}
            for w in watchlists_all:
                if w.tmdb_id and w.title:
                    clean_w = re.sub(r"[^\w\u4e00-\u9fa5]", "", str(w.title))
                    if clean_w:
                        title_to_tmdb[clean_w] = int(w.tmdb_id)

        try:
            auth_data = json.loads(auth_token_raw) if auth_token_raw.startswith("{") else {}
            ref_token = auth_data.get("refresh_token") or auth_token_raw
        except Exception:
            ref_token = auth_token_raw

        roots_to_scan = [r for r in ([ongoing_root, completed_root] if include_completed else [ongoing_root]) if r]
        if not roots_to_scan:
            return {"success": False, "error": "No roots configured"}

        connector = aiohttp.TCPConnector(family=socket.AF_INET)
        async with aiohttp.ClientSession(connector=connector) as session:
            # 1. Refresh access token with retry
            acc_token = None
            last_err = None
            for attempt in range(4):
                try:
                    t_resp = await session.post(
                        "https://account.guangyapan.com/v1/auth/token",
                        json={"client_id": "aMe-8VSlkrbQXpUR", "grant_type": "refresh_token", "refresh_token": ref_token},
                        headers={"Content-Type": "application/json", "Origin": "https://account.guangyapan.com", "User-Agent": "Mozilla/5.0"},
                        timeout=aiohttp.ClientTimeout(total=20)
                    )
                    t_data = await t_resp.json()
                    acc_token = t_data.get("access_token")
                    if acc_token:
                        break
                    last_err = f"Token refresh returned no token: {t_data}"
                except Exception as e:
                    last_err = e
                await asyncio.sleep(1.0 * (attempt + 1))

            if not acc_token:
                logger.error("MasterInventoryScanner: Connect/refresh account failed: %s", last_err)
                return {"success": False, "error": f"Connect account server failed: {last_err}"}
            headers = {
                "Accept": "application/json, text/plain, */*",
                "Content-Type": "application/json",
                "Authorization": f"Bearer {acc_token}",
                "Origin": "https://www.guangyapan.com",
                "User-Agent": "Mozilla/5.0",
            }

            sem = asyncio.Semaphore(4)

            async def list_dir(parent_id: str) -> List[Dict[str, Any]]:
                if not parent_id:
                    return []
                async with sem:
                    for attempt in range(4):
                        try:
                            r = await session.post(
                                "https://api.guangyapan.com/userres/v1/file/get_file_list",
                                json={"parentId": str(parent_id or ""), "pageNum": 1, "pageSize": 300},
                                headers=headers,
                                timeout=aiohttp.ClientTimeout(total=15)
                            )
                            if r.status != 200:
                                await asyncio.sleep(0.5 * (attempt + 1))
                                continue
                            res = await r.json()
                            data = res.get("data", {})
                            items = data.get("list", [])
                            total = data.get("total")
                            if total and total > len(items) and total <= 3000:
                                r_all = await session.post(
                                    "https://api.guangyapan.com/userres/v1/file/get_file_list",
                                    json={"parentId": str(parent_id or ""), "pageNum": 1, "pageSize": total},
                                    headers=headers,
                                    timeout=aiohttp.ClientTimeout(total=20)
                                )
                                if r_all.status == 200:
                                    res_all = await r_all.json()
                                    items = res_all.get("data", {}).get("list", []) or items
                            return items
                        except Exception:
                            await asyncio.sleep(0.5 * (attempt + 1))
                return []

            # 2. Traverse Level 1 & 2 categories
            all_l2 = []
            for root_id in roots_to_scan:
                l1 = await list_dir(root_id)
                l2_tasks = [list_dir(it1.get("fileId")) for it1 in l1 if it1.get("resType") == 2]
                l2_results = await asyncio.gather(*l2_tasks, return_exceptions=True)
                for res in l2_results:
                    if isinstance(res, list):
                        all_l2.extend(res)

            # 3. Traverse Level 3 (Show folders)
            l3_tasks = [list_dir(it2.get("fileId")) for it2 in all_l2 if it2.get("resType") == 2]
            l3_results = await asyncio.gather(*l3_tasks, return_exceptions=True)

            shows = []
            for i, it2 in enumerate([x for x in all_l2 if x.get("resType") == 2]):
                cat_name = it2.get("fileName")
                sub_items = l3_results[i] if i < len(l3_results) and isinstance(l3_results[i], list) else []
                # Check if it2 is already a show folder (contains video files)
                has_subfolders = any(x.get("resType") == 2 and not x.get("fileName", "").startswith(".") for x in sub_items)
                has_videos = any(any(x.get("fileName", "").lower().endswith(ext) for ext in VIDEO_EXTS) for x in sub_items)

                if has_videos and not has_subfolders:
                    shows.append((cat_name, it2.get("fileName"), it2.get("fileId"), sub_items))
                else:
                    for it3 in sub_items:
                        if it3.get("resType") == 2 and not it3.get("fileName", "").startswith("."):
                            shows.append((cat_name, it3.get("fileName"), it3.get("fileId"), None))

            logger.info("MasterInventoryScanner: Found %d show folders to inspect in %.1fs", len(shows), time.monotonic() - t0)

            # Fetch inner files for shows where items were not prefetched
            pending_shows = [s for s in shows if s[3] is None]
            if pending_shows:
                inner_tasks = [list_dir(s[2]) for s in pending_shows]
                inner_res = await asyncio.gather(*inner_tasks, return_exceptions=True)
                for idx, res in enumerate(inner_res):
                    items = res if isinstance(res, list) else []
                    s_orig = pending_shows[idx]
                    # Update back into shows
                    for s_idx, s in enumerate(shows):
                        if s[2] == s_orig[2]:
                            shows[s_idx] = (s[0], s[1], s[2], items)
                            break

            # 4. Gather inner files and season sub-folders
            season_tasks = []
            season_meta = []
            show_items = []
            cn_s_map = {"一": 1, "二": 2, "三": 3, "四": 4, "五": 5, "六": 6, "七": 7, "八": 8, "九": 9, "十": 10}

            for cat_name, folder_name, show_fid, inner_files in shows:
                m_tid = re.search(r"tmdbid-(\d+)", folder_name, re.I)
                tmdb_id = int(m_tid.group(1)) if m_tid else None

                m_clean = re.sub(r"\(.*?\)|\{.*?\}|\[.*?\]|（.*?）", "", folder_name)
                m_clean = re.sub(r"第\s*[一二三四五六七八九十0-9]+\s*季", "", m_clean)
                m_clean = re.sub(r"Season\s*\d+", "", m_clean, flags=re.I)
                m_clean = re.sub(r"\s*4K.*", "", m_clean).strip()
                clean_title = re.sub(r"[^\w\u4e00-\u9fa5]", "", m_clean)

                if not tmdb_id and clean_title in title_to_tmdb:
                    tmdb_id = title_to_tmdb[clean_title]

                m_s_folder = re.search(r"第\s*([一二三四五六七八九十0-9]+)\s*季", folder_name)
                folder_def_s = 1
                if m_s_folder:
                    val = m_s_folder.group(1)
                    folder_def_s = int(val) if val.isdigit() else cn_s_map.get(val, 1)
                else:
                    m_s_en = re.search(r"(?:Season\s*0*|S0*)(\d+)", folder_name, re.I)
                    if m_s_en:
                        folder_def_s = int(m_s_en.group(1))

                for item in (inner_files or []):
                    fn = str(item.get("fileName") or "").strip()
                    rtype = item.get("resType")
                    if rtype == 2 and not fn.startswith("."):
                        season_tasks.append(list_dir(str(item.get("fileId"))))
                        season_meta.append((m_clean, clean_title, tmdb_id, fn, show_fid))
                    elif any(fn.lower().endswith(ext) for ext in VIDEO_EXTS):
                        s, ep = parse_season_episode_from_filename(fn, default_season=folder_def_s)
                        if ep is not None:
                            show_items.append((m_clean, clean_title, tmdb_id, s, ep, fn, fn, show_fid))

            if season_tasks:
                season_res = await asyncio.gather(*season_tasks, return_exceptions=True)
                for (title, clean_t, tmdb_id, s_folder_name, parent_fid), s_files in zip(season_meta, season_res):
                    if not isinstance(s_files, list):
                        continue
                    m_s = re.search(r"第\s*([一二三四五六七八九十0-9]+)\s*季", s_folder_name)
                    def_s = 1
                    if m_s:
                        val = m_s.group(1)
                        def_s = int(val) if val.isdigit() else cn_s_map.get(val, 1)
                    else:
                        m_s_en = re.search(r"(?:Season\s*0*|S0*)(\d+)", s_folder_name, re.I)
                        if m_s_en:
                            def_s = int(m_s_en.group(1))

                    for item in s_files:
                        fn = str(item.get("fileName") or "").strip()
                        if any(fn.lower().endswith(ext) for ext in VIDEO_EXTS):
                            sea, ep = parse_season_episode_from_filename(fn, default_season=def_s)
                            if ep is not None:
                                show_items.append((title, clean_t, tmdb_id, sea, ep, fn, f"{s_folder_name}/{fn}", parent_fid))

        elapsed = round(time.monotonic() - t0, 2)
        logger.info("MasterInventoryScanner: Completed in %ss. Found %d physical files across %d shows.", elapsed, len(show_items), len(shows))
        if not show_items:
            return {"success": True, "elapsed": elapsed, "files_found": 0}

        # Deduplicate
        dedup_map = {}
        for item in show_items:
            title, clean_t, tmdb_id, sea, ep, fn, rel_p, show_fid = item
            dedup_map[(clean_t, tmdb_id, sea, ep)] = item
        unique_items = list(dedup_map.values())

        # 5. Persist to PostgreSQL
        async with AsyncSessionLocal() as db, db.begin():
            for title, clean_t, tmdb_id, sea, ep, fn, rel_p, _ in unique_items:
                if tmdb_id is not None:
                    await db.execute(text("""
                        INSERT INTO cloud_disk_inventory (title, clean_title, tmdb_id, season, episode, file_name, rel_path, updated_at)
                        VALUES (:title, :clean_title, :tmdb_id, :season, :episode, :file_name, :rel_path, NOW())
                        ON CONFLICT (clean_title, tmdb_id, season, episode)
                        DO UPDATE SET file_name = EXCLUDED.file_name, rel_path = EXCLUDED.rel_path, updated_at = NOW();
                    """), {
                        "title": title[:255],
                        "clean_title": clean_t[:255],
                        "tmdb_id": tmdb_id,
                        "season": sea,
                        "episode": ep,
                        "file_name": fn[:512],
                        "rel_path": rel_p[:1024],
                    })

            inv_by_tmdb: dict[tuple[int, int], set[str]] = {}
            inv_by_title: dict[tuple[str, int], set[str]] = {}
            folder_by_tmdb: dict[tuple[int, int], str] = {}
            folder_by_title: dict[tuple[str, int], str] = {}

            for _, clean_t, tmdb_id, sea, ep, _, _, show_fid in unique_items:
                ep_key = f"S{sea:02d}E{ep:02d}"
                if tmdb_id:
                    inv_by_tmdb.setdefault((tmdb_id, sea), set()).add(ep_key)
                    if show_fid:
                        folder_by_tmdb[(tmdb_id, sea)] = str(show_fid)
                if clean_t:
                    inv_by_title.setdefault((clean_t, sea), set()).add(ep_key)
                    if show_fid:
                        folder_by_title[(clean_t, sea)] = str(show_fid)

            watchlists = list((await db.scalars(select(SeriesWatchlist).where(SeriesWatchlist.status != "CANCELLED"))).all())
            sync_count = 0
            folder_sync_count = 0
            from app.follow.episode_keys import canonical_episode_key

            for w in watchlists:
                tid = int(w.tmdb_id or 0)
                sea = int(w.season or 1)
                clean_w = re.sub(r"[^\w\u4e00-\u9fa5]", "", str(w.title or ""))
                
                real_eps = inv_by_tmdb.get((tid, sea)) or inv_by_title.get((clean_w, sea)) or set()
                if real_eps:
                    norm_current = {
                        k for v in (w.collected_episodes or [])
                        if (k := canonical_episode_key(sea, v)) is not None
                    }
                    norm_real = {
                        k for v in real_eps
                        if (k := canonical_episode_key(sea, v)) is not None
                    }
                    merged = sorted(norm_current | norm_real)
                    if len(merged) != len(norm_current) or list(w.collected_episodes or []) != merged:
                        w.collected_episodes = merged
                        w.updated_at = datetime.now(UTC)
                        sync_count += 1
                
                if not w.remote_series_folder_id:
                    found_fid = folder_by_tmdb.get((tid, sea)) or folder_by_title.get((clean_w, sea))
                    if found_fid:
                        w.remote_series_folder_id = found_fid
                        w.remote_destination_kind = "ongoing"
                        w.updated_at = datetime.now(UTC)
                        folder_sync_count += 1

            logger.info("MasterInventoryScanner: Sync complete. Watchlists updated: %d, Folders linked: %d", sync_count, folder_sync_count)

        return {
            "success": True,
            "elapsed": elapsed,
            "shows_scanned": len(shows),
            "files_found": len(unique_items),
            "watchlists_updated": sync_count,
            "folders_linked": folder_sync_count,
        }
