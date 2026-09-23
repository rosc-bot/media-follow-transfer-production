"""Read-only physical cloud inventory scanning for promotion gates.

The scanner is intentionally provider-neutral.  A caller supplies a paginated,
read-only ``list_page(parent_id, page, page_size)`` callback.  It never invokes
restore, move, rename, delete or create APIs.  Any timeout, API failure,
pagination ambiguity or traversal limit returns an unverified result so a
promotion cannot be armed from partial evidence.
"""

from __future__ import annotations

import asyncio
import hashlib
import inspect
import json
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from app.transfer.episode_matcher import extract_video_episode_keys
from app.transfer.guangya_auth import is_video_filename

ListPage = Callable[[str, int, int], Awaitable[Any] | Any]


@dataclass(frozen=True)
class PhysicalCloudScanResult:
    tmdb_id: int
    series_root_id: str
    relevant_seasons: tuple[int, ...]
    cloud_episode_keys_by_season: dict[int, tuple[str, ...]]
    file_count: int
    scan_status: str
    scan_timestamp: str
    scan_watermark: str | None
    file_sizes_by_name: dict[str, int] = field(default_factory=dict)
    pages_read: int = 0
    items_read: int = 0
    unparsed_video_count: int = 0
    error: str | None = None
    from_cache: bool = False
    _observed_files: tuple[dict[str, str], ...] = field(default_factory=tuple, repr=False)

    @property
    def verified(self) -> bool:
        return self.scan_status == "VERIFIED"

    def as_dict(self) -> dict[str, Any]:
        return {
            "tmdb_id": self.tmdb_id,
            "series_root_id": self.series_root_id,
            "relevant_seasons": list(self.relevant_seasons),
            "cloud_episode_keys_by_season": {
                str(season): list(keys) for season, keys in self.cloud_episode_keys_by_season.items()
            },
            "file_count": self.file_count,
            "scan_status": self.scan_status,
            "scan_timestamp": self.scan_timestamp,
            "scan_watermark": self.scan_watermark,
            "file_sizes_by_name": dict(self.file_sizes_by_name),
            "pages_read": self.pages_read,
            "items_read": self.items_read,
            "unparsed_video_count": self.unparsed_video_count,
            "error": self.error,
            "from_cache": self.from_cache,
            "verified_files": [dict(item) for item in self._observed_files] if self.verified else [],
        }


class PhysicalCloudInventoryScanner:
    """Bounded, cached, rate-limited, fail-closed recursive scanner."""

    def __init__(
        self,
        list_page: ListPage,
        *,
        timeout_seconds: float = 30.0,
        max_depth: int = 6,
        max_items: int = 5000,
        page_size: int = 100,
        max_pages_per_directory: int = 100,
        cache_ttl_seconds: float = 900.0,
        rate_limit_seconds: float = 0.05,
    ) -> None:
        self.list_page = list_page
        self.timeout_seconds = max(0.1, float(timeout_seconds))
        self.max_depth = max(0, int(max_depth))
        self.max_items = max(1, int(max_items))
        self.page_size = max(1, int(page_size))
        self.max_pages_per_directory = max(1, int(max_pages_per_directory))
        self.cache_ttl_seconds = max(0.0, float(cache_ttl_seconds))
        self.rate_limit_seconds = max(0.0, float(rate_limit_seconds))
        self._cache: dict[tuple[int, str, tuple[int, ...]], tuple[float, PhysicalCloudScanResult]] = {}

    @staticmethod
    def _is_directory(item: dict[str, Any]) -> bool:
        return bool(
            item.get("is_dir")
            or item.get("isDir")
            or item.get("directory")
            or item.get("resType") == 2
            or str(item.get("type") or "").casefold() in {"dir", "directory", "folder"}
        )

    @staticmethod
    def _item_name(item: dict[str, Any]) -> str:
        return str(item.get("name") or item.get("fileName") or item.get("file_name") or "").strip()

    @staticmethod
    def _item_id(item: dict[str, Any]) -> str:
        return str(item.get("fileId") or item.get("file_id") or item.get("id") or "").strip()

    @classmethod
    def _page_parts(cls, raw: Any) -> tuple[list[dict[str, Any]], bool | None]:
        if isinstance(raw, list):
            return [dict(item) for item in raw if isinstance(item, dict)], None
        if not isinstance(raw, dict):
            raise TypeError("cloud listing page must be a list or object")
        data = raw.get("data") if isinstance(raw.get("data"), dict) else raw
        items = data.get("items") or data.get("list") or data.get("files") or []
        items = [dict(item) for item in items if isinstance(item, dict)] if isinstance(items, list) else []
        for key in ("has_more", "hasMore", "more"):
            if key in data:
                return items, str(data[key]).casefold() in {"1", "true", "yes"}
        for key in ("total_pages", "totalPage", "totalPages", "pageCount"):
            if key in data:
                try:
                    return items, int(data[key]) > int(data.get("page") or 0) + 1
                except (TypeError, ValueError):
                    return items, None
        if "total" in data or "totalCount" in data:
            try:
                total = int(data.get("total") or data.get("totalCount"))
                page = int(data.get("page") or data.get("pageNum") or 0)
                return items, (page + 1) * int(data.get("page_size") or data.get("pageSize") or 1) < total
            except (TypeError, ValueError):
                return items, None
        return items, None

    async def _call_page(self, parent_id: str, page: int) -> Any:
        raw = self.list_page(parent_id, page, self.page_size)
        if inspect.isawaitable(raw):
            return await asyncio.wait_for(raw, timeout=self.timeout_seconds)
        return raw

    @staticmethod
    def _watermark(tmdb_id: int, root_id: str, files: list[dict[str, str]], timestamp: str) -> str:
        payload = {
            "tmdb_id": int(tmdb_id),
            "root_id": root_id,
            "timestamp": timestamp,
            "files": sorted(files, key=lambda item: (item.get("season", ""), item.get("episode_key", ""), item.get("name", ""), item.get("file_id", ""))),
        }
        digest = hashlib.sha256(json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")).hexdigest()
        return f"physical:{int(tmdb_id)}:{digest}"

    async def scan(
        self,
        *,
        tmdb_id: int,
        series_root_id: str,
        relevant_seasons: list[int] | tuple[int, ...] | set[int],
        force_refresh: bool = False,
    ) -> PhysicalCloudScanResult:
        seasons = tuple(sorted({int(value) for value in relevant_seasons if int(value) > 0}))
        root_id = str(series_root_id or "").strip()
        cache_key = (int(tmdb_id), root_id, seasons)
        now = datetime.now(UTC)
        now_epoch = now.timestamp()
        cached = self._cache.get(cache_key)
        if not force_refresh and cached and now_epoch - cached[0] <= self.cache_ttl_seconds:
            previous = cached[1]
            return PhysicalCloudScanResult(
                **{**previous.__dict__, "from_cache": True}
            )
        timestamp = now.isoformat()
        if int(tmdb_id) <= 0 or not root_id or not seasons:
            result = PhysicalCloudScanResult(
                int(tmdb_id), root_id, seasons, {}, 0, "API_ERROR", timestamp, None, error="IDENTITY_REQUIRED"
            )
            self._cache[cache_key] = (now_epoch, result)
            return result

        queue: list[tuple[str, int, str]] = [(root_id, 0, "")]
        visited: set[str] = set()
        files: list[dict[str, str]] = []
        file_sizes_by_name: dict[str, int] = {}
        pages_read = 0
        items_read = 0
        unparsed = 0
        status = "VERIFIED"
        error: str | None = None
        try:
            while queue:
                parent_id, depth, relative = queue.pop(0)
                if parent_id in visited:
                    continue
                visited.add(parent_id)
                if depth > self.max_depth:
                    status = "LIMIT_UNVERIFIED"
                    error = "MAX_DEPTH_REACHED"
                    break
                page = 0
                while True:
                    if pages_read >= self.max_pages_per_directory * max(1, len(visited)):
                        status = "LIMIT_UNVERIFIED"
                        error = "MAX_PAGES_REACHED"
                        break
                    raw = await self._call_page(parent_id, page)
                    page_items, has_more = self._page_parts(raw)
                    pages_read += 1
                    items_read += len(page_items)
                    if items_read > self.max_items:
                        status = "LIMIT_UNVERIFIED"
                        error = "MAX_ITEMS_REACHED"
                        break
                    for item in page_items:
                        name = self._item_name(item)
                        if not name:
                            continue
                        item_id = self._item_id(item)
                        child_path = f"{relative}/{name}".strip("/")
                        if self._is_directory(item):
                            if depth >= self.max_depth:
                                status = "LIMIT_UNVERIFIED"
                                error = "MAX_DEPTH_REACHED"
                            elif item_id:
                                queue.append((item_id, depth + 1, child_path))
                            continue
                        if not is_video_filename(name):
                            continue
                        keys = extract_video_episode_keys(name)
                        if len(keys) != 1:
                            unparsed += 1
                            continue
                        key = keys[0]
                        season = int(key[1:3])
                        if season not in seasons:
                            continue
                        try:
                            file_sizes_by_name[name] = int(
                                item.get("size")
                                or item.get("fileSize")
                                or item.get("sizeBytes")
                                or item.get("bytes")
                                or item.get("file_size")
                                or 0
                            )
                        except (TypeError, ValueError):
                            file_sizes_by_name[name] = 0
                        files.append({
                            "season": f"S{season:02d}",
                            "episode_key": key,
                            "name": name,
                            "file_id": item_id,
                            "path": child_path,
                        })
                    if status != "VERIFIED":
                        break
                    if has_more is True:
                        page += 1
                        if self.rate_limit_seconds:
                            await asyncio.sleep(self.rate_limit_seconds)
                        continue
                    if has_more is None and len(page_items) >= self.page_size:
                        status = "PAGINATION_INCOMPLETE"
                        error = "FULL_PAGE_WITHOUT_PAGINATION_PROOF"
                    break
                if status != "VERIFIED":
                    break
                if self.rate_limit_seconds and queue:
                    await asyncio.sleep(self.rate_limit_seconds)
        except TimeoutError:
            status = "TIMEOUT_UNVERIFIED"
            error = "LIST_PAGE_TIMEOUT"
        except Exception as exc:  # noqa: BLE001 - scanner must fail closed
            status = "API_ERROR"
            error = type(exc).__name__

        if unparsed and status == "VERIFIED":
            status = "UNPARSED_UNVERIFIED"
            error = "VIDEO_EPISODE_KEY_UNPARSED"
        by_season: dict[int, tuple[str, ...]] = {
            season: tuple(sorted({item["episode_key"] for item in files if item["season"] == f"S{season:02d}"}))
            for season in seasons
        }
        watermark = self._watermark(int(tmdb_id), root_id, files, timestamp) if status == "VERIFIED" else None
        result = PhysicalCloudScanResult(
            tmdb_id=int(tmdb_id),
            series_root_id=root_id,
            relevant_seasons=seasons,
            cloud_episode_keys_by_season=by_season,
            file_count=len(files),
            scan_status=status,
            scan_timestamp=timestamp,
            scan_watermark=watermark,
            file_sizes_by_name=file_sizes_by_name,
            pages_read=pages_read,
            items_read=items_read,
            unparsed_video_count=unparsed,
            error=error,
            _observed_files=tuple(files),
        )
        self._cache[cache_key] = (now_epoch, result)
        return result


__all__ = ["PhysicalCloudInventoryScanner", "PhysicalCloudScanResult"]
