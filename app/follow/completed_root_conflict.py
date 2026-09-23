"""Direct-child-only completed-root conflict detection."""

from __future__ import annotations

import re
from typing import Any


class CompletedRootConflictScanner:
    """Never recursively scan the completed library to check one series."""

    @staticmethod
    def _name(item: dict[str, Any]) -> str:
        return str(item.get("name") or item.get("fileName") or "").strip()

    @staticmethod
    def _is_directory(item: dict[str, Any]) -> bool:
        return bool(
            item.get("is_dir")
            or item.get("isDir")
            or item.get("resType") == 2
            or str(item.get("type") or "").casefold() in {"dir", "directory", "folder"}
        )

    @staticmethod
    def _clean(value: str) -> str:
        value = re.sub(r"\{\s*tmdb(?:id)?[-:_= ]*\d+\s*\}", " ", value, flags=re.IGNORECASE)
        value = re.sub(r"(?:19|20)\d{2}", " ", value)
        value = value.replace("【完结】", " ")
        return re.sub(r"[^0-9A-Za-z\u4e00-\u9fff]+", "", value).casefold()

    @classmethod
    def inspect_direct_children(
        cls,
        items: list[dict[str, Any]],
        *,
        tmdb_id: int,
        title: str,
    ) -> dict[str, Any]:
        identity = f"tmdbid-{int(tmdb_id)}".casefold()
        clean_title = cls._clean(str(title or ""))
        matches: list[dict[str, str]] = []
        for item in items:
            if not cls._is_directory(item):
                continue
            name = cls._name(item)
            if identity in name.casefold() or (clean_title and clean_title == cls._clean(name)):
                matches.append({
                    "file_id": str(item.get("fileId") or item.get("id") or "").strip(),
                    "name": name,
                })
        return {
            "status": "VERIFIED",
            "conflict": bool(matches),
            "matched_direct_children": matches,
            "recursive_scan": False,
        }


__all__ = ["CompletedRootConflictScanner"]
