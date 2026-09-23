"""Pure read-only Guangya share diagnostics.

The probe never invokes restore, directory creation, move, rename or deletion.
"""

from __future__ import annotations

from typing import Any

import httpx

from app.transfer.errors import GuangyaTransferError, TransferErrorCategory, classify_error


class GuangyaShareProbe:
    """Small adapter facade with a report contract safe for diagnostics and logs."""

    def __init__(self, *, adapter: Any, max_depth: int = 3, max_items: int = 500, max_pages: int = 50) -> None:
        self.adapter = adapter
        self.max_depth = max(0, max_depth)
        self.max_items = max(1, max_items)
        self.max_pages = max(1, max_pages)

    @staticmethod
    def _error_code(exc: BaseException) -> tuple[str, bool]:
        if isinstance(exc, GuangyaTransferError):
            if exc.category in {TransferErrorCategory.INVALID_SHARE, TransferErrorCategory.SHARE_NOT_FOUND}:
                return "INVALID_SHARE", True
            if exc.category in {TransferErrorCategory.AUTH_INVALID, TransferErrorCategory.AUTH_EXPIRED}:
                return "SHARE_PASSWORD_REQUIRED", False
            if exc.category == TransferErrorCategory.RATE_LIMITED:
                return "RATE_LIMITED", False
            if exc.category == TransferErrorCategory.NETWORK_TIMEOUT:
                return "NETWORK_TIMEOUT", False
            return "SHARE_API_ERROR", False
        category = classify_error(exc)
        if category in {TransferErrorCategory.INVALID_SHARE, TransferErrorCategory.SHARE_NOT_FOUND}:
            return "INVALID_SHARE", True
        if category == TransferErrorCategory.NETWORK_TIMEOUT:
            return "NETWORK_TIMEOUT", False
        if category == TransferErrorCategory.RATE_LIMITED:
            return "RATE_LIMITED", False
        if category in {TransferErrorCategory.AUTH_INVALID, TransferErrorCategory.AUTH_EXPIRED}:
            return "SHARE_PASSWORD_REQUIRED", False
        if isinstance(exc, (httpx.HTTPError, OSError)):
            return "SHARE_API_ERROR", False
        return "SHARE_API_ERROR", False

    async def probe(self, share_url: str) -> dict:
        """Read and classify exactly one public share; never persist or mutate it."""
        try:
            raw = await self.adapter.inspect_share(
                share_url=share_url,
                max_depth=self.max_depth,
                max_items=self.max_items,
                max_pages=self.max_pages,
            )
        except TypeError:
            # Compatibility with mocked/pre-Phase2E adapters, still read-only.
            try:
                raw = await self.adapter.inspect_share(share_url=share_url)
            except Exception as exc:  # noqa: BLE001 - classification must fail closed
                code, deterministic = self._error_code(exc)
                return self._failure(code, exc, deterministic)
        except Exception as exc:  # noqa: BLE001 - classification must fail closed
            code, deterministic = self._error_code(exc)
            return self._failure(code, exc, deterministic)

        if not raw.get("share_accessible", raw.get("share_readable", False)):
            return self._failure(str(raw.get("error_code") or "INVALID_SHARE"), None, True, raw)
        video_files = list(raw.get("video_files") or [])
        video_names = [str(item.get("name") if isinstance(item, dict) else item) for item in video_files]
        return {
            "share_accessible": True,
            "share_id": raw.get("share_id"),
            "items": int(raw.get("items") or len(raw.get("files") or []) + int(raw.get("directories") or 0)),
            "directories": int(raw.get("directories") or 0),
            "files": list(raw.get("files") or []),
            "video_files": video_files,
            "video_names": video_names,
            "video_count": len(video_names),
            "errors": list(raw.get("errors") or []),
            "error_code": "SHARE_VALID",
            "deterministic_failure": False,
            "truncated": bool(raw.get("truncated", False)),
        }

    @staticmethod
    def _failure(code: str, exc: BaseException | None, deterministic: bool, raw: dict | None = None) -> dict:
        detail = "" if exc is None else f"{type(exc).__name__}: {str(exc)[:240]}"
        return {
            "share_accessible": False,
            "share_id": (raw or {}).get("share_id"),
            "items": 0,
            "directories": 0,
            "files": [],
            "video_files": [],
            "video_names": [],
            "video_count": 0,
            "errors": [detail] if detail else list((raw or {}).get("errors") or []),
            "error_code": code,
            "deterministic_failure": deterministic,
            "truncated": False,
        }
