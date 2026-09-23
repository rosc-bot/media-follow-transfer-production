"""Idempotent cloud-inventory synchronization.

The inventory table is a logical episode ledger, not a second provider API.
Only a provider readback that has already been verified may call
``upsert_verified_transfer``.  Reconciliation scans use the same upsert
boundary after producing a read-only plan.

The current schema deliberately remains the source of truth for persistence:
``title``, ``clean_title``, ``tmdb_id``, ``season``, ``episode``, ``file_name``,
``rel_path`` and ``updated_at``.  Provider-only metadata such as remote file
IDs, provider names, and scan timestamps is accepted for diagnostics but is
not smuggled into an unrelated column.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.follow.episode_keys import canonical_episode_key, episode_number, episode_sort_key
from app.ingest.media_identity import clean_title as clean_inventory_title
from app.models.cloud import CloudDiskInventory
from app.transfer.episode_matcher import extract_video_episode_keys
from app.transfer.guangya_auth import is_video_filename


class InventorySyncError(RuntimeError):
    """Raised when a verified inventory write cannot be completed safely."""


@dataclass(frozen=True)
class InventoryEntry:
    """One proposed logical inventory row.

    ``remote_file_id`` and the provenance fields are intentionally kept in the
    in-memory observation.  The deployed table does not have corresponding
    columns, so ``as_dict`` reports presence rather than leaking provider IDs.
    """

    tmdb_id: int
    title: str
    season: int
    episode: int
    episode_key: str
    file_name: str
    rel_path: str | None = None
    remote_file_id: str | None = None
    remote_folder_id: str | None = None
    provider: str | None = None
    source: str | None = None
    verified_at: datetime | None = None
    scanned_at: datetime | None = None

    def as_dict(self) -> dict[str, Any]:
        data: dict[str, Any] = {
            "tmdb_id": self.tmdb_id,
            "title": self.title,
            "season": self.season,
            "episode": self.episode,
            "episode_key": self.episode_key,
            "file_name": self.file_name,
            "rel_path": self.rel_path,
        }
        if self.remote_file_id:
            data["remote_file_id_present"] = True
        if self.remote_folder_id:
            data["remote_folder_id_present"] = True
        if self.provider:
            data["provider"] = self.provider
        if self.source:
            data["source"] = self.source
        return data


@dataclass(frozen=True)
class InventoryUpsertResult:
    status: str
    persisted: bool
    row_id: int | None = None
    episode_key: str | None = None
    reason: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "persisted": self.persisted,
            "row_id": self.row_id,
            "episode_key": self.episode_key,
            "reason": self.reason,
        }


@dataclass
class InventoryScanPlan:
    tmdb_id: int
    season: int
    title: str
    provider: str | None = None
    source: str = "cloud_reconciliation_scan"
    existing_db: list[dict[str, Any]] = field(default_factory=list)
    cloud_observed: list[InventoryEntry] = field(default_factory=list)
    to_insert: list[InventoryEntry] = field(default_factory=list)
    to_update: list[InventoryEntry] = field(default_factory=list)
    unchanged: list[InventoryEntry] = field(default_factory=list)
    ambiguous: list[dict[str, Any]] = field(default_factory=list)
    unparsed: list[dict[str, Any]] = field(default_factory=list)
    ignored_non_video: int = 0

    @property
    def counts(self) -> dict[str, int]:
        return {
            "existing_db": len(self.existing_db),
            "cloud_observed": len(self.cloud_observed),
            "to_insert": len(self.to_insert),
            "to_update": len(self.to_update),
            "unchanged": len(self.unchanged),
            "ambiguous": len(self.ambiguous),
            "unparsed": len(self.unparsed),
            "ignored_non_video": self.ignored_non_video,
        }

    def as_dict(self) -> dict[str, Any]:
        return {
            "tmdb_id": self.tmdb_id,
            "season": self.season,
            "title": self.title,
            "provider": self.provider,
            "source": self.source,
            "existing_db": self.existing_db,
            "cloud_observed": [entry.as_dict() for entry in self.cloud_observed],
            "to_insert": [entry.as_dict() for entry in self.to_insert],
            "to_update": [entry.as_dict() for entry in self.to_update],
            "unchanged": [entry.as_dict() for entry in self.unchanged],
            "ambiguous": self.ambiguous,
            "unparsed": self.unparsed,
            "counts": self.counts,
        }


class CloudInventoryService:
    """Shared verified-transfer and read-only reconciliation boundary."""

    PERSISTED_FIELDS = frozenset({
        "title", "clean_title", "tmdb_id", "season", "episode", "file_name", "rel_path", "updated_at",
    })
    UNSUPPORTED_OBSERVATION_FIELDS = frozenset({
        "remote_file_id", "remote_folder_id", "provider", "source", "verified_at", "scanned_at",
    })

    @staticmethod
    def _clean_title(title: str, tmdb_id: int) -> str:
        value = clean_inventory_title(str(title or "")).strip()
        return value or str(tmdb_id)

    @staticmethod
    def _remote_name(item: object) -> str:
        if isinstance(item, str):
            return item.strip()
        if isinstance(item, dict):
            return str(item.get("name") or item.get("fileName") or item.get("file_name") or "").strip()
        return ""

    @staticmethod
    def _remote_id(item: object) -> str | None:
        if not isinstance(item, dict):
            return None
        value = item.get("fileId") or item.get("file_id") or item.get("id")
        return str(value).strip() if value is not None and str(value).strip() else None

    @staticmethod
    def _row_dict(row: CloudDiskInventory) -> dict[str, Any]:
        key = f"S{int(row.season):02d}E{int(row.episode):02d}"
        return {
            "id": row.id,
            "tmdb_id": row.tmdb_id,
            "title": row.title,
            "clean_title": row.clean_title,
            "season": row.season,
            "episode": row.episode,
            "episode_key": key,
            "file_name": row.file_name,
            "rel_path": row.rel_path,
            "updated_at": row.updated_at.isoformat() if row.updated_at else None,
        }

    @classmethod
    async def _find_identity_rows(
        cls,
        db: AsyncSession,
        *,
        tmdb_id: int,
        season: int,
        episode: int,
    ) -> list[CloudDiskInventory]:
        rows = list((await db.scalars(
            select(CloudDiskInventory).where(
                CloudDiskInventory.tmdb_id == tmdb_id,
                CloudDiskInventory.season == season,
                CloudDiskInventory.episode == episode,
            ).order_by(CloudDiskInventory.id.asc())
        )).all())
        if len(rows) > 1:
            raise InventorySyncError(
                f"ambiguous inventory identity tmdb_id={tmdb_id} season={season} episode={episode} rows={len(rows)}"
            )
        return rows

    @classmethod
    async def upsert_verified_transfer(
        cls,
        db: AsyncSession,
        *,
        tmdb_id: int | None,
        title: str | None,
        season: int | None,
        file_name: str | None,
        episode_key: str | None = None,
        verified: bool,
        rel_path: str | None = None,
        remote_file_id: str | None = None,
        remote_folder_id: str | None = None,
        provider: str | None = None,
        source: str | None = None,
        verified_at: datetime | None = None,
        scanned_at: datetime | None = None,
    ) -> InventoryUpsertResult:
        """Upsert one verified file, or explicitly refuse to write.

        This method never calls a provider.  ``verified=False`` returns before
        any database query that could lead to a write, which makes the
        readback-failure fence easy to test and audit.
        """
        if not verified:
            return InventoryUpsertResult("SKIPPED_UNVERIFIED", False, reason="READBACK_NOT_VERIFIED")
        if tmdb_id is None or int(tmdb_id) <= 0:
            return InventoryUpsertResult("SKIPPED_INVALID_IDENTITY", False, reason="TMDB_ID_REQUIRED")
        if season is None or int(season) <= 0:
            return InventoryUpsertResult("SKIPPED_INVALID_IDENTITY", False, reason="SEASON_REQUIRED")
        name = str(file_name or "").strip()
        if not name or not is_video_filename(name):
            return InventoryUpsertResult("SKIPPED_UNPARSED", False, reason="VIDEO_FILENAME_REQUIRED")

        resolved_season = int(season)
        key = canonical_episode_key(resolved_season, episode_key) if episode_key else None
        if key is None:
            parsed = extract_video_episode_keys(name, known_season=resolved_season)
            if len(parsed) != 1:
                return InventoryUpsertResult("SKIPPED_UNPARSED", False, reason="EPISODE_KEY_NOT_UNAMBIGUOUS")
            key = parsed[0]
        key_season = int(key[1:3])
        number = episode_number(key)
        if number is None or key_season != resolved_season:
            return InventoryUpsertResult("SKIPPED_INVALID_IDENTITY", False, reason="EPISODE_SEASON_MISMATCH")

        entry = InventoryEntry(
            tmdb_id=int(tmdb_id),
            title=str(title or "").strip() or str(tmdb_id),
            season=resolved_season,
            episode=number,
            episode_key=key,
            file_name=name,
            rel_path=rel_path,
            remote_file_id=remote_file_id,
            remote_folder_id=remote_folder_id,
            provider=provider,
            source=source,
            verified_at=verified_at or datetime.now(UTC),
            scanned_at=scanned_at,
        )
        rows = await cls._find_identity_rows(
            db,
            tmdb_id=entry.tmdb_id,
            season=entry.season,
            episode=entry.episode,
        )
        clean = cls._clean_title(entry.title, entry.tmdb_id)
        if not rows:
            row = CloudDiskInventory(
                title=entry.title,
                clean_title=clean,
                tmdb_id=entry.tmdb_id,
                season=entry.season,
                episode=entry.episode,
                file_name=entry.file_name,
                rel_path=entry.rel_path,
                updated_at=datetime.now(UTC),
            )
            db.add(row)
            try:
                await db.flush()
            except Exception as exc:
                raise InventorySyncError(f"inventory insert failed: {type(exc).__name__}") from exc
            return InventoryUpsertResult("INSERTED", True, row_id=row.id, episode_key=entry.episode_key)

        row = rows[0]
        changed = any((
            row.title != entry.title,
            row.clean_title != clean,
            row.file_name != entry.file_name,
            entry.rel_path is not None and row.rel_path != entry.rel_path,
        ))
        if changed:
            row.title = entry.title
            row.clean_title = clean
            row.file_name = entry.file_name
            if entry.rel_path is not None:
                row.rel_path = entry.rel_path
            row.updated_at = datetime.now(UTC)
            try:
                await db.flush()
            except Exception as exc:
                raise InventorySyncError(f"inventory update failed: {type(exc).__name__}") from exc
            return InventoryUpsertResult("UPDATED", True, row_id=row.id, episode_key=entry.episode_key)
        return InventoryUpsertResult("UNCHANGED", True, row_id=row.id, episode_key=entry.episode_key)

    @classmethod
    async def update_rel_paths_after_promotion(
        cls,
        db: AsyncSession,
        *,
        tmdb_id: int,
        series_folder_name: str,
        completed_root_name: str | None = None,
        destination_prefix: str | None = None,
        relevant_seasons: list[int] | tuple[int, ...] | set[int] | None = None,
    ) -> dict[str, int]:
        """Move logical inventory paths to completed without dropping category layers."""

        rows = list((await db.scalars(select(CloudDiskInventory).where(
            CloudDiskInventory.tmdb_id == int(tmdb_id),
        ))).all())
        prefix = str(destination_prefix or series_folder_name or '').strip('/')
        if completed_root_name:
            prefix = f"{str(completed_root_name).strip('/')}/{prefix}".strip('/')
        layout_seasons = None if relevant_seasons is None else {
            int(value) for value in relevant_seasons if int(value) > 0
        }
        changed = 0
        unchanged = 0
        for row in rows:
            season_name = f"S{int(row.season):02d}" if layout_seasons is None or len(layout_seasons) > 1 else None
            final_path = "/".join(
                part for part in (prefix, season_name, str(row.file_name).strip()) if part
            )
            if row.rel_path == final_path:
                unchanged += 1
                continue
            row.rel_path = final_path
            row.updated_at = datetime.now(UTC)
            changed += 1
        await db.flush()
        return {"changed": changed, "unchanged": unchanged, "rows": len(rows)}

    @classmethod
    async def build_reconciliation_plan(
        cls,
        db: AsyncSession,
        *,
        tmdb_id: int,
        season: int,
        title: str,
        cloud_items: list[Any],
        provider: str | None = None,
        rel_path_prefix: str | None = None,
        source: str = "cloud_reconciliation_scan",
    ) -> InventoryScanPlan:
        """Compare a verified read-only provider listing with DB inventory."""
        if int(tmdb_id) <= 0 or int(season) <= 0:
            raise InventorySyncError("tmdb_id and season must be positive")
        existing_rows = list((await db.scalars(
            select(CloudDiskInventory).where(
                CloudDiskInventory.tmdb_id == int(tmdb_id),
                CloudDiskInventory.season == int(season),
            ).order_by(CloudDiskInventory.episode.asc(), CloudDiskInventory.id.asc())
        )).all())
        plan = InventoryScanPlan(
            tmdb_id=int(tmdb_id),
            season=int(season),
            title=str(title or "").strip() or str(tmdb_id),
            provider=provider,
            source=source,
            existing_db=[cls._row_dict(row) for row in existing_rows],
        )

        by_episode: dict[int, InventoryEntry] = {}
        duplicate_signatures: set[tuple[int, str]] = set()
        for item in cloud_items:
            name = cls._remote_name(item)
            if not name:
                plan.unparsed.append({"file_name": "", "reason": "MISSING_NAME"})
                continue
            if not is_video_filename(name):
                plan.ignored_non_video += 1
                continue
            keys = extract_video_episode_keys(name, known_season=int(season))
            if len(keys) != 1:
                plan.unparsed.append({
                    "file_name": name,
                    "reason": "EPISODE_KEY_NOT_UNAMBIGUOUS",
                })
                continue
            key = keys[0]
            number = episode_number(key)
            if number is None or int(key[1:3]) != int(season):
                plan.unparsed.append({"file_name": name, "reason": "SEASON_MISMATCH"})
                continue
            remote_id = cls._remote_id(item)
            signature = (number, remote_id or name.casefold())
            if signature in duplicate_signatures:
                continue
            duplicate_signatures.add(signature)
            entry = InventoryEntry(
                tmdb_id=int(tmdb_id),
                title=plan.title,
                season=int(season),
                episode=number,
                episode_key=key,
                file_name=name,
                rel_path=(
                    f"{str(rel_path_prefix).strip('/')}/{name}"
                    if rel_path_prefix and str(rel_path_prefix).strip('/')
                    else None
                ),
                remote_file_id=remote_id,
                provider=provider,
                source=source,
                scanned_at=datetime.now(UTC),
            )
            prior = by_episode.get(number)
            if prior is not None and (prior.file_name != entry.file_name or prior.remote_file_id != entry.remote_file_id):
                plan.ambiguous.append({
                    "episode_key": key,
                    "reason": "MULTIPLE_FILES_SAME_EPISODE",
                    "file_names": sorted({prior.file_name, entry.file_name}),
                })
                by_episode.pop(number, None)
                continue
            by_episode[number] = entry

        ambiguous_keys = {
            item.get("episode_key")
            for item in plan.ambiguous
            if item.get("episode_key")
        }
        plan.cloud_observed = [
            entry for entry in sorted(by_episode.values(), key=lambda value: episode_sort_key(value.episode_key))
            if entry.episode_key not in ambiguous_keys
        ]
        existing_by_episode = {int(row.episode): row for row in existing_rows}
        for entry in plan.cloud_observed:
            current = existing_by_episode.get(entry.episode)
            if current is None:
                plan.to_insert.append(entry)
                continue
            clean = cls._clean_title(entry.title, entry.tmdb_id)
            if (
                current.title != entry.title
                or current.clean_title != clean
                or current.file_name != entry.file_name
                or (entry.rel_path is not None and current.rel_path != entry.rel_path)
            ):
                plan.to_update.append(entry)
            else:
                plan.unchanged.append(entry)
        return plan

    @classmethod
    async def apply_reconciliation_plan(
        cls,
        db: AsyncSession,
        plan: InventoryScanPlan,
    ) -> list[InventoryUpsertResult]:
        """Apply only the plan's deterministic cloud observations to PostgreSQL."""
        results: list[InventoryUpsertResult] = []
        for entry in plan.cloud_observed:
            results.append(await cls.upsert_verified_transfer(
                db,
                tmdb_id=entry.tmdb_id,
                title=entry.title,
                season=entry.season,
                episode_key=entry.episode_key,
                file_name=entry.file_name,
                verified=True,
                rel_path=entry.rel_path,
                remote_file_id=entry.remote_file_id,
                remote_folder_id=entry.remote_folder_id,
                provider=entry.provider,
                source=entry.source,
                scanned_at=entry.scanned_at,
            ))
        return results