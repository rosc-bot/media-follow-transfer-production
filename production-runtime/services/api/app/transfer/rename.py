"""Conditional, selected-file-only media filename normalization.

The final Phase 2G.5 rule deliberately does *not* normalize every media file to
``SxxExx.ext``.  The standard formatters below are compatible with the retired
media-bot implementation's ``MediaFilenameNormalizer``: a verified canonical
Chinese title/identity is followed by the episode or movie identity and the
release specification that was present in the source filename.

The module is pure planning code except for the provider readback performed by
the adapter.  It never broadens a selection and never decides that an API
response alone is a verified rename.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from typing import Any

from app.transfer.normalization import canonical_file_name

_VIDEO_EXTENSIONS = frozenset({".mp4", ".mkv", ".mov", ".ts", ".m2ts", ".flv", ".avi", ".wmv"})
STANDARD_CHINESE_MOVIE_FORMAT = "{title} ({year}) {tmdbid}.{release_spec}{extension}"
STANDARD_CHINESE_EPISODE_FORMAT = "{title} ({year}) {tmdbid}.SxxExx.{release_spec}{extension}"
_ENDED_STATUSES = frozenset({"ended", "canceled", "cancelled"})
_GENERIC_NAMES = frozenset({
    "video", "video1", "output", "download", "downloaded", "file", "media", "movie", "episode",
    "正片", "视频", "影片", "电影", "资源", "未知", "untitled", "unnamed", "sample", "test",
})
_QUALITY_PATTERN = re.compile(
    r"(?i)(?:2160p|1080p|720p|576p|480p|4k|uhd|web[- .]?dl|webrip|web[- .]?rip|bluray|bdrip|hdtv|remux|hevc|h\.?265|h\.?264|x264|x265|av1|ddp(?:[ .]?5\.1)?|aac|atmos|truehd|flac|dts(?:-hd)?|hdr10\+?|hdr|dolby[ ._-]?vision|dv|10bit|edr|\d{2,3}fps|fps)",
)
_EPISODE_PATTERN = re.compile(
    r"(?i)(?<![A-Za-z0-9])S(?P<season>\d{1,2})[ ._\-–—]*E(?P<start>\d{1,4})(?:[ ._\-–—]*(?:-|~|到|至)[ ._\-–—]*(?:E|EP)?(?P<end>\d{1,4}))?",
)
_UUID_PATTERN = re.compile(r"(?i)^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$")
_HEX_HASH_PATTERN = re.compile(r"(?i)^[0-9a-f]{16,}$")


@dataclass(frozen=True)
class RenameOperation:
    file_id: str
    old_name: str
    new_name: str

    def as_dict(self) -> dict[str, str]:
        return {"file_id": self.file_id, "old_name": self.old_name, "new_name": self.new_name}


@dataclass(frozen=True)
class RenamePlan:
    status: str
    operations: tuple[RenameOperation, ...] = ()
    skipped: tuple[str, ...] = ()
    conflicts: tuple[dict[str, str], ...] = ()
    reason: str | None = None
    decision: str | None = None

    @property
    def rename_required(self) -> bool:
        return bool(self.operations)

    def as_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "decision": self.decision,
            "rename_decision": self.decision,
            "rename_required": self.rename_required,
            "operations": [item.as_dict() for item in self.operations],
            "skipped": list(self.skipped),
            "conflicts": list(self.conflicts),
            "reason": self.reason,
        }


def _record_id(record: dict[str, Any]) -> str:
    return str(record.get("fileId") or record.get("file_id") or record.get("id") or "").strip()


def _record_name(record: dict[str, Any]) -> str:
    return str(record.get("name") or record.get("fileName") or record.get("file_name") or "").strip()


def _basename_parts(filename: str) -> tuple[str, str]:
    basename = os.path.basename(str(filename or "").strip())
    stem, extension = os.path.splitext(basename)
    return stem, extension.lower()


def _clean_candidate(value: str) -> str:
    value = re.sub(r"\{\s*tmdb(?:id)?[-:_= ]*\d+\s*\}", " ", value, flags=re.IGNORECASE)
    value = re.sub(r"[._]+", " ", value)
    value = re.sub(r"\s+", " ", value)
    return value.strip(" ._-–—()[]【】")


def _media_title_candidate(filename: str, *, media_type: str) -> str:
    stem, _extension = _basename_parts(filename)
    stem = stem.replace("#追新转存", " ").strip()
    kind = str(media_type or "tv").casefold()
    episode = _EPISODE_PATTERN.search(stem) if kind not in {"movie", "电影"} else None
    if episode:
        return _clean_candidate(stem[: episode.start()])
    year = re.search(r"(?<!\d)(?:19|20)\d{2}(?!\d)", stem)
    quality = _QUALITY_PATTERN.search(stem)
    boundary = year.start() if year else (quality.start() if quality else len(stem))
    return _clean_candidate(stem[:boundary])


def _is_placeholder_candidate(candidate: str) -> bool:
    normalized = re.sub(r"[^0-9A-Za-z\u4e00-\u9fff]", "", candidate).casefold()
    if not normalized:
        return True
    if normalized in {re.sub(r"[^0-9A-Za-z\u4e00-\u9fff]", "", value).casefold() for value in _GENERIC_NAMES}:
        return True
    if re.fullmatch(r"\d{1,8}", normalized):
        return True
    if _UUID_PATTERN.fullmatch(normalized) or _HEX_HASH_PATTERN.fullmatch(normalized):
        return True
    if "hash" in normalized and re.search(r"\d", normalized):
        return True
    return re.fullmatch(r"(?:video|output|download|file|media|movie|episode)\d*", normalized) is not None


def has_meaningful_media_name(
    filename: str,
    *,
    media_type: str = "tv",
    title: str | None = None,
    aliases: list[str] | None = None,
) -> bool:
    """Return whether a filename carries a recognizable work/release title.

    This intentionally accepts English titles, romaji, aliases and ordinary
    release names.  It rejects only structural/placeholder names; the absence
    of CJK characters is not a rejection criterion.
    """

    stem, extension = _basename_parts(filename)
    if not stem or extension not in _VIDEO_EXTENSIONS:
        return False
    candidate = _media_title_candidate(filename, media_type=media_type)
    if _is_placeholder_candidate(candidate):
        return False
    if not re.search(r"[A-Za-z\u4e00-\u9fff]", candidate):
        return False
    # A caller-provided identity can make a short alias meaningful, but it
    # must not turn an obvious placeholder into a title.
    if title or aliases:
        expected = [str(title or "").strip(), *(str(value).strip() for value in aliases or [])]
        if any(value and _clean_candidate(value).casefold() in candidate.casefold() for value in expected):
            return True
    return True


def _release_spec(filename: str, *, media_type: str, episode_key: str | None = None) -> str:
    """Reuse the old release-suffix extraction semantics without importing the retired app."""

    stem, _extension = _basename_parts(filename)
    kind = str(media_type or "tv").casefold()
    if kind in {"movie", "电影"}:
        year = re.search(r"(?<!\d)(?:19|20)\d{2}(?!\d)", stem)
        quality = _QUALITY_PATTERN.search(stem)
        start = year.end() if year else (quality.start() if quality else len(stem))
    else:
        match = _EPISODE_PATTERN.search(stem)
        if match:
            start = match.end()
        elif episode_key:
            escaped = re.search(re.escape(str(episode_key)), stem, flags=re.IGNORECASE)
            start = escaped.end() if escaped else 0
        else:
            quality = _QUALITY_PATTERN.search(stem)
            start = quality.start() if quality else len(stem)
    suffix = stem[start:]
    suffix = re.sub(r"^[\s._\-–—()\[\]]+", "", suffix)
    suffix = re.sub(r"(?i)^(?:19|20)\d{2}[\s._\-–—]+", "", suffix)
    suffix = re.sub(r"(?i)\{\s*tmdb(?:id)?[-:_= ]*\d+\s*\}", "", suffix)
    suffix = re.sub(r"\s+", ".", suffix)
    suffix = re.sub(r"\.{2,}", ".", suffix).strip(".")
    if not _QUALITY_PATTERN.search(suffix):
        return ""
    return suffix


def _identity_prefix(title: str, year: int | None, tmdb_id: int | None) -> str:
    clean_title = str(title or "").strip()
    if not clean_title:
        raise ValueError("standard Chinese filename requires a verified title")
    year_part = f" ({int(year)})" if year is not None else ""
    tmdb_part = f" {{tmdbid-{int(tmdb_id)}}}" if tmdb_id is not None else ""
    return f"{clean_title}{year_part}{tmdb_part}"


def build_standard_chinese_movie_filename(
    *,
    title: str,
    year: int | None,
    tmdb_id: int | None,
    source_filename: str,
) -> str:
    """Legacy-compatible standard movie formatter."""

    _stem, extension = _basename_parts(source_filename)
    if not extension:
        extension = ".mp4"
    prefix = _identity_prefix(title, year, tmdb_id)
    spec = _release_spec(source_filename, media_type="movie")
    return f"{prefix}.{spec}{extension}" if spec else f"{prefix}{extension}"


def _episode_identity(episode_key: str | None, source_filename: str, season: int | None) -> str:
    raw = str(episode_key or "").strip()
    match = _EPISODE_PATTERN.search(raw)
    if match:
        season_no = int(match.group("season"))
        start = int(match.group("start"))
        end = match.group("end")
        return (
            f"S{season_no:02d}E{start:02d}-E{int(end):02d}"
            if end is not None
            else f"S{season_no:02d}E{start:02d}"
        )
    match = _EPISODE_PATTERN.search(str(source_filename or ""))
    if match:
        season_no = int(match.group("season"))
        start = int(match.group("start"))
        end = match.group("end")
        return (
            f"S{season_no:02d}E{start:02d}-E{int(end):02d}"
            if end is not None
            else f"S{season_no:02d}E{start:02d}"
        )
    if season and season > 0:
        raise ValueError("episode key is required for standard episode filename")
    raise ValueError("episode identity is required for standard episode filename")


def build_standard_chinese_episode_filename(
    *,
    title: str,
    year: int | None,
    tmdb_id: int | None,
    season: int | None,
    episode_key: str | None,
    source_filename: str,
) -> str:
    """Legacy-compatible standard TV/anime episode formatter."""

    _stem, extension = _basename_parts(source_filename)
    if not extension:
        extension = ".mp4"
    identity = _episode_identity(episode_key, source_filename, season)
    prefix = _identity_prefix(title, year, tmdb_id)
    spec = _release_spec(source_filename, media_type="tv", episode_key=identity)
    return f"{prefix}.{identity}.{spec}{extension}" if spec else f"{prefix}.{identity}{extension}"


def _content_complete(
    *,
    media_type: str,
    series_status: str | None,
    content_complete: bool | None,
    lifecycle_verified: bool,
    total_episodes: int | None,
    collected_episodes: list[Any] | None,
    inventory_count: int | None,
    cloud_count: int | None,
    active_transfer_count: int | None,
) -> bool:
    if str(media_type or "tv").casefold() in {"movie", "电影"}:
        return True
    if content_complete is not None:
        return bool(content_complete)
    if lifecycle_verified:
        return True
    if str(series_status or "").casefold() not in _ENDED_STATUSES:
        return False
    try:
        total = int(total_episodes or 0)
        collected = len({str(value).strip() for value in collected_episodes or [] if str(value).strip()})
        inventory = int(inventory_count or 0)
        cloud = int(cloud_count or 0)
        active = int(active_transfer_count or 0)
    except (TypeError, ValueError):
        return False
    return total > 0 and collected >= total and inventory >= total and cloud >= total and active == 0


def canonical_filename(
    original_name: str,
    *,
    title: str | None = None,
    year: int | None = None,
    season: int | None = None,
    episode_key: str | None = None,
    version_key: str | None = None,
    media_type: str = "tv",
    series_status: str | None = None,
    content_complete: bool | None = None,
) -> str:
    """Compatibility helper.

    With no verified identity metadata it retains the old whitespace/marker
    cleanup.  The production rename path always calls ``build_rename_plan``
    with an explicit identity and never uses this fallback to skip a rename.
    """

    if not title:
        del year, season, episode_key, version_key, media_type, series_status, content_complete
        return canonical_file_name(original_name)
    if str(media_type).casefold() in {"movie", "电影"}:
        return build_standard_chinese_movie_filename(
            title=title,
            year=year,
            tmdb_id=None,
            source_filename=original_name,
        )
    return build_standard_chinese_episode_filename(
        title=title,
        year=year,
        tmdb_id=None,
        season=season,
        episode_key=episode_key,
        source_filename=original_name,
    )


def build_rename_plan(
    verified_records: list[dict[str, Any]],
    *,
    selected_file_ids: list[str],
    destination_kind: str = "ongoing",
    lifecycle_verified: bool = False,
    title: str | None = None,
    year: int | None = None,
    tmdb_id: int | None = None,
    season: int | None = None,
    episode_key: str | None = None,
    version_key: str | None = None,
    media_type: str = "tv",
    series_status: str | None = None,
    content_complete: bool | None = None,
    total_episodes: int | None = None,
    collected_episodes: list[Any] | None = None,
    inventory_count: int | None = None,
    cloud_count: int | None = None,
    active_transfer_count: int | None = None,
    aliases: list[str] | None = None,
) -> RenamePlan:
    """Plan a conditional rename only for selected files in verified readback."""

    del version_key  # retained in the call contract for queue compatibility
    selected = {str(value).strip() for value in selected_file_ids if str(value).strip()}
    records = [
        {"file_id": _record_id(record), "name": _record_name(record)}
        for record in verified_records
        if _record_id(record) and _record_name(record)
    ]
    if not selected:
        return RenamePlan("RENAME_UNVERIFIED", reason="SELECTED_FILE_IDS_REQUIRED")
    selected_records = [record for record in records if record["file_id"] in selected]
    if len(selected_records) != len(selected):
        return RenamePlan("RENAME_UNVERIFIED", reason="SELECTED_FILE_NOT_IN_READBACK")
    if not title:
        return RenamePlan("RENAME_UNVERIFIED", reason="VERIFIED_TITLE_REQUIRED")

    complete = _content_complete(
        media_type=media_type,
        series_status=series_status,
        content_complete=content_complete,
        lifecycle_verified=lifecycle_verified and destination_kind == "completed",
        total_episodes=total_episodes,
        collected_episodes=collected_episodes,
        inventory_count=inventory_count,
        cloud_count=cloud_count,
        active_transfer_count=active_transfer_count,
    )
    is_movie = str(media_type or "tv").casefold() in {"movie", "电影"}
    selected_targets: dict[str, str] = {}
    operations: list[RenameOperation] = []
    skipped: list[str] = []
    decision_by_id: dict[str, str] = {}

    for record in selected_records:
        old_name = record["name"]
        meaningful = has_meaningful_media_name(
            old_name,
            media_type="movie" if is_movie else "tv",
            title=title,
            aliases=aliases,
        )
        keep = is_movie or complete
        if keep and meaningful:
            decision_by_id[record["file_id"]] = "KEEP"
            skipped.append(record["file_id"])
            continue
        decision_by_id[record["file_id"]] = "RENAME_STANDARD_CHINESE"
        if is_movie:
            target = build_standard_chinese_movie_filename(
                title=title,
                year=year,
                tmdb_id=tmdb_id,
                source_filename=old_name,
            )
        else:
            target = build_standard_chinese_episode_filename(
                title=title,
                year=year,
                tmdb_id=tmdb_id,
                season=season,
                episode_key=episode_key,
                source_filename=old_name,
            )
        selected_targets[record["file_id"]] = target
        if target != old_name:
            operations.append(RenameOperation(record["file_id"], old_name, target))
        else:
            skipped.append(record["file_id"])

    decisions = set(decision_by_id.values())
    decision = "KEEP" if decisions == {"KEEP"} else "RENAME_STANDARD_CHINESE"
    names_by_id = {record["file_id"]: record["name"] for record in records}
    conflicts: list[dict[str, str]] = []
    for file_id, target in selected_targets.items():
        old_name = names_by_id[file_id]
        for other_id, other_name in names_by_id.items():
            if other_id != file_id and other_name == target:
                conflicts.append({
                    "file_id": file_id,
                    "old_name": old_name,
                    "new_name": target,
                    "conflicting_file_id": other_id,
                })
        for other_id, other_target in selected_targets.items():
            if other_id != file_id and other_target == target:
                conflicts.append({
                    "file_id": file_id,
                    "old_name": old_name,
                    "new_name": target,
                    "conflicting_file_id": "selected-set",
                })
    if conflicts:
        return RenamePlan(
            "RENAME_CONFLICT",
            conflicts=tuple(conflicts),
            reason="TARGET_NAME_EXISTS",
            decision=decision,
        )
    if not operations:
        status = "RENAME_SKIPPED_KEEP_EXISTING_NAME" if decision == "KEEP" else "RENAME_SKIPPED_STANDARD_CHINESE"
        return RenamePlan(status, skipped=tuple(skipped), reason="NAMES_ALREADY_COMPLIANT", decision=decision)
    return RenamePlan("RENAME_READY", operations=tuple(operations), skipped=tuple(skipped), decision=decision)


__all__ = [
    "STANDARD_CHINESE_EPISODE_FORMAT",
    "STANDARD_CHINESE_MOVIE_FORMAT",
    "RenameOperation",
    "RenamePlan",
    "build_rename_plan",
    "build_standard_chinese_episode_filename",
    "build_standard_chinese_movie_filename",
    "canonical_filename",
    "has_meaningful_media_name",
]
