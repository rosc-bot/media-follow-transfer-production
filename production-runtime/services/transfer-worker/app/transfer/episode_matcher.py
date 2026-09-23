"""Deterministic, diagnostic-only matching of share video names to one episode."""

from __future__ import annotations

import re
from pathlib import PurePosixPath

from app.follow.episode_keys import canonical_episode_key, episode_sort_key
from app.transfer.guangya_auth import is_video_filename

_FULL_KEY = re.compile(r"(?i)\bS(?P<season>\d{1,3})[ ._-]*E(?P<episode>\d{1,4})\b")
_SHORT_EPISODE = re.compile(r"(?i)(?:\bEP?|第)\s*0*(?P<episode>\d{1,4})(?:\s*集)?\b")
_COLLECTION = re.compile(r"(?i)(全集|合集|complete|collection|all[ ._-]?episodes)")
_SPECIAL = re.compile(r"(?i)(?:\bSP\b|\bOVA\b|预告|花絮|\bPV\b)")


def extract_video_episode_keys(name: str, known_season: int | None = None) -> tuple[str, ...]:
    """Extract unambiguous regular episode keys from one video filename.

    This is the shared filename rule used by inventory reconciliation and the
    diagnostic matcher. Specials, collections, non-video files, and filenames
    containing no season/episode evidence return an empty tuple.
    """
    basename = PurePosixPath(str(name or "").replace("\\", "/")).name
    if not is_video_filename(basename) or _COLLECTION.search(basename) or _SPECIAL.search(basename):
        return ()
    keys: set[str] = set()
    for full in _FULL_KEY.finditer(basename):
        key = canonical_episode_key(
            int(full.group("season")),
            f"E{int(full.group('episode'))}",
        )
        if key is not None:
            keys.add(key)
    if not keys and known_season is not None:
        for short in _SHORT_EPISODE.finditer(basename):
            key = canonical_episode_key(known_season, f"E{int(short.group('episode'))}")
            if key is not None:
                keys.add(key)
    return tuple(sorted(keys, key=episode_sort_key))


def _target_parts(episode_key: str) -> tuple[int, int]:
    matched = _FULL_KEY.fullmatch(str(episode_key).strip())
    if not matched:
        raise ValueError(f"invalid target episode key: {episode_key!r}")
    return int(matched.group("season")), int(matched.group("episode"))


def diagnose_episode_match(
    *, target_episode_key: str, known_season: int | None, video_names: list[str]
) -> list[dict]:
    """Describe why each file does or does not match a target episode.

    Bare numbers remain deliberately unsupported. Short forms (``E101``,
    ``EP101``, ``第101集``) are accepted only with an externally confirmed season.
    """
    target_season, target_episode = _target_parts(target_episode_key)
    rows: list[dict] = []
    for raw_name in video_names:
        name = str(raw_name or "")
        basename = PurePosixPath(name.replace("\\", "/")).name
        row = {
            "name": name,
            "normalized_name": basename.casefold(),
            "detected_season": None,
            "detected_episode": None,
            "match_result": "REJECT",
            "reject_reason": "",
        }
        if not is_video_filename(basename):
            row["reject_reason"] = "NOT_VIDEO_FILE"
        elif _COLLECTION.search(basename):
            row["match_result"] = "REVIEW"
            row["reject_reason"] = "COLLECTION_NOT_SINGLE_EPISODE"
        elif _SPECIAL.search(basename):
            row["match_result"] = "REVIEW"
            row["reject_reason"] = "SPECIAL_NOT_REGULAR_EPISODE"
        elif full := _FULL_KEY.search(basename):
            row["detected_season"] = int(full.group("season"))
            row["detected_episode"] = int(full.group("episode"))
            if (row["detected_season"], row["detected_episode"]) == (target_season, target_episode):
                row["match_result"] = "MATCH"
                row["reject_reason"] = ""
            else:
                row["reject_reason"] = "FULL_KEY_MISMATCH"
        elif short := _SHORT_EPISODE.search(basename):
            row["detected_episode"] = int(short.group("episode"))
            if known_season is None:
                row["reject_reason"] = "SHORT_FORM_REQUIRES_CONFIRMED_SEASON"
            else:
                row["detected_season"] = int(known_season)
                if (int(known_season), row["detected_episode"]) == (target_season, target_episode):
                    row["match_result"] = "MATCH"
                    row["reject_reason"] = ""
                else:
                    row["reject_reason"] = "SHORT_FORM_EPISODE_MISMATCH"
        else:
            row["reject_reason"] = "BARE_NUMBER_UNSUPPORTED"
        rows.append(row)
    return rows


def has_episode_match(*, episode_key: str, season: int | None, video_names: list[str]) -> bool:
    return any(
        row["match_result"] == "MATCH"
        for row in diagnose_episode_match(
            target_episode_key=episode_key, known_season=season, video_names=video_names
        )
    )
