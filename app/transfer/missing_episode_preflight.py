"""Pure fail-closed planner for restoring only absent TV episodes from a share."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from app.follow.episode_keys import canonical_episode_key
from app.transfer.episode_matcher import extract_video_episode_keys
from app.transfer.file_selection import SelectionMode, select_files

AUTO_SAFE = "AUTO_SAFE"
NEEDS_REVIEW = "NEEDS_REVIEW"
REJECTED = "REJECTED"


@dataclass(frozen=True)
class MissingEpisodePlan:
    classification: str
    reason: str
    season: int
    share_episode_keys: tuple[str, ...] = ()
    missing_episode_keys: tuple[str, ...] = ()
    episode_file_map: dict[str, str] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {
            "classification": self.classification,
            "reason": self.reason,
            "season": self.season,
            "share_episode_keys": list(self.share_episode_keys),
            "missing_episode_keys": list(self.missing_episode_keys),
            "episode_file_map": dict(self.episode_file_map),
        }


def _keys(values: list[object] | set[object] | tuple[object, ...], season: int) -> set[str]:
    return {
        key
        for value in values
        if (key := canonical_episode_key(season, value)) is not None
    }


def plan_missing_episode_transfer(
    video_files: list[dict[str, Any]],
    *,
    season: int,
    trigger_episode_keys: list[str],
    collected_episode_keys: list[object] | set[object] | tuple[object, ...],
    inventory_episode_keys: list[object] | set[object] | tuple[object, ...],
    cloud_episode_keys: list[object] | set[object] | tuple[object, ...],
    completed_episode_keys: list[object] | set[object] | tuple[object, ...] = (),
    active_episode_keys: list[object] | set[object] | tuple[object, ...] = (),
) -> MissingEpisodePlan:
    """Return the exact missing set only when all three presence ledgers agree."""
    season_number = int(season)
    if season_number <= 0:
        return MissingEpisodePlan(REJECTED, "INVALID_SEASON", season_number)
    triggers = _keys(trigger_episode_keys, season_number)
    if not triggers:
        return MissingEpisodePlan(REJECTED, "TRIGGER_EPISODE_IDENTITY_MISSING", season_number)

    share_keys: list[str] = []
    for item in video_files:
        name = str(item.get("name") or item.get("fileName") or item.get("file_name") or "").strip()
        parsed = extract_video_episode_keys(name, known_season=season_number)
        if len(parsed) != 1 or int(parsed[0][1:3]) != season_number:
            return MissingEpisodePlan(NEEDS_REVIEW, "SHARE_EPISODE_MAP_AMBIGUOUS", season_number)
        key = parsed[0]
        if key not in share_keys:
            share_keys.append(key)
    if not share_keys:
        return MissingEpisodePlan(REJECTED, "SHARE_HAS_NO_MAPPABLE_EPISODES", season_number)
    if not triggers.issubset(set(share_keys)):
        return MissingEpisodePlan(NEEDS_REVIEW, "TRIGGER_EPISODE_NOT_IN_SHARE", season_number, tuple(share_keys))

    selection = select_files(
        video_files,
        selection_mode=SelectionMode.MISSING_EPISODES,
        episode_keys=share_keys,
        season=season_number,
    )
    if selection.decision != "MISSING_EPISODES":
        return MissingEpisodePlan(
            NEEDS_REVIEW,
            selection.decision or "SHARE_EPISODE_MAP_AMBIGUOUS",
            season_number,
            tuple(share_keys),
        )

    collected = _keys(collected_episode_keys, season_number)
    inventory = _keys(inventory_episode_keys, season_number)
    cloud = _keys(cloud_episode_keys, season_number)
    completed = _keys(completed_episode_keys, season_number)
    active = _keys(active_episode_keys, season_number)
    missing: list[str] = []
    for key in share_keys:
        presence = (key in collected, key in inventory, key in cloud)
        if len(set(presence)) != 1:
            return MissingEpisodePlan(NEEDS_REVIEW, f"PRESENCE_LEDGER_MISMATCH:{key}", season_number, tuple(share_keys))
        if key in active:
            return MissingEpisodePlan(NEEDS_REVIEW, f"ACTIVE_TRANSFER_OVERLAP:{key}", season_number, tuple(share_keys))
        if key in completed and not all(presence):
            return MissingEpisodePlan(NEEDS_REVIEW, f"COMPLETED_TASK_WITHOUT_CLOUD_CLOSURE:{key}", season_number, tuple(share_keys))
        if not all(presence):
            missing.append(key)
    if not missing:
        return MissingEpisodePlan(
            REJECTED,
            "NO_MISSING_EPISODES",
            season_number,
            tuple(share_keys),
            (),
            dict(selection.episode_file_map),
        )
    return MissingEpisodePlan(
        AUTO_SAFE,
        "PRESENCE_LEDGERS_AGREE_AND_SHARE_MAP_IS_UNIQUE",
        season_number,
        tuple(share_keys),
        tuple(missing),
        dict(selection.episode_file_map),
    )


__all__ = ["AUTO_SAFE", "NEEDS_REVIEW", "REJECTED", "MissingEpisodePlan", "plan_missing_episode_transfer"]
