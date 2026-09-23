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
    presence_decisions: dict[str, dict[str, Any]] = field(default_factory=dict)
    metadata_reconcile: dict[str, tuple[str, ...]] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {
            "classification": self.classification,
            "reason": self.reason,
            "season": self.season,
            "share_episode_keys": list(self.share_episode_keys),
            "missing_episode_keys": list(self.missing_episode_keys),
            "episode_file_map": dict(self.episode_file_map),
            "presence_decisions": {key: dict(value) for key, value in self.presence_decisions.items()},
            "metadata_reconcile": {key: list(value) for key, value in self.metadata_reconcile.items()},
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
    cloud_scan_verified: bool = False,
    cloud_scan_truncated: bool = False,
    cloud_pagination_complete: bool = True,
) -> MissingEpisodePlan:
    """Plan batch restores only from a complete verified cloud listing.

    Cloud is the physical-presence authority. Database ledgers may be repaired
    from verified cloud evidence, but a DB-only presence with an absent cloud
    file is a conflict and never authorizes restore.
    """
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

    if not cloud_scan_verified or cloud_scan_truncated or not cloud_pagination_complete:
        return MissingEpisodePlan(
            NEEDS_REVIEW,
            "CLOUD_UNVERIFIED",
            season_number,
            tuple(share_keys),
        )

    collected = _keys(collected_episode_keys, season_number)
    inventory = _keys(inventory_episode_keys, season_number)
    cloud = _keys(cloud_episode_keys, season_number)
    completed = _keys(completed_episode_keys, season_number)
    active = _keys(active_episode_keys, season_number)
    missing: list[str] = []
    presence_decisions: dict[str, dict[str, Any]] = {}
    metadata_reconcile: dict[str, tuple[str, ...]] = {}
    for key in share_keys:
        collected_present = key in collected
        inventory_present = key in inventory
        cloud_present = key in cloud
        if cloud_present:
            presence_decisions[key] = {
                "classification": "PRESENT_CONFIRMED",
                "cloud_present": True,
                "collected_present": collected_present,
                "inventory_present": inventory_present,
            }
            reconcile = tuple(
                ledger
                for ledger, present in (
                    ("collected", collected_present),
                    ("inventory", inventory_present),
                )
                if not present
            )
            if reconcile:
                metadata_reconcile[key] = reconcile
            continue

        if key in completed:
            presence_decisions[key] = {
                "classification": "COMPLETED_TASK_WITHOUT_CLOUD_CLOSURE",
                "cloud_present": False,
                "collected_present": collected_present,
                "inventory_present": inventory_present,
            }
            return MissingEpisodePlan(
                NEEDS_REVIEW,
                f"COMPLETED_TASK_WITHOUT_CLOUD_CLOSURE:{key}",
                season_number,
                tuple(share_keys),
                presence_decisions=presence_decisions,
            )
        if collected_present or inventory_present:
            presence_decisions[key] = {
                "classification": "LEDGER_CONFLICT",
                "cloud_present": False,
                "collected_present": collected_present,
                "inventory_present": inventory_present,
            }
            return MissingEpisodePlan(
                NEEDS_REVIEW,
                f"LEDGER_CONFLICT:{key}",
                season_number,
                tuple(share_keys),
                presence_decisions=presence_decisions,
            )
        if key in active:
            presence_decisions[key] = {
                "classification": "ACTIVE_TRANSFER_OVERLAP",
                "cloud_present": False,
                "collected_present": False,
                "inventory_present": False,
            }
            return MissingEpisodePlan(
                NEEDS_REVIEW,
                f"ACTIVE_TRANSFER_OVERLAP:{key}",
                season_number,
                tuple(share_keys),
                presence_decisions=presence_decisions,
            )
        presence_decisions[key] = {
            "classification": "MISSING_CONFIRMED",
            "cloud_present": False,
            "collected_present": False,
            "inventory_present": False,
        }
        missing.append(key)

    if not missing:
        return MissingEpisodePlan(
            REJECTED,
            "NO_MISSING_EPISODES",
            season_number,
            tuple(share_keys),
            (),
            dict(selection.episode_file_map),
            presence_decisions,
            metadata_reconcile,
        )
    return MissingEpisodePlan(
        AUTO_SAFE,
        "MISSING_EPISODES_CONFIRMED_BY_VERIFIED_CLOUD",
        season_number,
        tuple(share_keys),
        tuple(missing),
        dict(selection.episode_file_map),
        presence_decisions,
        metadata_reconcile,
    )


__all__ = ["AUTO_SAFE", "NEEDS_REVIEW", "REJECTED", "MissingEpisodePlan", "plan_missing_episode_transfer"]
