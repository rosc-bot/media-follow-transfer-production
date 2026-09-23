"""Fail-closed selection of exact remote video files for transfer.

A missing ``expected_files`` value is not a request to restore an entire share.
The selector derives the safe scope from an explicit mode or from an exact
single-episode identity, then deduplicates recursive share listings by remote
file ID before any provider write is possible.
"""

from __future__ import annotations

import re
from dataclasses import asdict, dataclass, field
from enum import StrEnum
from typing import Any

from app.follow.episode_keys import canonical_episode_key
from app.transfer.episode_matcher import diagnose_episode_match, extract_video_episode_keys
from app.transfer.errors import FileSelectionError, TransferErrorCategory
from app.transfer.guangya_auth import is_video_filename


class SelectionMode(StrEnum):
    SINGLE_EPISODE = "SINGLE_EPISODE"
    MISSING_EPISODES = "MISSING_EPISODES"
    WHOLE_SHARE = "WHOLE_SHARE"
    COLLECTION = "COLLECTION"


@dataclass
class FileSelectionResult:
    selection_mode: SelectionMode
    target_episode_key: str | None
    raw_listing_count: int
    share_video_count: int
    unique_video_count: int
    matched_listing_count: int
    matched_unique_file_count: int
    unique_file_ids: list[str] = field(default_factory=list)
    matched_file_ids: list[str] = field(default_factory=list)
    selected_file_ids: list[str] = field(default_factory=list)
    selected_file_names: list[str] = field(default_factory=list)
    rejected_files: list[dict[str, Any]] = field(default_factory=list)
    decision: str = ""
    requested_episode_keys: list[str] = field(default_factory=list)
    selected_episode_keys: list[str] = field(default_factory=list)
    episode_file_map: dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        result = asdict(self)
        result["selection_mode"] = self.selection_mode.value
        return result


# Keep this helper public so the worker and future canary path can use the same
# identity normalization instead of rebuilding a second selection rule.
def resolve_selection_mode(
    *,
    selection_mode: SelectionMode | str | None,
    episode_keys: list[str] | None = None,
    expected_files: list[str] | None = None,
) -> SelectionMode:
    if selection_mode is not None and str(selection_mode).strip():
        try:
            return SelectionMode(str(selection_mode).strip().upper())
        except ValueError as exc:
            raise FileSelectionError(
                "UNKNOWN_SELECTION_MODE",
                f"unsupported selection_mode={selection_mode!r}",
            ) from exc
    if episode_keys:
        return SelectionMode.SINGLE_EPISODE
    # Compatibility for old explicit filename lists: this is a bounded named
    # collection, never an implicit whole-share selection.
    if expected_files:
        return SelectionMode.COLLECTION
    raise FileSelectionError(
        "SELECTION_MODE_REQUIRED",
        "selection_mode is required when no episode key or explicit file list exists",
    )


def _name(item: dict[str, Any]) -> str:
    return str(item.get("name") or item.get("fileName") or item.get("file_name") or "").strip()


def _file_id(item: dict[str, Any]) -> str:
    return str(item.get("fileId") or item.get("file_id") or item.get("id") or "").strip()


def _normalized_item(item: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(item)
    normalized["name"] = _name(item)
    normalized["file_id"] = _file_id(item)
    return normalized


def deduplicate_video_files(video_files: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Deduplicate recursive listings by stable remote ``file_id``.

    A provider response may repeat the same file on an implicit/repeated page.
    A missing ID is retained under a name key for diagnostics, but it cannot be
    selected for a real restore because restore_share requires file IDs.
    """
    unique: dict[str, dict[str, Any]] = {}
    for raw in video_files:
        item = _normalized_item(raw)
        name = item["name"]
        if not name or not is_video_filename(name):
            continue
        remote_id = item["file_id"]
        key = f"id:{remote_id}" if remote_id else f"name:{name.casefold()}"
        prior = unique.get(key)
        if prior is not None and prior["name"] != name:
            raise FileSelectionError(
                "DUPLICATE_FILE_ID_CONFLICT",
                f"remote file_id {remote_id!r} appeared with conflicting names",
            )
        unique.setdefault(key, item)
    return list(unique.values())


def _episode_target(episode_keys: list[str] | None, target_episode_key: str | None) -> str | None:
    if target_episode_key:
        return str(target_episode_key).strip()
    if episode_keys and len(episode_keys) == 1:
        return str(episode_keys[0]).strip()
    return None


def _diagnose(
    *, target_episode_key: str | None, season: int | None, names: list[str]
) -> list[dict[str, Any]]:
    if not target_episode_key:
        return []
    try:
        return diagnose_episode_match(
            target_episode_key=target_episode_key,
            known_season=season,
            video_names=names,
        )
    except ValueError as exc:
        raise FileSelectionError("INVALID_TARGET_EPISODE", str(exc)) from exc


def _rejected_files(
    unique_files: list[dict[str, Any]], diagnostics: list[dict[str, Any]], selected_ids: set[str]
) -> list[dict[str, Any]]:
    reasons = {str(row["name"]): str(row.get("reject_reason") or "NOT_SELECTED") for row in diagnostics}
    return [
        {
            "file_id": item["file_id"],
            "file_name": item["name"],
            "reason": "SELECTED" if item["file_id"] in selected_ids else reasons.get(item["name"], "NOT_SELECTED"),
        }
        for item in unique_files
        if item["file_id"] not in selected_ids
    ]


def select_files(
    video_files: list[dict[str, Any]],
    *,
    selection_mode: SelectionMode | str | None = None,
    target_episode_key: str | None = None,
    episode_keys: list[str] | None = None,
    season: int | None = None,
    expected_files: list[str] | None = None,
    selected_file_ids: list[str] | None = None,
    selected_file_names: list[str] | None = None,
) -> FileSelectionResult:
    """Build a deterministic, auditable selection without calling a provider."""
    episode_keys = [str(key).strip() for key in (episode_keys or []) if str(key).strip()]
    expected = {str(name).strip() for name in (expected_files or []) if str(name).strip()}
    mode = resolve_selection_mode(
        selection_mode=selection_mode,
        episode_keys=episode_keys,
        expected_files=list(expected),
    )
    target = _episode_target(episode_keys, target_episode_key)
    if mode is SelectionMode.SINGLE_EPISODE and episode_keys and len(episode_keys) != 1 and not target_episode_key:
        raise FileSelectionError(
            "FILE_SELECTION_REVIEW",
            "SINGLE_EPISODE requires exactly one target episode key",
        )

    raw_video_files = [_normalized_item(item) for item in video_files if is_video_filename(_name(item))]
    unique_files = deduplicate_video_files(raw_video_files)
    unique_file_ids = [item["file_id"] for item in unique_files if item["file_id"]]
    raw_names = [item["name"] for item in raw_video_files]
    unique_names = [item["name"] for item in unique_files]
    raw_diagnostics = _diagnose(target_episode_key=target, season=season, names=raw_names)
    unique_diagnostics = _diagnose(target_episode_key=target, season=season, names=unique_names)
    matched_listing_count = sum(row["match_result"] == "MATCH" for row in raw_diagnostics)
    matched_unique_items = [
        item
        for item, row in zip(unique_files, unique_diagnostics, strict=False)
        if row.get("match_result") == "MATCH"
    ]
    matched_ids = [item["file_id"] for item in matched_unique_items if item["file_id"]]

    selected: list[dict[str, Any]] = []
    selected_episode_keys: list[str] = []
    episode_file_map: dict[str, str] = {}
    requested_episode_keys: list[str] = []
    batch_matched_items: list[dict[str, Any]] = []
    decision = ""
    if mode is SelectionMode.SINGLE_EPISODE:
        if matched_unique_items == []:
            decision = "EPISODE_MISMATCH"
        elif len(matched_unique_items) > 1:
            decision = "FILE_SELECTION_REVIEW"
        else:
            selected = matched_unique_items
            decision = "EXACT_SINGLE_EPISODE"
            if target:
                canonical_target = canonical_episode_key(season, target)
                if canonical_target:
                    requested_episode_keys = [canonical_target]
                    selected_episode_keys = [canonical_target]
                    episode_file_map = {canonical_target: selected[0]["file_id"]}
    elif mode is SelectionMode.MISSING_EPISODES:
        for value in episode_keys:
            key = canonical_episode_key(season, value)
            if key and key not in requested_episode_keys:
                requested_episode_keys.append(key)
        if not requested_episode_keys or len(requested_episode_keys) != len(episode_keys):
            decision = "FILE_SELECTION_REVIEW"
        else:
            by_episode: dict[str, dict[str, Any]] = {}
            ambiguous = False
            for item in unique_files:
                name = item["name"]
                # Ranges and multi-episode files cannot be safely split by restore.
                if re.search(
                    r"(?i)\bS\d{1,3}[ ._-]*E\d{1,4}[ ._-]*(?:-|~|到|至)[ ._-]*(?:E)?\d{1,4}\b",
                    name,
                ):
                    ambiguous = True
                    break
                parsed_keys = extract_video_episode_keys(name, known_season=season)
                if len(parsed_keys) != 1 or not item["file_id"]:
                    ambiguous = True
                    break
                key = parsed_keys[0]
                if season is not None and int(key[1:3]) != int(season):
                    ambiguous = True
                    break
                previous = by_episode.get(key)
                if previous is not None and previous["file_id"] != item["file_id"]:
                    ambiguous = True
                    break
                by_episode[key] = item
            if ambiguous:
                decision = "FILE_SELECTION_REVIEW"
            else:
                missing = [key for key in requested_episode_keys if key not in by_episode]
                batch_matched_items = [by_episode[key] for key in requested_episode_keys if key in by_episode]
                matched_ids = [item["file_id"] for item in batch_matched_items if item["file_id"]]
                matched_listing_count = sum(
                    1
                    for item in raw_video_files
                    if set(extract_video_episode_keys(item["name"], known_season=season)) & set(requested_episode_keys)
                )
                duplicate_names = [
                    key for key in requested_episode_keys
                    if key in by_episode and sum(
                        1 for value in by_episode.values() if value["name"] == by_episode[key]["name"]
                    ) > 1
                ]
                if duplicate_names:
                    decision = "FILE_SELECTION_REVIEW"
                elif missing:
                    decision = "EPISODE_MISMATCH"
                else:
                    selected_episode_keys = list(requested_episode_keys)
                    selected = [by_episode[key] for key in selected_episode_keys]
                    episode_file_map = {key: by_episode[key]["file_id"] for key in selected_episode_keys}
                    decision = "MISSING_EPISODES"
    elif mode is SelectionMode.WHOLE_SHARE:
        selected = unique_files
        decision = "WHOLE_SHARE"
        if season is not None:
            by_episode: dict[str, dict[str, Any]] = {}
            for item in selected:
                keys = extract_video_episode_keys(item["name"], known_season=season)
                if len(keys) != 1 or not item["file_id"] or int(keys[0][1:3]) != int(season):
                    decision = "FILE_SELECTION_REVIEW"
                    selected = []
                    break
                key = keys[0]
                if key in by_episode:
                    decision = "FILE_SELECTION_REVIEW"
                    selected = []
                    break
                by_episode[key] = item
            if decision == "WHOLE_SHARE":
                selected_episode_keys = list(by_episode)
                requested_episode_keys = list(selected_episode_keys)
                episode_file_map = {key: item["file_id"] for key, item in by_episode.items()}
    else:
        requested_ids = {str(value).strip() for value in (selected_file_ids or []) if str(value).strip()}
        requested_names = {str(value).strip() for value in (selected_file_names or []) if str(value).strip()}
        if not requested_ids and not requested_names:
            requested_names = expected
        if not requested_ids and not requested_names:
            decision = "COLLECTION_SELECTION_REQUIRED"
        else:
            selected = [
                item
                for item in unique_files
                if (item["file_id"] and item["file_id"] in requested_ids) or item["name"] in requested_names
            ]
            selected_names = {item["name"] for item in selected}
            selected_ids = {item["file_id"] for item in selected if item["file_id"]}
            if (requested_names and not requested_names.issubset(selected_names)) or (
                requested_ids and not requested_ids.issubset(selected_ids)
            ):
                selected = []
                decision = "COLLECTION_SELECTION_MISMATCH"
            else:
                decision = "COLLECTION"

    selected_ids = [item["file_id"] for item in selected if item["file_id"]]
    selected_names = [item["name"] for item in selected]
    result = FileSelectionResult(
        selection_mode=mode,
        target_episode_key=target,
        raw_listing_count=len(raw_video_files),
        share_video_count=len(raw_video_files),
        unique_video_count=len(unique_files),
        matched_listing_count=matched_listing_count,
        matched_unique_file_count=len(batch_matched_items) if mode is SelectionMode.MISSING_EPISODES else len(matched_unique_items),
        unique_file_ids=unique_file_ids,
        matched_file_ids=matched_ids,
        selected_file_ids=selected_ids,
        selected_file_names=selected_names,
        rejected_files=_rejected_files(unique_files, unique_diagnostics or raw_diagnostics, set(selected_ids)),
        decision=decision,
        requested_episode_keys=requested_episode_keys,
        selected_episode_keys=selected_episode_keys,
        episode_file_map=episode_file_map,
    )
    return result


def assert_selection_scope(result: FileSelectionResult) -> FileSelectionResult:
    """Assert the final file IDs are safe immediately before restore_share."""
    selected = list(dict.fromkeys(str(value).strip() for value in result.selected_file_ids if str(value).strip()))
    matched = set(result.matched_file_ids)
    unique = set(result.unique_file_ids)
    if result.selection_mode is SelectionMode.SINGLE_EPISODE:
        if result.decision == "FILE_SELECTION_REVIEW":
            raise FileSelectionError(
                "FILE_SELECTION_REVIEW",
                "multiple unique remote files match the target episode",
            )
        if result.decision == "EPISODE_MISMATCH" or not selected:
            raise FileSelectionError(
                "EPISODE_MISMATCH",
                "no unique remote file matches the target episode",
                category=TransferErrorCategory.EPISODE_MISMATCH,
            )
        if result.decision == "FILE_SELECTION_REVIEW" or len(selected) != 1:
            raise FileSelectionError(
                "CANARY_ABORTED_SELECTION_TOO_BROAD",
                "SINGLE_EPISODE selection must contain exactly one unique file ID",
            )
        if not set(selected).issubset(matched):
            raise FileSelectionError(
                "TRANSFER_SCOPE_VIOLATION",
                "selected file IDs are not a subset of matched episode file IDs",
                category=TransferErrorCategory.TRANSFER_SCOPE_VIOLATION,
            )
    elif result.selection_mode is SelectionMode.MISSING_EPISODES:
        if result.decision == "FILE_SELECTION_REVIEW":
            raise FileSelectionError(
                "FILE_SELECTION_REVIEW",
                "share episode map is ambiguous or contains a multi-episode file",
            )
        if result.decision == "EPISODE_MISMATCH":
            raise FileSelectionError(
                "EPISODE_MISMATCH",
                "share does not contain one unique file for every requested missing episode",
                category=TransferErrorCategory.EPISODE_MISMATCH,
            )
        requested = list(result.requested_episode_keys)
        mapping = dict(result.episode_file_map)
        expected_ids = [mapping.get(key, "") for key in requested]
        if (
            result.decision != "MISSING_EPISODES"
            or not requested
            or result.selected_episode_keys != requested
            or set(mapping) != set(requested)
            or not all(expected_ids)
            or selected != expected_ids
            or len(selected) != len(set(selected))
            or len(result.selected_file_names) != len(requested)
            or not set(selected).issubset(unique)
        ):
            raise FileSelectionError(
                "MISSING_EPISODE_SELECTION_INVALID",
                "selected IDs must map one-to-one to every requested missing episode",
            )
    elif result.selection_mode is SelectionMode.WHOLE_SHARE:
        if not selected or set(selected) != unique:
            raise FileSelectionError(
                "WHOLE_SHARE_SELECTION_INVALID",
                "WHOLE_SHARE must explicitly select every unique remote video file",
            )
    elif result.selection_mode is SelectionMode.COLLECTION:
        if result.decision == "COLLECTION_SELECTION_MISMATCH":
            raise FileSelectionError(
                "COLLECTION_SELECTION_MISMATCH",
                "share listing did not contain every explicitly selected collection file",
                category=TransferErrorCategory.EPISODE_MISMATCH,
            )
        if not selected or not set(selected).issubset(unique):
            raise FileSelectionError(
                "COLLECTION_SELECTION_INVALID",
                "COLLECTION selected file IDs are not a subset of the share",
            )
    else:  # pragma: no cover - enum construction prevents this
        raise FileSelectionError("UNKNOWN_SELECTION_MODE", "selection mode is not recognized")
    return result
