import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[2] / "tools"))
from audit_queue_safety import classify_task, normalize_episode_key, season_from_episode_key


def test_normalize_episode_key_accepts_legacy_numbers_and_canonical_keys():
    assert normalize_episode_key(2, 7) == "S02E07"
    assert normalize_episode_key(2, "7") == "S02E07"
    assert normalize_episode_key(2, "S2E7") == "S02E07"


def test_classify_task_marks_auth_and_historical_collection_without_safe_label():
    labels = classify_task(
        old_collected=True, new_collected=False, in_cloud=False, completed_task=False,
        duplicate_idempotency=False, active_same_episode=False, error_type="AUTH_401",
    )
    assert labels == ["ALREADY_COLLECTED_OLD", "AUTH_BLOCKED"]


def test_classify_task_marks_new_clean_task_safe():
    assert classify_task(
        old_collected=False, new_collected=False, in_cloud=False, completed_task=False,
        duplicate_idempotency=False, active_same_episode=False, error_type=None,
    ) == ["SAFE_NEW"]


def test_episode_key_supplies_season_when_resource_season_is_missing():
    assert season_from_episode_key("S02E01") == 2
