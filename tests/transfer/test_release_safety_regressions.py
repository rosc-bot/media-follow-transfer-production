"""Release regressions preserve quality priority and active-task fencing."""
import pytest

from app.transfer.file_selection import assert_selection_scope, select_files
from app.transfer.missing_episode_preflight import NEEDS_REVIEW, plan_missing_episode_transfer


def test_4k_webdl_is_preferred_over_1080p_remux():
    files = [
        {'fileId': 'fhd-remux', 'name': 'Show.S01E01.1080p.REMUX.mkv', 'resType': 1},
        {'fileId': 'uhd-webdl', 'name': 'Show.S01E01.2160p.WEB-DL.mkv', 'resType': 1},
    ]
    selection = select_files(files, episode_keys=['S01E01'], season=1)
    assert selection.selected_file_ids == ['uhd-webdl']
    assert_selection_scope(selection)


@pytest.mark.parametrize('ledger', ['collected', 'inventory'])
def test_stale_ledger_does_not_bypass_real_active_transfer(ledger):
    plan = plan_missing_episode_transfer(
        [{'fileId': 'e1', 'name': 'Show.S01E01.mkv', 'resType': 1}],
        season=1,
        trigger_episode_keys=['S01E01'],
        collected_episode_keys=['S01E01'] if ledger == 'collected' else [],
        inventory_episode_keys=['S01E01'] if ledger == 'inventory' else [],
        cloud_episode_keys=[],
        active_episode_keys=['S01E01'],
        cloud_scan_verified=True,
        cloud_scan_truncated=False,
        cloud_pagination_complete=True,
    )
    assert plan.classification == NEEDS_REVIEW
    assert plan.reason == 'ACTIVE_TRANSFER_OVERLAP:S01E01'
    assert plan.missing_episode_keys == ()
