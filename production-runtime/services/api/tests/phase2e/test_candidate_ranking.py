"""Phase 2E: candidate ranking honours single-video shares last."""

from datetime import UTC, datetime, timedelta


def test_candidate_sort_prefers_single_video_share_after_other_priority_keys():
    from app.transfer.candidate_canary import _candidate_sort_key

    now = datetime.now(UTC)
    common = {
        "status": "VALIDATED",
        "source_type": "watchlist_scout",
        "explicit_full_key": True,
    }
    single = {
        **common,
        "candidate_id": 1,
        "discovered_at": now,
        "remote_validation": {"share": {"video_count": 1}},
    }
    collection = {
        **common,
        "candidate_id": 2,
        "discovered_at": now - timedelta(seconds=1),
        "remote_validation": {"share": {"video_count": 20}},
    }

    assert _candidate_sort_key(single) < _candidate_sort_key(collection)
