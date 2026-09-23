from app.follow.completed_root_conflict import CompletedRootConflictScanner


def test_completed_root_conflict_scans_direct_children_only():
    result = CompletedRootConflictScanner.inspect_direct_children(
        [
            {"id": "same", "name": "测试剧 (2026) {tmdbid-123}", "resType": 2},
            {"id": "other", "name": "其它剧 {tmdbid-999}", "resType": 2},
            {"id": "nested", "name": "测试剧.S01E01.mkv", "resType": 1},
        ],
        tmdb_id=123,
        title="测试剧",
    )
    assert result["status"] == "VERIFIED"
    assert result["conflict"] is True
    assert result["recursive_scan"] is False
    assert [item["file_id"] for item in result["matched_direct_children"]] == ["same"]
