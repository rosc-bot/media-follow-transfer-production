"""Phase 2E list-safe must use the expanded preflight rather than the legacy one."""

import pytest

from tests.phase2c.test_phase2c import _canary_task


@pytest.mark.asyncio
async def test_list_safe_returns_named_check_matrix(tmp_path):
    from tools.run_transfer_canary import list_safe_candidates

    engine, sessions, _task_id = await _canary_task(tmp_path)

    async def remote_ok(**_kwargs):
        return {
            "share_accessible": True, "has_video": True, "episode_match": True,
            "destination_auth": True, "destination_read": True,
        }

    report = await list_safe_candidates(limit=1, session_factory=sessions, remote_validator=remote_ok)
    assert report["total_checked"] == 1
    assert report["rows"][0]["validation"]["checks"]["check_task_status"]["result"] == "PASS"
    await engine.dispose()
