"""Phase 2D remote-read Canary safety gates (all adapter calls are mocked)."""

import pytest

from tests.phase2c.test_phase2c import _canary_task


@pytest.mark.asyncio
async def test_canary_safe_requires_readable_video_and_episode_match(tmp_path):
    from tools.run_transfer_canary import run_preflight

    engine, sessions, task_id = await _canary_task(tmp_path)

    async def readable_video(**_kwargs):
        return {"share_readable": True, "has_video": True, "episode_match": True, "auth_usable": True}

    report = await run_preflight(task_id, session_factory=sessions, remote_validator=readable_video)
    assert report["verdict"] == "CANARY_SAFE"
    assert any(check["check"] == "share_readable" and check["ok"] for check in report["checks"])
    await engine.dispose()


@pytest.mark.asyncio
async def test_canary_rejects_share_without_video_before_execute(tmp_path):
    from tools.run_transfer_canary import run_preflight

    engine, sessions, task_id = await _canary_task(tmp_path)

    async def no_video(**_kwargs):
        return {"share_readable": True, "has_video": False, "episode_match": False, "auth_usable": True}

    report = await run_preflight(task_id, session_factory=sessions, remote_validator=no_video)
    assert report["verdict"] == "CANARY_REJECTED"
    assert any(item["check"] == "share_has_video" for item in report["rejections"])
    await engine.dispose()
