"""Phase 2E: Canary reports separate resource safety from execution gates."""

import pytest

from tests.phase2c.test_phase2c import _canary_task


@pytest.mark.asyncio
async def test_dry_run_resource_safe_when_pause_and_cloud_write_are_closed(tmp_path, monkeypatch):
    from tools.run_transfer_canary import run_preflight

    engine, sessions, task_id = await _canary_task(tmp_path)
    monkeypatch.setattr(
        "tools.run_transfer_canary.validate_remote_canary",
        lambda **_kwargs: None,
    )

    async def valid_remote(**_kwargs):
        return {
            "share_accessible": True,
            "has_video": True,
            "episode_match": True,
            "destination_auth": True,
            "destination_read": True,
        }

    report = await run_preflight(task_id, session_factory=sessions, remote_validator=valid_remote)

    assert report["preflight_status"] == "CANARY_SAFE"
    assert report["execution_gate"] == "CLOSED"
    assert report["checks"]["check_task_status"]["result"] == "PASS"
    assert report["checks"]["check_share_access"]["result"] == "PASS"
    assert report["checks"]["check_account_auth"]["result"] == "PASS"
    await engine.dispose()


@pytest.mark.asyncio
async def test_remote_preflight_reports_share_and_destination_auth_separately(tmp_path):
    from tools.run_transfer_canary import run_preflight

    engine, sessions, task_id = await _canary_task(tmp_path)

    async def share_ok_auth_failed(**_kwargs):
        return {
            "share_accessible": True,
            "has_video": True,
            "episode_match": True,
            "destination_auth": False,
            "destination_read": False,
            "failure_code": "ACCOUNT_AUTH_FAILED",
            "failure_detail": "destination list API returned 401",
        }

    report = await run_preflight(task_id, session_factory=sessions, remote_validator=share_ok_auth_failed)

    assert report["checks"]["check_share_access"]["result"] == "PASS"
    assert report["checks"]["check_account_auth"]["result"] == "FAIL"
    assert report["checks"]["check_destination"]["result"] == "FAIL"
    assert report["failure_code"] == "ACCOUNT_AUTH_FAILED"
    await engine.dispose()
