"""Phase 2G.5 final conditional filename naming contracts."""

import pytest

from app.transfer.rename import (
    build_rename_plan,
    build_standard_chinese_episode_filename,
    build_standard_chinese_movie_filename,
    has_meaningful_media_name,
)


def test_meaningful_name_accepts_english_release_and_rejects_placeholders():
    assert has_meaningful_media_name(
        "The.Rapture.S01E02.Episode.2.2160p.iP.WEB-DL.H265.mkv",
        media_type="tv",
    ) is True
    assert has_meaningful_media_name("Movie.Name.2026.2160p.WEB-DL.mkv", media_type="movie") is True
    for name in ("123456.mkv", "video1.mkv", "output.mkv", "download.mkv", "550e8400-e29b-41d4-a716-446655440000.mkv", "S01E02-Gyy.mkv"):
        assert has_meaningful_media_name(name, media_type="tv") is False


def test_reused_standard_chinese_formatters_do_not_collapse_to_sxxexx_only():
    assert build_standard_chinese_movie_filename(
        title="灵魂摆渡·天女之梦",
        year=2026,
        tmdb_id=1754100,
        source_filename="tnzm.2026.2160p.WEB-DL.H265.mkv",
    ) == "灵魂摆渡·天女之梦 (2026) {tmdbid-1754100}.2160p.WEB-DL.H265.mkv"
    assert build_standard_chinese_episode_filename(
        title="修复错误！",
        year=None,
        tmdb_id=324487,
        season=1,
        episode_key="S01E02",
        source_filename="S01E02-Gyy.mkv",
    ) == "修复错误！ {tmdbid-324487}.S01E02.mkv"


def _plan(name: str, *, media_type: str = "tv", status: str = "Returning Series", complete: bool = False):
    return build_rename_plan(
        [{"fileId": "selected", "name": name}],
        selected_file_ids=["selected"],
        title="测试剧",
        year=2026,
        tmdb_id=123,
        season=1,
        episode_key="S01E02",
        media_type=media_type,
        series_status=status,
        content_complete=complete,
    )


def test_movie_with_meaningful_name_is_keep():
    plan = _plan("Movie.Name.2026.2160p.WEB-DL.mkv", media_type="movie", status=None, complete=True)
    assert plan.decision == "KEEP"
    assert plan.status == "RENAME_SKIPPED_KEEP_EXISTING_NAME"
    assert not plan.operations


def test_movie_placeholder_is_standard_chinese_rename():
    plan = _plan("123456.mkv", media_type="movie", status=None, complete=True)
    assert plan.decision == "RENAME_STANDARD_CHINESE"
    assert plan.operations[0].new_name == "测试剧 (2026) {tmdbid-123}.mkv"


def test_ongoing_tv_always_uses_standard_chinese_name_even_for_english_release():
    plan = _plan("The.Rapture.S01E02.2160p.WEB-DL.H265.mkv")
    assert plan.decision == "RENAME_STANDARD_CHINESE"
    assert plan.operations[0].new_name == "测试剧 (2026) {tmdbid-123}.S01E02.2160p.WEB-DL.H265.mkv"


def test_ended_but_incomplete_tv_stays_ongoing_naming():
    plan = _plan("The.Rapture.S01E02.2160p.WEB-DL.H265.mkv", status="Ended", complete=False)
    assert plan.decision == "RENAME_STANDARD_CHINESE"
    assert plan.operations


def test_ended_complete_meaningful_tv_is_keep():
    plan = _plan(
        "The.Rapture.S01E02.2160p.WEB-DL.H265.mkv",
        status="Ended",
        complete=True,
    )
    assert plan.decision == "KEEP"
    assert plan.status == "RENAME_SKIPPED_KEEP_EXISTING_NAME"


def test_ended_complete_placeholder_tv_is_standard_chinese_rename():
    plan = _plan("S01E02-Gyy.mkv", status="Ended", complete=True)
    assert plan.decision == "RENAME_STANDARD_CHINESE"
    assert plan.operations[0].new_name == "测试剧 (2026) {tmdbid-123}.S01E02.mkv"

def test_plan_only_touches_selected_verified_file():
    plan = build_rename_plan(
        [
            {"fileId": "selected", "name": "S01E02-Gyy.mkv"},
            {"fileId": "other", "name": "S01E01-Gyy.mkv"},
        ],
        selected_file_ids=["selected"],
        title="测试剧",
        year=2026,
        tmdb_id=123,
        season=1,
        episode_key="S01E02",
        media_type="tv",
        series_status="Returning Series",
    )
    assert [operation.file_id for operation in plan.operations] == ["selected"]
    assert "other" not in {operation.file_id for operation in plan.operations}


async def _poster_fetcher(tmdb_id, payload):
    assert tmdb_id in {328303, 328304}
    return {"poster_path": "/cached-by-provider.jpg"}


@pytest.mark.asyncio
async def test_poster_resolution_uses_cache_before_provider():
    from app.transfer.notifier import resolve_poster_url

    result = await resolve_poster_url(
        {"tmdb_id": 328303, "tmdb_poster_cache": {"328303": "/cached.jpg"}},
        fetcher=_poster_fetcher,
    )
    assert result == {
        "url": "https://image.tmdb.org/t/p/w500/cached.jpg",
        "status": "POSTER_AVAILABLE",
        "source": "tmdb_poster_cache",
    }


@pytest.mark.asyncio
async def test_poster_resolution_falls_back_to_provider_and_text_when_missing():
    from app.transfer.notifier import resolve_poster_url

    available = await resolve_poster_url({"tmdb_id": 328304}, fetcher=_poster_fetcher)
    assert available["url"].endswith("cached-by-provider.jpg")
    unavailable = await resolve_poster_url({"tmdb_id": 999999}, fetcher=lambda *_args: None)
    assert unavailable["status"] == "POSTER_UNAVAILABLE"
    assert unavailable["url"] is None
