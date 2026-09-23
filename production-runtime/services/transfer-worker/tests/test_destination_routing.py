from types import SimpleNamespace

from app.transfer.destination_routing import DestinationRouter


def test_destination_router_carries_one_canonical_category_for_both_roots():
    resource = SimpleNamespace(tmdb_id=324487, media_type="tv", season=1, title="修复错误！", year=2026)
    config = SimpleNamespace(target_folder_id="completed", ongoing_target_folder_id="ongoing")
    metadata = {
        "id": 324487,
        "media_type": "tv",
        "origin_country": ["CN"],
        "original_language": "zh",
        "genres": [{"id": 18, "name": "Drama"}],
        "metadata_complete": True,
        "seasons": [{"season_number": 1}, {"season_number": 2}],
    }
    ongoing = DestinationRouter.resolve(
        resource=resource,
        cloud_config=config,
        watchlist=None,
        incoming_episode_keys=["S01E02"],
        metadata=metadata,
    )
    assert ongoing.kind == "ongoing"
    assert ongoing.media_category == "国产剧"
    assert ongoing.inventory_prefix.endswith("/S01")


def test_destination_router_rejects_missing_tmdb_metadata():
    resource = SimpleNamespace(tmdb_id=1, media_type="tv", season=1, title="未知", year=None)
    config = SimpleNamespace(target_folder_id="completed", ongoing_target_folder_id="ongoing")
    try:
        DestinationRouter.resolve(
            resource=resource,
            cloud_config=config,
            watchlist=None,
            incoming_episode_keys=["S01E01"],
        )
    except ValueError as exc:
        assert "metadata" in str(exc).casefold() or "tmdb" in str(exc).casefold()
    else:  # pragma: no cover
        raise AssertionError("missing metadata must fail closed")
