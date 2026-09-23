"""Canonical TMDB destination and routing contracts."""

from app.transfer.canonical_destination import CanonicalDestinationBuilder


def metadata(*, country, language, genres=(), genre_ids=(), media_type="tv", keywords=()):
    return {
        "id": 1,
        "media_type": media_type,
        "origin_country": country,
        "original_language": language,
        "genres": [{"id": value, "name": name} for value, name in genres],
        "genre_ids": list(genre_ids),
        "keywords": list(keywords),
        "metadata_complete": True,
        "seasons": [{"season_number": 1}, {"season_number": 2}],
    }


def test_tv_tmdb_categories_use_country_plus_genre_metadata():
    cases = [
        (metadata(country=["CN"], language="zh", genres=[(18, "Drama")]), "国产剧"),
        (metadata(country=["JP"], language="ja", genres=[(18, "Drama")]), "日韩剧"),
        (metadata(country=["US"], language="en", genres=[(18, "Drama")]), "欧美剧"),
        (metadata(country=["JP"], language="ja", genre_ids=[16]), "日番"),
        (metadata(country=["CN"], language="zh", genre_ids=[16]), "国漫"),
        (metadata(country=["US"], language="en", genre_ids=[16]), "欧美动漫"),
        (metadata(country=["GB"], language="en", genre_ids=[99]), "纪录片"),
        (metadata(country=["US"], language="en", genre_ids=[10762]), "儿童"),
        (metadata(country=["KR"], language="ko", genre_ids=[10764]), "综艺"),
        (metadata(country=["ZZ"], language="xx", genres=[(18, "Drama")]), "其他剧"),
    ]
    for item, expected in cases:
        result = CanonicalDestinationBuilder.resolve_category(item)
        assert result.category == expected


def test_canonical_tv_paths_match_between_ongoing_and_completed():
    item = metadata(country=["CN"], language="zh", genres=[(18, "Drama")])
    ongoing = CanonicalDestinationBuilder.build(
        metadata=item,
        tmdb_id=324487,
        media_type="tv",
        title="修复错误！",
        year=2026,
        destination_kind="ongoing",
        season=1,
    )
    completed = CanonicalDestinationBuilder.build(
        metadata=item,
        tmdb_id=324487,
        media_type="tv",
        title="修复错误！",
        year=2026,
        destination_kind="completed",
        season=1,
    )
    assert ongoing.inventory_prefix == "电视剧/国产剧/修复错误！ (2026) {tmdbid-324487}/S01"
    assert completed.inventory_prefix == ongoing.inventory_prefix
    assert ongoing.archive_directory.startswith("未完结追新 / 电视剧 / 国产剧")
    assert completed.archive_directory.startswith("影视转存总目录 / 电视剧 / 国产剧")


def test_movie_uses_movie_category_and_no_tv_season():
    item = metadata(country=["CN"], language="zh", media_type="movie", genres=[(18, "Drama")])
    result = CanonicalDestinationBuilder.build(
        metadata=item,
        tmdb_id=7,
        media_type="movie",
        title="测试电影",
        year=2026,
        destination_kind="completed",
    )
    assert result.media_root == "电影"
    assert result.media_category == "华语电影"
    assert result.season_name is None
    assert result.inventory_prefix == "电影/华语电影/测试电影 (2026) {tmdbid-7}"


def test_category_change_is_review_not_automatic_move():
    assert CanonicalDestinationBuilder.category_change_status("国产剧", "欧美剧") == "CATEGORY_MISMATCH_REVIEW"
    assert CanonicalDestinationBuilder.category_change_status("国产剧", "国产剧") == "UNCHANGED"
