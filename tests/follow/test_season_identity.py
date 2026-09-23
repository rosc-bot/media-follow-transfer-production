import pytest

from app.follow.episode_keys import season_identity


@pytest.mark.parametrize(
    ("name", "expected"),
    [
        ("S01", 1),
        ("S1", 1),
        ("Season 1", 1),
        ("Season01", 1),
        ("第一季", 1),
        ("第1季", 1),
        ("第01季", 1),
        ("S02", 2),
        ("S2", 2),
        ("Season 2", 2),
        ("第二季", 2),
        ("第2季", 2),
        ("第02季", 2),
        ("第十二季", 12),
    ],
)
def test_season_identity_maps_explicit_folder_names(name, expected):
    assert season_identity(name) == expected


@pytest.mark.parametrize("name", ["S02 extras", "Season 2 Special", "第二季花絮", "season unknown", "第一季/第二季"])
def test_season_identity_rejects_ambiguous_names(name):
    assert season_identity(name) is None
