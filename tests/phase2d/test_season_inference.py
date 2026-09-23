"""Phase 2D: strict, evidence-backed historic season inference."""

import pytest

from app.season_inference import (
    EvidenceConflict,
    infer_season,
    parse_episode_key,
)


def test_parse_episode_key_accepts_structured_season_and_episode():
    assert parse_episode_key("S01E101") == (1, 101)
    assert parse_episode_key("S2E7") == (2, 7)
    assert parse_episode_key("S02E101") == (2, 101)


@pytest.mark.parametrize("episode_key", ["E07", "07", "第7集", "S01", "S01E", "xS01E07"])
def test_parse_episode_key_rejects_unstructured_or_partial_values(episode_key):
    assert parse_episode_key(episode_key) is None


def test_episode_key_and_unique_watchlist_agreement_is_safe_infer():
    result = infer_season(
        episode_key="S01E101",
        watchlist_seasons={1},
        candidate_seasons={1},
    )
    assert result.inferred_season == 1
    assert result.confidence == "SAFE_INFER"
    assert result.evidence == ["episode_key:S01E101", "watchlist:season=1", "candidate:season=1"]


def test_episode_key_watchlist_conflict_requires_review():
    result = infer_season(
        episode_key="S01E07",
        watchlist_seasons={2},
        candidate_seasons=set(),
    )
    assert result.inferred_season is None
    assert result.confidence == "NEEDS_REVIEW"
    assert isinstance(result.conflict, EvidenceConflict)


def test_missing_s_segment_is_needs_review_not_inferred_from_watchlist():
    result = infer_season(
        episode_key="E07",
        watchlist_seasons={1},
        candidate_seasons={1},
    )
    assert result.inferred_season is None
    assert result.confidence == "NEEDS_REVIEW"
