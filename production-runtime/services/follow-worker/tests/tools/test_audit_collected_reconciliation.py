import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[2] / "tools"))
from audit_collected_reconciliation import normalize_episode_key


def test_reconciliation_uses_exact_tmdb_season_episode_identity():
    assert normalize_episode_key(2, "S2E3") == "S02E03"
