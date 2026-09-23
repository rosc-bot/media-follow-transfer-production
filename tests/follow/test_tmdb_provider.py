from datetime import date

import pytest

from app.follow.tmdb_provider import TMDBSeasonProvider


@pytest.mark.asyncio
async def test_tmdb_provider_uses_season_endpoint_and_counts_only_aired_episodes():
    seen = {}

    async def fetch_json(url, params):
        seen['url'] = url
        seen['params'] = params
        return {
            'episodes': [
                {'episode_number': 1, 'air_date': '2026-09-19'},
                {'episode_number': 2, 'air_date': '2026-09-20'},
                {'episode_number': 3, 'air_date': '2026-09-21'},
                {'episode_number': 4, 'air_date': None},
            ],
        }

    provider = TMDBSeasonProvider(api_key='tmdb-key', fetch_json=fetch_json, today=lambda: date(2026, 9, 20))

    schedule = await provider.fetch_schedule(tmdb_id=42, season=2)

    assert seen == {
        'url': 'https://api.themoviedb.org/3/tv/42/season/2',
        'params': {'api_key': 'tmdb-key'},
    }
    assert schedule == {'total_episodes': 4, 'last_aired_episode': 2}


@pytest.mark.asyncio
async def test_tmdb_provider_rejects_missing_api_key_before_network_call():
    provider = TMDBSeasonProvider(api_key='')

    with pytest.raises(ValueError, match='TMDB_API_KEY'):
        await provider.fetch_schedule(tmdb_id=42, season=1)
