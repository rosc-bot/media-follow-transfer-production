from collections.abc import Awaitable, Callable
from datetime import date
from typing import Any

import aiohttp

FetchJSON = Callable[[str, dict[str, str]], Awaitable[dict[str, Any]]]


class TMDBSeasonProvider:
    """Minimal TMDB season reader: it only retrieves schedule metadata, never media files."""

    def __init__(
        self,
        *,
        api_key: str,
        base_url: str = 'https://api.themoviedb.org/3',
        fetch_json: FetchJSON | None = None,
        today: Callable[[], date] = date.today,
    ) -> None:
        self.api_key = api_key.strip()
        self.base_url = base_url.rstrip('/')
        self.fetch_json = fetch_json or self._fetch_json
        self.today = today

    async def _fetch_json(self, url: str, params: dict[str, str]) -> dict[str, Any]:
        timeout = aiohttp.ClientTimeout(total=20)
        async with aiohttp.ClientSession(timeout=timeout) as client, client.get(url, params=params) as response:
            if response.status >= 400:
                raise RuntimeError(f'TMDB season request failed with HTTP {response.status}')
            payload = await response.json()
        if not isinstance(payload, dict):
            raise TypeError('TMDB season response is not an object')
        return payload

    async def fetch_schedule(self, tmdb_id: int, season: int) -> dict[str, Any]:
        if not self.api_key:
            raise ValueError('TMDB_API_KEY is required for TMDB schedule synchronization')
        if tmdb_id <= 0 or season <= 0:
            raise ValueError('tmdb_id and season must be positive')
        payload = await self.fetch_json(
            f'{self.base_url}/tv/{tmdb_id}/season/{season}',
            {'api_key': self.api_key},
        )
        episodes = payload.get('episodes') or []
        if not isinstance(episodes, list):
            raise TypeError('TMDB season response has invalid episodes')
        last_aired = 0
        for item in episodes:
            if not isinstance(item, dict) or not isinstance(item.get('episode_number'), int):
                continue
            air_date = item.get('air_date')
            try:
                aired = date.fromisoformat(air_date) if air_date else None
            except ValueError:
                aired = None
            if aired is not None and aired <= self.today():
                last_aired = max(last_aired, item['episode_number'])
        return {'total_episodes': len(episodes), 'last_aired_episode': last_aired}

    async def fetch_series_status(self, tmdb_id: int) -> str | None:
        if not self.api_key:
            raise ValueError('TMDB_API_KEY is required for TMDB schedule synchronization')
        if tmdb_id <= 0:
            raise ValueError('tmdb_id must be positive')
        payload = await self.fetch_json(
            f'{self.base_url}/tv/{tmdb_id}',
            {'api_key': self.api_key},
        )
        status = payload.get('status')
        if status is not None and not isinstance(status, str):
            raise TypeError('TMDB series response has invalid status')
        return status
