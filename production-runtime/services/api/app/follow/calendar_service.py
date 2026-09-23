from typing import Protocol


class CalendarProvider(Protocol):
    async def fetch_schedule(self, tmdb_id: int, season: int) -> dict: ...


class CalendarService:
    def __init__(self, provider: CalendarProvider | None = None): self.provider=provider

    async def sync(self, watchlist):
        if self.provider is None: return {'total_episodes': watchlist.total_episodes, 'last_aired_episode': watchlist.last_aired_episode}
        return await self.provider.fetch_schedule(watchlist.tmdb_id, watchlist.season)
