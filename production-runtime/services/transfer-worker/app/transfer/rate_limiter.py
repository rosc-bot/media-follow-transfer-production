import asyncio
from collections import defaultdict


class RateLimiter:
    def __init__(self, per_provider: int = 1) -> None:
        self._limits = defaultdict(lambda: asyncio.Semaphore(per_provider))

    def acquire(self, provider: str):
        return self._limits[provider.lower()]
