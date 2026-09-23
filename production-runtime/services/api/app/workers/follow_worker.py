import asyncio

from app.core.config import get_settings
from app.follow.follow_worker import run_follow_worker

if __name__ == '__main__':
    asyncio.run(run_follow_worker(interval_seconds=get_settings().follow_poll_interval_seconds))
