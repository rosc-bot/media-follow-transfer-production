"""Legacy module entry point; keep a single transfer runtime implementation."""

import asyncio

from app.workers.transfer_worker import recover_stale_tasks, run_once, run_transfer_worker

__all__ = ['recover_stale_tasks', 'run_once', 'run_transfer_worker']


if __name__ == '__main__':
    asyncio.run(run_transfer_worker())
