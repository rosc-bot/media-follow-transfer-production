import asyncio
import logging

from app.monitor.telegram_gateway import run_gateway

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
)
logger = logging.getLogger(__name__)


async def main() -> None:
    while True:
        try:
            await run_gateway()
        except Exception as exc:  # noqa: BLE001 - long-lived gateway must retry transport failures.
            logger.error("Telegram gateway encountered error: %s", exc)
        await asyncio.sleep(10)


if __name__ == '__main__':
    asyncio.run(main())
