from app.scout.scout_worker import run_once

if __name__ == '__main__':
    import asyncio
    print(asyncio.run(run_once()))
