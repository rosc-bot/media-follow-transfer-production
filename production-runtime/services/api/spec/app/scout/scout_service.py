import logging
import re

import aiohttp
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.constants import SOURCE_FRAMEHDR, SOURCE_WATCHLIST_SCOUT
from app.ingest.channel_ingest_service import ChannelIngestService
from app.schemas.telegram_source import TelegramSourceMessage
from app.scout.candidate_selector import CandidateSelector
from app.scout.framehdr import FrameHdrService
from app.scout.message_search import MessageSearch

logger = logging.getLogger(__name__)


class ScoutService:
    def __init__(self, search: MessageSearch):
        self.search = search

    async def scout_missing(self, db: AsyncSession, *, tmdb_id: int, title: str, season: int, missing_episodes: list[str], year: int | None = None) -> list[dict]:
        results = []
        still_missing: list[str] = []

        # 1. Local message-store scouting (fast, per-episode LIKE on SQLite)
        for episode_key in missing_episodes:
            candidates = CandidateSelector.select(self.search.search(title, episode_key), title=title, episode_key=episode_key)
            if candidates:
                chosen = candidates[0].message
                source = TelegramSourceMessage(
                    source_type=SOURCE_WATCHLIST_SCOUT,
                    channel_id=chosen.chat_id,
                    channel_title=chosen.chat_title,
                    message_id=chosen.message_id,
                    text=chosen.text,
                    urls=chosen.urls,
                    metadata={'tmdb_id': tmdb_id, 'title': title, 'year': year, 'season': season, 'episode_keys': [episode_key]},
                )
                results.append(await ChannelIngestService.process_source_message(db, source))
            else:
                still_missing.append(episode_key)

        # 2. FrameHDR fallback: ONE batched call for all still-missing episodes
        #    (each search_series call fires up to N HTTP queries, so per-episode
        #    calls blow up to hundreds of requests for season-long gaps).
        if not still_missing:
            return results

        ep_nums = sorted({
            int(m.group(1)) for m in (re.search(r'E(\d+)', k, re.IGNORECASE) for k in still_missing)
            if m
        })

        try:
            fh_results = await FrameHdrService.search_series(
                title=title,
                season=season,
                episodes=ep_nums or None,
                tmdb_id=tmdb_id,
                limit=2,
            )
            matched_set = set(ep_nums)
            for fh in fh_results:
                fh_matched = set(fh.get('matched_episodes') or []) & matched_set
                if not fh_matched:
                    continue
                for ep_num in sorted(fh_matched):
                    episode_key = f'S{season:02d}E{ep_num:02d}'
                    source = TelegramSourceMessage(
                        source_type=SOURCE_FRAMEHDR,
                        channel_id='framehdr',
                        channel_title=fh.get('chat_title') or '帧影分享',
                        message_id=fh.get('msg_id') or 800000001,
                        text=fh.get('snippet') or f'[帧影分享] {title} {episode_key}',
                        urls=[fh['url']] if fh.get('url') else [],
                        metadata={'tmdb_id': tmdb_id, 'title': title, 'year': year, 'season': season, 'episode_keys': [episode_key]},
                    )
                    results.append(await ChannelIngestService.process_source_message(db, source))
        except (TimeoutError, aiohttp.ClientError, RuntimeError, ValueError) as exc:
            logger.warning('FrameHdr search failed for %s S%02d: %s', title, season, exc)

        return results
