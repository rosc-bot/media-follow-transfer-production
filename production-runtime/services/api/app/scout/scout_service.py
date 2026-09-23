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
from app.transfer.candidate_service import mark_candidate_used, record_candidate

logger = logging.getLogger(__name__)

FRAMEHDR_EPISODE_BATCH_SIZE = 8

#: Structured per-episode Scout outcomes — three LAYERS (Phase 2C).
#: local stage — always recorded, never overwritten by the FrameHDR stage.
LOCAL_MATCH = 'LOCAL_MATCH'
LOCAL_NO_MATCH = 'LOCAL_NO_MATCH'
#: framehdr stage.
FRAMEHDR_MATCH = 'FRAMEHDR_MATCH'
FRAMEHDR_NO_MATCH = 'FRAMEHDR_NO_MATCH'
FRAMEHDR_NOT_ATTEMPTED = 'NOT_ATTEMPTED'
#: final stage — what was actually done with the episode.
FINAL_QUEUED = 'QUEUED'
FINAL_NEEDS_REVIEW = 'NEEDS_REVIEW'
FINAL_NO_RESOURCE = 'NO_RESOURCE'
#: legacy single-value outcome (Phase 2B compatibility).
NEEDS_REVIEW = 'NEEDS_REVIEW'


def _chunks(values: list[int], size: int):
    for index in range(0, len(values), size):
        yield values[index:index + size]


def _empty_stats() -> dict:
    return {
        'target_episodes': 0,
        'local_hit': 0,
        'local_miss': 0,
        'framehdr_attempted': 0,
        'framehdr_hit': 0,
        'framehdr_miss': 0,
        'transfer_eligible': 0,
        'new_queue_tasks_created': 0,
        'deduplicated_existing_tasks': 0,
        'queue_reused': 0,
        'needs_review': 0,
    }


class ScoutService:
    def __init__(self, search: MessageSearch):
        self.search = search

    @staticmethod
    async def _persist_candidate(
        db: AsyncSession,
        *,
        tmdb_id: int,
        title: str,
        season: int,
        episode_key: str,
        share_url: str,
        source_channel_id: str,
        source_message_id: int,
        year: int | None = None,
        source_type: str = 'watchlist_scout',
        resource_id: int | None = None,
        queue_task_id: int | None = None,
    ):
        """Idempotently persist one discovered candidate (Phase 2C §五)."""
        provider = next((p for p in ('guangya', 'quark', 'baidu', '115', 'aliyun', 'tianyi', 'xunlei')
                         if p in share_url.lower()), 'guangya')
        return await record_candidate(
            db,
            tmdb_id=tmdb_id, title=title, year=year, season=season, episode_key=episode_key,
            provider=provider, share_url=share_url,
            source_type=source_type,
            source_channel_id=source_channel_id,
            source_message_id=source_message_id,
            resource_id=resource_id,
            queue_task_id=queue_task_id,
        )

    @staticmethod
    def _apply_ingest_outcome(entry: dict, ingest: dict) -> None:
        """Normalize ingest semantics into unambiguous cycle counters."""
        deduplicated = bool(ingest.get('deduplicated'))
        terminal_dedup = bool(ingest.get('deduplicated_existing_task')) or ingest.get('transfer_status') == 'SKIPPED_DUPLICATE'
        queued_new = bool(ingest.get('queued')) and not deduplicated
        queue_reused = bool(ingest.get('queue_reused'))
        eligible = queued_new or queue_reused
        entry['transfer_eligible'] = eligible
        entry['new_queue_task_created'] = queued_new
        entry['deduplicated_existing_task'] = terminal_dedup or (deduplicated and not queue_reused)
        entry['queue_reused'] = queue_reused
        if terminal_dedup:
            entry['final_status'] = 'SKIPPED_DUPLICATE'
            entry['outcome'] = 'ALREADY_TRANSFERRED'
        elif ingest.get('status') == 'NEEDS_REVIEW':
            entry['final_status'] = FINAL_NEEDS_REVIEW
            entry['outcome'] = NEEDS_REVIEW
        elif eligible:
            entry['final_status'] = FINAL_QUEUED

    async def scout_missing(
        self, db: AsyncSession, *, tmdb_id: int, title: str, season: int,
        missing_episodes: list[str], year: int | None = None,
    ) -> list[dict]:
        """Scout each missing episode; return layered per-episode records.

        Each entry carries THREE independent layers so no stage can mask
        another (Phase 2C fix for the 2B distortion):

        * ``local_status``     — LOCAL_MATCH / LOCAL_NO_MATCH (never overwritten)
        * ``framehdr_status``  — FRAMEHDR_MATCH / FRAMEHDR_NO_MATCH / NOT_ATTEMPTED
        * ``final_status``     — QUEUED / NEEDS_REVIEW / NO_RESOURCE

        Compatibility keys kept for bot handlers: ``outcome`` (best single
        value), ``status``/``queued`` (from ingest).
        """
        results: list[dict] = []
        still_missing: list[dict] = []

        # 1. Local message-store scouting (fast, per-episode search).
        for episode_key in missing_episodes:
            candidates = self.search.search(title, episode_key)
            explanation = CandidateSelector.explain(candidates, title=title, episode_key=episode_key)
            chosen = CandidateSelector.select(candidates, title=title, episode_key=episode_key, limit=1)
            entry: dict = {
                'episode_key': episode_key,
                'candidate_count': explanation['candidate_count'],
                'accepted_count': explanation['accepted_count'],
                'candidate_explanation': explanation,
                'local_status': LOCAL_MATCH if chosen else LOCAL_NO_MATCH,
                'framehdr_status': FRAMEHDR_NOT_ATTEMPTED,
                'final_status': FINAL_NO_RESOURCE,
                'outcome': LOCAL_MATCH if chosen else LOCAL_NO_MATCH,
            }
            # Phase 2C §五: persist EVERY local candidate (not just the chosen
            # URL) — idempotent per tmdb/season/episode/share_hash.
            for candidate in candidates:
                matched_url = getattr(candidate, 'url', None)
                if not matched_url:
                    continue
                await self._persist_candidate(
                    db,
                    tmdb_id=tmdb_id, title=title, year=year, season=season,
                    episode_key=episode_key, share_url=matched_url,
                    source_channel_id=str(getattr(candidate, 'chat_id', '')),
                    source_message_id=int(getattr(candidate, 'message_id', 0) or 0),
                )
            if not chosen:
                still_missing.append(entry)
                results.append(entry)
                continue
            selected = chosen[0].message
            share_url = choice_url = chosen[0].matched_url
            source = TelegramSourceMessage(
                source_type=SOURCE_WATCHLIST_SCOUT,
                channel_id=str(getattr(selected, 'chat_id', '')),
                channel_title=getattr(selected, 'chat_title', None),
                message_id=int(getattr(selected, 'message_id', 0) or 0),
                text=getattr(selected, 'text', ''),
                urls=getattr(selected, 'urls', []),
                metadata={'tmdb_id': tmdb_id, 'title': title, 'year': year, 'season': season,
                          'episode_keys': [episode_key], 'share_url': share_url,
                          'source_channel_id': str(getattr(selected, 'chat_id', '')),
                          'source_message_id': int(getattr(selected, 'message_id', 0) or 0)},
            )
            try:
                ingest = await ChannelIngestService.process_source_message(db, source)
            except Exception as exc:  # noqa: BLE001 - local ingest failures are recorded, never fatal
                logger.warning('Ingest failed for %s %s from %s: %s', title, episode_key, source.channel_id, exc)
                entry.update({
                    'final_status': FINAL_NEEDS_REVIEW,
                    'outcome': NEEDS_REVIEW,
                    'ingest_error': str(exc)[:300],
                })
                results.append(entry)
                continue
            entry.update(ingest)
            self._apply_ingest_outcome(entry, ingest)
            entry['chosen_url'] = choice_url
            # Phase 2C §五: link the chosen candidate to its resource & task.
            if choice_url:
                chosen_candidate = await self._persist_candidate(
                    db,
                    tmdb_id=tmdb_id, title=title, year=year, season=season,
                    episode_key=episode_key, share_url=choice_url,
                    source_channel_id=str(getattr(selected, 'chat_id', '')),
                    source_message_id=int(getattr(selected, 'message_id', 0) or 0),
                    resource_id=ingest.get('resource_id'),
                )
                if ingest.get('queued'):
                    await mark_candidate_used(db, candidate=chosen_candidate)
            results.append(entry)

        # 2. FrameHDR fallback: ONE batched call for all still-missing episodes.
        if not still_missing:
            return results

        ep_nums = sorted({
            int(m.group(1)) for m in (re.search(r'E(\d+)', s['episode_key'], re.IGNORECASE) for s in still_missing)
            if m
        })
        fallback_flags: set[str] = {s['episode_key'] for s in still_missing}

        try:
            for batch in _chunks(ep_nums, FRAMEHDR_EPISODE_BATCH_SIZE):
                fh_results = await FrameHdrService.search_series(
                    title=title,
                    season=season,
                    episodes=batch or None,
                    tmdb_id=tmdb_id,
                    limit=2,
                )
                matched_in_batch: set[int] = set()
                for fh in fh_results:
                    matched_in_batch.update(fh.get('matched_episodes') or [])
                    # Phase 2C §五: persist EVERY FrameHDR share as a candidate.
                    fh_url = fh.get('url')
                    if fh_url:
                        for fh_ep in (fh.get('matched_episodes') or []) or batch:
                            await self._persist_candidate(
                                db,
                                tmdb_id=tmdb_id, title=title, year=year, season=season,
                                episode_key=f'S{season:02d}E{fh_ep:02d}',
                                share_url=fh_url,
                                source_channel_id='framehdr',
                                source_message_id=int(fh.get('msg_id') or 800000001),
                                source_type='framehdr',
                            )
                for missing_entry in still_missing:
                    episode_key = missing_entry['episode_key']
                    m = re.search(r'E(\d+)', episode_key, re.IGNORECASE)
                    ep_num = int(m.group(1)) if m else None
                    if ep_num is None:
                        continue
                    entry = next(s for s in results if s['episode_key'] == episode_key)
                    # Layered framehdr stage — local_status stays untouched.
                    entry['framehdr_status'] = (
                        FRAMEHDR_MATCH if ep_num in matched_in_batch else FRAMEHDR_NO_MATCH
                    )
                    if ep_num not in matched_in_batch:
                        continue
                    fh = next((f for f in fh_results if ep_num in (f.get('matched_episodes') or [])), None)
                    if fh is None:
                        entry['framehdr_status'] = FRAMEHDR_NO_MATCH
                        continue
                    episode_key_full = f'S{season:02d}E{ep_num:02d}'
                    source = TelegramSourceMessage(
                        source_type=SOURCE_FRAMEHDR,
                        channel_id='framehdr',
                        channel_title=fh.get('chat_title') or '帧影分享',
                        message_id=fh.get('msg_id') or 800000001,
                        text=fh.get('snippet') or f'[帧影分享] {title} {episode_key_full}',
                        urls=[fh['url']] if fh.get('url') else [],
                        metadata={'tmdb_id': tmdb_id, 'title': title, 'year': year, 'season': season,
                                  'episode_keys': [episode_key_full], 'share_url': fh.get('url'),
                                  'provider': fh.get('provider') or 'guangya'},
                    )
                    try:
                        ingest = await ChannelIngestService.process_source_message(db, source)
                    except Exception as exc:  # noqa: BLE001
                        logger.warning('FrameHdr ingest failed for %s %s: %s', title, episode_key_full, exc)
                        entry.update({
                            'final_status': FINAL_NEEDS_REVIEW,
                            'outcome': NEEDS_REVIEW,
                            'chosen_url': fh.get('url'),
                            'ingest_error': str(exc)[:300],
                        })
                        continue
                    entry.update(ingest)
                    self._apply_ingest_outcome(entry, ingest)
                    entry['chosen_url'] = fh.get('url')
        except (TimeoutError, aiohttp.ClientError, RuntimeError, ValueError) as exc:
            logger.warning('FrameHdr search failed for %s S%02d: %s', title, season, exc)
            for entry in results:
                if entry['episode_key'] in fallback_flags and entry['framehdr_status'] == FRAMEHDR_NOT_ATTEMPTED:
                    entry['framehdr_status'] = FRAMEHDR_NO_MATCH
                    entry['framehdr_error'] = str(exc)[:300]

        return results

    @staticmethod
    def summarize(results: list[dict]) -> dict:
        """Aggregate layered per-episode records into 8-dimension cycle stats.

        Keys (Phase 2D): targets / local_hit / local_miss /
        framehdr_attempted / framehdr_hit / framehdr_miss / transfer_eligible /
        new_queue_tasks_created / deduplicated_existing_tasks / queue_reused /
        needs_review.  ``local_*`` are read from ``local_status`` so a
        FrameHDR fallback can never zero them (2B bug fixed).
        """
        stats = _empty_stats()
        for entry in results:
            stats['target_episodes'] += 1
            if entry.get('local_status') == LOCAL_MATCH:
                stats['local_hit'] += 1
            elif entry.get('local_status') == LOCAL_NO_MATCH:
                stats['local_miss'] += 1
            fh_status = entry.get('framehdr_status')
            if fh_status in (FRAMEHDR_MATCH, FRAMEHDR_NO_MATCH):
                stats['framehdr_attempted'] += 1
            if fh_status == FRAMEHDR_MATCH:
                stats['framehdr_hit'] += 1
            elif fh_status == FRAMEHDR_NO_MATCH:
                stats['framehdr_miss'] += 1
            if entry.get('transfer_eligible'):
                stats['transfer_eligible'] += 1
            if entry.get('new_queue_task_created'):
                stats['new_queue_tasks_created'] += 1
            if entry.get('deduplicated_existing_task'):
                stats['deduplicated_existing_tasks'] += 1
            if entry.get('queue_reused'):
                stats['queue_reused'] += 1
            if entry.get('final_status') == FINAL_NEEDS_REVIEW:
                stats['needs_review'] += 1
        return stats
