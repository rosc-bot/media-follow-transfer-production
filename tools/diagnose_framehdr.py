"""FrameHDR diagnostic (read-only) — Phase 2B.

Checks the current FrameHDR integration end-to-end for a small selection of
titles (3 known-popular + 2 currently-followed) WITHOUT changing any database
or trigger any transfer:

  1. login / session validity
  2. search HTTP status
  3. page structure expectation
  4. HTML/card selector parseability
  5. raw result count
  6. title-normalized count
  7. season/episode filtered count
  8. final reason for 0 hits

Never prints cookie values (session is loaded into memory only).

Usage:
  python -m tools.diagnose_framehdr --titles "怪奇物语,斗破苍穹" --season 1
  python -m tools.diagnose_framehdr   # default selection
"""

from __future__ import annotations

import argparse
import asyncio
import json
import re
import sys
import urllib.parse

import aiohttp

from app.core.config import get_settings

DEFAULT_TITLES = [
    ('怪奇物语', 1),
    ('黑袍纠察队', 1),
    ('斗破苍穹', 1),
]


async def probe(title: str, season: int, base_url: str, session: aiohttp.ClientSession) -> dict:
    entry: dict = {
        'title': title,
        'season': season,
        'steps': {},
        'reduced_by': {},
    }
    clean_title = re.sub(r'[^\w\u4e00-\u9fa5]', '', title)
    search_url = f"{base_url.rstrip('/')}/search.php?q={urllib.parse.quote(title)}"
    try:
        async with session.get(search_url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
            html = await resp.text()
            entry['steps']['search_http_status'] = resp.status
    except (TimeoutError, aiohttp.ClientError, OSError) as exc:
        entry['steps']['search_http_status'] = f'error:{type(exc).__name__}'
        entry['final_reason'] = 'search_request_failed'
        return entry

    # Page structure expectations
    entry['steps']['has_search_page_marker'] = 'search.php' in html or 'card' in html.lower()
    card_matches = list(re.finditer(
        r'detail\.php\?id=(\d+).*?<h3[^>]*class=[^>]*card-title[^>]*>([^<]+)</h3>',
        html,
        re.DOTALL,
    ))
    cards = [{'id': m.group(1), 'title': m.group(2).strip()} for m in card_matches]
    entry['steps']['raw_card_count'] = len(cards)
    entry['steps']['card_selector_ok'] = len(cards) > 0 or ('card' in html.lower())

    # title normalized — PRODUCTION matcher (Phase 2C §二: never a second copy)
    from app.scout.framehdr import card_matches_season, card_title_normalized

    c_clean = card_title_normalized(clean_title)
    title_matches = [
        c for c in cards
        if c_clean in card_title_normalized(c['title']) or card_title_normalized(c['title']) in c_clean
    ]
    entry['steps']['title_normalized_count'] = len(title_matches)
    entry['reduced_by']['title_filter'] = len(cards) - len(title_matches)

    # season filtered — the SAME function production search_series() uses
    # (card_matches_season). If production matches, the diagnostic matches.
    season_matches = [c for c in title_matches if card_matches_season(c['title'], season)]
    entry['steps']['season_filtered_count'] = len(season_matches)
    entry['reduced_by']['season_filter'] = len(title_matches) - len(season_matches)
    entry['steps']['season_matcher'] = {
        'shared_with_production': True,
        'matched_card_titles': [c['title'] for c in season_matches][:5],
    }

    if not cards:
        entry['final_reason'] = 'source_no_result' if entry['steps']['search_http_status'] == 200 else 'source_http_error'
    elif not title_matches:
        entry['final_reason'] = 'matcher_excluded_all_titles'
        entry['reduced_by']['all_titles'] = len(cards)
    elif not season_matches:
        entry['final_reason'] = 'matcher_excluded_by_season'
    else:
        entry['final_reason'] = 'matched'
        # detail page probe for the first match
        detail_url = f"{base_url.rstrip('/')}/detail.php?id={season_matches[0]['id']}"
        try:
            async with session.get(detail_url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                d_html = await resp.text()
            entry['steps']['detail_http_status'] = resp.status
            entry['steps']['detail_page_ok'] = 'copyToClipboard' in d_html or 'link-description' in d_html
            entry['steps']['detail_logged_out'] = '_isLoggedIn = false' in d_html
        except (TimeoutError, aiohttp.ClientError, OSError) as exc:
            entry['steps']['detail_http_status'] = f'error:{type(exc).__name__}'
            entry['steps']['detail_page_ok'] = False
        entry['detail_first'] = {k: v for k, v in season_matches[0].items()}

    return entry


async def run(titles: list[tuple[str, int]]) -> dict:
    settings = get_settings()
    report: dict = {
        'enabled': settings.framehdr_enabled,
        'base_url': settings.framehdr_base_url,
        'username': settings.framehdr_username,
        'has_username': bool(settings.framehdr_username),
        'has_password': bool(settings.framehdr_password),
        'cookie_file': settings.framehdr_cookie_file,
        'cookie_output': 'REDACTED',  # deliberate: never dump cookies
        'results': [],
    }
    if not settings.framehdr_enabled or not settings.framehdr_username:
        report['login_status'] = 'disabled_or_no_credentials'
        report['conclusion'] = 'FrameHdr disabled or credentials missing — 0 hits is expected.'
        return report

    headers = {'User-Agent': 'Mozilla/5.0 (compatible; HermesDiagnose/1.0)'}
    async with aiohttp.ClientSession(headers=headers) as session:
        # 1. login/session validity
        from app.scout.framehdr import FrameHdrService
        login_ok = await FrameHdrService.login()
        report['login_status'] = 'ok' if login_ok else 'failed'
        for title, season in titles:
            report['results'].append(await probe(title, season, settings.framehdr_base_url, session))

    report['conclusion'] = (
        'integration healthy' if any(r.get('steps', {}).get('raw_card_count', 0) > 0 for r in report['results'])
        else 'source_or_parser_problem'
    )
    return report


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('--titles', help='comma separated titles')
    parser.add_argument('--season', type=int, default=1)
    args = parser.parse_args()
    if args.titles:
        titles = [(t.strip(), args.season) for t in args.titles.split(',') if t.strip()]
    else:
        titles = DEFAULT_TITLES
    report = asyncio.run(run(titles))
    print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
    sys.exit(0)


if __name__ == '__main__':
    main()
