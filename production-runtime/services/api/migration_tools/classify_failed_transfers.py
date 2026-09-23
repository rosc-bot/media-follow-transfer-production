"""Read-only re-classification audit of FAILED transfer queue tasks.

Phase 2A does NOT touch the 80 FAILED rows: this tool only re-categorizes them
against the new error taxonomy and reports the distribution. It never updates,
retries, cancels or deletes anything.

Output categories mirror the structured taxonomy:
  AUTH                - token/authorization failures (401, refresh, auth)
  INVALID_RESOURCE    - bad share/resource (share token, invalid share)
  NO_VIDEO            - share without video files
  READBACK            - readback/verification failures
  NETWORK             - timeouts / connect / DNS / network errors
  LEGACY_BROKEN_TASK  - task whose resource row no longer exists
  OTHER               - everything else
"""

from __future__ import annotations

import argparse
import asyncio
import json
import re
from pathlib import Path

from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

_PATTERNS: list[tuple[str, re.Pattern[str]]] = [
    ('AUTH', re.compile(r'(401|unauthorized|auth|token|refresh)', re.IGNORECASE)),
    ('NO_VIDEO', re.compile(r'(no video|video files|\.mkv|subtitle|no transferable)', re.IGNORECASE)),
    ('NETWORK', re.compile(r'(timeout|timed out|connect|dns|network|reset by peer|gaierror)', re.IGNORECASE)),
    ('READBACK', re.compile(r'(readback|verify|verification|expected files)', re.IGNORECASE)),
    ('INVALID_RESOURCE', re.compile(r'(share|access token|invalid|cannot find|not found)', re.IGNORECASE)),
]


def classify_message(message: str) -> str:
    """Classify one error_message into a Phase 2A bucket."""
    text_message = message or ''
    for bucket, pattern in _PATTERNS:
        if pattern.search(text_message):
            return bucket
    return 'OTHER'


async def audit(database_url: str) -> dict:
    engine = create_async_engine(database_url, pool_pre_ping=True)
    try:
        async with engine.connect() as connection:
            await connection.execute(text('SET TRANSACTION READ ONLY'))
            rows = (
                await connection.execute(text("""
                    SELECT t.id, t.error_message, t.resource_id, r.status AS resource_status
                    FROM transfer_queue_tasks t
                    LEFT JOIN resources r ON r.id = t.resource_id
                    WHERE t.status = 'FAILED'
                    ORDER BY t.id
                """))
            ).mappings().all()
            distribution: dict[str, int] = {}
            detail: list[dict] = []
            broken = 0
            for row in rows:
                resource_missing = row['resource_id'] is None or row['resource_status'] is None
                if resource_missing:
                    bucket = 'LEGACY_BROKEN_TASK'
                    broken += 1
                else:
                    bucket = classify_message(row['error_message'] or '')
                distribution[bucket] = distribution.get(bucket, 0) + 1
                detail.append({
                    'task_id': row['id'],
                    'bucket': bucket,
                    'resource_id': row['resource_id'],
                    'resource_status': row['resource_status'],
                    'error_message': (row['error_message'] or '')[:300],
                })
            return {
                'mode': 'dry-run',
                'read_only': True,
                'scope': "only FAILED tasks; nothing is updated",
                'total_failed': len(rows),
                'legacy_broken_tasks': broken,
                'distribution': dict(sorted(distribution.items())),
                'detail': detail,
            }
    finally:
        await engine.dispose()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('--database-url', required=True)
    parser.add_argument('--output', default='-')
    args = parser.parse_args()
    report = asyncio.run(audit(args.database_url))
    rendered = json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + '\n'
    if args.output == '-':
        print(rendered, end='')
    else:
        Path(args.output).write_text(rendered, encoding='utf-8')
        print(json.dumps({
            'output': args.output,
            'total_failed': report['total_failed'],
            'distribution': report['distribution'],
        }, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
