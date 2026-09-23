"""Read-only Phase 2D audit for historical missing season data.

Usage: python -m tools.audit_missing_season [--output-dir data]

The database path is read only: this command runs SELECT queries only.  It
writes JSON/Markdown evidence files but never updates business rows.
"""

from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path
from typing import Any

from app.core.database import AsyncSessionLocal
from app.season_audit import audit_missing_seasons


def render_markdown(report: dict[str, Any]) -> str:
    status = report["by_task_status"]
    lines = [
        "# 历史 Season 缺失审计（只读）",
        "",
        "| 指标 | 数量 |",
        "|---|---:|",
        f"| TOTAL_MISSING_SEASON | {report['TOTAL_MISSING_SEASON']} |",
        f"| SAFE_INFER | {report['SAFE_INFER']} |",
        f"| CONFLICT | {report['CONFLICT']} |",
        f"| NO_EVIDENCE | {report['NO_EVIDENCE']} |",
        f"| NEEDS_REVIEW | {report['NEEDS_REVIEW']} |",
        f"| QUEUED 缺季任务 | {status.get('QUEUED', 0)} |",
        f"| RETRY_WAIT 缺季任务 | {status.get('RETRY_WAIT', 0)} |",
        f"| FAILED 缺季任务 | {status.get('FAILED', 0)} |",
        f"| resource_candidates.season IS NULL | {report['resource_candidates_season_null']} |",
        "",
        "## 记录",
        "",
        "| task_id | resource_id | tmdb_id | title | episode_key | inferred_season | confidence | evidence |",
        "|---:|---:|---:|---|---|---:|---|---|",
    ]
    for row in report["records"]:
        evidence = "; ".join(row["evidence"])
        title = str(row.get("title") or "").replace("|", "\\|")
        lines.append(
            f"| {row.get('task_id') or ''} | {row.get('resource_id') or ''} | {row.get('tmdb_id') or ''} | "
            f"{title} | {row.get('episode_key') or ''} | "
            f"{row.get('inferred_season') or ''} | {row['confidence']} | {evidence} |"
        )
    return "\n".join(lines) + "\n"


async def main_async(output_dir: Path) -> dict[str, Any]:
    async with AsyncSessionLocal() as db:
        report = await audit_missing_seasons(db)
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "missing_season_audit.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    )
    (output_dir / "missing_season_audit.md").write_text(render_markdown(report))
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description="Read-only audit of historic missing season values")
    parser.add_argument("--output-dir", type=Path, default=Path("data"))
    args = parser.parse_args()
    report = asyncio.run(main_async(args.output_dir))
    print(json.dumps({key: value for key, value in report.items() if key != "records"}, ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
