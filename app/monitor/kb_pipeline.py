"""Knowledge Base (人🐔局白嫖资源库) Pipeline for decoupled extraction and indexing."""

from __future__ import annotations

import asyncio
import logging
import re
import sqlite3
import time
import urllib.parse
from datetime import datetime
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

KB_CHAT_ID = -1004495899387

KB_SCHEMA = """
CREATE TABLE IF NOT EXISTS free_resources (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    chat_id INTEGER DEFAULT -1004495899387,
    chat_title TEXT DEFAULT '人🐔局（执着白嫖）',
    message_id INTEGER,
    category TEXT,
    title TEXT,
    url TEXT,
    free_tier TEXT,
    usage_guide TEXT,
    sharer TEXT,
    date REAL,
    source_url TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(chat_id, message_id, url)
);
CREATE VIRTUAL TABLE IF NOT EXISTS free_resources_fts USING fts5(
    title, url, free_tier, usage_guide, category, sharer,
    content='free_resources', content_rowid='id'
);
CREATE TRIGGER IF NOT EXISTS trg_free_resources_ai AFTER INSERT ON free_resources BEGIN
    INSERT INTO free_resources_fts(rowid, title, url, free_tier, usage_guide, category, sharer)
    VALUES (new.id, new.title, new.url, new.free_tier, new.usage_guide, new.category, new.sharer);
END;
CREATE TRIGGER IF NOT EXISTS trg_free_resources_ad AFTER DELETE ON free_resources BEGIN
    INSERT INTO free_resources_fts(free_resources_fts, rowid, title, url, free_tier, usage_guide, category, sharer)
    VALUES('delete', old.id, old.title, old.url, old.free_tier, old.usage_guide, old.category, old.sharer);
END;
CREATE TRIGGER IF NOT EXISTS trg_free_resources_au AFTER UPDATE ON free_resources BEGIN
    INSERT INTO free_resources_fts(free_resources_fts, rowid, title, url, free_tier, usage_guide, category, sharer)
    VALUES('delete', old.id, old.title, old.url, old.free_tier, old.usage_guide, old.category, old.sharer);
    INSERT INTO free_resources_fts(rowid, title, url, free_tier, usage_guide, category, sharer)
    VALUES (new.id, new.title, new.url, new.free_tier, new.usage_guide, new.category, new.sharer);
END;
"""

URL_REGEX = re.compile(r"https?://[^\s<>\"')]+", re.IGNORECASE)

IGNORE_DOMAINS = [
    "t.me/addstickers", "t.me/c/", "t.me/joinchat", "t.me/+",
    "t.me/setlanguage", "t.me/socks?", "t.me/proxy?",
    "douyin.com", "weibo.com", "zhihu.com", "bilibili.com/video/BV",
    "google.com/search", "baidu.com", "bing.com",
]


def clean_url(url: str) -> str:
    return url.rstrip(".,;!?:")


def guess_category_and_info(url: str, text: str, context_texts: list[str] | None = None) -> tuple[str, str, str, str]:
    full_text = text + " " + " ".join(context_texts or [])
    url_lower = url.lower()

    cat = "实用工具与脚本"
    title = ""
    free_tier = ""
    usage_guide = ""

    # 1. GitHub 源码
    if "github.com" in url_lower:
        cat = "源码与项目"
        parts = url.split("github.com/")[-1].split("/")
        if len(parts) >= 2:
            repo_name = f"{parts[0]}/{parts[1]}"
            title = f"GitHub: {repo_name}"
        else:
            title = "GitHub 开源仓库"
        free_tier = "开源免费自托管"
        usage_guide = text.strip() if len(text.strip()) > 5 else "群友分享的开源项目/部署工具"

    # 2. VPS / 节点 / 机场 / 订阅
    elif any(k in full_text.lower() or k in url_lower for k in [
        "vps", "流量", "服务器", "开机", "轻量", "host", "cloud", "sub",
        "节点", "机场", "订阅", "cf", "优选", "dgn", "ovh"
    ]):
        cat = "VPS与网络节点"
        if "dgnlinks.com" in url_lower or "dng" in full_text.lower():
            title = "DNG Cloud 免费 4 个月香港 VPS"
            free_tier = "免费 4 个月 (2核/2G/20G/1T流量)"
            usage_guide = "最低配开机可用4个月，需在网络中手动创建 1T 流量包方可点亮创建按钮。"
        elif "sub.ehb.cc.cd" in url_lower:
            title = "Cloudflare 实时优选 IP 与订阅池"
            free_tier = "全免费无门槛公开接口"
            usage_guide = "直连拉取移动/电信/联通优选IP，配合 CloudflareSpeedTest 自动测速或填入节点 Server 地址。"
        elif "404.do/lhgroup" in url_lower or "腾讯云" in full_text:
            title = "腾讯云轻量服务器限时秒杀"
            free_tier = "¥139/1年 (2核4G5M/500G流量)"
            usage_guide = "单账号限购1台，适合建站或轻量挂机测试。"
        elif "ovheco.com" in url_lower or "ovh" in full_text.lower():
            title = "OVH 官方特价独服/VPS 监控与下单"
            free_tier = "低价特价机监控 (€9.99/月)"
            usage_guide = "实时监控法国/加拿大高性价比独服与低价机房补货。"
        else:
            domain = urllib.parse.urlparse(url).netloc
            title = f"{domain} 节点/VPS 资源"
            free_tier = "群友实测分享"
            usage_guide = text.strip()

    # 3. AI / API / 模型 / 中转
    elif any(k in full_text.lower() or k in url_lower for k in [
        "api", "token", "中转", "gpt", "claude", "gemini", "qwen",
        "倍率", "模型", "对话", "true-sota"
    ]):
        cat = "AI模型与中转"
        if "true-sota.com" in url_lower:
            title = "True-SOTA AI 聚合/模型平台"
            free_tier = "邀请注册/白嫖测试额度"
            usage_guide = "群友 L 分享的 AI 平台注册与接入渠道。"
        else:
            domain = urllib.parse.urlparse(url).netloc
            title = f"{domain} AI API/中转资源"
            free_tier = "注册赠额度 / 低倍率调用"
            usage_guide = text.strip()

    # 4. Emby / 影视服
    elif any(k in full_text.lower() or k in url_lower for k in [
        "emby", "jellyfin", "公益服", "电影", "影视", "开号", "保号"
    ]):
        cat = "公益影视Emby"
        domain = urllib.parse.urlparse(url).netloc
        title = f"{domain} 公益 Emby/影视服务"
        free_tier = "公益免保号/开放注册"
        usage_guide = text.strip()

    else:
        domain = urllib.parse.urlparse(url).netloc
        title = f"{domain} 实用工具"
        free_tier = "免费工具/资源"
        usage_guide = text.strip()

    return cat, title, free_tier, usage_guide


class KBStorage:
    def __init__(self, db_path: str):
        self.db_path = Path(db_path).resolve()
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.init_db()

    def get_conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=15.0)
        conn.row_factory = sqlite3.Row
        return conn

    def init_db(self) -> None:
        try:
            with self.get_conn() as conn:
                conn.executescript(KB_SCHEMA)
        except Exception as e:  # noqa: BLE001
            logger.error("Could not init KB tables/FTS5 in %s: %s", self.db_path, e)

    def process_kb_message(self, chat_id: int, chat_title: str, message_id: int, text: str, sender_name: str | None, date_val: Any) -> int:
        if chat_id != KB_CHAT_ID:
            return 0
        if not text or not ("http://" in text or "https://" in text):
            return 0

        urls = URL_REGEX.findall(text)
        if not urls:
            return 0

        date_ts = date_val.timestamp() if isinstance(date_val, datetime) else (float(date_val) if isinstance(date_val, (int, float)) else time.time())
        source_url = f"https://t.me/c/4495899387/{message_id}"
        inserted = 0

        with self.get_conn() as conn:
            for u in urls:
                u_clean = clean_url(u)
                if any(ign in u_clean.lower() for ign in IGNORE_DOMAINS):
                    continue
                cat, title, free_tier, usage = guess_category_and_info(u_clean, text)
                try:
                    conn.execute(
                        """
                        INSERT INTO free_resources(
                            chat_id, chat_title, message_id, category, title, url,
                            free_tier, usage_guide, sharer, date, source_url
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(chat_id, message_id, url) DO UPDATE SET
                            category=excluded.category,
                            title=excluded.title,
                            free_tier=excluded.free_tier,
                            usage_guide=excluded.usage_guide,
                            sharer=excluded.sharer
                        """,
                        (KB_CHAT_ID, chat_title or "人🐔局（执着白嫖）", message_id, cat, title, u_clean, free_tier, usage, sender_name, date_ts, source_url)
                    )
                    inserted += 1
                except Exception as ex:  # noqa: BLE001
                    logger.warning("KB insert error for url %s: %s", u_clean, ex)
        return inserted


async def kb_worker(
    queue: asyncio.Queue,
    storage: KBStorage,
    stop_event: asyncio.Event | None = None,
) -> None:
    """Worker dedicated to Knowledge Base indexing with absolute fault isolation."""
    logger.info("KBWorker started.")
    while stop_event is None or not stop_event.is_set():
        try:
            item = await asyncio.wait_for(queue.get(), timeout=1.0)
        except TimeoutError:
            continue
        except asyncio.CancelledError:
            break

        try:
            storage.process_kb_message(
                chat_id=item["chat_id"],
                chat_title=item.get("chat_title", ""),
                message_id=item["message_id"],
                text=item.get("text", ""),
                sender_name=item.get("sender_name"),
                date_val=item.get("date"),
            )
        except Exception:
            # Fault isolation: KB failure MUST NOT crash process or affect anything else
            logger.exception("KBWorker error processing item: %s", item)
        finally:
            queue.task_done()
