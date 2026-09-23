#!/usr/bin/env python3
"""
tg_query.py — 查询 Telegram 群聊消息记录，供 Hermes Agent 总结使用。

调用方式（给 Hermes 内部用）：
  python3 tg_query.py --chats                          # 列出所有群聊
  python3 tg_query.py --chat "群名" --hours 24          # 最近24小时的消息
  python3 tg_query.py --chat "群名" --since "2026-08-10" --until "2026-08-12"
  python3 tg_query.py --chat "群名" --count 50          # 最近50条消息
  python3 tg_query.py --chat "群名" --search "关键词"    # 搜索关键词
  python3 tg_query.py --stats                            # 数据库统计
"""
import argparse
import os
import sqlite3
import sys
from datetime import datetime, timedelta, timezone

DEFAULT_DB = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data", "tg_messages.db")
DB_PATH = os.getenv("SUMMARY_DB_PATH", DEFAULT_DB)

BEIJING = timezone(timedelta(hours=8))


def get_db():
    if not os.path.exists(DB_PATH):
        # Check fallback to common paths
        fallbacks = [
            "/home/ubuntu/如昔项目归总/群聊监听服务/data/tg_messages.db",
            "/home/ubuntu/如昔项目归总/影视追新转存一体化/data/tg_messages.db",
            os.path.join(os.path.dirname(os.path.abspath(__file__)), "tg_messages.db"),
        ]
        for fb in fallbacks:
            if os.path.exists(fb):
                conn = sqlite3.connect(fb)
                conn.row_factory = sqlite3.Row
                return conn
        print(f"⚠️ 数据库不存在: {DB_PATH}")
        sys.exit(1)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def list_chats():
    conn = get_db()
    rows = conn.execute(
        "SELECT chat_id, title, kind, last_seen_at, (SELECT COUNT(*) FROM messages WHERE chat_id=c.chat_id) as msg_count "
        "FROM chats c ORDER BY last_seen_at DESC"
    ).fetchall()
    conn.close()
    if not rows:
        print("⚠️ 数据库中没有群聊记录")
        return
    print(f"{'群聊名称':<30} {'ID':<15} {'类型':<10} {'消息数':<8} {'最后活跃'}")
    print("-" * 80)
    for r in rows:
        ts = datetime.fromtimestamp(r["last_seen_at"], tz=BEIJING).strftime("%m-%d %H:%M") if r["last_seen_at"] else "?"
        print(f"{str(r['title'] or '')[:28]:<30} {r['chat_id']:<15} {r['kind'] or ''!s:<10} {r['msg_count']:<8} {ts}")


def query_messages(chat_name=None, chat_id=None, hours=None, since=None, until=None, count=50, search=None, date=None, sender=None):
    conn = get_db()
    where = []
    params = []

    target_chat_id = None
    if chat_id:
        target_chat_id = chat_id
    elif chat_name:
        row = conn.execute("SELECT chat_id, title FROM chats WHERE title LIKE ?", (f"%{chat_name}%",)).fetchone()
        if not row:
            print(f"⚠️ 未找到匹配群聊: {chat_name}")
            conn.close()
            return
        target_chat_id = row["chat_id"]
        print(f"📌 匹配到群聊: {row['title']} (ID: {target_chat_id})")

    if target_chat_id:
        where.append("chat_id = ?")
        params.append(target_chat_id)

    now = datetime.now(BEIJING)
    if hours:
        since_ts = (now - timedelta(hours=hours)).timestamp()
        where.append("date >= ?")
        params.append(since_ts)

    if since:
        dt = datetime.strptime(since, "%Y-%m-%d").replace(tzinfo=BEIJING)
        where.append("date >= ?")
        params.append(dt.timestamp())

    if until:
        dt = datetime.strptime(until, "%Y-%m-%d").replace(hour=23, minute=59, second=59, tzinfo=BEIJING)
        where.append("date <= ?")
        params.append(dt.timestamp())

    if date:
        dt_start = datetime.strptime(date, "%Y-%m-%d").replace(tzinfo=BEIJING)
        dt_end = dt_start + timedelta(days=1)
        where.append("date >= ? AND date < ?")
        params.extend([dt_start.timestamp(), dt_end.timestamp()])

    if search:
        where.append("text LIKE ?")
        params.append(f"%{search}%")

    if sender:
        where.append("sender_name LIKE ?")
        params.append(f"%{sender}%")

    where_clause = " AND ".join(where) if where else "1=1"
    sql = f"SELECT chat_id, chat_title, message_id, sender_name, text, date FROM messages WHERE {where_clause} ORDER BY date DESC LIMIT ?"
    params.append(count)

    rows = conn.execute(sql, params).fetchall()
    conn.close()

    if not rows:
        print("⚠️ 未找到匹配消息")
        return

    print(f"共查询到 {len(rows)} 条消息（按时间倒序）：\n")
    for r in reversed(rows):
        ts = datetime.fromtimestamp(r["date"], tz=BEIJING).strftime("%Y-%m-%d %H:%M:%S") if r["date"] else "?"
        sender_str = r["sender_name"] or "匿名"
        clean_cid = str(r["chat_id"]).replace("-100", "")
        link = f"https://t.me/c/{clean_cid}/{r['message_id']}"
        print(f"[{ts}] {sender_str} ({link}):\n{r['text']}\n")


def show_stats():
    conn = get_db()
    chat_cnt = conn.execute("SELECT COUNT(*) FROM chats").fetchone()[0]
    msg_cnt = conn.execute("SELECT COUNT(*) FROM messages").fetchone()[0]
    first_msg = conn.execute("SELECT MIN(date) FROM messages WHERE date > 0").fetchone()[0]
    last_msg = conn.execute("SELECT MAX(date) FROM messages").fetchone()[0]
    conn.close()

    first_ts = datetime.fromtimestamp(first_msg, tz=BEIJING).strftime("%Y-%m-%d %H:%M") if first_msg else "?"
    last_ts = datetime.fromtimestamp(last_msg, tz=BEIJING).strftime("%Y-%m-%d %H:%M") if last_msg else "?"

    print("📊 Telegram 消息监听数据库统计")
    print("-" * 40)
    print(f"数据库文件: {DB_PATH}")
    print(f"监控群聊数: {chat_cnt}")
    print(f"消息总条数: {msg_cnt}")
    print(f"最早消息:   {first_ts}")
    print(f"最新消息:   {last_ts}")


def main():
    parser = argparse.ArgumentParser(description="Telegram 群聊消息记录查询工具")
    parser.add_argument("--chats", action="store_true", help="列出所有监控的群聊")
    parser.add_argument("--chat", type=str, help="群聊名称（模糊匹配）")
    parser.add_argument("--chat-id", type=int, help="群聊 ID")
    parser.add_argument("--hours", type=float, help="查询最近 N 小时的消息")
    parser.add_argument("--since", type=str, help="起始日期 (YYYY-MM-DD)")
    parser.add_argument("--until", type=str, help="截止日期 (YYYY-MM-DD)")
    parser.add_argument("--date", type=str, help="指定某一天 (YYYY-MM-DD)")
    parser.add_argument("--count", type=int, default=50, help="最多返回条数（默认 50）")
    parser.add_argument("--search", type=str, help="按文本关键词搜索")
    parser.add_argument("--sender", type=str, help="按发送者姓名搜索")
    parser.add_argument("--stats", action="store_true", help="查看数据库统计")

    args = parser.parse_args()

    if args.chats:
        list_chats()
    elif args.stats:
        show_stats()
    elif args.chat or args.chat_id or args.hours or args.since or args.date or args.search or args.sender:
        query_messages(
            chat_name=args.chat,
            chat_id=args.chat_id,
            hours=args.hours,
            since=args.since,
            until=args.until,
            date=args.date,
            count=args.count,
            search=args.search,
            sender=args.sender,
        )
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
