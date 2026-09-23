#!/usr/bin/env python3
"""
tg_query_remote.py — 甲骨云容器内执行，只读直查实时 tg_messages.db

用法（在 media-follow-transfer-resource-monitor-1 容器内运行）：
  python tg_query_remote.py --chats                 # 列出所有群聊及最后活跃
  python tg_query_remote.py --chat "拾光" --hours 1 # 最近1小时指定群消息
  python tg_query_remote.py --chat "拾光" --count 5 # 最近5条
  python tg_query_remote.py --chat "拾光" --since 2026-09-21 --until 2026-09-21
  python tg_query_remote.py --chat "拾光" --search 广告
  python tg_query_remote.py --stats                # 数据库统计

输出统一为北京时间（UTC+8）。
"""
import argparse
import sqlite3
import sys
from datetime import datetime, timedelta, timezone

DB = "/app/data/tg_messages.db"
BJ = timezone(timedelta(hours=8))


def get_db():
    return sqlite3.connect("file:%s?mode=ro" % DB, uri=True)


def fmt(ts):
    if not ts:
        return "?"
    return datetime.fromtimestamp(ts, tz=BJ).strftime("%Y-%m-%d %H:%M:%S")


def cmd_chats(conn):
    rows = conn.execute(
        "SELECT chat_id, chat_title, COUNT(*) n, MAX(date) last FROM messages "
        "GROUP BY chat_id, chat_title ORDER BY last DESC"
    ).fetchall()
    print("%-36s %-20s %-9s %s" % ("群聊名称", "ID", "消息数", "最后活跃(北京)"))
    print("-" * 92)
    for chat_id, title, n, last in rows:
        print("%-36s %-20s %-9d %s" % ((title or "?")[:34], chat_id, n, fmt(last)))


def cmd_query(conn, args):
    where, params = [], []
    if args.chat:
        where.append("(chat_title LIKE ? OR chat_id = ?)")
        params += ["%" + args.chat + "%", args.chat]
    if args.chat_id is not None:
        where.append("chat_id = ?")
        params.append(args.chat_id)
    if args.hours is not None:
        cutoff = datetime.now(timezone.utc).timestamp() - args.hours * 3600
        where.append("date > ?")
        params.append(cutoff)
    if args.since:
        t0 = datetime.strptime(args.since, "%Y-%m-%d").replace(tzinfo=BJ).timestamp()
        where.append("date >= ?")
        params.append(t0)
    if args.until:
        t1 = datetime.strptime(args.until, "%Y-%m-%d").replace(
            tzinfo=BJ, hour=23, minute=59, second=59
        ).timestamp()
        where.append("date <= ?")
        params.append(t1)
    if args.sender:
        where.append("(sender_name LIKE ? OR sender_id = ?)")
        params += ["%" + args.sender + "%", args.sender]
    if args.search:
        where.append("text LIKE ?")
        params.append("%" + args.search + "%")
    cond = (" WHERE " + " AND ".join(where)) if where else ""
    order = " ASC" if args.since else " DESC"
    sql = (
        "SELECT id, chat_id, message_id, sender_id, sender_name, text, date, chat_title "
        "FROM messages" + cond + " ORDER BY date" + order + " LIMIT " + str(int(args.count))
    )
    rows = conn.execute(sql, params).fetchall()
    if not rows:
        print("⚠️ 未找到符合条件的消息")
        return
    print("共返回 %d 条（默认最近优先；--since 按时间升序）：" % len(rows))
    priv = lambda cid: str(cid)[4:] if str(cid).startswith("-100") else str(cid)
    for rid, chat_id, mid, sid, sname, text, ts, ctitle in rows:
        t = fmt(ts)
        txt = (text or "[媒体消息/无文本]").replace("\n", " / ")
        url = "https://t.me/c/%s/%s" % (priv(chat_id), mid)
        print("[%s] %s (uid=%s) @ %s\n     %s\n     %s" % (t, sname or repr(sid), sid, ctitle, txt[:200], url))


def cmd_stats(conn):
    n = conn.execute("SELECT COUNT(*) FROM messages").fetchone()[0]
    last = conn.execute("SELECT MAX(date) FROM messages").fetchone()[0]
    chats = conn.execute("SELECT COUNT(DISTINCT chat_id) FROM messages").fetchone()[0]
    print("总消息数: %d | 群聊数: %d | 最后消息: %s (北京时间)" % (n, chats, fmt(last)))


def main():
    ap = argparse.ArgumentParser(description="查询甲骨云实时 tg_messages.db")
    ap.add_argument("--chats", action="store_true", help="列出所有群聊")
    ap.add_argument("--chat", help="按群名模糊匹配")
    ap.add_argument("--chat_id", type=int, help="按 chat_id 精确匹配")
    ap.add_argument("--hours", type=int, help="最近 N 小时")
    ap.add_argument("--since", help="起始日期 YYYY-MM-DD（同时切换为升序）")
    ap.add_argument("--until", help="截止日期 YYYY-MM-DD")
    ap.add_argument("--count", type=int, default=50, help="最大返回条数")
    ap.add_argument("--search", help="关键词搜索")
    ap.add_argument("--sender", help="按发送者名称/ID 过滤")
    ap.add_argument("--stats", action="store_true", help="数据库统计")
    args = ap.parse_args()

    conn = get_db()
    try:
        if args.chats:
            cmd_chats(conn)
        elif args.stats:
            cmd_stats(conn)
        else:
            cmd_query(conn, args)
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
