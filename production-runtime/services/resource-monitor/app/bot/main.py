"""
影视追新机器人 — 完整交互 Bot（从旧项目移植 + 新增手动控制开关）

功能清单:
  /start /help   — 主菜单 Inline Keyboard（9 大功能入口）
  /calendar      — 追剧日历 8 分类导航 + 分页
  /radar         — 缺集雷达看板 (LATEST/FULL) + 忽略管理
  /follow        — 追更清单 (分页 + 模式切换 + 删除)
  /hot           — 热播自动追新管理面板
  /queue         — 实时转存队列看板
  /scan          — 扫描网盘物理文件
  /sync          — 全库一键打捞
  /help          — 使用说明

新增:
  手动控制开关 — 全局暂停/恢复追更与转存
"""

from __future__ import annotations

import asyncio
import hashlib
import html as _html
import json
import logging
import re
from datetime import datetime
from typing import Any

from aiogram import Bot, Dispatcher, F, Router, types
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import BotCommand, BotCommandScopeAllPrivateChats, BotCommandScopeChat
from aiogram.utils.keyboard import InlineKeyboardBuilder
from sqlalchemy import delete, func, select

from app.core.config import get_settings
from app.core.database import AsyncSessionLocal
from app.core.logging import configure_logging
from app.follow.bot_settings_service import BotSettingsService
from app.follow.follow_mode import FULL, LATEST, normalize_follow_mode
from app.follow.missing_episode_service import MissingEpisodeService
from app.follow.radar_service import RadarService
from app.follow.watchlist_service import WatchlistService
from app.models.auto_ingest_history import AutoIngestHistory
from app.models.cloud import CloudDiskInventory
from app.models.ignored_missing import IgnoredMissing
from app.models.resource import Resource
from app.models.transfer import TransferQueueTask
from app.models.watchlist import SeriesWatchlist

logger = logging.getLogger(__name__)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Callback payload cache (callback_data ≤ 64 bytes)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
CB_PAYLOAD_CACHE: dict[str, dict[str, Any]] = {}


def register_cb_payload(prefix: str, data: dict[str, Any]) -> str:
    serialized = json.dumps(data, sort_keys=True, ensure_ascii=True)
    h = hashlib.md5(serialized.encode("utf-8")).hexdigest()[:8]
    CB_PAYLOAD_CACHE[h] = data
    return f"{prefix}:{h}"


def get_cb_payload(key: str) -> dict[str, Any] | None:
    return CB_PAYLOAD_CACHE.get(key)


def escape(text: Any) -> str:
    return _html.escape(str(text or ""))


class SearchFSM(StatesGroup):
    waiting_for_query = State()


def _is_admin(user_id: int) -> bool:
    admin = get_settings().admin_tg_id
    return admin is None or user_id == admin


async def _safe_edit(msg: types.Message, text: str, markup=None, **kw):
    try:
        await msg.edit_text(text, reply_markup=markup, parse_mode="HTML", **kw)
    except Exception:
        await msg.answer(text, reply_markup=markup, parse_mode="HTML", **kw)


ALL_AUTO_INGEST_CATS = {
    "domestic": "🇨🇳 国产剧集",
    "anime": "🌸 动画新番",
    "western": "🇺🇸 欧美剧集",
    "jp-kr": "🇯🇵 日韩剧集",
    "movie": "🎬 电影上映",
}


async def _main_menu_content(db):
    follow_paused = await BotSettingsService.is_follow_paused(db)
    transfer_paused = await BotSettingsService.is_transfer_paused(db)
    pause_badge = " · ".join(filter(None, [
        "⏸️ 追新已暂停" if follow_paused else "",
        "⏸️ 转存已暂停" if transfer_paused else "",
    ]))
    text = (
        "👋 <b>爸爸好！欢迎使用【影视追新机器人】！</b> ✨\n\n"
        "小助手专为您的私人影视库打造，提供每日追剧排期、缺集智能诊断、锁定影视群聊/频道资源打捞以及网盘全自动转存！\n\n"
        "💡 <b>核心功能指南：</b>\n"
        "• <b>📅 追剧日历：</b>数字影视全量 15 大分类每日排期，支持一键追更\n"
        "• <b>📡 缺集雷达：</b>严谨对齐 TMDB 分季结构，智能识别缺集、断档与追新\n"
        "• <b>📺 追更清单：</b>管理在追影视，支持【仅追最新】与【全量补齐】\n"
        "• <b>🔄 自动转存：</b>缺集命中后自动送入转存队列，Emby 规范化扫库！"
    )
    if pause_badge:
        text += f"\n\n⚠️ <b>{pause_badge}</b>"
    builder = InlineKeyboardBuilder()
    builder.row(
        types.InlineKeyboardButton(text="📅 追剧日历 (分类导航)", callback_data="calendar_menu"),
        types.InlineKeyboardButton(text="📡 缺集与追新雷达", callback_data="menu:radar:LATEST"),
    )
    builder.row(
        types.InlineKeyboardButton(text="📺 我的追更清单", callback_data="menu:my_follow"),
        types.InlineKeyboardButton(text="🔥 热播自动追新", callback_data="menu:auto_ingest"),
    )
    builder.row(
        types.InlineKeyboardButton(text="🔍 搜索剧名加追", callback_data="menu:search_prompt"),
        types.InlineKeyboardButton(text="🔍 扫描网盘物理文件", callback_data="menu:scan_cloud"),
    )
    builder.row(
        types.InlineKeyboardButton(text="⚡ 实时转存队列", callback_data="menu:queue_status"),
        types.InlineKeyboardButton(text="🔄 全库打捞资源", callback_data="menu:sync_now"),
    )
    follow_action = "▶️ 恢复追新" if follow_paused else "⏸️ 暂停追新"
    transfer_action = "▶️ 恢复转存" if transfer_paused else "⏸️ 暂停转存"
    builder.row(
        types.InlineKeyboardButton(
            text=follow_action,
            callback_data="menu:follow_resume" if follow_paused else "menu:follow_pause",
        ),
        types.InlineKeyboardButton(
            text=transfer_action,
            callback_data="menu:transfer_resume" if transfer_paused else "menu:transfer_pause",
        ),
    )
    builder.row(types.InlineKeyboardButton(text="⚙️ 追更管理", callback_data="menu:manage"))
    return text, builder.as_markup()


def build_dispatcher() -> Dispatcher:
    dp = Dispatcher()
    router = Router(name="media_follow_transfer")

    # ── /start /help ──
    @router.message(CommandStart())
    @router.message(Command("help"))
    async def cmd_start(message: types.Message, state: FSMContext) -> None:
        await state.clear()
        async with AsyncSessionLocal() as db:
            text, markup = await _main_menu_content(db)
        await message.answer(text, reply_markup=markup, parse_mode="HTML")

    @router.callback_query(F.data == "menu:overview")
    async def cb_overview(call: types.CallbackQuery) -> None:
        await call.answer()
        async with AsyncSessionLocal() as db:
            text, markup = await _main_menu_content(db)
        await _safe_edit(call.message, text, markup)

    # ── 手动控制开关 ──
    @router.callback_query(F.data == "menu:global_pause_toggle")
    async def cb_legacy_global_pause(call: types.CallbackQuery) -> None:
        """Fail closed for old inline keyboards that still carry this callback."""
        if not _is_admin(call.from_user.id):
            await call.answer("⛔ 仅管理员可操作", show_alert=True)
            return
        async with AsyncSessionLocal() as db:
            await BotSettingsService.set(db, "global_pause", "1")
            await BotSettingsService.set_follow_paused(db, True)
            await BotSettingsService.set_transfer_paused(db, True)
            await db.commit()
        await call.answer("旧全局按钮已安全降级：追新与转存均保持暂停。", show_alert=True)
        async with AsyncSessionLocal() as db:
            text, markup = await _main_menu_content(db)
        await _safe_edit(call.message, text, markup)

    async def _change_worker_pause(call: types.CallbackQuery, *, worker: str, paused: bool) -> None:
        if not _is_admin(call.from_user.id):
            await call.answer("⛔ 仅管理员可操作", show_alert=True)
            return
        async with AsyncSessionLocal() as db:
            # A targeted resume explicitly lifts the compatibility gate, while the
            # other worker retains its own persisted pause state.
            if not paused:
                await BotSettingsService.set(db, "global_pause", "0")
            if worker == "follow":
                await BotSettingsService.set_follow_paused(db, paused)
                label = "追新"
            else:
                await BotSettingsService.set_transfer_paused(db, paused)
                label = "转存"
            await db.commit()
        await call.answer(f"{'⏸️ 已暂停' if paused else '▶️ 已恢复'}{label}", show_alert=True)
        async with AsyncSessionLocal() as db:
            text, markup = await _main_menu_content(db)
        await _safe_edit(call.message, text, markup)

    @router.callback_query(F.data == "menu:follow_pause")
    async def cb_follow_pause(call: types.CallbackQuery) -> None:
        await _change_worker_pause(call, worker="follow", paused=True)

    @router.callback_query(F.data == "menu:follow_resume")
    async def cb_follow_resume(call: types.CallbackQuery) -> None:
        await _change_worker_pause(call, worker="follow", paused=False)

    @router.callback_query(F.data == "menu:transfer_pause")
    async def cb_transfer_pause(call: types.CallbackQuery) -> None:
        await _change_worker_pause(call, worker="transfer", paused=True)

    @router.callback_query(F.data == "menu:transfer_resume")
    async def cb_transfer_resume(call: types.CallbackQuery) -> None:
        await _change_worker_pause(call, worker="transfer", paused=False)

    @router.message(Command("pause"))
    async def cmd_pause(message: types.Message) -> None:
        if not _is_admin(message.from_user.id):
            await message.answer("⛔ 仅管理员可以暂停追更与转存。")
            return
        async with AsyncSessionLocal() as db:
            await BotSettingsService.set(db, "global_pause", "1")
            await BotSettingsService.set_follow_paused(db, True)
            await BotSettingsService.set_transfer_paused(db, True)
            await db.commit()
        await message.answer("⏸️ <b>已暂停追更与转存</b>\nWorker 不会再领取新的转存任务，也不会执行新的巡更打捞。", parse_mode="HTML")

    @router.message(Command("resume"))
    async def cmd_resume(message: types.Message) -> None:
        if not _is_admin(message.from_user.id):
            await message.answer("⛔ 仅管理员可以恢复追更与转存。")
            return
        async with AsyncSessionLocal() as db:
            # Legacy /resume only lifts the compatibility gate.  Individual worker
            # gates are intentionally left unchanged so this command cannot start
            # cloud writes unexpectedly after the split-pause migration.
            await BotSettingsService.set(db, "global_pause", "0")
            await db.commit()
        await message.answer(
            "ℹ️ <b>全局兼容暂停已解除</b>\n"
            "追新和转存仍由各自暂停开关保护；请通过 /settings 分别确认与恢复。",
            parse_mode="HTML",
        )

    # ── 追剧日历 ──
    @router.message(Command("calendar"))
    async def cmd_calendar(message: types.Message) -> None:
        await _show_calendar_categories(message)

    @router.callback_query(F.data == "calendar_menu")
    async def cb_calendar_menu(call: types.CallbackQuery) -> None:
        await call.answer()
        await _show_calendar_categories(call)

    async def _show_calendar_categories(event):
        text = "📅 <b>追剧日历 · 分类导航</b>\n\n选择一个分类查看今日排期与近日上新：\n"
        builder = InlineKeyboardBuilder()
        builder.row(
            types.InlineKeyboardButton(text="⭐ 我的在追", callback_data="cal_view:my_following:0:1"),
            types.InlineKeyboardButton(text="📺 国产剧集", callback_data="cal_view:domestic:0:1"),
        )
        builder.row(
            types.InlineKeyboardButton(text="🎌 动漫番剧", callback_data="cal_view:anime:0:1"),
            types.InlineKeyboardButton(text="🎬 欧美剧集", callback_data="cal_view:western:0:1"),
        )
        builder.row(
            types.InlineKeyboardButton(text="🌸 日韩剧集", callback_data="cal_view:jp-kr:0:1"),
            types.InlineKeyboardButton(text="🎥 院线电影", callback_data="cal_view:movie:0:1"),
        )
        builder.row(
            types.InlineKeyboardButton(text="🎪 热门综艺", callback_data="cal_view:reality:0:1"),
            types.InlineKeyboardButton(text="🌍 纪录大片", callback_data="cal_view:documentary:0:1"),
        )
        builder.row(types.InlineKeyboardButton(text="🔙 返回主菜单", callback_data="menu:overview"))
        if isinstance(event, types.CallbackQuery):
            await _safe_edit(event.message, text, builder.as_markup())
        else:
            await event.answer(text, reply_markup=builder.as_markup(), parse_mode="HTML")

    @router.callback_query(F.data.startswith("cal_view:"))
    async def cb_calendar_view(call: types.CallbackQuery) -> None:
        await call.answer()
        parts = call.data.split(":")
        cat_key = parts[1] if len(parts) > 1 else "domestic"
        day_offset = int(parts[2]) if len(parts) > 2 and parts[2].lstrip('-').isdigit() else 0
        page = int(parts[3]) if len(parts) > 3 and parts[3].isdigit() else 1
        try:
            from app.bot.calendar_data import CalendarService as CalData
            data = await CalData.get_category_schedule(cat_key, day_offset)
        except Exception as exc:
            logger.warning("Calendar data fetch failed: %s", exc)
            await call.message.answer(f"⚠️ 日历数据获取失败: {escape(str(exc))}", parse_mode="HTML")
            return
        shows = data.get("shows", [])
        day_text = data.get("day_text", "")
        cat_name = data.get("cat_name", cat_key)
        PAGE_SIZE = 8
        total_pages = max(1, (len(shows) + PAGE_SIZE - 1) // PAGE_SIZE)
        page = max(1, min(page, total_pages))
        page_shows = shows[(page - 1) * PAGE_SIZE: page * PAGE_SIZE]
        action_type = "follow" if cat_key == "my_following" else "scout"
        text = f"📅 <b>{escape(cat_name)}</b>\n📆 <b>{escape(day_text)}</b>\n\n"
        builder = InlineKeyboardBuilder()
        if not shows:
            text += "<i>该分类今日暂无排期数据。</i>"
        else:
            for idx, show in enumerate(page_shows, start=(page - 1) * PAGE_SIZE + 1):
                title = show.get("title", "")
                sea = show.get("season", 1)
                ep_disp = show.get("ep_display", "")
                prem_tag = " 🆕首播" if show.get("is_premiere") else ""
                text += f"{idx}. <b>《{escape(title)}》</b> 第 {sea} 季 <code>{escape(ep_disp)}</code>{prem_tag}\n"
                if action_type == "scout":
                    cb_key = register_cb_payload("sq", {
                        "title": title,
                        "season": sea,
                        "tmdb_id": show.get("tmdb_id"),
                        "target_eps": list(show.get("episodes") or []),
                    })
                    builder.button(text=f"🔍 打捞《{title[:6]}》", callback_data=cb_key)
                else:
                    cb_key = register_cb_payload("fq", {"title": title, "season": sea, "poster": show.get("poster")})
                    builder.button(text=f"➕ 追更《{title[:6]}》", callback_data=cb_key)
            builder.adjust(2)
        nav = []
        if day_offset > -7:
            nav.append(types.InlineKeyboardButton(text="⬅️ 前一天", callback_data=f"cal_view:{cat_key}:{day_offset - 1}:1"))
        if day_offset < 7:
            nav.append(types.InlineKeyboardButton(text="后一天 ➡️", callback_data=f"cal_view:{cat_key}:{day_offset + 1}:1"))
        if nav:
            builder.row(*nav)
        if total_pages > 1:
            pnav = []
            if page > 1:
                pnav.append(types.InlineKeyboardButton(text="◀️", callback_data=f"cal_view:{cat_key}:{day_offset}:{page - 1}"))
            pnav.append(types.InlineKeyboardButton(text=f"{page}/{total_pages}", callback_data="noop"))
            if page < total_pages:
                pnav.append(types.InlineKeyboardButton(text="▶️", callback_data=f"cal_view:{cat_key}:{day_offset}:{page + 1}"))
            builder.row(*pnav)
        builder.row(
            types.InlineKeyboardButton(text="📅 返回日历分类", callback_data="calendar_menu"),
            types.InlineKeyboardButton(text="🔙 返回主菜单", callback_data="menu:overview"),
        )
        await _safe_edit(call.message, text, builder.as_markup())

    # ── 缺集雷达 ──
    @router.message(Command("radar"))
    async def cmd_radar(message: types.Message) -> None:
        await _show_radar(message, "LATEST")

    @router.callback_query(F.data.startswith("menu:radar:"))
    async def cb_radar(call: types.CallbackQuery) -> None:
        await call.answer()
        mode = call.data.split(":")[-1]
        await _show_radar(call, mode)

    async def _show_radar(event, mode):
        settings = get_settings()
        async with AsyncSessionLocal() as db:
            rows = await WatchlistService.list_following(db)
            radar = await RadarService.build(
                db,
                rows,
                follow_mode=mode,
                recent_limit=settings.follow_recent_episode_window,
            )
        relevant = [item for item in radar if item["missing_episodes"]]
        mode_badge = "⚡ 仅追最新" if mode == "LATEST" else "🔄 全量补齐"
        alt_mode = "FULL" if mode == "LATEST" else "LATEST"
        alt_badge = "🔄 切换为全量补齐" if mode == "LATEST" else "⚡ 切换为仅追最新"
        if not relevant:
            text = f"📡 <b>缺集与追新雷达看板</b> · <code>{mode_badge}</code>\n\n✅ 所有已播集均已收集，或已主动忽略。暂无缺集！"
        else:
            text = f"📡 <b>缺集与追新雷达看板</b> · <code>{mode_badge}</code>\n📊 检测到 <b>{len(relevant)}</b> 部剧集存在缺集：\n\n"
            for idx, item in enumerate(relevant[:15], start=1):
                eps_display = ", ".join(item["missing_episodes"][:8])
                if len(item["missing_episodes"]) > 8:
                    eps_display += f" ... (共{len(item['missing_episodes'])}集)"
                text += f"{idx}. <b>《{escape(item['title'])}》</b> S{item['season']:02d}\n   └ 缺集：<code>{eps_display}</code>\n"
        builder = InlineKeyboardBuilder()
        for item in relevant[:10]:
            sq_key = register_cb_payload("sq", {
                "title": item["title"],
                "season": item["season"],
                "tmdb_id": item["tmdb_id"],
                "target_eps": [int(e.split("E")[1]) for e in item["missing_episodes"]],
            })
            ign_key = register_cb_payload("ign", {"act": "menu", "title": item["title"], "season": item["season"],
                                                    "missing": [int(e.split("E")[1]) for e in item["missing_episodes"]],
                                                    "mode": mode, "page": 0})
            builder.row(
                types.InlineKeyboardButton(text=f"🔍 打捞《{item['title'][:6]}》", callback_data=sq_key),
                types.InlineKeyboardButton(text="⚙️ 缺集管理", callback_data=ign_key),
            )
        builder.row(types.InlineKeyboardButton(text=alt_badge, callback_data=f"menu:radar:{alt_mode}"))
        builder.row(
            types.InlineKeyboardButton(text="📋 已忽略缺集清单", callback_data="menu:ignored_list"),
            types.InlineKeyboardButton(text="🔙 返回主菜单", callback_data="menu:overview"),
        )
        if isinstance(event, types.CallbackQuery):
            await _safe_edit(event.message, text, builder.as_markup())
        else:
            await event.answer(text, reply_markup=builder.as_markup(), parse_mode="HTML")

    # ── 缺集忽略管理 ──
    @router.callback_query(F.data.startswith("ign:"))
    async def cb_ignore_handler(call: types.CallbackQuery) -> None:
        key = call.data[4:]
        payload = get_cb_payload(key)
        if not payload:
            await call.answer("⚠️ 按钮已过期，请重新打开菜单。", show_alert=True)
            return
        act = payload.get("act", "")
        title = payload.get("title", "")
        season = int(payload.get("season") or 1)
        mode = payload.get("mode", "LATEST")
        if act == "single":
            ep = payload.get("ep", 0)
            async with AsyncSessionLocal() as db:
                db.add(IgnoredMissing(title=title, season=season, episode=ep))
                try: await db.commit()
                except Exception: await db.rollback()
            await call.answer(f"已忽略 E{ep:02d}", show_alert=True)
            missing = [m for m in payload.get("missing", []) if m != ep]
            await _show_ignore_menu(call, title, season, missing, mode, payload.get("page", 0))
        elif act == "ignore_page":
            eps = payload.get("eps", [])
            async with AsyncSessionLocal() as db:
                for ep in eps:
                    db.add(IgnoredMissing(title=title, season=season, episode=ep))
                try: await db.commit()
                except Exception: await db.rollback()
            await call.answer(f"已忽略本页 {len(eps)} 集", show_alert=True)
            missing = [m for m in payload.get("missing", []) if m not in eps]
            await _show_ignore_menu(call, title, season, missing, mode, payload.get("page", 0))
        elif act == "all":
            async with AsyncSessionLocal() as db:
                db.add(IgnoredMissing(title=title, season=season, episode=0))
                try: await db.commit()
                except Exception: await db.rollback()
            await call.answer(f"已关闭《{title}》全季缺集提醒", show_alert=True)
            await _show_radar(call, mode)
        elif act == "unignore_all":
            async with AsyncSessionLocal() as db:
                await db.execute(delete(IgnoredMissing).where(IgnoredMissing.title == title, IgnoredMissing.season == season))
                await db.commit()
            await call.answer(f"已恢复《{title}》监控", show_alert=True)
            call.data = f"menu:ignored_list:{payload.get('list_page', 0)}"
            await cb_ignored_list(call)
        elif act == "clear_all":
            async with AsyncSessionLocal() as db:
                await db.execute(delete(IgnoredMissing))
                await db.commit()
            await call.answer("已清空所有忽略规则", show_alert=True)
            call.data = "menu:ignored_list"
            await cb_ignored_list(call)
        elif act == "menu":
            await call.answer()
            await _show_ignore_menu(call, title, season, payload.get("missing", []), mode, payload.get("page", 0))
        else:
            await call.answer()

    async def _show_ignore_menu(call, title, season, missing, mode, page):
        page_size = 10
        total_pages = max(1, (len(missing) + page_size - 1) // page_size if missing else 1)
        page = max(0, min(page, total_pages - 1))
        cur_eps = missing[page * page_size: (page + 1) * page_size]
        sorted_all = sorted(missing)
        if len(sorted_all) <= 10:
            all_preview = ", ".join(f"E{e:02d}" for e in sorted_all)
        elif sorted_all == list(range(min(sorted_all), max(sorted_all) + 1)):
            all_preview = f"E{min(sorted_all):02d}~E{max(sorted_all):02d} (共{len(sorted_all)}集)"
        else:
            all_preview = ", ".join(f"E{e:02d}" for e in sorted_all[:5]) + f"... (共{len(sorted_all)}集)"
        text = (
            f"⚙️ <b>《{escape(title)}》第 {season} 季 · 缺集管理</b>\n\n"
            f"📌 <b>待补清单：</b><code>{all_preview}</code>\n"
            f"📄 第 {page + 1}/{total_pages} 页\n\n"
            "💡 <i>点击下方按钮忽略对应集数：</i>"
        )
        builder = InlineKeyboardBuilder()
        if len(cur_eps) > 1:
            scout_k = register_cb_payload("sq", {"title": title, "season": season, "target_eps": cur_eps})
            ign_k = register_cb_payload("ign", {"act": "ignore_page", "title": title, "season": season, "eps": cur_eps, "missing": missing, "mode": mode, "page": page})
            builder.row(
                types.InlineKeyboardButton(text=f"🔥 打捞本页 ({len(cur_eps)}集)", callback_data=scout_k),
                types.InlineKeyboardButton(text=f"🚫 忽略本页 ({len(cur_eps)}集)", callback_data=ign_k),
            )
        for ep in cur_eps:
            k = register_cb_payload("ign", {"act": "single", "title": title, "season": season, "ep": ep, "mode": mode, "page": page, "missing": missing})
            builder.button(text=f"🚫 忽略 E{ep:02d}", callback_data=k)
        builder.adjust(2)
        nav_btns = []
        if page > 0:
            nav_btns.append(types.InlineKeyboardButton(text="⬅️ 上一页", callback_data=register_cb_payload("ign", {"act": "menu", "title": title, "season": season, "missing": missing, "mode": mode, "page": page - 1})))
        if page < total_pages - 1:
            nav_btns.append(types.InlineKeyboardButton(text="下一页 ➡️", callback_data=register_cb_payload("ign", {"act": "menu", "title": title, "season": season, "missing": missing, "mode": mode, "page": page + 1})))
        if nav_btns:
            builder.row(*nav_btns)
        builder.row(types.InlineKeyboardButton(text="🛑 关闭全季缺集提醒", callback_data=register_cb_payload("ign", {"act": "all", "title": title, "season": season, "mode": mode})))
        builder.row(types.InlineKeyboardButton(text="🔙 返回缺集雷达", callback_data=f"menu:radar:{mode}"))
        await _safe_edit(call.message, text, builder.as_markup())

    # ── 已忽略缺集清单 ──
    @router.callback_query(F.data.startswith("menu:ignored_list"))
    async def cb_ignored_list(call: types.CallbackQuery) -> None:
        parts = call.data.split(":")
        page = int(parts[2]) if len(parts) > 2 and parts[2].isdigit() else 0
        async with AsyncSessionLocal() as db:
            items = list((await db.scalars(select(IgnoredMissing).order_by(IgnoredMissing.title))).all())
        if not items:
            builder = InlineKeyboardBuilder()
            builder.row(types.InlineKeyboardButton(text="🔙 返回缺集雷达", callback_data="menu:radar:LATEST"))
            await _safe_edit(call.message, "📋 <b>已关闭/忽略缺集清单</b>\n\n目前暂无忽略规则。", builder.as_markup())
            return
        grouped: dict[tuple[str, int], list[int]] = {}
        for it in items:
            grouped.setdefault((it.title, it.season), []).append(it.episode)
        groups = sorted(grouped.items())
        page_size = 12
        total_pages = max(1, (len(groups) + page_size - 1) // page_size)
        page = max(0, min(page, total_pages - 1))
        page_groups = groups[page * page_size: (page + 1) * page_size]
        text = f"📋 <b>已忽略缺集清单</b> ({page + 1}/{total_pages} 页)\n\n"
        builder = InlineKeyboardBuilder()
        for (t, s), eps in page_groups:
            ep_desc = "整季" if 0 in eps else "已忽略 " + ", ".join(f"E{e:02d}" for e in sorted(set(eps))[:6])
            text += f" • <b>《{escape(t)}》</b> S{s:02d} ➔ <code>{ep_desc}</code>\n"
            builder.button(text=f"🔄 恢复《{t[:6]}》S{s}", callback_data=register_cb_payload("ign", {"act": "unignore_all", "title": t, "season": s, "list_page": page}))
        builder.adjust(1)
        nav = []
        if page > 0:
            nav.append(types.InlineKeyboardButton(text="⬅️", callback_data=f"menu:ignored_list:{page - 1}"))
        if page < total_pages - 1:
            nav.append(types.InlineKeyboardButton(text="➡️", callback_data=f"menu:ignored_list:{page + 1}"))
        if nav:
            builder.row(*nav)
        builder.row(types.InlineKeyboardButton(text="🗑️ 清空所有忽略规则", callback_data=register_cb_payload("ign", {"act": "clear_all"})))
        builder.row(types.InlineKeyboardButton(text="🔙 返回缺集雷达", callback_data="menu:radar:LATEST"))
        await _safe_edit(call.message, text, builder.as_markup())

    # ── 追更清单 ──
    @router.message(Command("follow"))
    @router.callback_query(F.data.startswith("menu:my_follow"))
    async def show_my_follow(event: types.Message | types.CallbackQuery) -> None:
        page = 1
        if isinstance(event, types.CallbackQuery):
            await event.answer()
            parts = event.data.split(":")
            if len(parts) >= 3 and parts[2].isdigit():
                page = int(parts[2])
        settings = get_settings()
        async with AsyncSessionLocal() as db:
            subs = await WatchlistService.list_following(db)
            radar = await RadarService.build(
                db,
                subs,
                recent_limit=settings.follow_recent_episode_window,
            )
        missing_by_id = {item["watchlist_id"]: item["missing_episodes"] for item in radar}
        PAGE_SIZE = 6
        total_pages = max(1, (len(subs) + PAGE_SIZE - 1) // PAGE_SIZE)
        page = max(1, min(page, total_pages))
        text = f"📺 <b>我的追剧清单</b>\n\n共 <b>{len(subs)}</b> 部 (第 {page}/{total_pages} 页)\n──────────────────────\n"
        builder = InlineKeyboardBuilder()
        if not subs:
            text += "<i>暂无追更剧集，去日历或搜索添加吧~</i>"
        else:
            start = (page - 1) * PAGE_SIZE
            for idx, sub in enumerate(subs[start:start + PAGE_SIZE], start=start + 1):
                fm = normalize_follow_mode(sub.follow_mode)
                mb = "⚡ 仅追最新" if fm == LATEST else "🔥 全量补齐"
                collected_count = len(MissingEpisodeService._canonical_collected_keys(sub))
                missing_count = len(missing_by_id.get(sub.id, []))
                aired = sub.last_aired_episode or sub.total_episodes or 0
                text += (f"{idx}. <b>《{escape(sub.title)}》</b> S{sub.season:02d}\n"
                         f"   └ 已播{aired} | 已收{collected_count} | 缺失{missing_count} | <code>{mb}</code>\n")
                builder.row(
                    types.InlineKeyboardButton(text=f"⚙️ 管理《{sub.title[:6]}》", callback_data=f"sub_detail:{sub.id}:{page}"),
                    types.InlineKeyboardButton(text="🗑️ 取消追更", callback_data=f"sub_del:{sub.id}:{page}"),
                )
            if total_pages > 1:
                nr = []
                if page > 1: nr.append(types.InlineKeyboardButton(text="◀️", callback_data=f"menu:my_follow:{page-1}"))
                nr.append(types.InlineKeyboardButton(text=f"{page}/{total_pages}", callback_data="noop"))
                if page < total_pages: nr.append(types.InlineKeyboardButton(text="▶️", callback_data=f"menu:my_follow:{page+1}"))
                builder.row(*nr)
        builder.row(
            types.InlineKeyboardButton(text="📅 追剧日历", callback_data="calendar_menu"),
            types.InlineKeyboardButton(text="📡 缺集雷达", callback_data="menu:radar:LATEST"),
        )
        builder.row(
            types.InlineKeyboardButton(text="🔍 搜索剧名", callback_data="menu:search_prompt"),
            types.InlineKeyboardButton(text="🔙 返回主菜单", callback_data="menu:overview"),
        )
        if isinstance(event, types.CallbackQuery):
            await _safe_edit(event.message, text, builder.as_markup())
        else:
            await event.answer(text, reply_markup=builder.as_markup(), parse_mode="HTML")

    @router.callback_query(F.data.startswith("sub_detail:"))
    async def cb_sub_detail(call: types.CallbackQuery) -> None:
        parts = call.data.split(":")
        sub_id, page = int(parts[1]), int(parts[2]) if len(parts) >= 3 else 1
        async with AsyncSessionLocal() as db:
            row = await db.get(SeriesWatchlist, sub_id)
            if row is None:
                await call.answer("该追更记录已不存在", show_alert=True)
                return
            radar = await RadarService.build(
                db,
                [row],
                recent_limit=get_settings().follow_recent_episode_window,
            )
            missing = radar[0]["missing_episodes"]
            mode = normalize_follow_mode(row.follow_mode)
            aired = row.last_aired_episode or row.total_episodes or 0
            collected = len(MissingEpisodeService._canonical_collected_keys(row))
        preview = ", ".join(missing[:12]) or "无"
        if len(missing) > 12:
            preview += f" ...（共{len(missing)}集）"
        text = (
            f"📺 <b>《{escape(row.title)}》S{row.season:02d}</b>\n\n"
            f"已播：<b>{aired}</b>\n已收：<b>{collected}</b>\n"
            f"缺失：<b>{len(missing)}</b>\n追更模式：<code>{mode}</code>\n\n"
            f"<b>当前缺集：</b><code>{preview}</code>"
        )
        builder = InlineKeyboardBuilder()
        builder.row(
            types.InlineKeyboardButton(text="⚡ 仅追最新", callback_data=f"sub_mode:{sub_id}:LATEST:{page}"),
            types.InlineKeyboardButton(text="🔥 全量补齐", callback_data=f"sub_full_confirm:{sub_id}:{page}"),
        )
        builder.row(
            types.InlineKeyboardButton(text="📋 全部缺集", callback_data=f"sub_missing_list:{sub_id}:{page}:0"),
            types.InlineKeyboardButton(
                text="🔍 立即打捞",
                callback_data=register_cb_payload("sq", {
                    "title": row.title,
                    "season": row.season,
                    "tmdb_id": row.tmdb_id,
                    "target_eps": [int(key.split("E")[1]) for key in missing],
                }),
            ),
        )
        builder.row(
            types.InlineKeyboardButton(text="🗑️ 取消追更", callback_data=f"sub_del:{sub_id}:{page}"),
            types.InlineKeyboardButton(text="🔙 返回清单", callback_data=f"menu:my_follow:{page}"),
        )
        await call.answer()
        await _safe_edit(call.message, text, builder.as_markup())

    @router.callback_query(F.data.startswith("sub_missing_list:"))
    async def cb_sub_missing_list(call: types.CallbackQuery) -> None:
        parts = call.data.split(":")
        sub_id, follow_page, missing_page = int(parts[1]), int(parts[2]), int(parts[3])
        async with AsyncSessionLocal() as db:
            row = await db.get(SeriesWatchlist, sub_id)
            if row is None:
                await call.answer("该追更记录已不存在", show_alert=True)
                return
            radar = await RadarService.build(db, [row], recent_limit=get_settings().follow_recent_episode_window)
            missing = radar[0]["missing_episodes"]
        page_size = 30
        total_pages = max(1, (len(missing) + page_size - 1) // page_size)
        missing_page = max(0, min(missing_page, total_pages - 1))
        current = missing[missing_page * page_size:(missing_page + 1) * page_size]
        text = (
            f"📋 <b>《{escape(row.title)}》S{row.season:02d} 全部缺集</b>\n"
            f"第 {missing_page + 1}/{total_pages} 页，共 {len(missing)} 集\n\n"
            f"<code>{', '.join(current) or '无'}</code>"
        )
        builder = InlineKeyboardBuilder()
        nav = []
        if missing_page:
            nav.append(types.InlineKeyboardButton(text="⬅️", callback_data=f"sub_missing_list:{sub_id}:{follow_page}:{missing_page - 1}"))
        if missing_page + 1 < total_pages:
            nav.append(types.InlineKeyboardButton(text="➡️", callback_data=f"sub_missing_list:{sub_id}:{follow_page}:{missing_page + 1}"))
        if nav:
            builder.row(*nav)
        builder.row(types.InlineKeyboardButton(text="🔙 返回单剧管理", callback_data=f"sub_detail:{sub_id}:{follow_page}"))
        await call.answer()
        await _safe_edit(call.message, text, builder.as_markup())

    @router.callback_query(F.data.startswith("sub_full_confirm:"))
    async def cb_sub_full_confirm(call: types.CallbackQuery) -> None:
        parts = call.data.split(":")
        sub_id, page = int(parts[1]), int(parts[2])
        async with AsyncSessionLocal() as db:
            row = await db.get(SeriesWatchlist, sub_id)
            if row is None:
                await call.answer("该追更记录已不存在", show_alert=True)
                return
            radar = await RadarService.build(db, [row], follow_mode=FULL, recent_limit=get_settings().follow_recent_episode_window)
            missing = radar[0]["missing_episodes"]
            aired = row.last_aired_episode or row.total_episodes or 0
            collected = len(MissingEpisodeService._canonical_collected_keys(row))
        text = (
            f"🔥 <b>确认全量补齐《{escape(row.title)}》S{row.season:02d}</b>\n\n"
            f"已播：{aired}\n已收：{collected}\n缺失：<b>{len(missing)}</b>\n\n"
            "确认后将以 FULL 语义在后续 Follow 周期分批打捞；转存暂停状态不会改变。"
        )
        builder = InlineKeyboardBuilder()
        builder.row(
            types.InlineKeyboardButton(text="✅ 启动全量补齐", callback_data=f"sub_mode_apply:{sub_id}:FULL:{page}"),
            types.InlineKeyboardButton(text="❌ 取消", callback_data=f"sub_detail:{sub_id}:{page}"),
        )
        await call.answer()
        await _safe_edit(call.message, text, builder.as_markup())

    @router.callback_query(F.data.startswith("sub_mode_apply:"))
    @router.callback_query(F.data.startswith("sub_mode:"))
    async def cb_sub_mode(call: types.CallbackQuery) -> None:
        parts = call.data.split(":")
        sub_id, mode, page = int(parts[1]), normalize_follow_mode(parts[2]), int(parts[3])
        async with AsyncSessionLocal() as db:
            row = await db.get(SeriesWatchlist, sub_id)
            if row:
                row.follow_mode = mode
                await db.commit()
        await call.answer(f"已设置为 {mode}", show_alert=True)
        call.data = f"sub_detail:{sub_id}:{page}"
        await cb_sub_detail(call)

    @router.callback_query(F.data.startswith("sub_toggle:"))
    async def cb_sub_toggle(call: types.CallbackQuery) -> None:
        # Compatibility for still-cached legacy buttons.
        parts = call.data.split(":")
        sub_id, page = int(parts[1]), int(parts[2]) if len(parts) >= 3 else 1
        call.data = f"sub_detail:{sub_id}:{page}"
        await cb_sub_detail(call)

    @router.callback_query(F.data.startswith("sub_del:"))
    async def cb_sub_delete(call: types.CallbackQuery) -> None:
        parts = call.data.split(":")
        sub_id, page = int(parts[1]), int(parts[2]) if len(parts) >= 3 else 1
        async with AsyncSessionLocal() as db:
            row = await db.get(SeriesWatchlist, sub_id)
            if row:
                row.status = "CANCELLED"
                await db.commit()
        await call.answer("🗑️ 已取消追更", show_alert=True)
        call.data = f"menu:my_follow:{page}"
        await show_my_follow(call)

    @router.callback_query(F.data == "noop")
    async def cb_noop(call: types.CallbackQuery) -> None:
        await call.answer()

    @router.callback_query(F.data == "menu:manage")
    async def cb_manage(call: types.CallbackQuery) -> None:
        await call.answer()
        call.data = "menu:my_follow"
        await show_my_follow(call)

    # ── 快捷追更 ──
    @router.callback_query(F.data.startswith("fq:"))
    async def cb_follow_quick(call: types.CallbackQuery) -> None:
        await call.answer()
        payload = get_cb_payload(call.data[3:])
        if not payload:
            await call.message.answer("⚠️ 按钮已过期。", parse_mode="HTML")
            return
        title, season = payload["title"], int(payload.get("season") or 1)
        async with AsyncSessionLocal() as db:
            await WatchlistService.add(db, title=title, season=season, tmdb_id=0, subscriber_tg_id=call.from_user.id, follow_mode="LATEST", status="FOLLOWING")
            await db.commit()
        builder = InlineKeyboardBuilder()
        builder.row(
            types.InlineKeyboardButton(text="📺 查看追更清单", callback_data="menu:my_follow"),
            types.InlineKeyboardButton(text="🔙 继续浏览日历", callback_data="calendar_menu"),
        )
        await call.message.answer(
            f"✅ <b>已加入追更清单！</b>\n\n🎬 《{escape(title)}》第 {season} 季\n⚙️ 模式：<code>⚡ 仅追最新</code>",
            reply_markup=builder.as_markup(), parse_mode="HTML"
        )

    # ── 快捷打捞 ──
    @router.callback_query(F.data.startswith("sq:"))
    async def cb_scout_quick(call: types.CallbackQuery) -> None:
        await call.answer("🔍 正在检索已监听资源库...", show_alert=False)
        payload = get_cb_payload(call.data[3:])
        if not payload:
            await call.message.answer("⚠️ 按钮已过期。", parse_mode="HTML")
            return
        title = str(payload["title"])
        season = int(payload.get("season") or 1)
        tmdb_id = int(payload.get("tmdb_id") or 0)
        raw_eps = list(payload.get("target_eps") or [])
        episode_keys = [
            value if isinstance(value, str) and value.upper().startswith("S")
            else f"S{season:02d}E{int(value):02d}"
            for value in raw_eps
            if str(value).strip()
        ]
        if tmdb_id <= 0 or not episode_keys:
            await call.message.answer(
                f"⚠️ <b>无法对《{escape(title)}》执行自动打捞</b>\n"
                "缺少已验证的 TMDB 身份或目标集数；为避免误转存，未创建任务。",
                parse_mode="HTML",
            )
            return
        from app.core.config import get_settings
        from app.scout.message_search import MessageSearch
        from app.scout.scout_service import ScoutService

        settings = get_settings()
        async with AsyncSessionLocal() as db, db.begin():
            if await BotSettingsService.is_global_paused(db):
                await call.message.answer("⏸️ 系统当前已暂停；未执行打捞或创建转存任务。", parse_mode="HTML")
                return
            results = await ScoutService(MessageSearch(settings.resource_messages_db)).scout_missing(
                db, tmdb_id=tmdb_id, title=title, season=season,
                missing_episodes=episode_keys,
            )
        queued = sum(1 for result in results if result.get("queued"))
        reviews = sum(1 for result in results if result.get("status") == "NEEDS_REVIEW")
        await call.message.answer(
            f"🔍 <b>定向打捞完成</b>《{escape(title)}》S{season:02d}\n"
            f"• 检索目标：<code>{', '.join(episode_keys)}</code>\n"
            f"• 已进入转存队列：<b>{queued}</b>\n"
            f"• 待人工核对：<b>{reviews}</b>\n"
            "<i>未命中资源不会伪造成功，也不会创建空转存任务。</i>",
            parse_mode="HTML",
        )

    # ── 热播自动追新 ──
    @router.message(Command("hot"))
    async def cmd_hot(message: types.Message) -> None:
        await _show_auto_ingest(message)

    @router.callback_query(F.data == "menu:auto_ingest")
    async def cb_auto_ingest(call: types.CallbackQuery) -> None:
        await call.answer()
        await _show_auto_ingest(call)

    async def _show_auto_ingest(event):
        async with AsyncSessionLocal() as db:
            enabled = await BotSettingsService.is_auto_ingest_enabled(db)
            cats = await BotSettingsService.get_auto_ingest_categories(db)
        st_badge = "🟢 已开启" if enabled else "🔴 已暂停"
        cat_lines = []
        for k, v in ALL_AUTO_INGEST_CATS.items():
            cat_lines.append(f"  • {v}：{'✅' if k in cats else '❌'}")
        text = (
            "🔥 <b>热播全自动追新入库</b>\n\n"
            f"⚙️ 状态：<b>{st_badge}</b>\n\n📂 <b>监控分类：</b>\n"
            + "\n".join(cat_lines) + "\n\n"
            "🎯 资源：TG频道 + FrameHdr\n💾 网盘：光鸭优先"
        )
        builder = InlineKeyboardBuilder()
        builder.row(
            types.InlineKeyboardButton(text="🛑 暂停" if enabled else "▶️ 开启", callback_data="auto_ingest:toggle"),
            types.InlineKeyboardButton(text="⚡ 立即执行", callback_data="auto_ingest:run_now"),
        )
        builder.row(
            types.InlineKeyboardButton(text="📋 今日热播", callback_data="auto_ingest:today_shows"),
            types.InlineKeyboardButton(text="🎯 入库历史", callback_data="auto_ingest:history"),
        )
        cat_btns = [types.InlineKeyboardButton(text=f"{'✅' if k in cats else '⬜'} {v[:6]}", callback_data=f"auto_ingest:cat:{k}")
                    for k, v in ALL_AUTO_INGEST_CATS.items()]
        builder.row(cat_btns[0], cat_btns[1])
        builder.row(cat_btns[2], cat_btns[3])
        builder.row(cat_btns[4])
        builder.row(types.InlineKeyboardButton(text="🔙 返回主菜单", callback_data="menu:overview"))
        if isinstance(event, types.CallbackQuery):
            await _safe_edit(event.message, text, builder.as_markup())
        else:
            await event.answer(text, reply_markup=builder.as_markup(), parse_mode="HTML")

    @router.callback_query(F.data == "auto_ingest:toggle")
    async def cb_toggle_ai(call: types.CallbackQuery) -> None:
        async with AsyncSessionLocal() as db:
            ns = await BotSettingsService.toggle_auto_ingest_enabled(db)
            await db.commit()
        await call.answer("✅ 已开启！" if ns else "🛑 已暂停！", show_alert=True)
        await _show_auto_ingest(call)

    @router.callback_query(F.data.startswith("auto_ingest:cat:"))
    async def cb_toggle_cat(call: types.CallbackQuery) -> None:
        cat = call.data.split(":")[2]
        async with AsyncSessionLocal() as db:
            await BotSettingsService.toggle_auto_ingest_category(db, cat)
            await db.commit()
        await call.answer(f"已更新 {ALL_AUTO_INGEST_CATS.get(cat, cat)}", show_alert=False)
        await _show_auto_ingest(call)

    @router.callback_query(F.data == "auto_ingest:run_now")
    async def cb_run_ai(call: types.CallbackQuery) -> None:
        await call.answer("⚡ 正在执行已验证的在追巡更...", show_alert=False)
        from app.follow.follow_worker import run_once
        try:
            result = await run_once()
            text = (
                "✅ <b>本轮在追巡更完成</b>\n\n"
                f"• 已同步：<b>{result['synced_watchlists']}</b> 部\n"
                f"• 已处理资源候选：<b>{result['scout_jobs']}</b> 条\n"
                "<i>热播日历仅用于浏览；自动转存只依据在追清单和已验证资源，不会盲转。</i>"
            )
        except Exception as exc:
            logger.exception("Manual follow cycle from hot menu failed")
            text = f"❌ <b>巡更失败</b>\n<code>{escape(str(exc))[:300]}</code>"
        builder = InlineKeyboardBuilder()
        builder.row(types.InlineKeyboardButton(text="🔙 返回热播面板", callback_data="menu:auto_ingest"))
        await _safe_edit(call.message, text, builder.as_markup())

    @router.callback_query(F.data == "auto_ingest:today_shows")
    async def cb_today(call: types.CallbackQuery) -> None:
        await call.answer("📋 正在读取今日日历...", show_alert=False)
        from app.bot.calendar_data import CalendarService as CalData
        categories = ("domestic", "anime", "western", "jp-kr", "movie")
        results = await asyncio.gather(
            *(CalData.get_category_schedule(category, 0) for category in categories),
            return_exceptions=True,
        )
        lines = ["📋 <b>今日热播排期</b>\n"]
        available = 0
        for category, data in zip(categories, results):
            if isinstance(data, Exception):
                logger.warning("Today schedule failed for %s: %s", category, data)
                continue
            shows = data.get("shows") or []
            available += len(shows)
            sample = "、".join(escape(item.get("title") or "") for item in shows[:3]) or "暂无"
            lines.append(f"• <b>{escape(data.get('cat_name') or category)}</b>：{len(shows)} 部\n  └ {sample}")
        if not available:
            lines.append("\n⚠️ 各日历源均未返回可用数据，请稍后重试。")
        text = "\n".join(lines)
        builder = InlineKeyboardBuilder()
        builder.row(types.InlineKeyboardButton(text="📅 打开完整日历", callback_data="calendar_menu"), types.InlineKeyboardButton(text="🔙 返回热播面板", callback_data="menu:auto_ingest"))
        await _safe_edit(call.message, text, builder.as_markup())

    @router.callback_query(F.data == "auto_ingest:history")
    async def cb_history(call: types.CallbackQuery) -> None:
        await call.answer()
        async with AsyncSessionLocal() as db:
            rows = list((await db.scalars(select(AutoIngestHistory).order_by(AutoIngestHistory.id.desc()).limit(15))).all())
        if not rows:
            text = "🎯 <b>入库历史</b>\n\n暂无记录。"
        else:
            lines = [f"• <b>《{escape(h.title)}》</b> S{h.season:02d} ({escape(h.provider or 'guangya')})" for h in rows]
            text = f"🎯 <b>入库历史</b> (最近 {len(rows)} 条)\n\n" + "\n".join(lines)
        builder = InlineKeyboardBuilder()
        builder.row(types.InlineKeyboardButton(text="🔙 返回热播面板", callback_data="menu:auto_ingest"))
        await _safe_edit(call.message, text, builder.as_markup(), disable_web_page_preview=True)

    # ── 搜索剧名加追 ──
    @router.callback_query(F.data == "menu:search_prompt")
    async def cb_search_prompt(call: types.CallbackQuery, state: FSMContext) -> None:
        await call.answer()
        await state.set_state(SearchFSM.waiting_for_query)
        builder = InlineKeyboardBuilder()
        builder.row(types.InlineKeyboardButton(text="🔙 取消", callback_data="menu:overview"))
        await _safe_edit(call.message, "🔍 <b>搜索剧集</b>\n\n请发送剧名关键词：", builder.as_markup())

    @router.message(SearchFSM.waiting_for_query)
    async def process_search(message: types.Message, state: FSMContext) -> None:
        await state.clear()
        query = (message.text or "").strip()
        if not query:
            await message.answer("⚠️ 搜索内容不能为空。")
            return
        loading = await message.answer(f"🔍 检索《{escape(query)}》...")
        try:
            from app.bot.calendar_data import CalendarService as CalData
            candidates = await CalData.search_tmdb(query)
        except Exception:
            candidates = []
        await loading.delete()
        if not candidates:
            builder = InlineKeyboardBuilder()
            builder.row(
                types.InlineKeyboardButton(text="🔍 重新搜索", callback_data="menu:search_prompt"),
                types.InlineKeyboardButton(text="🔙 返回主菜单", callback_data="menu:overview"),
            )
            await message.answer(f"未找到与 <b>{escape(query)}</b> 相关的条目。", reply_markup=builder.as_markup(), parse_mode="HTML")
            return
        text = f"🔍 搜索结果：<b>{escape(query)}</b>\n\n"
        builder = InlineKeyboardBuilder()
        for idx, c in enumerate(candidates[:5], 1):
            t = c["title"]
            text += f"{idx}. <b>《{escape(t)}》</b> ({c.get('year', '?')})\n   📝 {escape((c.get('overview') or '')[:80])}...\n\n"
            builder.button(text=f"➕ 追更《{t[:8]}》", callback_data=register_cb_payload("fq", {"title": t, "season": 1, "poster": c.get("poster_path")}))
        builder.adjust(1)
        builder.row(
            types.InlineKeyboardButton(text="🔍 换关键词", callback_data="menu:search_prompt"),
            types.InlineKeyboardButton(text="🔙 主菜单", callback_data="menu:overview"),
        )
        await message.answer(text, reply_markup=builder.as_markup(), parse_mode="HTML")

    # ── 转存队列看板 ──
    @router.message(Command("queue"))
    @router.callback_query(F.data == "menu:queue_status")
    async def cb_queue(event: types.Message | types.CallbackQuery) -> None:
        if isinstance(event, types.CallbackQuery):
            await event.answer()
        async with AsyncSessionLocal() as db:
            rows = list((await db.scalars(
                select(TransferQueueTask).where(TransferQueueTask.status.in_(["RUNNING", "QUEUED", "RETRY_WAIT"]))
                .order_by(TransferQueueTask.id.asc()).limit(15)
            )).all())
            failed_count = (await db.scalar(
                select(func.count(TransferQueueTask.id)).where(TransferQueueTask.status == "FAILED")
            )) or 0
        running = [r for r in rows if r.status == "RUNNING"]
        queued = [r for r in rows if r.status == "QUEUED"]
        retry = [r for r in rows if r.status == "RETRY_WAIT"]
        lines = ["⚡ <b>转存队列看板</b>\n",
                 f"🚀 执行中：<b>{len(running)}</b>  ⏳ 等待：<b>{len(queued)}</b>  🔄 重试：<b>{len(retry)}</b>  ❌ 失败：<b>{failed_count}</b>\n"]
        if running:
            lines.append("🚀 <b>执行中：</b>")
            for t in running:
                lines.append(f"• 任务 #{t.id} 资源{t.resource_id}")
        else:
            lines.append("💤 Worker 空闲")
        if queued:
            lines.append(f"\n⏳ <b>排队 ({len(queued)})：</b>")
            for t in queued[:5]:
                lines.append(f"• #{t.id} 资源{t.resource_id}")
        if retry:
            lines.append(f"\n🔄 <b>重试等待 ({len(retry)})：</b>")
            for t in retry[:5]:
                lines.append(f"• #{t.id}")
        text = "\n".join(lines)
        builder = InlineKeyboardBuilder()
        if failed_count:
            builder.row(types.InlineKeyboardButton(text=f"📋 查看失败任务 ({failed_count})", callback_data="queue_failed:0"))
        builder.row(
            types.InlineKeyboardButton(text="🔄 刷新", callback_data="menu:queue_status"),
            types.InlineKeyboardButton(text="🏠 主菜单", callback_data="menu:overview"),
        )
        if isinstance(event, types.CallbackQuery):
            await _safe_edit(event.message, text, builder.as_markup())
        else:
            await event.answer(text, reply_markup=builder.as_markup(), parse_mode="HTML")

    @router.callback_query(F.data.startswith("queue_failed:"))
    async def cb_queue_failed(call: types.CallbackQuery) -> None:
        await call.answer()
        page = 0
        parts = (call.data or "").split(":")
        if len(parts) >= 2 and parts[1].isdigit():
            page = int(parts[1])
        PAGE_SIZE = 8
        async with AsyncSessionLocal() as db:
            total = (await db.scalar(
                select(func.count(TransferQueueTask.id)).where(TransferQueueTask.status == "FAILED")
            )) or 0
            rows = list((await db.scalars(
                select(TransferQueueTask).where(TransferQueueTask.status == "FAILED")
                .order_by(TransferQueueTask.id.asc()).offset(page * PAGE_SIZE).limit(PAGE_SIZE)
            )).all())
        total_pages = max(1, (total + PAGE_SIZE - 1) // PAGE_SIZE)
        page = max(0, min(page, total_pages - 1))
        lines = [f"📋 <b>失败任务</b> · 第 {page + 1}/{total_pages} 页 (共 {total} 条)\n"]
        builder = InlineKeyboardBuilder()
        for t in rows:
            payload = t.payload if isinstance(t.payload, dict) else (json.loads(t.payload or "{}") if t.payload else {})
            title = payload.get("title", "?")
            eps = payload.get("episode_keys") or []
            eps_str = ",".join(str(e) for e in eps[:3])
            err = (t.error_message or "")[:60]
            lines.append(f"• #{t.id} <b>《{escape(title)}》</b><code>{eps_str}</code>")
            if err:
                lines.append(f"  └ <code>{escape(err)}</code>")
            builder.row(
                types.InlineKeyboardButton(text="🔄 重试", callback_data=f"tx_fail_retry:{t.id}"),
                types.InlineKeyboardButton(text="🔗 换源", callback_data=f"tx_fail_switch:{t.id}"),
                types.InlineKeyboardButton(text="🕵️ 详情", callback_data=f"tx_fail_detail:{t.id}"),
            )
        nav = []
        if page > 0:
            nav.append(types.InlineKeyboardButton(text="⬅️", callback_data=f"queue_failed:{page - 1}"))
        nav.append(types.InlineKeyboardButton(text=f"{page + 1}/{total_pages}", callback_data="noop"))
        if page < total_pages - 1:
            nav.append(types.InlineKeyboardButton(text="➡️", callback_data=f"queue_failed:{page + 1}"))
        if nav:
            builder.row(*nav)
        builder.row(
            types.InlineKeyboardButton(text="⚡ 返回队列", callback_data="menu:queue_status"),
            types.InlineKeyboardButton(text="🏠 主菜单", callback_data="menu:overview"),
        )
        await _safe_edit(call.message, "\n".join(lines), builder.as_markup())

    @router.callback_query(F.data.startswith("tx_fail_detail:"))
    async def cb_tx_fail_detail(call: types.CallbackQuery) -> None:
        await call.answer()
        tid = int((call.data or "").split(":", 1)[1])
        async with AsyncSessionLocal() as db:
            t = await db.get(TransferQueueTask, tid)
            if t is None:
                await call.answer("⚠️ 任务不存在", show_alert=True)
                return
            payload = t.payload if isinstance(t.payload, dict) else (json.loads(t.payload or "{}") if t.payload else {})
            res = await db.get(Resource, t.resource_id) if t.resource_id else None
        payload = payload or {}
        title = payload.get("title") or (res.title if res else "?")
        season = payload.get("season") or (res.season if res else 1)
        eps = payload.get("episode_keys") or ([res.episode_key] if res and res.episode_key else [])
        share = payload.get("share_url") or (res.share_url if res else "")
        lines = [
            f"🕵️ <b>任务 #{tid} 详情</b>\n",
            f"片名：《{escape(title)}》 S{int(season or 1):02d}",
            f"集数：<code>{escape(', '.join(str(e) for e in eps))}</code>",
            f"状态：<code>{escape(str(t.status))}</code>",
            f"尝试：{t.attempt_count}/{t.max_retries}",
            f"资源ID：<code>{t.resource_id}</code>",
            f"链接：<code>{escape(str(share))[:120]}</code>",
            f"错误：<code>{escape(str(t.error_message or '无'))[:300]}</code>",
        ]
        builder = InlineKeyboardBuilder()
        builder.row(
            types.InlineKeyboardButton(text="🔄 重试", callback_data=f"tx_fail_retry:{t.id}"),
            types.InlineKeyboardButton(text="🔗 换源", callback_data=f"tx_fail_switch:{t.id}"),
        )
        builder.row(
            types.InlineKeyboardButton(text="🛑 取消", callback_data=f"tx_fail_cancel:{t.id}"),
            types.InlineKeyboardButton(text="🚫 忽略", callback_data=f"tx_fail_ignore:{t.id}"),
        )
        builder.row(types.InlineKeyboardButton(text="📋 返回失败列表", callback_data="queue_failed:0"))
        await _safe_edit(call.message, "\n".join(lines), builder.as_markup())

    # ── 网盘库存核销（只读） ──
    @router.message(Command("scan"))
    @router.callback_query(F.data == "menu:scan_cloud")
    async def cb_scan(event: types.Message | types.CallbackQuery) -> None:
        if isinstance(event, types.CallbackQuery):
            await event.answer("🔍 正在读取最近一次网盘库存...", show_alert=False)
        async with AsyncSessionLocal() as db:
            rows = list((await db.scalars(
                select(CloudDiskInventory).order_by(CloudDiskInventory.updated_at.desc()).limit(12)
            )).all())
            total = len((await db.scalars(select(CloudDiskInventory.id))).all())
        if rows:
            lines = [f"☁️ <b>网盘库存核销快照</b>\n\n已登记物理文件：<b>{total}</b> 个\n"]
            for row in rows:
                lines.append(f"• 《{escape(row.title)}》S{row.season:02d}E{row.episode:02d}\n  └ <code>{escape(row.file_name[:70])}</code>")
            text = "\n".join(lines)
        else:
            text = (
                "☁️ <b>网盘库存核销快照</b>\n\n"
                "当前没有已验证的物理库存快照。为避免把缓存或队列状态冒充真实网盘文件，未显示假数据。"
            )
        builder = InlineKeyboardBuilder()
        builder.row(types.InlineKeyboardButton(text="📡 缺集雷达", callback_data="menu:radar:LATEST"), types.InlineKeyboardButton(text="🏠 主菜单", callback_data="menu:overview"))
        if isinstance(event, types.CallbackQuery):
            await _safe_edit(event.message, text, builder.as_markup())
        else:
            await event.answer(text, reply_markup=builder.as_markup(), parse_mode="HTML")

    @router.message(Command("sync"))
    @router.callback_query(F.data == "menu:sync_now")
    async def cb_sync(event: types.Message | types.CallbackQuery) -> None:
        if isinstance(event, types.CallbackQuery):
            await event.answer("🔄 正在执行全库巡更...", show_alert=False)
            msg = event.message
        else:
            msg = await event.answer("🔄 正在执行全库巡更...", parse_mode="HTML")
        from app.follow.follow_worker import run_once
        try:
            result = await run_once()
            text = (
                "✅ <b>全库巡更完成</b>\n\n"
                f"• 已同步追更剧：<b>{result['synced_watchlists']}</b> 部\n"
                f"• 已提交资源处理：<b>{result['scout_jobs']}</b> 条\n"
                "<i>只会对已验证的TMDB剧集和集数创建转存候选；无资源命中不会伪造任务。</i>"
            )
        except Exception as exc:
            logger.exception("Manual full-library follow cycle failed")
            text = f"❌ <b>全库巡更失败</b>\n<code>{escape(str(exc))[:300]}</code>"
        builder = InlineKeyboardBuilder()
        builder.row(types.InlineKeyboardButton(text="📡 缺集雷达", callback_data="menu:radar:LATEST"), types.InlineKeyboardButton(text="⚡ 转存队列", callback_data="menu:queue_status"))
        builder.row(types.InlineKeyboardButton(text="🏠 主菜单", callback_data="menu:overview"))
        if isinstance(event, types.CallbackQuery):
            await _safe_edit(msg, text, builder.as_markup())
        else:
            await _safe_edit(msg, text, builder.as_markup())

    # ── 转存失败告警按钮 ──
    @router.callback_query(F.data.startswith("tx_fail_retry:"))
    async def cb_tx_retry(call: types.CallbackQuery) -> None:
        if not _is_admin(call.from_user.id):
            await call.answer("⛔ 仅管理员可操作", show_alert=True)
            return
        tid = int(call.data.split(":", 1)[1])
        from datetime import UTC
        async with AsyncSessionLocal() as db:
            t = await db.get(TransferQueueTask, tid)
            if t and t.status in ("FAILED", "RETRY_WAIT"):
                t.status = "QUEUED"
                t.error_message = None
                t.locked_at = None
                t.locked_by = None
                t.next_run_at = datetime.now(UTC)
                await db.commit()
        await call.answer("🔄 已重新排队（转存暂停状态下不会执行）", show_alert=True)

    @router.callback_query(F.data.startswith("tx_fail_switch:"))
    async def cb_tx_switch(call: types.CallbackQuery) -> None:
        if not _is_admin(call.from_user.id):
            await call.answer("⛔ 仅管理员可操作", show_alert=True)
            return
        tid = int(call.data.split(":", 1)[1])
        from app.transfer.switch_resource import NoAlternativeResourceError, switch_resource
        settings = get_settings()
        async with AsyncSessionLocal() as db:
            try:
                detail = await switch_resource(
                    db, task_id=tid, resource_db_path=settings.resource_messages_db,
                )
                await db.commit()
            except NoAlternativeResourceError as exc:
                await db.rollback()
                await call.answer("⚠️ 暂未找到其它资源", show_alert=True)
                await call.message.answer(f"🔗 <b>更换资源</b>\n\n{escape(str(exc))}\n\n<i>该集仍保持原状态，未创建新任务。</i>", parse_mode="HTML")
                return
            except Exception:
                logger.exception("Switch resource failed for task %s", tid)
                await db.rollback()
                await call.answer("❌ 换源失败，请查看日志", show_alert=True)
                return
        lines = [f"🔗 <b>更换资源完成</b>（原任务 #{tid}）", f"《{escape(detail.get('title'))}》S{detail.get('season', 1):02d}", ""]
        for item in detail.get("new_tasks", []):
            tag = "复用已有任务" if item.get("reused") else "新建任务"
            lines.append(f"• {escape(item.get('episode_key'))} → #{item.get('task_id')} <code>{item.get('status')}</code> ({tag})")
        lines.append("\n<i>transfer_paused=1：新任务不会执行真实转存。</i>")
        await call.message.answer("\n".join(lines), parse_mode="HTML")

    @router.callback_query(F.data.startswith("tx_fail_rescout:"))
    async def cb_tx_rescout(call: types.CallbackQuery) -> None:
        if not _is_admin(call.from_user.id):
            await call.answer("⛔ 仅管理员可操作", show_alert=True)
            return
        tid = int(call.data.split(":", 1)[1])
        settings = get_settings()
        await call.answer("🔎 正在重新打捞...")
        async with AsyncSessionLocal() as db:
            t = await db.get(TransferQueueTask, tid)
            if not t:
                await call.answer("⚠️ 任务不存在", show_alert=True)
                return
            payload = t.payload if isinstance(t.payload, dict) else json.loads(t.payload or "{}")
            tmdb_id = payload.get("tmdb_id")
            title = payload.get("title")
            season = payload.get("season")
            episode_keys = payload.get("episode_keys") or []
            if not tmdb_id or not title or not episode_keys:
                await call.answer("⚠️ 任务缺少身份信息，无法重打捞", show_alert=True)
                return
            from app.scout.message_search import MessageSearch
            from app.scout.scout_service import ScoutService
            scout = ScoutService(MessageSearch(settings.resource_messages_db))
            results = await scout.scout_missing(
                db, tmdb_id=int(tmdb_id), title=title, season=int(season or 1),
                missing_episodes=[str(k) for k in episode_keys],
            )
            await db.commit()
        queued = sum(1 for r in results if r.get("queued"))
        reviews = sum(1 for r in results if r.get("status") == "NEEDS_REVIEW")
        await call.message.answer(
            f"🔎 <b>重新打捞完成</b>《{escape(title)}》S{int(season or 1):02d}\n"
            f"• 进入转存队列：<b>{queued}</b>\n• 待人工核对：<b>{reviews}</b>\n"
            "<i>transfer_paused=1：不会执行真实转存。</i>",
            parse_mode="HTML",
        )

    @router.callback_query(F.data.startswith("tx_fail_cancel:"))
    async def cb_tx_cancel(call: types.CallbackQuery) -> None:
        if not _is_admin(call.from_user.id):
            await call.answer("⛔ 仅管理员可操作", show_alert=True)
            return
        tid = int(call.data.split(":", 1)[1])
        async with AsyncSessionLocal() as db:
            t = await db.get(TransferQueueTask, tid)
            if t:
                t.status = "CANCELLED"
                t.locked_at = None
                t.locked_by = None
                await db.commit()
        await call.answer("🛑 已取消（仅本次任务，未来仍可继续追）", show_alert=True)

    @router.callback_query(F.data.startswith("tx_fail_ignore:"))
    async def cb_tx_ignore(call: types.CallbackQuery) -> None:
        if not _is_admin(call.from_user.id):
            await call.answer("⛔ 仅管理员可操作", show_alert=True)
            return
        tid = int(call.data.split(":", 1)[1])
        async with AsyncSessionLocal() as db:
            t = await db.get(TransferQueueTask, tid)
            if t and t.payload:
                p = t.payload if isinstance(t.payload, dict) else json.loads(t.payload or "{}")
                title = p.get("title", "")
                season = int(p.get("season") or 1)
                episode_keys = p.get("episode_keys") or []
                if title and episode_keys:
                    for key in episode_keys[:1]:
                        m = re.search(r"E(\d+)", str(key))
                        episode = int(m.group(1)) if m else 0
                        db.add(IgnoredMissing(title=title, season=season, episode=episode))
                elif title:
                    db.add(IgnoredMissing(title=title, season=season, episode=0))
                try:
                    await db.commit()
                except Exception:  # noqa: BLE001 - rollback is best effort after ignore write
                    await db.rollback()
        await call.answer("🚫 已忽略（该集后续不再追）", show_alert=True)

    # ── Settings ──
    @router.message(Command("settings"))
    async def cmd_settings(message: types.Message) -> None:
        cfg = get_settings()
        async with AsyncSessionLocal() as db:
            follow_paused = await BotSettingsService.is_follow_paused(db)
            transfer_paused = await BotSettingsService.is_transfer_paused(db)
            ai = await BotSettingsService.is_auto_ingest_enabled(db)
            queue_counts = dict(
                (await db.execute(
                    select(TransferQueueTask.status, func.count(TransferQueueTask.id)).group_by(TransferQueueTask.status)
                )).all()
            )
        follow_state = "⏸ 已暂停" if follow_paused else "▶️ 运行"
        transfer_state = "⏸ 已暂停" if transfer_paused else "▶️ 运行"
        builder = InlineKeyboardBuilder()
        builder.row(
            types.InlineKeyboardButton(
                text="▶️ 恢复追新" if follow_paused else "⏸ 暂停追新",
                callback_data="menu:follow_resume" if follow_paused else "menu:follow_pause",
            ),
            types.InlineKeyboardButton(
                text="▶️ 恢复转存" if transfer_paused else "⏸ 暂停转存",
                callback_data="menu:transfer_resume" if transfer_paused else "menu:transfer_pause",
            ),
        )
        await message.answer(
            "⚙️ <b>系统状态</b>\n\n"
            f"🌐 环境：<code>{cfg.app_env}</code>\n"
            f"追新：<code>{follow_state}</code>\n"
            f"转存：<code>{transfer_state}</code>\n"
            f'☁️ 云盘写入：<code>{"启用" if cfg.cloud_write_enabled else "禁用"}</code>\n'
            f'🔥 自动入库：<code>{"开" if ai else "关"}</code>\n\n'
            "<b>队列</b>\n"
            f"QUEUED: <code>{queue_counts.get('QUEUED', 0)}</code>\n"
            f"RETRY_WAIT: <code>{queue_counts.get('RETRY_WAIT', 0)}</code>\n"
            f"FAILED: <code>{queue_counts.get('FAILED', 0)}</code>",
            reply_markup=builder.as_markup(),
            parse_mode="HTML",
        )

    # ── 全局错误处理 ──
    async def _err(event):
        exc = getattr(event, "exception", None)
        logger.error("Unhandled: %s", exc, exc_info=exc)
        try:
            b = getattr(event, "bot", None)
            u = getattr(event, "update", None)
            m = getattr(u, "message", None) if u else None
            cid = getattr(getattr(m, "chat", None), "id", None) if m else None
            if b and cid:
                await b.send_message(cid, "⚠️ 操作出错，请重试。")
        except Exception:
            pass

    dp.errors.register(_err)
    dp.include_router(router)
    return dp


def bot_commands() -> list[BotCommand]:
    return [
        BotCommand(command="start", description="🏠 追新助手全能主菜单"),
        BotCommand(command="hot", description="🔥 热播自动追新入库管理"),
        BotCommand(command="calendar", description="📅 追剧日历 (8大分类导航)"),
        BotCommand(command="radar", description="📡 缺集与追新雷达看板"),
        BotCommand(command="follow", description="📺 我的追更清单管理"),
        BotCommand(command="queue", description="⚡ 转存队列"),
        BotCommand(command="scan", description="🔍 扫描网盘物理文件"),
        BotCommand(command="sync", description="🔄 全库打捞资源"),
        BotCommand(command="pause", description="⏸️ 暂停追更与转存"),
        BotCommand(command="resume", description="▶️ 恢复追更与转存"),
        BotCommand(command="help", description="💡 使用说明"),
    ]


async def register_command_menu(bot: Bot) -> None:
    cmds = bot_commands()
    await bot.set_my_commands(cmds, scope=BotCommandScopeAllPrivateChats())
    admin = get_settings().admin_tg_id
    if admin:
        try:
            await bot.set_my_commands(cmds, scope=BotCommandScopeChat(chat_id=admin))
        except Exception:
            pass


async def main() -> None:
    configure_logging()
    settings = get_settings()
    if not settings.telegram_bot_token:
        logger.warning("TELEGRAM_BOT_TOKEN is empty; bot process is idle")
        return
    bot = Bot(settings.telegram_bot_token)
    try:
        await register_command_menu(bot)
        logger.info("ZhuiXin Bot starting polling...")
        await build_dispatcher().start_polling(bot)
    finally:
        await bot.session.close()


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
