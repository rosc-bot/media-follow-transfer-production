# media-follow-transfer

全新、隔离的影视主项目：**追新 + 资源监听 + 测试频道人工 Forward 入库 + 异步验证式转存**。

> 当前仅完成本地工程与隔离数据库验证，**尚未接入生产 Telegram、TMDB、云盘或旧生产数据库，也没有切流**。

## 业务链路

```text
SeriesWatchlist
  → TMDB Season Provider
  → MissingEpisodeService
  → ResourceScout（只读 resource_messages.db）
  → CandidateSelector
  → ChannelIngestService
  → Resource / DedupService
  → TransferQueue
  → TransferWorker
  → Cloud Adapter
  → 目标目录读回验证
  → Resource 状态 / SeriesWatchlist.collected_episodes
```

测试频道的人工入口：

```text
人工 Forward → MANUAL_INGEST 测试频道 → ResourceMonitor
→ is_forward=true → ChannelIngestService → Dedup → TransferQueue
```

三种统一来源：`telegram_channel`、`manual_forward`、`watchlist_scout`。`is_forward` 会从 Telegram 源消息保留到 Ingest Job、解析数据、策略判断和队列 payload。

## 边界与安全

- 旧仓库、旧生产数据库、旧 SQLite、旧 Docker volume 仅作只读参考与回滚来源；本仓库不原地改造它们。
- `resource_messages.db` 仅保存影视资源历史供 Scout 查询；不与总结监听共享 session 或 SQLite。
- `CLOUD_WRITE_ENABLED=false` 是默认值。关闭时所有实际云盘写请求都会 fail-closed。
- 转存成功的唯一条件是云盘目标目录读回验证；队列接受、HTTP 返回或 restore 发起都不是成功。
- Guangya 适配器支持分页、重复页防护、refresh-token 获取 access token、恢复后目录轮询；`skip_readback` 被明确拒绝。
- `RESOURCE` 频道与 `MANUAL_INGEST` 频道均通过 `channel_settings` 独立配置；测试频道必须同时满足 `enabled=true`、`accept_forward=true`、`transfer_mode=AUTO` 和真实 Forward 标记。

## 本地验证

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements-dev.txt
.venv/bin/alembic upgrade head
.venv/bin/pytest -q
.venv/bin/ruff check app migrations migration_tools tests
.venv/bin/alembic check
```

本仓库的 Compose 由 `migrate` 一次性服务作为门禁：`api`、`bot`、`follow-worker`、`resource-monitor`、`transfer-worker` 都只会在迁移成功后启动。当前 Compose 未发布端口；生产接入必须另行明确授权并按既有端口/反向代理规范执行。

## 必需配置

从 `.env.example` 创建本地 `.env`，并按实际环境填写：

- 新项目独立的 `DATABASE_URL` / `SYNC_DATABASE_URL` / `POSTGRES_PASSWORD`；
- `TELEGRAM_BOT_TOKEN`、`TELEGRAM_API_ID`、`TELEGRAM_API_HASH`；
- `TMDB_API_KEY`；
- 经授权且完成独立目录配置后才可设 `CLOUD_WRITE_ENABLED=true`。

不要把密码、Bot Token、TMDB Key、云盘 token 或 session 文件提交到 Git。

## Bot 与 API 控制面

私聊命令菜单在 Bot 启动时通过 `set_my_commands` 同步：

- `/start` 主菜单
- `/follow` 追更清单
- `/radar` 缺集雷达
- `/queue` 转存队列
- `/settings` 安全运行状态

API 包含健康检查、频道配置、Watchlist、统一 Ingest、资源和转存队列读取接口。新增/更新频道配置应通过 `POST /channels`；`POST /ingest/source-message` 会读取对应的频道策略后再入库。

## 旧 watchlist 只读迁移

迁移源使用 SQLite `mode=ro`，不会写、删、清空或覆盖旧库：

```bash
.venv/bin/python migration_tools/import_watchlist.py \
  /absolute/path/to/legacy/watchlist.db \
  --report data/migration-reports/watchlist-import.json
```

报告记录源文件前后 SHA-256、创建数、既有身份跳过数、无效行及其 `legacy_rowid`。重跑是幂等的。缺失 TMDB ID 的记录不会猜测匹配，也不会静默丢失，必须由报告驱动后续人工补全。

## 未授权前禁止的操作

- 不部署、不重启、不连接生产 Telegram/网盘、不启用云盘写入；
- 不改变旧项目、旧生产数据库、旧 SQLite、旧 Docker volume；
- 不执行 `DROP`、`TRUNCATE`、`alembic downgrade` 或 `docker compose down -v`；
- 不把总结监听接入本项目。
