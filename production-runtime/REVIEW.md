# 生产运行态审查证据（2026-09-23 CST）

## 证据范围及限制

此文档根据 Oracle 生产宿主机运行容器只读采集；采集项包括容器状态/镜像、`/app` 源码树（排除 `data/`、`backups/`、`.env`、`.git`、虚拟环境与缓存）、生产 PostgreSQL schema-only dump、只读 DB 查询、Telegram/TMDB GET 请求及既有 Guangya 只读验证工具结果。它不是切流记录，也不是“所有云盘联调通过”的声明。

**本仓库原始版本的根目录 README 曾错误写成“只完成隔离数据库验证且未接生产”。** 因此原始 root project 不能单独代表活动生产服务。现将运行容器实际源码置于 `services/`，与根目录快照区分开。根目录仍包含当时生产 checkout 的工程源码/未提交工作树，不能与各容器镜像视为同一 build。

生产凭据、生产 `.env`、真实数据库行、Telegram session、SQLite、生产 DB backup/dump 均未写入本仓库。

## 活动服务及实际版本

| 服务 | 容器状态 | 镜像摘要 | RELEASE_COMMIT | 源码快照 |
|---|---|---|---|---|
| API | running | `sha256:3c5525204dcf1510521b8532f4ba0e3a687cae1368ef92fb28a1c71e1a5b758b` | `03092f8059958cc7eab30252f013a585125f2150` | `services/api/` |
| Telegram Bot | running | 与 API 相同镜像摘要 | `03092f8059958cc7eab30252f013a585125f2150` | `/app` 文件树与 API 逐文件完全相同；共用 `services/api/` |
| Follow Worker | running | `sha256:0d852c968e638400e9c81758431046ba6e7395bdc08842979329863c30415d1e` | `6e5f24af209107f3eb38f5fde14f467eb03b7489` | `services/follow-worker/` |
| Transfer Worker | running | `sha256:9cc9efd5a22227753d3d76d2128ee331879493b47df3d2ddc3a6b0bf3c8e6088` | `fb4ed1c8a49d48754b97abfe0cc82bb6b47554ea` | `services/transfer-worker/` |
| resource-monitor | running | `sha256:dd2d1f126242bb686f476acc0e50616e4ed35a2a6ce37382e6f7aeaf9e03f9d7` | `44ce8049124009919c2077e9ddcf445358480a53` | `services/resource-monitor/` |
| PostgreSQL | healthy | `postgres:16-alpine` | n/a | 数据未导出；仅提供 schema-only |

运行容器并非同一 release：API/Bot 落后于当前 Follow/Transfer Worker 的 marker。这是实际生产版本差异，不能通过根目录单一 `RELEASE_COMMIT` 抹平。

### 源码树快照校验

各服务树 SHA-256 使用相对路径、路径长度、文件内容 SHA-256 依序组成：

- API/Bot：`d10cfb4182bd03907a5be57c2b5b15cae1e385c7d528dfb6f52ea4a87a9823f3`，341 files；API 与 Bot 完全相同。
- Follow Worker：`12bdc7de569e7c6491f2aa2a30c3d252df43d5890faae0abc7c6f013684f1592`，343 files。
- Transfer Worker：`75fd4f435417865c4ff0de17a984ba548dcf5c6a4be7c3bd841ea46b5a9b5b66`，350 files。
- resource-monitor：`0b274525afa2f9db53cba0dbb03e31b7310b6635e9d2c8d1a22be37783a5ac5c`，305 files。

捕获时明确排除应用内 `backups/`（含生产 PostgreSQL backup）、`data/`、`.env` 和运行态密钥/缓存；快照中不含敏感文件路径及常见 token/private-key 签名命中。

## 实际生产集成探针（只读）

| 集成 | 实际检查 | 结果 | 含义 |
|---|---|---|---|
| Telegram Bot API | 使用运行容器配置在内存中调用 `getMe`，凭据值不打印、不保存 | HTTP 200，`ok=true`；Bot ID/username 不在报告中输出 | 证明 Bot Token 当前可用；不等于验证了消息发送路由或权限行为 |
| TMDB | 使用运行配置 GET `/tv/223564?language=zh-CN` | HTTP 200，返回 `id=223564` 且有名称 | 证明生产环境 TMDB 只读 API 连通；其他季条目日志中的 HTTP 404 仍是单独数据问题 |
| 生产 PostgreSQL | `BEGIN READ ONLY` SQL 查询；连接于当前 compose PostgreSQL | `alembic_version=0013_telegram_roles_and_admins`；Following watchlists=445 | 这是旧生产 DB 的实时只读查询，不是隔离数据库；本仓库没有复制业务数据 |
| Guangya 云盘 | 调用仓库既有 `tools.verify_guangya_auth`，只做目录读取与认证诊断 | **未通过**：刷新请求 `guangya refresh timed out`；`directory_read_success=false`、`refresh_occurred=false`、`credential_persisted=false` | 不能宣称云盘联调成功；无转存、建目录、改名、移动或 DB 凭据写入 |

Telegram/TMDB 临时探针不会把凭据值写入报告、命令输出或仓库。Guangya 验证器报告未发生 refresh，也未持久化凭据。

## 生产数据库实时状态（只读摘要）

- Migration head：`0013_telegram_roles_and_admins`
- `watchlists_following=445`
- Queue：`CANCELLED=39, COMPLETED=7, FAILED=80, PENDING=45, QUEUED=59, SUCCESS=1152`
- Bot settings：`transfer_paused=1, global_pause=0, follow_paused=0`
- `cloud_configs` 行数：1；不输出配置内容、凭据或目标路径。
- `database/schema-only.sql` 是 `pg_dump --schema-only --no-owner --no-privileges` 产生的 DDL；没有 `COPY` / `INSERT` 数据记录。

队列数是该次只读查询的现场状态，Follow 仍在运行，之后可能变化。80 条 FAILED 未处理。

## Worker 运行证据

- Follow 周期日志（CST）`2026-09-23 17:46:29`：`synced 431 watchlists`，`targets=1211`，`transfer_eligible=216`，`new_queue_tasks_created=0`，`queue_reused=216`，`auto_safe=0`，`pending_review=1`，`no_resource=978`。这是运行日志，不是生产云盘转存验收。
- Transfer Worker 日志持续显示 `Transfer queue consumption skipped: transfer pause is enabled`。
- 该审计未观察到真实 restore、rename、Inventory/collected 写入或成功通知。

## 切流/部署状态

- 新 GitHub 仓库是源代码及证据备份，不是运行中的生产部署；没有从 GitHub 新仓库切流，也没有从该仓库重建 API/Bot。
- 生产当前仍使用上表容器版本；Follow、Transfer Worker 存在不同 release marker；resource-monitor 未重建。
- 没有改生产 Telegram、TMDB、云盘凭据，没有迁移、替换或回放旧生产数据库。
- Transfer 保持暂停。由于 Guangya refresh 超时，不能启用生产写入或宣称已完成生产集成验收。

## 本仓库文件核查入口

- 逐服务生产运行源码：`services/{api,follow-worker,transfer-worker,resource-monitor}/`
- API 与 Bot 相同源码树：`services/api/`
- 生产数据库结构：`database/schema-only.sql`
- 新仓库根目录工程版本说明：仓库根 `README.md`
