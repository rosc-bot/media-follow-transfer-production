# media-follow-transfer-production

**唯一 canonical production source 为仓库根目录 `app/`。** 本版本按运行态快照逐模块融合：API/Bot/Admin 以 `services/api/` 为准，Follow/Scout/Ingest 以 `services/follow-worker/` 为准，Transfer 以 `services/transfer-worker/` 为准；兼容性由全量测试和 import smoke 验证。`production-runtime/services/{api,follow-worker,transfer-worker}/app/` 是同步后的镜像，三份应用树必须保持字节一致。resource-monitor 保留历史 release，本轮不重启。

`production-runtime/REVIEW.md`、服务 marker 与数据库统计记录的是**本轮部署前的历史基线**，不是部署后的运行证明。镜像构建会用 Git release commit 覆盖运行时 `RELEASE_COMMIT`；只有生产容器回读一致才算部署完成。

仓库同时保留：

1. `production-runtime/services/`：来自生产容器 `/app` 的审计快照与 canonical mirrors；每个历史 snapshot marker 和原始文件树 hash 仅用于追溯。
2. `production-runtime/database/schema-only.sql`：只读导出的 schema-only SQL；不含业务表数据、凭据或记录。

## 生产运行证据

请将 [`production-runtime/REVIEW.md`](production-runtime/REVIEW.md) 作为本轮**部署前历史基线**读取；其中的容器 release、数据库统计和 Guangya 探针结果不能代替部署后现场回读。

## 重要边界

- 仓库不包含生产 `.env`、Bot/API/TMDB/云盘凭据、Telegram session、SQLite/数据库业务记录或生产数据库备份。
- GitHub commit、镜像构建成功或源码目录相同，都不能单独证明生产已部署；必须回读 API/Bot/Follow/Transfer 容器的 `RELEASE_COMMIT` 和运行态。
- `REVIEW.md` 中 `transfer_paused=1` 与 Guangya refresh 超时是部署前基线；运行后状态以生产数据库和正式只读工具的最新结果为准。
- `production-runtime/services/{api,follow-worker,transfer-worker}/app/` 是 canonical `app/` 的逐文件镜像；Docker 镜像由 canonical root 工程构建，运行所需凭据仅来自受保护的生产 `.env`。
- DB schema 可用于审查迁移结构；不得将生产数据、凭据或 Telegram Session 提交至 GitHub。

## 快速定位

- Canonical application source：`app/`
- API/Bot/Follow/Transfer runtime mirrors：`production-runtime/services/{api,follow-worker,transfer-worker}/app/`
- 本轮未重启的 resource-monitor 历史快照：`production-runtime/services/resource-monitor/`
- 生产 PostgreSQL schema：`production-runtime/database/schema-only.sql`
- 部署前历史运行证据：`production-runtime/REVIEW.md`
