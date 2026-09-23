# media-follow-transfer-production

**这是审查/备份仓库，不是已切流的新部署。** 该仓库现在同时保存：

1. `production-runtime/services/`：从当前运行中的生产容器 `/app` 文件树直接取出的逐服务源码快照；每个服务带生产 `RELEASE_COMMIT`，并在审计文档中列出文件树 SHA-256。
2. 仓库根目录工程：生产服务器 checkout 在 `6e5f24a` 时的源码工作树快照，含当时未提交变更；它**不等同**于所有正在运行的容器版本。请不要把根目录 `README` 原描述当作切流证明。
3. `production-runtime/database/schema-only.sql`：从旧生产 PostgreSQL 实例只读导出的 schema-only SQL；不含业务表数据、凭据或记录。

## 生产运行证据

请先读 [`production-runtime/REVIEW.md`](production-runtime/REVIEW.md)。其中区分实际运行的容器、各服务版本差异、真实 Telegram/TMDB 只读探针、旧生产数据库版本/计数、云盘认证失败证据，以及当前 Transfer 暂停状态。

## 重要边界

- 本仓库没有生产 `.env`、Bot/API/TMDB/云盘凭据、Telegram session、SQLite/数据库业务记录或数据库备份文件。
- 将代码放入 GitHub **不代表服务已由这个仓库部署**。本次没有切流、重建容器或变更业务数据。
- 生产 Transfer 当前保持暂停。云盘 Guangya 认证的只读检查失败于 refresh 网络超时；报告中明确列为未通过，不能据此启用写入。
- `production-runtime/services/` 是运行容器的代码快照，不承诺其中可独立启动：需要外部生产环境变量与网络/凭据。缺少的秘密不应写入公开仓库。
- DB schema 可用于审查迁移结构；如需生产数据回放，应另做受控、脱敏、隔离的副本，不得将原生产数据提交到公开 GitHub。

## 快速定位

- 正在运行的最新 Follow Worker：`production-runtime/services/follow-worker/`
- 正在运行的 Transfer Worker：`production-runtime/services/transfer-worker/`
- 正在运行的 API/Bot：`production-runtime/services/api/`（本次核对二者 `/app` 文件树相同）
- 正在运行的 resource-monitor：`production-runtime/services/resource-monitor/`
- 生产 PostgreSQL schema：`production-runtime/database/schema-only.sql`
- 生产运行态与联调证据：`production-runtime/REVIEW.md`
