# 运行与交付门禁

## 进程职责

| 服务 | 职责 | 禁止事项 |
|---|---|---|
| `follow-worker` | TMDB 季排期同步、缺集计算、历史 Scout | 不直接调用云盘 |
| `resource-monitor` | 仅监听已启用的 `RESOURCE` / `MANUAL_INGEST` 频道，先写 durable outbox | 不订阅总结群，不共享总结 session/SQLite |
| `transfer-worker` | 领取队列、恢复 stale RUNNING、调用适配器、读回验证 | 不把 HTTP/restore 提交当作完成 |
| `api` | 内部控制面 | 不绕过频道策略 |
| `bot` | 私聊可见性与只读运维入口 | 不在群里自行响应/改变规则 |

## 频道策略

- 正式资源频道：`role=RESOURCE`、`enabled=true`、`transfer_mode=AUTO`。
- 测试人工入口：`role=MANUAL_INGEST`、`enabled=true`、`accept_forward=true`、`transfer_mode=AUTO`。
- `MANUAL_INGEST` 的非 Forward 消息进入 `NEEDS_REVIEW`，绝不自动转存。
- 原始 Telegram 标志 `is_forward` 必须持续出现在 `ChannelIngestMessage`、`ChannelIngestJob.parsed_data` 与 `TransferQueueTask.payload`。

## 转存完成语义

1. `TransferQueueTask` 被领取不是完成。
2. Guangya `restore_share` 接受请求不是完成。
3. 目标目录分页读取到所有期望文件，且 adapter 返回 `verified=true`，才可标记 `COMPLETED`。
4. 成功后才可回写 `SeriesWatchlist.collected_episodes`。
5. 进程重启时将超时的 `RUNNING` 任务恢复为可重试状态；同资源/提供者/集键的 idempotency key 防止重复入队。

## 预生产前检查清单

- [ ] 新的 PostgreSQL 实例、账号、备份与权限均已明确，不指向任何旧库。
- [ ] 迁移已在只读副本上完成；报告已复核 `unmigrated_rows`。
- [ ] 已明确生产资源频道和测试频道的真实 ID，并通过 `POST /channels` 配置。
- [ ] Telegram Bot / Telethon session 均为本项目独立凭据。
- [ ] TMDB Key 已通过最小只读请求验证。
- [ ] 云盘凭据、目标目录和 `CLOUD_WRITE_ENABLED=true` 均有明确授权；先执行小样本真实读回验证。
- [ ] 回滚路径只允许停止新服务，不得修改旧项目或旧数据。

## 生产切流授权边界

未获得明确授权时，本项目只能运行单元/集成测试与静态检查。生产部署、真实 Telegram 登录、云盘写入、服务启动、反向代理或端口变更，必须逐项授权并在实施后读回验证。
