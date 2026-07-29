# Supabase 备份恢复与凭据轮换

本文是自托管 Supabase 的运维运行手册。应用数据备份覆盖 `audit`、`capital`、`classification`、`core`、`derived`、`ingestion` 和 `metrics` Schema。凭据必须从密钥管理器注入环境变量或 Supabase 主机上的 `.env`，不得把数据库 URL、密码、JWT、API key 或备份内容写入 Git、Issue、PR、CI 日志和操作记录。

## 1. 恢复目标

一次备份只有在独立数据库成功恢复并通过以下检查后才算有效：

- `audit`、`core`、`ingestion` 领域表行数与源库一致；
- `supabase_migrations.schema_migrations` 版本一致；
- `api_v1` 视图集合一致；
- `core` 事实表不存在指向缺失 `ingestion_run` 的记录；
- PostgREST 冒烟查询可用，匿名/登录用户仍为只读；
- 原始文件存储已单独复制并通过校验和核对。

RTO 目标为 60 分钟，RPO 目标为 24 小时。上线后根据实际数据量和演练耗时调整。

## 2. 应用数据备份

仓库工具仅备份 Market Data Center 的 `audit`、`core`、`ingestion` 数据，不把凭据放入进程参数。备份默认写入已被 Git 忽略的 `backups/`。

```bash
export SOURCE_DATABASE_URL='从密钥管理器注入'
uv run python scripts/backup_restore.py backup \
  --file backups/application-$(date +%Y%m%d-%H%M%S).dump
uv run python scripts/backup_restore.py snapshot > backups/source-snapshot.json
unset SOURCE_DATABASE_URL
```

将 dump、snapshot 和工具输出的 SHA-256 保存到受控备份存储。`data/raw/` 不在数据库 dump 内，必须使用对象存储/文件系统快照单独备份，并生成文件清单和校验和。

完整 Supabase 灾备还应按官方迁移方式分别导出 roles、schema 和 data。不要直接对整个 Supabase 集群执行未经筛选的 `pg_dump`；官方文档指出这样会包含 Supabase 管理的内部对象并可能在恢复时产生权限错误：

```bash
supabase db dump --db-url "$SOURCE_DATABASE_URL" -f roles.sql --role-only
supabase db dump --db-url "$SOURCE_DATABASE_URL" -f schema.sql
supabase db dump --db-url "$SOURCE_DATABASE_URL" -f data.sql --use-copy --data-only
```

Supabase CLI 的 `--db-url` 会成为进程参数，因此这些完整导出命令只能在受控运维主机执行；执行期间应限制进程查看权限，结束后立即清理环境变量和 shell 历史中的展开值。

参考：[Supabase 自托管数据恢复](https://supabase.com/docs/guides/self-hosting/restore-from-platform)。

## 3. 独立恢复演练

禁止把恢复目标指向生产库。先创建隔离的空 PostgreSQL 数据库，限制网络入口，并从密钥管理器注入目标 URL。

```bash
export TARGET_DATABASE_URL='从密钥管理器注入的隔离目标库'

# 先在空库执行仓库迁移，再恢复应用数据
export MIGRATION_DATABASE_URL="$TARGET_DATABASE_URL"
uv run python scripts/apply_migrations.py apply
unset MIGRATION_DATABASE_URL

uv run python scripts/backup_restore.py restore \
  --file backups/application-YYYYMMDD-HHMMSS.dump

export SOURCE_DATABASE_URL='从密钥管理器注入的源库只读连接'
uv run python scripts/backup_restore.py verify
unset SOURCE_DATABASE_URL TARGET_DATABASE_URL
```

然后执行数据库集成测试和 [PostgREST 权限验证](PostgREST-api_v1权限验证.md)。原始文件恢复到隔离目录，比较文件数量、相对路径和 SHA-256；不要把原始行情文件提交到仓库。

记录以下无敏感信息的演练证据：时间、操作者、备份对象名、SHA-256、源/目标快照、恢复耗时、PostgREST 冒烟结果、原始文件校验结果和异常处理。演练目标库保留到验收完成后，再按环境管理规范删除。

## 4. 凭据轮换

所有轮换在维护窗口执行。轮换前确认可登录 Supabase 主机、已有最近一次通过恢复验证的备份、旧值仍可从密钥管理器回滚，并列出所有消费者。轮换后更新密钥管理器和服务环境，重建容器，检查健康状态、数据库连接、Studio 登录、PostgREST 只读查询及采集写入。

### 数据库密码

在 Supabase Docker 项目目录执行官方脚本，然后重建服务：

```bash
sh utils/db-passwd.sh
sh run.sh recreate
```

若验证失败，在密钥管理器恢复旧密码和服务配置，再次重建。参考：[Supabase Docker 自托管](https://supabase.com/docs/guides/self-hosting/docker)。

### Studio 密码

更新 Docker `.env` 的 `DASHBOARD_PASSWORD`，同步密钥管理器并重建服务。不要在操作记录中保存新旧值。验证新密码可登录且旧密码失效；失败时恢复旧配置并重建。

### API key 与 JWT 签名密钥

优先使用新的 publishable/secret API keys。按官方脚本新增签名密钥和 API keys，更新环境后重建服务：

```bash
sh utils/add-new-auth-keys.sh --update-env
sh utils/rotate-new-api-keys.sh --update-env
sh run.sh recreate
```

先部署同时接受新旧密钥的过渡配置，再逐个更新消费者，最后撤销旧密钥。轮换非对称签名密钥会使现有 ES256 会话失效，必须提前安排重新登录窗口。参考：[自托管 Auth 签名密钥](https://supabase.com/docs/guides/self-hosting/self-hosted-auth-keys)。

## 5. 数据库网络收口

生产 PostgreSQL 不应对公网任意来源开放。首选取消数据库端口的公网映射，让应用通过 Supabase 内部网络或 Supavisor 访问；确需直接运维连接时，只允许固定管理出口 IP 或 VPN 网段。

执行顺序：

1. 记录当前防火墙和容器端口配置，但不记录凭据；
2. 从允许网段验证数据库、PostgREST、Studio 和采集任务；
3. 删除公网宽泛规则，或把数据库端口规则改为允许名单；
4. 再次执行允许网段成功、非允许网段失败的双向验证；
5. 若健康检查失败，恢复上一份已验证的防火墙/端口配置并复测。

不得在未确认主机管理通道和回滚规则时修改防火墙，以免同时锁死数据库和主机访问。

## 6. 周期与验收

- 每日：应用数据备份、原始文件增量备份和校验和；
- 每周：检查备份可读性、大小趋势和失败告警；
- 每月及每次 schema/基础设施变更后：独立恢复演练；
- 每次人员/服务权限变化或疑似泄露后：立即轮换相关凭据；
- 每季度：复核数据库公网暴露、允许名单和消费者清单。

CI 中的 PostgreSQL 集成测试会为每次变更创建两个独立数据库，执行迁移、备份、恢复和快照比较，作为工具级持续证据；生产备份仍必须按本手册定期演练。
