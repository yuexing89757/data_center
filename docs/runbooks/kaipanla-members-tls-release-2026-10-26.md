# 开盘啦当天成分股 TLS 发布验收（2026-10-26）

Issue：[#98](https://github.com/yuexing89757/data_center/issues/98)。
生产代码提交：`1e86412`。上海时间2026-10-26 19:02:50完成线上验收。

## 范围

现有 `/api/v1/realtime/kaipanla/boards/{board_code}/members/history` 支持上海当天TLS当前快照。
过去日期继续HTTP历史，未来日期422；完整请求时限、证书与主机名校验、单页和字段校验见ADR-0059及领域设计。
本次仅发布API，未执行数据库迁移、采集或Worker重启。

## 验证

- 本地ruff格式、lint、mypy通过；pytest：2120通过、234跳过。跳过项需要隔离数据库或本机缺少的能力。
- [CI 40311430012](https://github.com/yuexing89757/data_center/actions/runs/40311430012)：格式604文件、mypy180文件通过，pytest2121通过、233跳过；隔离PostgreSQL集成232通过。
- [生产只读检查40311438248](https://github.com/yuexing89757/data_center/actions/runs/40311438248)：check模式成功，无迁移。
- 独立代码复核未发现阻塞项；公开证书已确认进入wheel及发布源码包。
- 上线前生产机新Provider读取803023当天offset60/limit2成功，证书和主机名校验开启。
- 线上803023当天(offset,limit)=(0,60)、(60,2)、(10000,2)均200，分别60、2、0条，来源日期2026-10-26。
- 2026-10-23历史成员limit2返回200及2条，来源日期一致；当天板块排行返回200。
- 无Key401、未来日期422、health/ready及有界证券查询通过。
- 线上完整OpenAPI与提交契约一致；Worker PID及API环境配置摘要未变。

## 发布与恢复

从干净提交构建源码包，SHA-256：
`ad42abf28a3d225dc7df3102291d38e47420849bb86cc0396668694518581d07`。
保留原依赖虚拟环境，已逐项确认pyproject.toml及uv.lock文本相同。
API Raw备份包含2217个文件，已下载异机并核对摘要：
`5feca2423af74d044fa8718d29b4a3ee2e98361b9afbed66bf27ee0eb95496ba`。
未进行数据库备份；该发布不修改数据库。
通过原子链接切换并仅重启API；验收失败时自动恢复旧链接并重启API。
本次全部验收成功，未触发回滚。旧版本及备份保留。

当天数据为来源当前快照，不保证跨页快照隔离或收盘终值。未保存本次上游行情原文。
