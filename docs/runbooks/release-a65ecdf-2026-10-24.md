# 2026-10-24 Worker/API 发布记录：a65ecdf

## 授权与发布范围

项目所有者明确要求「推送、部署线上」。2026-10-24 已将 master 的两个提交推送并发布：

- `b76c390`：BaoStock 新采集退役，Security/Trading Calendar 改为 Tushare 优先、AKShare 兜底；保留历史来源和 Raw 回放。
- `a65ecdf7cae9f320a9ab3255421b17f376cde21a`：开盘啦历史涨停列表补齐8个表格字段及回封/原因分组计数。

Worker/API 于上海时间 **2026-10-24 14:16:00** 切换到同一发布提交 `a65ecdf`。
原运行版本 `465a84a` 保留，可按发布回执切回；本次没有执行回退演练。
后续文档记录提交不改变本次运行代码版本。

关联 [Issue #94](https://github.com/yuexing89757/data_center/issues/94)、
[Issue #95](https://github.com/yuexing89757/data_center/issues/95)。

## 上线前证据

- 本地门禁：format 585文件、Ruff、mypy 176源文件及diff检查通过；pytest为2060 passed、218 skipped。
  本地缺少隔离TEST_DATABASE_URL，不能将跳过项目描述为通过。
- 发布提交的 [GitHub CI](https://github.com/yuexing89757/data_center/actions/runs/40068612209) 成功：
  Linux质量job为2061 passed、217 skipped；隔离PostgreSQL集成job为216 passed、2062 deselected。
- 同一提交的[受保护生产检查](https://github.com/yuexing89757/data_center/actions/runs/40068617316)以check模式成功。
  本次没有新增SQL迁移，没有执行apply、临时DDL或修改数据库授权。
- 用干净的提交副本构建发布包，不使用allow-dirty，也不把原工作区未跟踪的tinker_classN纳入发布。
  source归档SHA256为 `781693e6e2006149381f68bf1abd2bfea45c92484522bc93976ce21f10d77a90`；
  wheel SHA256为 `1755db7cf0c13d92479d7cfa3ec0a4d25e33ae617a6e4a5c092ddd741a09e9bc`。
- 新目录独立安装同一wheel和锁定依赖，依赖检查及导入检查通过；移除BaoStock运行依赖，旧版本目录保持可用。
- 用实际Worker配置只读验证新Tushare适配：5919条Raw响应行、5918条标准证券，
  沪深北均覆盖；精确排除1条已授权历史特殊身份，Raw回放结果一致；日历完整7天。
  这是来源只读探测，没有写入标准层，不能冒称下一次定时采集已执行。
- API专用身份的发布权限预检通过。切换前无running工作流/采集，单例锁1个、任务23项，
  且避开下一个任务的临近执行窗口。

## Raw 独立备份与配置保护

按宪法第12项原则，没有备份PostgreSQL数据库，也没有执行数据库恢复演练。
Raw单独核验：

- Worker当前在线物理Raw共307033文件、8055565170字节，全量重新计算哈希，
  均匹配既有已验证独立备份；新增未覆盖文件为0。
- API独立Raw目录共7904对象、348618500字节，生成只读归档并复制到另一台机器，
  校验整个归档与7904个成员的字节数和SHA256全部匹配。
  独立归档SHA256为 `371681addf0acc677f4562a5a22d91ed0b115c23a19397196d611d9008d1840b`。
- 下载链路发生超时；通过分块复制完成独立校验，不以零字节或未完成副本作为备份证据。
  备用压缩与传输任务已完成或停止，未修改在线Raw。
- 切换服务停止后，再次核对Raw文件数量/字节数以及manifest数量/最新时间未改变，
  避免将备份完成后的新增数据漏过门禁。
- 两份外置环境文件和systemd配置的哈希保持一致，持久化Raw、scheduler、节点池目录不迁移。
  沿用既有Worker/API运行身份，本次未调整身份、凭据、监听地址、权限或定时计划。

这是当前在线物理Raw的备份覆盖证明，不等于所有历史manifest均可恢复；
Issue #92等已登记历史缺口不由本次发布追认或修复。

## 发布后线上验收

两个服务均active，均运行新版本目录：Worker PID763756，API PID763767，NRestarts均为0。
`/healthz`、`/readyz`、本地任务页均成功；数据库单例锁仍为1，任务仍为23项。
Raw manifest数量仍为316244，无发布引起的采集或Raw写入。

线上带鉴权调用：

`GET /api/v1/realtime/kaipanla/market-emotion/stocks/limit-up?trade_date=2026-10-23&limit=30&offset=0`

再分别请求offset30和60，共82条唯一证券；每条包含新增10字段。
金额/比例按Decimal字符串返回，时间为上海墙钟格式，布尔与计数类型正确，
没有source_values或匿名数组；线上OpenAPI组件与发布契约一致。
38条涨停时间与最后涨停时间不同，回封38条；恩捷实际换手保留HTTP来源6.66%，
没有把App的6.64%当作覆盖依据。无API Key仍返回401。

生产运行包路由核验为Security/Trading Calendar `tushare → akshare`，普通Daily Bar仍为pytdx。
没有手工触发生产采集或回放；首次后续定时批次实际来源与终态仍需按正常任务记录观察，
不将「路由已部署」等同于「后续全链路采集已验收」。

独立发布回执、配置哈希、备份清单、线上响应及验证摘要存于操作者的忽略目录
`backups/kaipanla-release-a65ecdf-20261024`，不向仓库提交凭据、Raw、归档或响应数据。

本次没有数据库备份、迁移、历史修复、数据删除、快照替换或下游群消息。
