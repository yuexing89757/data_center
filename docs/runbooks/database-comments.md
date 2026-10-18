# 数据库表与字段注释

`20261018000100_add_table_column_comments.sql` 为项目业务表补充中文 PostgreSQL
`COMMENT ON TABLE` / `COMMENT ON COLUMN` 元数据。纳入行情、股本、分类、技术因子、
竞价、涨跌停、龙虎榜、监管、计算、采集审计及运行记录；不修改市场事实、字段类型、
约束、索引、权限或 API 返回结构。

迁移保留此前 migration 已定义的注释。竞价序列的现存月分区从各自父表补齐空缺注释，
保留分区已有注释。后续新表、新字段或新分区应在对应 migration 中维护注释；集成测试
`test_all_application_tables_and_columns_have_comments` 检查全部业务表和字段的覆盖情况。

不覆盖 `api_v1` 视图/RPC、PostgreSQL/扩展对象、迁移历史表和调度器内部存储。
专业版因子保存在 JSONB 字段中，各因子的名称、分组与类型继续以
`stock-technical-factor.v1` 目录和 StockTechnicalFactor 领域文档为准。

注释特别区分：股票成交量为股、可转债成交量为张、金额为人民币元（可转债发行/剩余规模
保留来源亿元口径）、指数价格为点位、百分点与小数比例、竞价指示值与成交值。
同日涨跌停快照的自由流通市值按同日未复权收盘价乘同日自由流通股本计算；
缺少事件流的累计封板时长保留为空，不按首末封板时间相减推算。

本地检查：

```bash
uv run pytest tests/test_database_comments.py
```

PostgreSQL 覆盖检查需要隔离的可丢弃数据库 `TEST_DATABASE_URL`，禁止指向生产。
生产应用仍按既有受保护迁移流程，使用迁移身份按顺序执行；需取得生产迁移的明确授权。
按项目部署约定，不为本次注释变更备份 PostgreSQL 数据库。
