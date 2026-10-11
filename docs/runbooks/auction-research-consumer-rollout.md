# 竞价字段补抓：上线及消费链交接方案

日期：2026-10-11。状态：待实施的可审查方案，不是部署记录。
项目所有者已授权先补能够取得的真实来源字段；不要求先证明池覆盖完整或09:25时点。
因此全市场来源和撮合时点仍为研究限制，不再作为现有来源补抓的启动门禁。

## 已核对的实际状态

- 线上历史竞价OpenAPI只有symbol、code、name、board_name、limit_up_bid_amount_cny、
  auction_change_pct六项；新增五项仍在本地。
- MySQL `data_auction_stock`仍是旧九列，没有新增五项字段。
- MySQL已有142289条记录，1209个日期，2021-10-08至2026-10-09。
  这是旧事实范围，不是新竞价成交额覆盖范围。
- JeecgBoot `AuctionService.fetchDay`仍调用旧历史路由，九参数写入，使用REPLACE。
  `/data/auction/fetch`只按日期循环，无持久断点；lastRunInfo只在内存。
- 当前实际未启动补抓，目标存储仍是JeecgBoot的MySQL，新增字段写入0行。
  本次只读检查未更改表、任务、服务或数据。

## 本项目发布边界

数据中心的Provider、领域记录、API模型、单测和FastAPI契约已具备五个新增字段。
发布仅需更新并重启API，无PostgreSQL迁移或Worker改动。正常部署不备份PostgreSQL。
须先完成这次字段版本的生产发布，不能用先前hot-talks发布记录替代。

上线后读取2026-09-29前三条，核对五项字段真实返回、日期匹配、金额字符串和null，
再允许消费端启动。401/502不能解释为成功空列表。

## JeecgBoot最小消费变更

以下SQL仅为消费项目迁移草案；**未执行**，不属于本项目PostgreSQL migrations。
在JeecgBoot既有迁移流程审查执行前，先核对实际列、目标库和当前采集任务状态。

```sql
ALTER TABLE data_auction_stock
  ADD COLUMN change_pct DECIMAL(38,18) NULL COMMENT '来源历史行情涨幅(%)，精确事件时点未知',
  ADD COLUMN auction_net_amount DECIMAL(38,18) NULL COMMENT '来源竞价净额(元)，可负',
  ADD COLUMN auction_amount DECIMAL(38,18) NULL COMMENT '来源竞价成交额(元)，最终撮合时点尚未独立核实',
  ADD COLUMN auction_turnover_rate_pct DECIMAL(38,18) NULL COMMENT '来源竞价换手率(%)，0.23表示0.23%',
  ADD COLUMN actual_float_market_value DECIMAL(38,18) NULL COMMENT '来源实际流通市值(元)，计价时点与股本口径未独立核实';
```

无需改动或清空旧列。先核对列不存在；迁移记录已存在时跳过，不重复运行ALTER。
新五列依次由API `change_pct`、`auction_net_amount_cny`、`auction_amount_cny`、
`auction_turnover_rate_pct`、`actual_float_market_value_cny`映射。
Java使用`new BigDecimal(node.asText())`，null/缺失返回null；非法非数值必须失败，
不得使用`asDouble()`的默认0或在NumberFormatException时返回null吞掉来源错误。

将`REPLACE INTO`替换为INSERT/重复键安全更新。保留原主键与`(trade_date,code)`唯一键；
每条记录还要验证标准symbol与code一致，不接受跨交易所静默冲突。
现有记录所有正确非null值保持原值；只填新列或原缺失字段。以成交额为例：

```sql
INSERT INTO data_auction_stock
  (id,trade_date,symbol,code,name,board_name,limit_up_bid_amount,
   auction_change_pct,observed_at,change_pct,auction_net_amount,
   auction_amount,auction_turnover_rate_pct,actual_float_market_value)
VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
ON DUPLICATE KEY UPDATE
  change_pct=COALESCE(change_pct,VALUES(change_pct)),
  auction_net_amount=COALESCE(auction_net_amount,VALUES(auction_net_amount)),
  auction_amount=COALESCE(auction_amount,VALUES(auction_amount)),
  auction_turnover_rate_pct=COALESCE(auction_turnover_rate_pct,VALUES(auction_turnover_rate_pct)),
  actual_float_market_value=COALESCE(actual_float_market_value,VALUES(actual_float_market_value));
```

这是最小补五列示例，旧列不更新、旧observed_at不冒充新字段获取时点。
消费端需额外保存这轮字段采集时间、source_layout、来源时点未知标记至其采集审计记录。
非null既有值与来源冲突时记录冲突，不覆盖；0也是非null正确值，不能用真假判断。
如果MySQL版本对VALUES()提供替代别名语法，按实测版本采用兼容写法。
查询接口的SELECT和概况统计也须加入新列及非null覆盖数，不能只修改写入SQL。

## 分批与断点

1. 由确切市场交易日历生成2022-01-01至2026-10-09的日期队列；不遍历自然日请求节假日。
2. 先按2026-09-29单日试写一页，核对委买额未改、新字段原始/落库值及null/零区别。
   再完整翻页该日，核对没有删除旧记录，下一次执行新增/更新数为0。
3. 消费项目需增加持久批次/日期/分页断点，至少记录任务范围、日期、next_offset、
   页序、成功/失败、更新时间、错误分类及新增字段缺失数量。
   只有数据库事务提交成功后推进next_offset；同一页重跑幂等；不使用内存lastRunInfo作断点。
4. 先每批5个交易日，单请求最多30条，一次只执行一条采集链，页间至少1秒。
   暂时来源错误有限重试，持续失败保留失败状态，禁止标记整日完成。
5. 正确处理满页追加空页和重复代码；日期错位立即停止该日；每个日期独立提交。
   下次只恢复未提交页或失败日期，不能无差别重跑并REPLACE历史记录。
6. 首板次日研究目标由首板名单×下一市场交易日独立建立。
   对每个目标记录matched/absent_from_source/source_error，以及五字段的各自缺失状态。
   现有池中找不到目标股票不插入伪0金额；无摸板失败股票必须留在缺失清单。

## 启动与验收

消费映射、迁移和断点功能部署并通过单日试写后，才可使用现有
`POST /data/auction/fetch?startDate=2022-01-01&endDate=2026-10-09`流程。
此URL是待满足条件后的启动入口，本次没有调用。

运行期间报告：目标MySQL库/表、新字段落库日期、已完成/失败日期、最近提交页、
真实有效金额行数、各字段缺失数、来源不覆盖股票数和停止原因。
已抓到的真实字段可先交付，但成功、未连板、未摸板三组分别验收覆盖。
全天成交额另取首板当天已核实日成交额，保持独立字段与收盘日时点，不能混用竞价金额。
