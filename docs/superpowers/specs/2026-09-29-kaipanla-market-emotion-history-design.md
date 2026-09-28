# 开盘啦市场情绪历史股票列表接入设计

日期：2026-09-29。状态：项目所有者已确认。目标是在现有 FastAPI「实时接口」分类中，按指定历史交易日只读查询开盘啦 App「行情 → 打板 → 历史数据 → 股票列表」的竞价、涨停、跌停、炸板四个列表。这里的「实时接口」指请求时读取来源，并非盘中实时行情；不保存数据库或 Raw，不触发 Worker。

## 范围与边界

新增四个独立 GET 路由，不修改现有 `/api/v1/realtime/kaipanla/auction-pool` 的语义或返回模型：

|列表|建议路由后缀|来源 `PidType`|来源 `Type` / `Order`|页面排序|
|---|---|---:|---|---|
|竞价|`market-emotion/stocks/auction`|8|18 / 1|涨停委买额降序|
|涨停|`market-emotion/stocks/limit-up`|1|6 / 1|涨停时间降序|
|跌停|`market-emotion/stocks/limit-down`|3|6 / 1|跌停时间降序|
|炸板|`market-emotion/stocks/broken-limit-up`|2|4 / 1|涨幅降序|

完整公共前缀为 `/api/v1/realtime/kaipanla/`。四个路由均要求 `trade_date=YYYY-MM-DD`，仅接受上海日期今天之前的日期；没有「省略日期取最新」或历史日期自动回退。`offset` 默认 0、最大 10000，`limit` 默认 30、范围 1–30。一次公共请求仅访问上游一页，不重试、不自动翻页，来源未提供可靠总数时 `total=null`，满页仅提示可尝试的 `next_offset`。

沿用 API Key、中文 OpenAPI、`FASTAPI_KAIPANLA_TIMEOUT_SECONDS`（默认 8 秒、允许 1–10 秒）、2 MB 响应上限、禁止重定向与系统 HTTP 代理。固定来源域名和 action，不接收用户给定的来源 URL、action、类别编号或任意排序表达式。无数据库迁移、PostgREST/Agent 契约变更、定时任务、登录凭据或新的数据持久化领域。沿用 ADR-0059 已接受的请求时直连例外，并补充该 ADR 与 FastAPI 契约文件。

## 来源核验与固定请求

MuMu 中安装的 App 为 `com.aiyu.kaipanla` 5.23.0.4；页面日期 2026-09-28，股票列表的四类数量为竞价 143、涨停 33、炸板 11、跌停 56。以项目既有、已验证的无凭据历史 HTTP 请求为基准，向以下固定地址发送 `application/x-www-form-urlencoded` POST：

```text
https://apphis.kaipanla.com/w1/api/index.php
c=HisHomeDingPan&a=HisDaBanList
Day=2026-09-28&Index=0&st=30
PidType=<上表固定值>&Type=<上表固定值>&Order=1
Is_st=1&Filter=0&FilterMotherboard=0&FilterGem=0&FilterTIB=0
apiv=w48&PhoneOSNew=1&VerSion=6.3.20.0
```

`VerSion` 是已验证请求参数，不表示当前安装包版本。`Is_st=1` 固定沿用页面与既有竞价池的 ST 过滤；本次不开放额外筛选。直接请求返回 `errcode="0"`、`day="2026-09-28"` 和 `list`。以 `st=30` 翻页核对所得去重数量与 App 计数一致。涨停前列的 001368、603968、605366，炸板前列的 301004、600707、603418，与页面一致；跌停的 000988、600522 同为 14:51，来源按秒排序与页面按分钟展示的并列顺序可能不同，不承诺同分钟稳定排序。`ttag` 为来源耗时字段，不作为业务数据。

这些是限定日期、版本及参数的实测证据，不代表来源协议永久稳定。文档只保存请求规则和脱敏结构摘要，不保存整页股票市场数据、账户标识、认证头或 App 抓包文件。

## 响应契约与字段

统一外层：`source_code="kaipanla"`、`list_type`、`persisted=false`、请求/来源交易日期、`observed_at`、`offset`、`limit`、`returned_count`、`total=null`、`next_offset` 和 `items`。`observed_at` 是取得响应的上海时间，使用既有 `ApiTimestamp`，不是股票事件时间。每条记录有六位 `code`、标准 `symbol`、`name`；只返回已与页面交叉核对的类别字段：

|列表|页面字段与标准输出|来源行下标 / 解释|
|---|---|---|
|竞价|`board_name`、`limit_up_bid_amount_cny`、`auction_change_pct`|11、18、19；金额元，涨幅百分比|
|涨停|`limit_up_at`、`status`、`reason`|6 为 Unix 秒，9 为首板/连板状态，16 为涨停原因|
|炸板|`change_pct`、`limit_up_at`、`opened_at`|4 为百分比，6/7 为 Unix 秒|
|跌停|`limit_down_at`、`sealed_amount_cny`、`board_name`|6 为 Unix 秒，8 为封单金额元，11 为来源板块文字|

页面核验例：2026-09-28 的 301004 炸板涨幅 17.06%、09:44 涨停、09:45 开板；000988 跌停时间 14:51、封单约 240.4 万元。来源数组可能包含更多值，但未核实含义的不作为公共字段，不返回 `source_values` 原数组。来源板块不是数据中心标准分类，来源状态/原因不是数据中心派生判断。

价格、金额、百分比通过 `Decimal` 转换并以 JSON 字符串输出；时间用 `Asia/Shanghai` 的 `YYYY-MM-DD HH:mm:ss`，缺失保持 `null`，零不转成缺失。不得把来源 Unix 秒直接返回或把金额换算为万后标成元。

## 错误、实现与验收

在 Provider 边界校验 JSON、`errcode`、实际 `day`、列表和行长、六位代码、同页重复、有限数值、非负金额、有效时间。明确成功且日期匹配的空列表可返回 200；来源失败、占位内容、日期不符或结构/字段错误统一 502，不伪装为空列表。日期和分页非法返回 422，缺失或错误 API Key 返回 401。没有跨页不可变快照保证，调用方需按代码去重。

实现沿用现有开盘啦竞价 Provider 的固定 HTTP 防护和 API 注册方式，四个路由共享请求边界，但保留各自的响应模型、类别字段和 OpenAPI 描述。对现有竞价池不做破坏性改动。测试使用合成来源样例核对四类参数、日期/分页、字段下标与单位、时间格式、零/null、失败路径、鉴权与 OpenAPI 契约；另做不保存响应的有限真实来源核对。更新接口说明、ADR-0059 澄清与 `contracts/fastapi-openapi-v1.json`。完整本地质量门禁通过后再交付；提交、推送或部署仍分别按用户授权执行。
