# 观潮行情 0.19.2 板块精选协议核查

日期：2026-10-26。本文记录协议分析时的证据；后续接入由Issue #98和ADR-0059当天TLS增补跟踪，
当前能力以[接口说明](../开盘啦板块精选实时接口.md)及实际发布记录为准。

## 样本与证据范围

- 用户提供样本：`观潮行情-0.19.2-单文件.exe`，50,134,566 字节。
- SHA-256：`8c94629f1b01f411f336fc9f570c1885af5bd32a6b107f1aa9ed6feb8d233442`。
- PyInstaller 单文件，包内运行时为 Python 3.13，入口为 `launcher`，业务包为 `marketdesk`。
- 使用标准库解析归档和 Python 3.13 `dis` 检查字节码；不是原始源代码恢复，也不是 REA 引擎输出。
- 未启动完整桌面应用。实时验证只执行已检查的 Socket 请求/解析函数，未加载登录模块、账号或会话。
- 临时反汇编、公开证书和验证摘要位于本机临时目录 `guanchao-0192-boards-analysis`；不保存市场原始响应。

证据标记及包内源位置（行号来自字节码）：

| 标记 | 位置 | 证明内容 |
| --- | --- | --- |
| E1 | `marketdesk/platepick_data.py:26` `_pages` | 排行与历史成员 HTTP 参数及分页 |
| E2 | `marketdesk/platepick_data.py:127` `load_platepick_members`，内部函数第133行 | 指定上海当天选择 Socket，其他日期选择 HTTP |
| E3 | `marketdesk/platepick_socket.py:13`、`:61`、`:95` | 固定连接目标、命令封装、TLS 请求 |
| E4 | `marketdesk/platepick_socket.py:105` `_parse_page` | 日期、板块、分页、字段映射校验 |
| E5 | `marketdesk/platepick_socket.py:156` `load_members` | 桌面软件自动全量翻页及跨页一致性检查 |
| E6 | 本次单页只读实测 | 当天 TLS 链路成功，见下文 |

## 功能对应接口

| 功能 | 协议及地址 | 关键参数 | 证据范围 |
| --- | --- | --- | --- |
| 最新板块排行 | GET `https://apphq.longhuvip.com/w1/api/index.php` | `c=ZhiShuRanking&a=RealRankingInfo&apiv=w21&ZSType=7` | E1；已有项目接口 |
| 历史板块排行 | GET `https://apphis.longhuvip.com/w1/api/index.php` | 同上，`apiv=w26&Date=YYYY-MM-DD` | E1；已有项目接口 |
| 历史成分股 | GET 同一 apphis 地址 | `c=ZhiShuRanking&a=ZhiShuStockList_W8&apiv=w41&PlateID=六位代码&Date=YYYY-MM-DD` | E1；已有项目接口 |
| 当天成分股 | TLS Socket `124.71.166.244:14000` | SNI `apphwshhq.kaipanla.com`，命令2501 | E2–E6，已实时验证 |
| 子板块 | POST `https://{apphis或apphwshhq}.kaipanla.com/w1/api/index.php` | `c=ZhiShuRanking&a=SonPlate_Info&apiv=w44&Type=6&PlateID=六位代码` | `platepick_data.py:153` 静态确认，未实测 |
| 板块日线 | GET `https://apphis.kaipanla.com/w1/api/index.php` | `c=StockLineData&a=GetKLineDay_W14&apiv=w44` | `platepick_history.py:14` 静态确认，未实测 |
| 历史涨跌原因 | POST `https://apphis.longhuvip.com/w1/api/index.php` | `c=ZhiShuKLine&a=GetDayBaseFaceListZDEvnArt` | `platepick_data.py:204` 静态确认，未实测 |
| 当前涨跌原因 | POST `https://apphwshhq.longhuvip.com/w1/api/index.php` | `c=ZhiShuL2Data&a=GetBaseFaceListZDEvnArtNew` | 同上，静态确认，未实测 |

表中仅列关键参数，不代表完整请求。前三个 HTTP 请求还带 `Order=1&Type=1&PhoneOSNew=1&VerSion=5&st=页长&Index=偏移`。
历史成员附加 `IsZZ=0&IsKZZType=0&filterType=0&TSZB=0&TSZB_Type=0`。
本次重点核查当天成员，不声称已完整核查所有子功能或全部接口。

## 当天成员协议

软件明确按上海当天分流，不能通过直接把历史成员 HTTP 请求改到 apphq 来实现兼容。
此链路没有 HTTP URL，也不使用 JSON 请求体。

请求帧（E3）：

1. 外层：`0x40 + body长度（2字节，大端）+ body`。
2. body 固定头：`00 17 09 c5 00 00 03 02 09 c5`；`0x09c5` 即2501。
3. 后接 `0a 06` 和六位 ASCII 板块代码。
4. 后接 `10 06 18 01`，分别为 protobuf 字段2=6、字段3=1；保持来源筛选语义，未额外推断其业务名称。
5. 非零偏移附加 `30 + varint(offset)`；页长附加 `38 + varint(count)`，软件默认60。

TLS 使用包内 `marketdesk/assets/kpl-quote.pem` 公开证书作为信任材料，`PROTOCOL_TLS_CLIENT` 保持证书及主机名校验；不得通过关闭校验替代。
软件连接超时5秒、Socket读取超时12秒。这是软件现状，不是数据中心已批准的超时配置。

响应（E4）：

- 帧前缀为 `0x40`，允许在列表前出现 `11 00 00` 心跳帧；软件最多检查8帧。
- 命令头必须匹配；随后读取类型3的描述符及其两字节长度。
- 描述符需匹配 `hqList|数字:20030/2501-0/{plate_id}:6:1:0:0:0:0:0`。
- protobuf 字段1为板块代码、6为偏移（可省略零）、11为总数、13为来源日期。
- 字段20为证券代码目录（软件要求长度为 `min(total,100)`），21为 packed 行索引，22为成分股子消息，23为预测标签。
- 每个股票子消息中，1为代码、2为名称、6为龙头标签、100为重复的 UTF-8 字段值。
- 必须验证实际日期与请求日一致、板块/偏移正确、行索引连续、代码无重复且与目录重叠部分一致。

软件先把 Socket 值映射到与历史 HTTP 相同的行布局，再使用共用股票转换器：

| Socket values 下标 | 历史行下标 |
| --- | --- |
| 0..17 | 4..21 |
| 18 / 19 / 21 / 22 / 24 | 23 / 25 / 28 / 29 / 33 |
| 28 / 29 / 33 | 37 / 38 / 42 |
| 40 / 44 / 45 / 46 / 47 / 48 / 50 / 52 / 53 | 保持同下标 |

股票代码/名称写入行0/1；子消息字段6写入行24。软件构造63列，未知槽位为None。
不能把 Socket 字段100的数组直接当成历史 HTTP 数组使用。
该映射是对软件实现的观察；接入数据中心时仍须逐项验证公开字段语义、Decimal、单位和零/缺失规则。

## 实时验证 E6

2026-10-26，对 `803023` 发起一次 TLS 请求，offset=0、count=60：

- 证书及主机名校验通过，无账号/会话凭据。
- 请求27字节，返回body 29,992字节。
- 来源日期 `2026-10-26`，页内60行，总数字段722。
- 9个标签；映射后每行63列。
- 原软件 `_parse_page` 的命令、板块、日期、代码目录和分页校验全部通过。
- 只验证首批60行，没有读取或验证全部722行，也未证明跨页快照隔离。

## 对数据中心的后续影响

可保留 `/api/v1/realtime/kaipanla/boards/{board_code}/members/history` 路径和响应模型：
过去日期保持HTTP，当天新增TLS适配，未来日期仍拒绝。当天源尚未更新时必须报错，不回退旧日期。

分析时这是待实施方案。后续项目所有者明确授权接入，Issue #98已登记，ADR-0059已补充当天TLS决策；
该分析的单页实测仍不等于Provider已上线，须以独立发布验收为准。
不需要为无持久化只读链路新增数据库表或迁移。

接入时不能直接复制桌面软件的全量翻页：公共请求只允许一个有界页面，需要验证1..60页长、
非零偏移及空页行为，设置完整请求时限、帧/响应上限，并维护受控证书。
禁止将本次只读验证表述为已完成Provider接入、公共API回归测试或生产部署。
