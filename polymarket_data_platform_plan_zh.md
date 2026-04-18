# Polymarket 数据平台实施规划

> 说明：本文件是架构规划文档，包含前瞻性设计。当前仓库“已实现行为、参数默认值、脚本用法”以 `README.md` 和 `scripts/start_primary.sh` 为准。

## 1. 项目目标

构建一个面向生产环境的 Polymarket 数据平台，使其能够：

- 持续采集公开的 Polymarket 市场数据
- 以适合研究、模型训练和商业交付的形式存储数据
- 在置信度足够高的情况下，将链下成交记录与链上地址活动关联
- 支持后续扩展到分析服务、用户画像、因子产品和客户交付服务

这个项目不应被设计为一次性的抓取脚本，而应被设计为一个分层清晰、边界明确、可长期维护和扩展的数据平台。

## 2. 产品设计原则

- 优先使用官方公开 API 和官方文档中提到的链上数据来源作为事实来源
- 严格区分原始数据保留层和标准化后的数据产品层
- 优先采用追加写入和版本化 schema，而不是原地修改历史数据
- 先以批量数据交付为核心，再逐步扩展在线查询服务
- 保留重放和重建能力，以便未来修复解析逻辑或升级处理流程
- 第一阶段保持基础设施简单，但从一开始就选择可扩展的文件格式和接口设计
- 避免让采集逻辑与下游分析、画像、交付逻辑紧耦合

## 3. 总体架构

平台应划分为六层：

1. 数据源层
- Polymarket 公开 API
- Polymarket 公开市场数据接口
- Polymarket 公开订单与成交数据接口
- 区块链事件源与索引器

2. 采集层
- 定时轮询 REST 资源
- 按需订阅交易流和盘口流
- 链上事件采集任务
- 失败重试、退避、去重和 checkpoint 逻辑

3. 原始数据层
- 不可变原始记录
- 最小转换的数据落盘
- 保留足够元数据以支持未来重放与重处理

4. 标准化数据层
- 类型清晰、schema 稳定的规范化数据集
- 面向研究与下游服务的事实表和维表

5. 增强分析层
- 成交与地址关联
- 市场状态重建
- 地址级和实体级特征生成
- 衍生指标、因子和标签

6. 交付层
- 面向客户的 Parquet 数据包
- manifest、schema 文档和版本说明
- 后续可扩展成查询 API 和分析服务

### 3.1 运行拓扑（成本优化版）

默认采用“双节点容灾”：

- 本地节点作为主采集节点
  - 持续采集并直接写入本地磁盘
  - 作为长期归档的主来源
- 云端节点作为应急缓冲节点
  - 平时同样采集，但只保留短期窗口（建议 3-7 天）
  - 默认不进行常态全量下行同步
- 故障回填机制
  - 仅当本地断网、断电、进程异常时，从云端按缺失时间窗口增量下载
  - 回填后执行去重与完整性校验

该拓扑的目标是将云出站流量从“持续付费”改为“故障时偶发付费”。

## 4. 项目目录与代码结构

推荐目录结构如下：

```text
polymarket-data-platform/
├── README.md
├── docs/
│   ├── architecture.md
│   ├── schemas/
│   ├── operations.md
│   └── product_definitions.md
├── config/
│   ├── base.yaml
│   ├── prod.yaml
│   └── datasets.yaml
├── src/
│   ├── collectors/
│   ├── parsers/
│   ├── writers/
│   ├── pipelines/
│   ├── matching/
│   ├── features/
│   ├── quality/
│   ├── manifests/
│   └── cli/
├── tests/
│   ├── unit/
│   ├── integration/
│   └── fixtures/
├── scripts/
├── state/
├── logs/
└── data/
```

结构约束如下：

- 所有与数据源相关的抓取逻辑放在 `collectors/`
- 所有 schema 标准化与字段清洗逻辑放在 `parsers/`
- 所有文件落盘、分区、压缩、写入逻辑放在 `writers/`
- 成交地址匹配、画像、特征逻辑独立于采集层
- 所有数据集定义都必须显式声明并进行版本管理

## 5. 存储层最佳实践

### 5.1 存储分层

从第一天开始就使用三层存储结构：

#### Raw 层

用途：

- 支持重放
- 支持审计
- 支持解析错误后的重建
- 支持验证原始来源

格式：

- 当前 MVP：`jsonl.gz`（gzip 压缩 JSONL）
- 后续可优化为：`JSONL.zst`

内容：

- 关键 API 响应的原始包体
- 交易流和可选盘口流的原始 WebSocket 消息
- 链上事件索引结果的原始记录

规则：

- 只追加，不修改历史文件
- 按数据源和时间分区

当前 MVP 路径：

```text
data/raw/source=gamma_markets/dt=YYYY-MM-DD/hour=HH/bucket_start=YYYYMMDDTHHMMSSZ_node=<node>.jsonl.gz
data/raw/source=gamma_events/dt=YYYY-MM-DD/hour=HH/bucket_start=YYYYMMDDTHHMMSSZ_node=<node>.jsonl.gz
data/raw/source=data_trades/dt=YYYY-MM-DD/hour=HH/bucket_start=YYYYMMDDTHHMMSSZ_node=<node>.jsonl.gz
data/raw/source=clob_books/dt=YYYY-MM-DD/hour=HH/bucket_start=YYYYMMDDTHHMMSSZ_node=<node>.jsonl.gz
data/raw/source=clob_midpoints/dt=YYYY-MM-DD/hour=HH/bucket_start=YYYYMMDDTHHMMSSZ_node=<node>.jsonl.gz
data/raw/source=clob_spreads/dt=YYYY-MM-DD/hour=HH/bucket_start=YYYYMMDDTHHMMSSZ_node=<node>.jsonl.gz
data/raw/source=clob_batch_prices_history/dt=YYYY-MM-DD/hour=HH/bucket_start=YYYYMMDDTHHMMSSZ_node=<node>.jsonl.gz
data/raw/source=data_oi/dt=YYYY-MM-DD/hour=HH/bucket_start=YYYYMMDDTHHMMSSZ_node=<node>.jsonl.gz
data/raw/source=data_holders/dt=YYYY-MM-DD/hour=HH/bucket_start=YYYYMMDDTHHMMSSZ_node=<node>.jsonl.gz
data/raw/source=ws_market/dt=YYYY-MM-DD/hour=HH/bucket_start=YYYYMMDDTHHMMSSZ_node=<node>.jsonl.gz
```

#### Silver 层

用途：

- 存放标准化、类型化、适合分析使用的规范数据

格式：

- `Parquet`，压缩使用 `zstd`

内容：

- 维表
- 事实表
- 稳定 schema 的中间层数据集

规则：

- 以追加分区方式写入
- 明确 schema version
- 通过 raw 层和确定性解析流程生成

#### Gold 层

用途：

- 面向客户交付
- 只输出经过质量校验和文档化的数据包

格式：

- `Parquet`，压缩使用 `zstd`

内容：

- 客户可直接使用的数据集
- 特征表
- 地址画像表
- 因子和衍生分析表

规则：

- 只有通过质量检查的数据才能发布
- 每次发布都附带 manifest、schema、字段说明和版本说明

### 5.2 为什么选择 Parquet

Parquet 应作为主交付格式，原因如下：

- 量化团队、做市团队、研究团队都可以直接使用 Python、DuckDB、Polars、Spark、ClickHouse 读取
- 对结构化历史数据压缩率高
- 支持按列读取，筛选效率高
- 相比 CSV 或普通 JSON，更适合大规模历史数据交付

### 5.3 压缩策略

- Parquet 使用 `zstd`
- Raw JSONL 使用 `zstd` 或 `gzip`
- 如果部署环境允许，优先统一使用 `zstd`

### 5.4 分区策略

基础分区方案：

- `dataset=<name>/dt=YYYY-MM-DD/hour=HH/`

对于高频、大体量表，追加 bucket：

- `market_bucket=<nn>`
- `address_bucket=<nn>`

这样可以避免单目录文件过多和热点目录问题。

### 5.5 文件大小目标

避免产生大量小文件。建议：

- 生产级 Parquet 文件控制在 `64 MB` 到 `512 MB`
- 早期调试时允许更小，但正式发布前应进行 compaction

## 6. 核心数据集设计

第一版应优先定义以下规范数据集。

### 6.1 维表

#### `markets`

字段建议：

- `market_id`
- `event_id`
- `condition_id`
- `question`
- `slug`
- `description`
- `category`
- `tags`
- `status`
- `active`
- `closed`
- `resolved`
- `start_time`
- `end_time`
- `resolution_time`
- `created_at`
- `updated_at`
- `schema_version`
- `ts_ingest`

说明：

- 该表应按慢变维方式管理
- 历史变更不能直接覆盖

#### `tokens`

字段建议：

- `token_id`
- `market_id`
- `outcome`
- `side_index`
- `condition_id`
- `created_at`
- `ts_ingest`

### 6.2 事实表

#### `price_snapshots`

字段建议：

- `ts_event`
- `ts_ingest`
- `market_id`
- `token_id`
- `mid_price`
- `best_bid`
- `best_ask`
- `spread`
- `source`
- `ingest_run_id`

建议频率：

- 第一版从 1 分钟快照开始
- 后续如有需要，再对重点市场增加到 5 秒或 15 秒

#### `trades`

字段建议：

- `trade_id`
- `ts_event`
- `ts_ingest`
- `market_id`
- `token_id`
- `price`
- `size`
- `notional`
- `side`
- `taker_order_id`
- `transaction_hash`
- `source`
- `ingest_run_id`

如果可获得 maker 侧明细：

- 建议单独建子表或关联映射表，不要直接堆到主表中

#### `order_fills_onchain`

字段建议：

- `block_number`
- `block_timestamp`
- `transaction_hash`
- `log_index`
- `order_hash`
- `maker_address`
- `taker_address`
- `token_id`
- `maker_amount_filled`
- `taker_amount_filled`
- `fee`
- `source`
- `ingest_run_id`

#### `orderbook_l2`

字段建议：

- `ts_event`
- `ts_ingest`
- `market_id`
- `token_id`
- `side`
- `price`
- `size`
- `level`
- `snapshot_id`

说明：

- 第一版默认不对所有市场持续保存全量盘口深度
- 只有在明确存在客户需求时才开启大规模长期归档

### 6.3 关联和增强表

#### `trade_order_links`

用途：

- 将链下 trade 与一个或多个 order hash 关联起来

字段建议：

- `trade_id`
- `order_hash`
- `role`
- `link_confidence`
- `matched_via`
- `ts_ingest`

#### `trade_address_links`

用途：

- 将 trade 与链上地址关联

字段建议：

- `trade_id`
- `order_hash`
- `maker_address`
- `taker_address`
- `link_confidence`
- `match_status`
- `match_notes`
- `ts_ingest`

#### `address_profiles`

用途：

- 按天或按周期生成地址级画像和特征快照

字段建议：

- `address`
- `profile_date`
- `total_trades`
- `total_volume`
- `realized_pnl_proxy`
- `avg_holding_time`
- `maker_ratio`
- `taker_ratio`
- `win_rate_proxy`
- `early_entry_score`
- `market_diversity_score`
- `recency_score`
- `cluster_id`
- `profile_version`

## 7. 分阶段实施计划

## 阶段 0：基础建设

### 目标

先把项目骨架和规范建好，避免后续出现架构债务。

### 工作内容

- 初始化 Python 项目结构
- 定义配置加载方式
- 定义数据集命名规范
- 定义 schema version 管理规则
- 定义分区规则
- 定义状态文件管理规范
- 定义日志格式和监控指标
- 定义本地、测试、生产环境隔离方式

### 输出物

- 项目目录骨架
- 配置文件模板
- schema 注册表基础文件
- 本地运行文档
- 生产运行文档

### 验收标准

- 新工程师能快速看懂采集、解析、匹配、交付各自的边界
- 所有目录、命名、分区、表名都不是临时拼凑的

## 阶段 1：稳定采集 MVP

### 目标

以最小运维复杂度，稳定持续地采集高价值市场数据。

### 范围

包含：

- 市场和事件元数据
- 定时价格快照
- 成交历史或成交流
- 链上与成交相关的事件

不包含：

- 所有数据源的全量历史回补
- 所有市场的长期全量盘口深度
- 面向客户的 API 服务

### 工作内容

- 实现市场发现采集器
- 实现元数据刷新任务
- 实现价格快照采集器
- 实现成交采集器
- 实现链上事件采集器
- 实现失败重试、超时和退避
- 实现 checkpoint 状态保存
- 实现 raw 层写入器
- 实现结构化日志
- 实现健康状态心跳
- 实现主备节点心跳监测与故障切换状态标记
- 实现按时间窗口的云端增量回填脚本
- 实现回填后的主键去重与缺口校验

### 输出物

- 可长期运行的采集管道
- 按源和时间分区的 raw 数据
- 基础运维指标
- 主备切换与回填工具（最小可用版）

### 验收标准

- 采集进程可稳定运行至少 72 小时
- 不出现静默失败
- 重启后不会丢失状态
- raw 数据可用于重放
- 本地故障期间云端可持续缓冲，恢复后可在指定时间窗完成增量回填

## 阶段 2：标准化数据湖

### 目标

将原始包体转为稳定、可分析、可交付的研究级数据集。

### 工作内容

- 为所有核心表定义 canonical schema
- 构建 raw 到 silver 的解析器
- 实现确定性去重
- 实现市场元数据的慢变维管理
- 将所有时间统一为 UTC
- 计算 notional 和基础衍生字段
- 生成分区化的 Parquet 数据集
- 为每个分区生成 manifest
- 生成字段字典

### 输出物

- `markets`
- `tokens`
- `price_snapshots`
- `trades`
- `order_fills_onchain`
- schema 注册文件
- manifest 文件

### 验收标准

- 相同 raw 数据重复重处理，生成结果一致
- 所有 schema 显式声明并带版本
- 使用 DuckDB 或 Polars 可以直接读取分析，无需额外清洗

## 阶段 3：成交与地址关联

### 目标

建立链下成交与链上地址之间的高置信度关联层。

### 匹配策略

不能假设所有记录都天然一一对应。

主要匹配键包括：

- `taker_order_id`
- maker order IDs
- `order_hash`
- `transaction_hash`
- `token_id`
- `market_id`
- 时间戳接近程度
- 数量和价格一致性

### 工作内容

- 规范化 API 侧订单引用字段
- 规范化链上订单成交事件
- 定义一对一、一对多和歧义匹配规则
- 实现置信度评分
- 将未匹配和低置信度记录单独存放
- 生成抽样核查报告

### 匹配状态分类

- `exact`
- `high_confidence`
- `ambiguous`
- `unmatched`
- `invalid`

### 输出物

- `trade_order_links`
- `trade_address_links`
- 置信度评分逻辑
- 匹配质量报告

### 验收标准

- 关联过程可重复执行
- 歧义情况被明确标注
- exact 和 high_confidence 记录可单独筛选供下游使用

## 阶段 4：地址画像与特征工程

### 目标

把地址关联结果转换为可直接销售或供模型使用的增强数据产品。

### 特征方向

- 活跃度特征
- 资金规模和换手特征
- maker/taker 行为特征
- 建仓时机特征
- 市场类别偏好
- 持仓时长代理特征
- 方向一致性特征
- 近期表现加权特征
- 集中度与分散度特征
- 对事件和波动的反应特征

### 工作内容

- 定义特征窗口：1 天、7 天、30 天、全历史
- 构建地址级聚合指标
- 构建市场级参与者摘要
- 在合理情况下构建地址聚类
- 定义特征版本控制方式
- 生成每日特征快照

### 输出物

- `address_profiles`
- `market_participant_stats`
- `address_feature_snapshots`

### 验收标准

- 所有特征都能从 silver 层稳定重建
- 特征定义清晰、可文档化、可版本化
- 输出结果可直接供量化团队做研究或建模

## 阶段 5：客户交付层

### 目标

把内部数据平台产物包装成客户能稳定接入的正式数据产品。

### 工作内容

- 定义发布节奏：按日全量包，按小时增量包可选
- 生成发布 manifest
- 输出 schema 文档和变更说明
- 加入数据包校验逻辑
- 设计客户目录结构
- 准备授权说明和数据使用说明模板
- 提供示例 notebook 或查询样例

### 输出物

- gold 层数据集
- 发布 manifest
- schema 包
- 使用文档

### 验收标准

- 客户不需要自己反向猜字段含义
- 每次发布都能通过 checksum 和 manifest 校验
- schema 变化可追踪

## 阶段 6：商业扩展

### 目标

从“卖数据文件”升级为“卖数据产品和服务”。

### 潜在产品

- smart money 数据流
- 地址观察列表
- 特定事件参与者流向分析
- 做市行为分析
- 模型可直接使用的因子库
- 异常交易提醒
- 风险与拥挤度分析
- 高价值历史包

### 潜在服务

- 查询 API
- 按市场、地址、分类、时间过滤后的定制导出
- 定制研究报告
- 回测用因子包
- 每日信号订阅
- 面向高端客户的 webhook 或流式推送

### 验收标准

- 新产品都建立在稳定的 canonical 数据集之上
- 扩展服务不需要推翻底层采集架构

## 8. 运维最佳实践

### 8.1 运行方式

- 使用 `systemd` 或容器托管采集进程
- 尽量做到一个进程负责一类职责
- 采集任务除显式 checkpoint 外保持无状态

### 8.2 状态与 Checkpoint

- 所有 checkpoint 放在独立的 `state/` 目录
- 状态文件必须原子写入
- 状态文件绝不能和交付数据混放
- 本地节点额外写入 `heartbeat` 文件，供云端判断主节点健康状态

### 8.3 观测指标

至少监控以下指标：

- 最近一次成功采集时间
- 每分钟采集事件数
- 每小时 raw 文件数
- 每小时 silver 分区生成数
- 重试次数和重连次数
- 重复率
- 未匹配 trade 比例
- 磁盘占用
- 端到端延迟

### 8.4 告警

出现以下情况必须告警：

- 超过阈值时间没有新数据
- 重试次数异常增加
- 磁盘空间低于阈值
- 分区生成失败
- 出现 schema 不匹配
- 未匹配关联比例异常升高

### 8.5 保留策略

建议初始策略：

- raw 热存储：7 到 30 天
- silver：长期保留
- gold：长期保留，且发布后不可变
- 日志：快速轮转并压缩

主备模式下建议：

- 本地：`raw/silver/gold` 长期保留
- 云端：仅保留短期 `raw`（建议 3-7 天）和必要的发布缓存
- 云端自动清理过期分区，防止云盘成本持续抬升

### 8.6 备份

- 每天备份 manifest、schema 和 gold 数据集
- 保留足够的 raw 数据以便重建近期 silver 和 gold
- 定期验证恢复流程，而不是只做备份不做演练

### 8.7 主备同步与回填策略

- 分区规范
  - 按 `source + dt + hour` 固定分区
  - 文件滚动按时间或大小触发，避免小文件爆炸
- 同步策略
  - 常态不进行云端全量回传
  - 仅同步缺失时间窗的数据分区
- 去重策略
  - 基于业务唯一键去重（如 `trade_id`、`order_hash+log_index`、消息序列键）
  - 回填后生成去重报告
- 完整性策略
  - 回填后按分区核对行数、时间范围、checksum
  - 不通过校验的分区进入重拉队列

## 9. 数据质量最佳实践

应自动化检查以下问题：

- 空值率异常
- 时间戳非法
- 主键重复
- 数量为负或不可能值
- 价格超范围
- 分区不完整
- schema 漂移
- 事件顺序异常

每次数据发布应至少附带：

- 行数
- 最小和最大时间戳
- 不同市场和 token 数量
- 重复记录数量
- 异常标记

## 10. 成交与地址关联的最佳实践

这个方向有商业价值，但必须被视为“带置信度的推断层”。

建议遵循：

- 不要把歧义匹配宣传成确定事实
- 为每条关联保留置信度和证据字段
- 明确区分 exact match 和 inferred match
- 保留所有匹配输入，支持日后审计
- 谨慎处理 proxy、funder、多钱包结构
- 把匹配逻辑设计成可重复重跑、可持续优化

推荐输出等级：

- 精确链上匹配
- 强 order-hash 匹配
- 强 transaction-plus-amount 匹配
- 低置信度推断匹配
- 未解决

## 11. 商业数据产品策略

不要只停留在卖原始数据包。

更合理的产品分层如下：

### 第 1 档：核心市场数据

- 市场元数据
- 价格快照
- 成交记录
- 可选重点盘口数据

### 第 2 档：地址关联数据

- trade-address 关联结果
- 地址按市场参与情况
- 活跃地址摘要
- 参与者流向视图

### 第 3 档：分析与因子

- smart money 分数
- 地址行为因子
- 事件动量参与度
- 可直接建模的特征
- 异常与拥挤度指标

### 第 4 档：服务

- API 接入
- 研究支持
- 定制导出
- 定制仪表盘

## 12. 开发优先级

建议按以下顺序实施：

1. 稳定采集
2. 干净的 schema 和 Parquet 输出
3. 成交地址关联层
4. 特征层
5. 客户交付层
6. 高级商业服务

这个顺序可以保证每个阶段都能产出有价值的成果，同时避免过早把系统做重。

## 13. 第一版建议范围

第一版生产数据集建议至少包含：

- `markets`
- `tokens`
- `price_snapshots`
- `trades`
- `order_fills_onchain`
- `trade_order_links`
- `trade_address_links`

第一版暂不包含：

- 所有市场的长期全量盘口深度
- 面向客户的在线 API
- 复杂的实体聚类体系

## 14. 成功标准

当满足以下条件时，项目说明走在正确方向上：

- 采集系统能稳定运行数周，而不是只跑几个小时
- raw 数据可重放，重建出的 silver 结果一致
- 客户拿到 gold 数据后无需自行大规模清洗
- trade-address 关联能稳定产出有意义的 exact/high-confidence 样本
- 新增特征或服务时，不需要重构底层存储架构

## 15. 下一步实施建议

在这份规划文档之后，下一步最合理的动作是：

- 搭建项目骨架
- 定义第一版 schema
- 实现 markets、prices、trades、chain fills 的采集 MVP
- 按本文规范写出 raw 和 silver 两层数据
