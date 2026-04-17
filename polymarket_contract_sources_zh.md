# Polymarket 合约源码索引

这份文档用于记录：

- Polymarket 关键合约地址
- 对应的本地源码仓库
- 后续优先分析的源码入口

当前源码通过 `forge install --no-git --shallow` 下载到本仓库的 `lib/` 目录。

## 1. 已下载的源码仓库

- `lib/ctf-exchange`
- `lib/neg-risk-ctf-adapter`
- `lib/uma-ctf-adapter`
- `lib/proxy-factories`
- `lib/conditional-tokens-contracts`
- `lib/safe-smart-account`
- `lib/protocol`

## 2. 合约地址与本地源码映射

### 核心交易合约

- `CTF Exchange`
  - 地址：`0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E`
  - 本地源码：
    - `lib/ctf-exchange/src/exchange/CTFExchange.sol`
    - `lib/ctf-exchange/src/exchange/interfaces/ITrading.sol`
  - 备注：
    - 这是后续事件监听和 trade-address matching 的第一优先级合约

- `Neg Risk CTF Exchange`
  - 地址：`0xC5d563A36AE78145C45a50134d48A1215220f80a`
  - 本地源码：
    - `lib/neg-risk-ctf-adapter/src/NegRiskCtfExchange.sol`
    - `lib/neg-risk-ctf-adapter/src/interfaces/ICTFExchange.sol`
  - 备注：
    - 用于 neg risk 市场，事件接口和标准交易所高度相关

- `Neg Risk Adapter`
  - 地址：`0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296`
  - 本地源码：
    - `lib/neg-risk-ctf-adapter/src/NegRiskAdapter.sol`
  - 备注：
    - 负责 neg risk 市场中的特殊仓位转换逻辑

- `Conditional Tokens`
  - 地址：`0x4D97DCd97eC945f40cF65F87097ACe5EA0476045`
  - 本地源码：
    - `lib/conditional-tokens-contracts/contracts/ConditionalTokens.sol`
  - 备注：
    - 处理拆分、合并、赎回和 ERC1155 仓位流转

### 代币合约

- `USDC.e`
  - 地址：`0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174`
  - 备注：
    - 标准外部代币合约，本仓库未单独下载其源码
    - 通常只需 ABI 和 decimals 信息即可支撑数据平台解析

### 钱包工厂合约

- `Gnosis Safe Factory`
  - 地址：`0xaacfeea03eb1561c4e67d661e40682bd20e3541b`
  - 本地源码候选：
    - `lib/safe-smart-account/contracts/proxies/SafeProxyFactory.sol`
    - `lib/proxy-factories/packages/safe-factory/contracts/SafeProxyFactory.sol`
  - 备注：
    - 需要后续核对部署版本和构造参数差异

- `Polymarket Proxy Factory`
  - 地址：`0xaB45c5A4B0c941a2F231C04C3f49182e1A254052`
  - 本地源码：
    - `lib/proxy-factories/packages/proxy-factory/contracts/ProxyWallet/ProxyWalletFactory.sol`
  - 备注：
    - 对地址画像很重要，便于识别代理钱包体系

### 判定与适配合约

- `UMA Adapter`
  - 地址：`0x6A9D222616C90FcA5754cd1333cFD9b7fb6a4F74`
  - 本地源码：
    - `lib/uma-ctf-adapter/src/UmaCtfAdapter.sol`

- `UMA Optimistic Oracle`
  - 地址：`0xCB1822859cEF82Cd2Eb4E6276C7916e692995130`
  - 本地源码候选：
    - `lib/protocol`
  - 备注：
    - UMA 主仓库较大，后续需要从中定位实际部署版本对应合约

## 3. 已确认的关键事件入口

当前已在源码中定位到最关键的交易事件定义：

- `OrderFilled`
  - 路径：
    - `lib/ctf-exchange/src/exchange/interfaces/ITrading.sol`
    - `lib/neg-risk-ctf-adapter/src/interfaces/ICTFExchange.sol`

- `OrdersMatched`
  - 路径：
    - `lib/ctf-exchange/src/exchange/interfaces/ITrading.sol`
    - `lib/neg-risk-ctf-adapter/src/interfaces/ICTFExchange.sol`

这些文件是后续确定“必须监听哪些链上事件”的第一入口。

## 4. 建议的分析优先级

第一优先级：

- `lib/ctf-exchange/src/exchange/CTFExchange.sol`
- `lib/ctf-exchange/src/exchange/interfaces/ITrading.sol`
- `lib/conditional-tokens-contracts/contracts/ConditionalTokens.sol`

第二优先级：

- `lib/neg-risk-ctf-adapter/src/NegRiskCtfExchange.sol`
- `lib/neg-risk-ctf-adapter/src/NegRiskAdapter.sol`
- `lib/proxy-factories/packages/proxy-factory/contracts/ProxyWallet/ProxyWalletFactory.sol`

第三优先级：

- `lib/uma-ctf-adapter/src/UmaCtfAdapter.sol`
- `lib/safe-smart-account/contracts/proxies/SafeProxyFactory.sol`
- `lib/protocol`

## 5. 下一步建议

基于当前源码落地情况，下一步最合理的是：

1. 逐个整理核心合约的事件列表
2. 明确哪些事件必须监听
3. 明确每个事件能解决什么数据问题
4. 再据此设计链上采集表结构和 trade-address matching 逻辑

