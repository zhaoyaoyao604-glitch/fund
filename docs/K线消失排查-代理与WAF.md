# 部分卡片 K 线消失：代理/WAF 排查记录

时间：2026-10-29　范围：f:\fund（13 张卡片）

## 一、结论

**与 VPN 无关。** 真因是腾讯 `ifzq.gtimg.cn` 的 **`fqkline`（日/周/月K）接口路径**被 WAF 挑战，返回 501 JS 验证页；同域名的
`kline/mkline`（分钟K）与 `minute/query`（分时）仍返回 200。所以"部分卡片"= 日K 卡全空、分钟K与美股卡正常。

## 二、代理侧取证（全部指向"没走代理"）

| 检查项 | 结果 |
|---|---|
| `HTTP_PROXY` / `HTTPS_PROXY` / `NO_PROXY` | 均为空 |
| 系统代理 `ProxyEnable` | 0（关闭，残留 `ProxyServer=127.0.0.1:7897` 未启用） |
| 7890 / 7891 / 7897 / 1080 / 10808 监听 | 无 |
| 代理内核进程 | 仅 `clash-verge-service`（服务常驻），内核未启动 |
| TUN 虚拟网卡 | 无（只有 Hyper-V 与 WLAN） |
| 默认路由 / IPv6 默认路由 | 单条 `192.168.2.1 → WLAN 2`；无 `::/0` |
| 出口 IP | 211.148.123.240　中国 广东 深圳 鹏博士 |
| DNS 服务器 | 192.168.2.1（路由器），`ifzq.gtimg.cn` 解析为 175.27.37.181 / 175.27.38.131（腾讯云国内 IP，无污染） |

后端代码本身已免疫代理：`server/main.py` 的 `get_json` / `get_text` 均 `httpx.AsyncClient(trust_env=False)`。

## 三、上游逐接口探测（直连，`--noproxy "*"`）

| 接口 | 结果 | 影响 |
|---|---|---|
| `ifzq.gtimg.cn/appstock/app/fqkline/get`（日/周/月K） | **501** | A股/港股/ETF/LOF 的日K卡全空 |
| `web.ifzq.gtimg.cn/.../fqkline/get` | **501** | 同上 |
| `proxy.finance.qq.com/ifzqgtimg/.../fqkline/get`（第三通道） | **501** | 三条通道同时被挑战，回退失效 |
| `ifzq.gtimg.cn/appstock/app/kline/mkline`（分钟K） | 200 | `sh512820` 的 m60 卡正常 |
| `ifzq.gtimg.cn/appstock/app/minute/query`（分时） | 200 | 分时正常 |
| `fqkline` 加完整浏览器 UA + Referer | 仍 501 | 非头字段可绕过 |
| `fqkline` 走 http | 302 | — |
| `qt.gtimg.cn`（实时报价） | 200 | 报价正常 |
| `stock.finance.sina.com.cn`（美股日K） | 200 | `usSOXL` / `usSOXS` 正常 |
| `searchapi.eastmoney.com`（搜索） | 200 | — |
| `push2his.eastmoney.com`（美股分时） | 000（TCP 443 可达但 TLS 被断） | 仅影响美股分时，非本次问题 |

## 四、卡片逐个体检（后端 /api/kline 直连后端）

```
sz159530 day 502     hk07747 day 502     sh501018 day 502
sh603286 day 502     sh600436 day 502     usSOXL  day 300 OK
sz161226 day 502     sz159011 day 502     sh512820 m60 300 OK
sh512010 day 502     sh515880 day 502     sh512480 day 502
usSOXS   day 300 OK
```
9 张日K卡 → 502（K线消失）；2 张美股卡 + 1 张分钟K卡 → 正常。

## 五、VPN 在什么情况下会真的产生影响

`trust_env=False` 只关闭"读取系统代理环境变量"这一条路，**挡不住 TUN / 全局路由劫持**：

- Clash Verge 开 **TUN（虚拟网卡）** 或系统代理全局模式时，后端 httpx 的直连也会被内核接走，
  请求从**境外节点 IP** 出网访问 `ifzq.gtimg.cn` → 腾讯 WAF 对境外/机房 IP 的挑战明显更严、冷却更久。
- 因此看盘期间建议：Clash 保持"系统代理关闭 + TUN 关闭"，或对 `ifzq.gtimg.cn`、`qt.gtimg.cn`、
  `sina.com.cn`、`eastmoney.com` 加直连（DIRECT）分流规则。
- 本次实测 VPN 完全没生效，所以 501 是直连国内住宅 IP 下发生的，属腾讯侧风控，不是本地代理配置问题。

## 六、可选修复方向

1. 加第四通道：日K 换成东财 `push2his.eastmoney.com/api/qt/stock/kline/get`（需先解决本机 TLS 000）。
2. 对 `fqkline` 失败做"指数退避 + 冷却标记"，避免每次刷新都去撞已被挑战的路径。
3. 前端在 502 时卡片显示明确文案而不是空白图（现在已有 err 文案，但 K 线区域是空画布）。
