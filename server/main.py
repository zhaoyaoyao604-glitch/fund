# 基金看盘后端：FastAPI -> 免费行情源（无 key）
# A股/港股/ETF：腾讯 ifzq（日/周/月K、分钟K、分时），web. 子域与裸域互为备份，WAF 风控时自动切换
# 美股：日/周/月K 走新浪全历史（不复权）+ 腾讯实时报价补当日活动K线；分时走东财 trends2（上游限流时返回提示）
# 注：腾讯 WAF 会不定时挑战 ifzq 的某个子域（返回 501 JS验证页），故请求层做双域回退
import asyncio
import datetime
import json
import math
import sqlite3
import subprocess
import time
from pathlib import Path
from urllib.parse import quote, urlsplit, urlunsplit

import httpx
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
# ifzq 双域：web. 与裸域内容一致，谁被 WAF 挑战就切另一个（pref 记住当前可用域）
IFZQ_HOSTS = ("web.ifzq.gtimg.cn", "ifzq.gtimg.cn")
# 第三通道：腾讯网页端 proxy.finance.qq.com/ifzqgtimg。它与 ifzq 共用同一套行情数据，但 WAF 规则独立：
# ifzq 两个域同时返回 501 JS 挑战页时它通常仍可用，返回结构与 fqkline/mkline 完全一致，路径前缀加 /ifzqgtimg 即可
PROXY_HOST = "proxy.finance.qq.com"
_chan = {"i": 0}  # 上次取数成功的通道下标（0/1=ifzq 双域，2=proxy）：命中后优先复用，避免每轮先撞已失效的通道
WEB = "https://web.ifzq.gtimg.cn/appstock/app"  # minute（分时）
BARE = "https://web.ifzq.gtimg.cn/appstock/app"  # fqkline（日/周/月K） / mkline（分钟K）
EM_SUGGEST = "https://searchapi.eastmoney.com/api/suggest/get"  # 东财搜索
EM_KLINE = "https://push2his.eastmoney.com/api/qt/stock/kline/get"  # 东财日/分钟K：ifzq 全域被 WAF 挑战时的兜底源
EM_SECID = {"sh": "1.", "sz": "0.", "hk": "116."}  # 东财市场号：沪/深/港（us 走独立链路）
EM_TOKEN = "D43BF722C8E33BDC906FB84D85E326E8"  # 东财网页端公开 token
QT = "https://qt.gtimg.cn/q="  # 腾讯实时报价（GBK文本），美股补当日活动K线用
SINA_US_DAILY = "https://stock.finance.sina.com.cn/usstock/api/jsonp_v2.php/var%20_t=/US_MinKService.getDailyK"  # 新浪美股日K全历史（不复权）
# 新浪美股分钟K（不复权）独走此源：腾讯 mkline 不接受 us 代码（返回 param error），东财 push2his 又被 CDN 掐 Python 客户端。
# type=分钟粒度，一次固定返回最近约 1023 根，时间戳为美东交易所时间（09:30~16:00）
SINA_US_MINK = "https://stock.finance.sina.com.cn/usstock/api/jsonp_v2.php/var%20_t=/US_MinKService.getMinK"
# type=1（1 分钟）：实测 sin a 一次给 1023 根 ≈ 最近 2.6 个交易日（美股每天约 391 根）
US_MIN_TYPE = {"m1": 1, "m5": 5, "m15": 15, "m30": 30, "m60": 60}
EM_TRENDS = "https://push2his.eastmoney.com/api/qt/stock/trends2/get"  # 东财分时（美股）
SINA_A_KLINE = "https://money.finance.sina.com.cn/quotes_service/api/json_v2.php/CN_MarketData.getKLineData"  # 新浪A股K线兜底（不复权）
SINA_HK_KLINE = "https://finance.sina.com.cn/stock/hkstock/{}/klc2_kl.js"  # 新浪港股全历史日K（K2 加密格式，node 解密，不复权）
# 新浪港股分时（1分钟粒度，一次给最近 5 个交易日）：港股分钟K在腾讯 mkline（param error）与东财
# push2his（本机 000 / push2 返回 dktotal=0）都拿不到，只能由这份 1 分钟数据本地聚合成 m5/m15/m30/m60
SINA_HK_MIN = "https://quotes.sina.cn/hk/api/openapi.php/HK_MinlineService.getMinline?symbol={}"
# 腾讯港股多日分时（同为最近 5 个交易日、每天 332 点，但当日收盘后比新浪先补齐）：新浪分时失败时顶替
TENCENT_HK_DAY = "https://web.ifzq.gtimg.cn/appstock/app/day/query?code=hk{}"
# 港股 1 分钟归档库：上游所有免费源（新浪分时/腾讯多日分时）都只给最近 5 个交易日，港股分钟K 想画出
# 和 A 股一样长的历史只能靠本地逐日累积——每跑一天归档多一天，m5/m15/m30/m60 的根数随之增长
HK_MIN_DB = Path(__file__).resolve().parent / "hkmin.db"
# 每个交易日聚合出的根数（港股 09:30~12:00 / 13:00~16:00 两段各按间隔分桶）：m5 66 / m15 22 / m30 11 / m60 6；
# m1 不聚合，直接取上游 1 分钟点，每天 332 点（09:30 首点 + 每分钟一桶）
HK_BARS_PER_DAY = {"m1": 332, "m5": 66, "m15": 22, "m30": 11, "m60": 6}
HK_MIN_KEEP = 420  # 每标的保留交易日上限：m60 每天 6 根，420 天 ≈ 2500 根，远超 424 根预热需求
DAY_PERIODS = {"day", "week", "month"}
# m1：A股走腾讯 mkline（cnt=800 实测正常返回 800 根，约 3.3 个交易日）；美股走新浪 type=1；港股取上游 1 分钟点不聚合
MIN_PERIODS = {"m1", "m5", "m15", "m30", "m60"}
TS_FMT = "%Y-%m-%d %H:%M"  # 订单时间戳统一成定长格式，SQL 里 ORDER BY ts 字典序即时间序

# ---------- 卡片列表持久化：SQLite（前端增/删/排序/周期变化均整体回写） ----------
DB_PATH = Path(__file__).resolve().parent / "cards.db"
# 首次建库时的种子自选股（BK1134 算力概念是东财板块指数，腾讯源无对应代码，未收入）
SEED_CARDS = [
    ("sh508060", "南方万国数据REIT"), ("sz159583", "通信ETF"), ("sh000990", "全指消费"),
    ("hk07552", "南方两倍纳指"), ("sh588810", "科创芯片ETF"), ("sz159153", "消费电子ETF"),
    ("sh501018", "南方原油LOF"), ("sz159713", "稀土ETF"), ("sh588130", "科创医药ETF"),
    ("sz159206", "卫星ETF"), ("sh588730", "科创AI ETF"), ("sh518880", "黄金ETF"),
    ("sz002756", "永兴材料"), ("sz159755", "电池ETF"), ("sz159326", "电网设备ETF"),
    ("sz159267", "航天ETF"), ("sz159865", "养殖ETF"), ("sh512560", "军工ETF"),
    ("sh588940", "科创50ETF"), ("sh560850", "信创ETF"), ("sh588170", "科创半导体ETF"),
    ("sz159870", "化工ETF"), ("sh513310", "中韩半导体ETF"), ("sh513500", "标普500ETF"),
    ("usSOXL", "三倍做多半导体"), ("sz159981", "能源化工ETF"), ("sh512220", "煤炭ETF"),
    ("sh513010", "恒生科技ETF"), ("sh510580", "中证500ETF"), ("sz159527", "云计算ETF"),
    ("sz161226", "国投白银LOF"), ("sh562500", "机器人ETF"), ("sh512820", "银行ETF"),
    ("sz159869", "游戏ETF"), ("sh560990", "证券ETF"), ("sz159663", "机床ETF"),
    ("sz159512", "汽车ETF"),
]


def _conn():
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    return con


def _init_db():
    con = _conn()
    has = con.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='cards'").fetchone()
    if not has:  # 只在首次建库时灌种子；此后即使用户清空全部卡片也不再重建
        with con:
            con.execute("CREATE TABLE cards (pos INTEGER NOT NULL, symbol TEXT PRIMARY KEY, "
                        "name TEXT NOT NULL, period TEXT NOT NULL DEFAULT 'day')")
            con.executemany("INSERT INTO cards (pos, symbol, name, period) VALUES (?, ?, ?, ?)",
                            [(i, s, n, "day") for i, (s, n) in enumerate(SEED_CARDS)])
    ohas = con.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='orders'").fetchone()
    if not ohas:  # 交易流水表：唯一事实源，持仓/收益/可卖全部由 _replay 重演推导，不落任何派生值
        with con:
            con.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY AUTOINCREMENT, symbol TEXT NOT NULL, "
                        "name TEXT NOT NULL, side TEXT NOT NULL CHECK(side IN ('buy','sell')), "
                        "price REAL NOT NULL CHECK(price > 0), qty REAL NOT NULL CHECK(qty > 0), "
                        "ts TEXT NOT NULL)")
            # 迁移：旧 holdings 表的持仓转成一周前的买入流水（自然过 T+1，页面数字与迁移前一致），随后废弃该表
            if con.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='holdings'").fetchone():
                past = (datetime.datetime.now() - datetime.timedelta(days=7)).strftime(TS_FMT)
                old = con.execute("SELECT symbol, shares, cost FROM holdings").fetchall()
                for r in old:
                    c = con.execute("SELECT name FROM cards WHERE symbol=?", (r["symbol"],)).fetchone()
                    con.execute("INSERT INTO orders (symbol, name, side, price, qty, ts) "
                                "VALUES (?, ?, 'buy', ?, ?, ?)",
                                (r["symbol"], c["name"] if c else r["symbol"], r["cost"], r["shares"], past))
                con.execute("DROP TABLE holdings")
    # 实时跟随仓（live）：表名一律加 _live 后缀，结构与历史模拟仓完全一致。
    # cards_live 只在首次建表时从当前 cards 复制一份（此后两页各自独立增删改，互不同步）；orders_live 建空表
    chas = con.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='cards_live'").fetchone()
    if not chas:
        with con:
            con.execute("CREATE TABLE cards_live (pos INTEGER NOT NULL, symbol TEXT PRIMARY KEY, "
                        "name TEXT NOT NULL, period TEXT NOT NULL DEFAULT 'day')")
            con.execute("INSERT INTO cards_live (pos, symbol, name, period) "
                        "SELECT pos, symbol, name, period FROM cards ORDER BY pos")
    ohas2 = con.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='orders_live'").fetchone()
    if not ohas2:
        with con:
            con.execute("CREATE TABLE orders_live (id INTEGER PRIMARY KEY AUTOINCREMENT, symbol TEXT NOT NULL, "
                        "name TEXT NOT NULL, side TEXT NOT NULL CHECK(side IN ('buy','sell')), "
                        "price REAL NOT NULL CHECK(price > 0), qty REAL NOT NULL CHECK(qty > 0), "
                        "ts TEXT NOT NULL)")
    # 迁移：cards/cards_live 补 pair 列（配对标的代码）——非空时前端把该卡渲染成上下双图：
    # 上=本卡标的、下=pair 标的，两图共用一根横轴与一个缩放条；空串=普通单标的卡
    for t in ("cards", "cards_live"):
        if "pair" not in [r["name"] for r in con.execute(f"PRAGMA table_info({t})")]:
            with con:
                con.execute(f"ALTER TABLE {t} ADD COLUMN pair TEXT NOT NULL DEFAULT ''")
    # 界面偏好表（key-value）：所在页/日期筛选等跨浏览器共享的 UI 偏好，跟库走不跟某个浏览器的 localStorage 走
    if not con.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='prefs'").fetchone():
        with con:
            con.execute("CREATE TABLE prefs (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
    # 最近搜过的标的：筛选框聚焦时直接列出来供点选；两个框共用一份，按最近一次选中时间倒序
    if not con.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='recent_sym'").fetchone():
        with con:
            con.execute("CREATE TABLE recent_sym (symbol TEXT PRIMARY KEY, name TEXT NOT NULL DEFAULT '', "
                        "ts TEXT NOT NULL)")
    # 买入次日卡片前置的每日游标：scope(hist/live) -> 上次执行日 YYYY-MM-DD
    if not con.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='promo'").fetchone():
        with con:
            con.execute("CREATE TABLE promo (scope TEXT PRIMARY KEY, last_date TEXT NOT NULL)")
    # 迁移：orders/orders_live 补 created_at 列（真实下单时刻，与业务 ts 分离）——买入次日前置锚它判定，
    # hist 回放给历史日期的单也按实际操作的第二天前置；存量为 '' 不追溯
    for t in ("orders", "orders_live"):
        if "created_at" not in [r["name"] for r in con.execute(f"PRAGMA table_info({t})")]:
            with con:
                con.execute(f"ALTER TABLE {t} ADD COLUMN created_at TEXT NOT NULL DEFAULT ''")
    # 账户表：每个作用域一条本金（历史模拟仓/实时跟随仓各自独立记账）；可用资金不落库，由流水重演推导
    if not con.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='account'").fetchone():
        with con:
            con.execute("CREATE TABLE account (scope TEXT PRIMARY KEY, capital REAL NOT NULL)")
            con.executemany("INSERT INTO account (scope, capital) VALUES (?, ?)",
                            [("hist", 1000000.0), ("live", 1000000.0)])
    # 画线表：趋势线/水平线按「标的+周期」存，pts 是 JSON 数组 [{x: 类目时间, y: 价格}]；两页共用同一份线
    if not con.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='drawings'").fetchone():
        with con:
            con.execute("CREATE TABLE drawings (id INTEGER PRIMARY KEY AUTOINCREMENT, symbol TEXT NOT NULL, "
                        "period TEXT NOT NULL DEFAULT 'day', kind TEXT NOT NULL CHECK(kind IN ('trend','h')), "
                        "pts TEXT NOT NULL, created TEXT NOT NULL)")
    # 回测记录表：一次回测存一条快照（payload 为 /api/backtest 返回的 K线/均线/成交/资金曲线/统计 JSON）。
    # 只服务「回测记录」展示，与 orders/orders_live 无关；同参数重复回测覆盖同一条，避免刷出一堆重复记录
    if not con.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='backtests'").fetchone():
        with con:
            con.execute("CREATE TABLE backtests (id INTEGER PRIMARY KEY AUTOINCREMENT, symbol TEXT NOT NULL, "
                        "name TEXT NOT NULL, start TEXT NOT NULL DEFAULT '', end TEXT NOT NULL DEFAULT '', "
                        "capital REAL NOT NULL, each REAL NOT NULL, tf TEXT NOT NULL DEFAULT 'day', "
                        "created TEXT NOT NULL, payload TEXT NOT NULL)")
            con.execute("CREATE INDEX idx_backtests_key ON backtests (symbol, start, end, capital, each, tf)")
    else:
        # 老库补列：tf=策略类型（day=日k占Ma3 / m60=1小时k占Ma3），历史记录一律回填 day
        if "tf" not in [r[1] for r in con.execute("PRAGMA table_info(backtests)").fetchall()]:
            with con:
                con.execute("ALTER TABLE backtests ADD COLUMN tf TEXT NOT NULL DEFAULT 'day'")
    con.close()


_init_db()


# ---------- 页面作用域：hist=历史模拟仓（原表），live=实时跟随仓（表名加 _live 后缀） ----------
def _tbl(scope, kind):
    return f"{kind}_live" if scope == "live" else kind  # kind: cards / orders；白名单映射防注入


# ---------- 流水重演引擎：orders 是唯一事实源，持仓/每笔卖出收益/可卖数量全由此推导 ----------
def _is_t0(sym):
    return sym.startswith(("hk", "us"))  # 港美股豁免 T+1；A股（sh/sz）当日买入次日可卖


def _norm_ts(raw):
    # 容忍 2026-09-27[ 10:30[:00]] / 2026/09/27 / 2026-09-27T10:30 等写法，统一成 TS_FMT
    s = str(raw or "").strip().replace("/", "-").replace("T", " ")
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            return datetime.datetime.strptime(s, fmt).strftime(TS_FMT)
        except ValueError:
            pass
    return None


def _norm_asof(as_of):
    # 视点参数规范化：接受 YYYY-MM-DD（补成当天 23:59 含全天）或完整 YYYY-MM-DD HH:MM；空=当前全量。
    # orders/holdings/account 三个接口共用，保证各处回放口径一致
    a = (as_of or "").strip().replace("/", "-")
    if a and len(a) == 10:
        a += " 23:59"
    return a or None


def _replay(con, exclude_id=None, scope="hist", as_of=None, splits=None, defer_today_sell=False):
    """按 (ts, id) 全量重演流水。返回 (pos, realized, conflict)：
    pos = symbol -> {shares, cost(移动加权均价), bought_today(当日买入的A股份额), sold_today(当日卖出份额),
    buy_ts(建仓首笔买入时间), name}；
    realized = 卖出单id -> 该笔已实现收益 (卖出价-当时均价)*数量；
    conflict = 重演中第一笔卖超的记录（正常数据恒为 None，仅删除预检时出现）。
    as_of 为空时按「现在」截断（晚于现在的流水视为尚未生效，不参与重演）；
    as_of（"YYYY-MM-DD HH:MM"）非空时只重演该时点前的流水——历史回放视点：
    回看某日时之后下的单不出现；T+1 的「当日买入」同样按 as_of 的日期判定，回放出的可卖数与当时一致。
    defer_today_sell（显示口径）：视点日（as_of 当天/实时仓的今天）的卖出「次日生效」——不扣份额、
    不计已实现收益，收益下个截止日才体现（基金赎回 T+1 到账式）；仅展示端（holdings/orders/account）开启，
    卖出校验与可卖数不受影响（当日卖出仍占用可卖，不能重复卖），清仓沉底照旧次日触发
    splits（symbol -> [(生效日"YYYY-MM-DD", 数量倍数)]，按生效日升序）：合股/分股事件——重演跨过
    生效日时把该标的持仓换到新股本基准（合股 m<1：shares×m、成本×1/m、市值不变），生效日晚于
    视点的不应用（回放合股前的视点仍是旧股本口径）；流水单的价/量按各自业务时点的基准执行，
    事件只做两段基准间的桥接，总市值跨事件自然连续。缺省 None 不做换算（A股/港股恒空）"""
    sql = f"SELECT id, symbol, name, side, price, qty, ts FROM {_tbl(scope, 'orders')}"
    # 截断口径：有视点按视点截断，无视点按「现在」——预填的未来单不能提前占用份额，
    # 否则它之后的正常卖出会被判成「当时超卖」而遭拒
    cut = as_of or datetime.datetime.now().strftime(TS_FMT)
    conds, args = ["ts <= ?"], [cut]
    if exclude_id is not None:
        conds.append("id != ?")
        args.append(exclude_id)
    if conds:
        sql += " WHERE " + " AND ".join(conds)
    rows = con.execute(sql + " ORDER BY ts, id", tuple(args)).fetchall()
    today = (as_of or datetime.datetime.now().strftime(TS_FMT))[:10]
    pos, realized, conflict = {}, {}, None
    evcur = {}  # symbol -> 已消费到第几个事件（事件与流水都按时间升序，游标单调推进）

    def _drain(sym, day, p):
        # 把生效日不晚于 day 的事件应用到持仓 p（清仓态数值全 0，换算无副作用）
        evs = (splits or {}).get(sym)
        if not evs:
            return
        i = evcur.get(sym, 0)
        while i < len(evs) and evs[i][0] <= day:
            m = evs[i][1]
            p["shares"] *= m
            p["cost"] /= m
            p["bought_today"] *= m
            p["sold_today"] *= m
            i += 1
        evcur[sym] = i

    for r in rows:
        p = pos.setdefault(r["symbol"], {"shares": 0.0, "cost": 0.0, "bought_today": 0.0, "sold_today": 0.0, "buy_ts": None, "name": r["name"]})
        p["name"] = r["name"]
        _drain(r["symbol"], r["ts"][:10], p)  # 该笔业务日之前生效的合股/分股先换基准（截断流水 ts≤as_of，日期天然不超前）
        if r["side"] == "buy":
            if p["shares"] <= 1e-9:  # 建仓（含清仓后重建）记该笔时间；加仓保持首笔买入时间（「最后一次买入」仅在前端展示层从 orders 派生，不改本口径）
                p["buy_ts"] = r["ts"]
            total = p["cost"] * p["shares"] + r["price"] * r["qty"]
            p["shares"] += r["qty"]
            p["cost"] = total / p["shares"]
            if not _is_t0(r["symbol"]) and r["ts"][:10] == today:
                p["bought_today"] += r["qty"]  # A股当日买入份额锁定到明日（T+1）
        else:
            if defer_today_sell and r["ts"][:10] == today:
                p["sold_today"] += r["qty"]  # 视点日的卖出次日才生效：份额与已实现收益都不动，仅记当日已卖（可卖仍要扣）
                continue
            avail = p["shares"] if _is_t0(r["symbol"]) else p["shares"] - p["bought_today"]
            if r["qty"] > avail + 1e-9:
                conflict = dict(r)
                break
            realized[r["id"]] = (r["price"] - p["cost"]) * r["qty"]
            p["shares"] -= r["qty"]
            if p["shares"] <= 1e-9:  # 清仓：均价、当日买入与买入时间一并归零
                p["shares"] = p["cost"] = p["bought_today"] = p["sold_today"] = 0.0
                p["buy_ts"] = None
    for sym, p in pos.items():
        _drain(sym, today, p)  # 尾部事件：买入后到视点日之间无流水也要换算（7-10买入、7-15合股、视点7-16）
    return pos, realized, conflict


def _promote_bought_cards(con, scope, as_of=None):
    """买入后卡片前置、清仓沉底，两视图口径分叉：
    hist（历史模拟仓）视点驱动：模拟仓的买卖业务时间就是筛选截止日，与真实时钟无关——「买入后的第二天」
    按回放轴判定（视点日晚于最后买入日）。每次取卡把仍持仓(_replay as_of 截断)且 last_buy 日期 < 视点日的
    卡整体提到最前，按 last_buy DESC, first_buy ASC, last_id ASC（与抽屉建仓序一致，抽屉第一的持仓卡即首页第一张）；
    视点未过买入日的不前置（当天买的不动）。清仓沉底与前置对称：有流水、视点重演已无持仓、且视点前最后一笔
    流水早于视点日（即前一(及更早)日卖掉清仓，当天卖的不动）的卡挪到列表末尾。promo 表 hist 行存
    「v2|上次重排的视点串」做视点内幂等：同一视点重复取卡不重排，用户手动拖拽的顺序不被覆盖；切换视点
    （推进/回拨日期）才重算；旧格式游标（无 v2| 前缀）视为未跑过，升级后该视点首次取卡即按新口径重排一次。
    hist 无视点不触发。
    live（实时跟随仓）真实时钟驱动：每天首次取卡把「游标日(含)到昨天」created_at（真实下单时刻，锚业务 ts 会永不
    命中）有买入且仍持仓的卡提到最前；昨天及更早清仓（当前无持仓、最后一笔真实下单早于今天，今天清仓的明天沉）
    的卡沉底。游标 promo.last_date 记真实日期，每日幂等一次；昨天没开页面下次打开补前置/补沉底。"""
    if scope == "hist":
        if not as_of:
            return  # 历史仓买卖时间锚筛选截止日，没视点就没有「第二天」，不动 pos
        row = con.execute("SELECT last_date FROM promo WHERE scope='hist'").fetchone()
        cur = row["last_date"][3:] if row and row["last_date"].startswith("v2|") else ""  # 旧游标按未跑过算
        if cur and cur == as_of:
            return  # 该视点已按新口径重排过，幂等跳过
        held = {s for s, p in _replay(con, scope=scope, as_of=as_of)[0].items() if p["shares"] > 1e-9}
        rows = con.execute(f"SELECT symbol, MAX(ts) AS last_buy, MIN(ts) AS first_buy, MAX(id) AS last_id "
                           f"FROM {_tbl(scope, 'orders')} WHERE side='buy' GROUP BY symbol "
                           f"HAVING substr(MAX(ts),1,10) < ? "
                           f"ORDER BY last_buy DESC, first_buy ASC, last_id ASC", (as_of[:10],)).fetchall()
        hit = [r["symbol"] for r in rows if r["symbol"] in held]  # 继续持有的才前置
        # 清仓沉底：视点前最后一笔流水早于视点日（当天卖的不动）且视点重演无持仓
        closed = {r["symbol"] for r in con.execute(
            f"SELECT symbol FROM {_tbl(scope, 'orders')} WHERE ts <= ? GROUP BY symbol HAVING MAX(ts) < ?",
            (as_of, as_of[:10])).fetchall() if r["symbol"] not in held}
        if hit or closed:
            cards = [r["symbol"] for r in
                     con.execute(f"SELECT symbol FROM {_tbl(scope, 'cards')} ORDER BY pos").fetchall()]
            hs, ds, cs = set(hit), closed, set(cards)
            # 前置持仓 → 未涉及的卡居中（各自保持原相对顺序）→ 清仓沉底
            ordered = ([s for s in hit if s in cs]
                       + [c for c in cards if c not in hs and c not in ds]
                       + [c for c in cards if c in ds])
            with con:
                con.executemany(f"UPDATE {_tbl(scope, 'cards')} SET pos=? WHERE symbol=?", list(enumerate(ordered)))
        with con:
            con.execute("INSERT OR REPLACE INTO promo (scope, last_date) VALUES ('hist', ?)", ("v2|" + as_of,))
        return
    # ---- live：真实时钟游标 ----
    today = datetime.datetime.now().strftime("%Y-%m-%d")
    row = con.execute("SELECT last_date FROM promo WHERE scope='live'").fetchone()
    last = row["last_date"] if row else ""
    if not last:  # 首次：只建游标，历史买入不触发
        with con:
            con.execute("INSERT OR REPLACE INTO promo (scope, last_date) VALUES ('live', ?)", (today,))
        return
    if last >= today:
        return  # 今天已处理，幂等跳过
    # 区间 [last, today)：定长时刻串与纯日期串的字典序比较即时间比较；存量单 created_at='' 天然不命中
    rows = con.execute(f"SELECT symbol, MAX(created_at) AS last_buy, MIN(ts) AS first_buy, MAX(id) AS last_id "
                       f"FROM {_tbl(scope, 'orders')} "
                       f"WHERE side='buy' AND created_at >= ? AND created_at < ? GROUP BY symbol "
                       f"ORDER BY last_buy DESC, first_buy ASC, last_id ASC",
                       (last, today)).fetchall()
    held = {s for s, p in _replay(con, scope=scope)[0].items() if p["shares"] > 1e-9}
    hit = [r["symbol"] for r in rows if r["symbol"] in held]  # 仍持仓才前置
    # 清仓沉底：最后一笔真实下单早于今天且当前无持仓（今天清仓的明天沉）
    closed = {r["symbol"] for r in con.execute(
        f"SELECT symbol FROM {_tbl(scope, 'orders')} WHERE created_at != '' "
        f"GROUP BY symbol HAVING MAX(created_at) < ?", (today,)).fetchall() if r["symbol"] not in held}
    if hit or closed:
        cards = [r["symbol"] for r in
                 con.execute(f"SELECT symbol FROM {_tbl(scope, 'cards')} ORDER BY pos").fetchall()]
        hs, ds, cs = set(hit), closed, set(cards)
        # 前置持仓 → 未涉及的卡居中 → 清仓沉底（不在自选卡的买入流水自然跳过）
        ordered = ([s for s in hit if s in cs]
                   + [c for c in cards if c not in hs and c not in ds]
                   + [c for c in cards if c in ds])
        with con:
            con.executemany(f"UPDATE {_tbl(scope, 'cards')} SET pos=? WHERE symbol=?", list(enumerate(ordered)))
    with con:
        con.execute("INSERT OR REPLACE INTO promo (scope, last_date) VALUES ('live', ?)", (today,))


app = FastAPI()
app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"],
)


_CACHE = {}
TTL = 30  # 秒：同 URL 30 秒内走内存。防止刷新页面 37 卡并发连击上游触发腾讯 WAF 风控
# 顶部信号条的全量扫描结果：几十只票逐个拉日K，比单次取数贵得多，单独给一份长缓存
HEAT_TTL = 180  # 秒
_HEAT = {"t": 0.0, "d": None}
_HEAT_LOCK = asyncio.Lock()


def _ifzq_urls(url):
    # 候选通道（上次成功的排最前，其余按序兜底）：ifzq 双域是同一套 WAF 策略，会同时被 501 挑战，
    # 故需要 proxy.finance 这条独立通路的兜底，否则一个接口被挑战就整屏 K 线全空
    p = urlsplit(url)
    cands = [urlunsplit((p.scheme, h, p.path, p.query, p.fragment)) for h in IFZQ_HOSTS]
    if p.path.startswith("/appstock/app/"):
        cands.append(urlunsplit(("https", PROXY_HOST, "/ifzqgtimg" + p.path, p.query, p.fragment)))
    i = min(_chan["i"], len(cands) - 1)
    return [cands[i]] + [u for j, u in enumerate(cands) if j != i]


async def get_json(url, referer=None, timeout=15):
    now = time.time()
    hit = _CACHE.get(url)
    if hit and now - hit[0] < TTL:
        return hit[1]
    headers = UA if not referer else {**UA, "Referer": referer}
    urls = _ifzq_urls(url) if "ifzq.gtimg.cn" in url else [url]
    data = None
    # trust_env=False：httpx 默认会读 macOS 系统代理（scutil --proxy），代理内核退出后端口悬空会让
    # 所有行情请求在本地被拒（表现就是「K线全部异常」）。行情源都是直连可达的国内接口，固定不走代理。
    async with httpx.AsyncClient(timeout=timeout, headers=headers, trust_env=False) as cli:
        for u in urls:
            try:
                r = await cli.get(u)
                data = json.loads(r.content.decode("utf-8"))  # 上游是 UTF-8，但 header 不标，得强解
            except Exception:  # 连不上 / 501 挑战页 / 非 JSON 都算该通道不可用
                data = None
                continue
            host = urlsplit(u).netloc
            _chan["i"] = IFZQ_HOSTS.index(host) if host in IFZQ_HOSTS else len(IFZQ_HOSTS)
            break
    if data is None:
        raise HTTPException(502, "上游行情源返回异常（腾讯 ifzq 与 proxy 通道均被 WAF 拦截），请稍等片刻再刷新")
    if len(_CACHE) > 300:
        _CACHE.clear()
    _CACHE[url] = (now, data)
    return data


async def get_text(url, gbk=False):
    # 同 30 秒缓存取原始文本：qt.gtimg.cn 是 GBK，新浪 jsonp 是 UTF-8
    now = time.time()
    hit = _CACHE.get(url)
    if hit and now - hit[0] < TTL:
        return hit[1]
    async with httpx.AsyncClient(timeout=15, headers=UA, trust_env=False) as cli:  # 同上：不读系统代理
        r = await cli.get(url)
    data = r.content.decode("gbk", errors="replace") if gbk else r.content.decode("utf-8", errors="replace")
    if len(_CACHE) > 300:
        _CACHE.clear()
    _CACHE[url] = (now, data)
    return data


def _em_secid(symbol):
    return EM_SECID[symbol[:2]] + symbol[2:]


def _curl_json(url, timeout=10):
    # 东财 CDN 按 TLS 指纹掐 Python 客户端（httpx 与标准库 urllib 直连都被断「Server disconnected」，
    # curl/浏览器正常）：借系统自带 curl.exe 直连取数。--noproxy 防环境变量里 VPN 残留的本地代理
    # （http://127.0.0.1:50625，VPN 一关就死）连累；在线程池里跑避免阻塞事件循环
    r = subprocess.run(["curl", "-s", "--noproxy", "*", "-m", str(timeout), "-A", UA["User-Agent"], url],
                       capture_output=True, timeout=timeout + 5,
                       # 后端可能跑在无控制台的分离进程里（pythonw / 分离式拉起），不给这个标志时
                       # 每个 curl 子进程都会在桌面新建一个黑窗闪一下——桌面一闪一闪就是它
                       creationflags=0x08000000)  # CREATE_NO_WINDOW
    return json.loads(r.stdout.decode("utf-8"))


_EM_KLINE_CACHE = {}  # url -> (ts, rows)：同 URL 30 秒内存缓存，口径同 _CACHE


async def em_kline(symbol, period, cnt):
    # 东财K线兜底：ifzq/proxy 全线被 WAF 挑战时降级取数。fqt=1 前复权与腾讯 qfq 口径一致；
    # fields2=f51..f56 字段序（日期,开,收,高,低,量）与腾讯 klines 完全相同，下游零改动。
    # end=20500101+lmt=N 返回最近 N 根（带 beg 会从头数，取不到最新）。
    klt = {"day": 101, "week": 102, "month": 103, "m1": 1, "m5": 5, "m15": 15, "m30": 30, "m60": 60}[period]
    url = (f"{EM_KLINE}?secid={_em_secid(symbol)}&ut={EM_TOKEN}"
           f"&fields1=f1,f2,f3,f4,f5,f6&fields2=f51,f52,f53,f54,f55,f56"
           f"&klt={klt}&fqt=1&end=20500101&lmt={cnt}")
    now = time.time()
    hit = _EM_KLINE_CACHE.get(url)
    if hit and now - hit[0] < TTL:
        ks = hit[1]
    else:
        try:
            d = await asyncio.to_thread(_curl_json, url)
        except Exception:
            raise HTTPException(502, "东财K线兜底源也返回异常，请稍后重试")
        ks = ((d.get("data") or {}).get("klines")) or []
        if len(_EM_KLINE_CACHE) > 300:
            _EM_KLINE_CACHE.clear()
        _EM_KLINE_CACHE[url] = (now, ks)
    rows = []
    for s in ks:
        p = s.split(",")
        if len(p[0]) > 10:  # 分钟K时间戳 "2026-10-28 14:50" → "202610281450"，对齐腾讯 12 位紧凑串口径
            p[0] = p[0].replace("-", "").replace(" ", "").replace(":", "")
        rows.append(p)
    return rows


async def sina_kline(symbol, period, cnt):
    # 新浪A股K线（末级兜底，仅 sh/sz）：腾讯被 WAF 挑战 + 东财限流时仍能出图。
    # 注意新浪是不复权价，除权日附近历史K线与腾讯/东财的前复权口径会有差异（服务恢复即回到主源）
    # 周/月K用日K粒度取数后本地聚合（口径同美股 resample）
    # 新浪最小粒度是 5 分钟（scale 传 1 拿不到 1 分钟数据），m1 档没有这道兜底，直接报错让上游链换源
    if period == "m1":
        raise HTTPException(502, "新浪K线兜底源不支持 1 分钟粒度")
    scale = {"day": 240, "week": 240, "month": 240, "m5": 5, "m15": 15, "m30": 30, "m60": 60}[period]
    txt = await get_text(f"{SINA_A_KLINE}?symbol={symbol}&scale={scale}&ma=no&datalen={cnt}")
    try:
        arr = json.loads(txt)
        rows = []
        for r in arr:
            d = r["day"] if period == "day" else r["day"].replace("-", "").replace(" ", "").replace(":", "")
            rows.append([d, r["open"], r["close"], r["high"], r["low"], r["volume"]])  # 对齐腾讯 klines 字段序
    except (ValueError, KeyError, TypeError):
        raise HTTPException(502, "新浪K线兜底源返回异常，请稍后重试")
    if period in ("week", "month"):
        rows = resample(rows, period)  # 新浪只有日K粒度：周/月K本地聚合（口径同美股）
    return rows


async def sina_hk_kline(symbol, period, cnt):
    # 新浪港股K线（末级兜底）：klc2_kl.js 是 K2 加密格式，用 hk_decode.js（node，解密逻辑取自
    # akshare 同款实现）解出全历史日K。不复权，除净日附近与腾讯/东财前复权口径有差异
    txt = await get_text(SINA_HK_KLINE.format(symbol[2:]))
    try:
        out = await asyncio.to_thread(
            subprocess.run, ["node", str(Path(__file__).resolve().parent / "hk_decode.js")],
            input=txt.encode("utf-8"), capture_output=True, timeout=20,
            creationflags=0x08000000)  # CREATE_NO_WINDOW：同 _curl_json，不闪黑框
        arr = json.loads(out.stdout.decode("utf-8"))
        rows = [[r["date"][:10], r["open"], r["close"], r["high"], r["low"], r["volume"]] for r in arr]
    except (ValueError, KeyError, TypeError, subprocess.SubprocessError):
        raise HTTPException(502, "新浪港股K线兜底源解析失败，请稍后重试")
    if period in ("week", "month"):
        rows = resample(rows, period)
    return rows


def _hk_bar_ts(d, hm):
    # 分钟K时间戳 = 桶内最后一分钟（如 09:30~09:35 的 5 分钟桶 → 0935），与东财/腾讯分钟K的结束时刻口径一致
    return d + hm


_HK_WROTE = {}  # sym -> 上次写库时间：30 秒内重复请求（同 TTL）只读不写，避免十几张卡片同时刷新把归档库写锁撞满


def _hkmin_sync(sym, pts, days):
    # 把上游最近 5 个交易日的 1 分钟点 upsert 进归档库，再取回最近 days 个交易日的全量点（含历史累积）。
    # pts 元素 = (date, hm, price, vol, prevclose)；库不可写（只读盘/被占）时抛异常，由调用方退回只用上游
    con = sqlite3.connect(HK_MIN_DB, timeout=15)
    try:
        con.execute("PRAGMA journal_mode=WAL")  # 读写共存：看盘页多卡片轮询时不能因写锁把读侧也堵住
        # 建表不受写入节流影响（幂等 DDL）：库被删重建时仍能立刻恢复
        con.execute("CREATE TABLE IF NOT EXISTS hkmin (sym TEXT NOT NULL, date TEXT NOT NULL, "
                    "hm TEXT NOT NULL, price REAL NOT NULL, vol REAL NOT NULL DEFAULT 0, "
                    "prevclose REAL NOT NULL DEFAULT 0, PRIMARY KEY (sym, date, hm))")
        now = time.time()
        if now - _HK_WROTE.get(sym, 0) > TTL:
            with con:
                con.executemany("INSERT OR REPLACE INTO hkmin (sym, date, hm, price, vol, prevclose) "
                                "VALUES (?, ?, ?, ?, ?, ?)", [(sym,) + tuple(p) for p in pts])
            _HK_WROTE[sym] = now
        dates = [r[0] for r in con.execute(
            "SELECT DISTINCT date FROM hkmin WHERE sym=? ORDER BY date DESC LIMIT ?", (sym, HK_MIN_KEEP))]
        if not dates:
            return []
        keep = dates[-1]  # 保留窗口里最老的一天：只按它截断，宁可多读也不删仍要用到的历史
        rows = con.execute("SELECT date, hm, price, vol, prevclose FROM hkmin "
                           "WHERE sym=? AND date>=? ORDER BY date, hm", (sym, keep)).fetchall()
        with con:
            con.execute("DELETE FROM hkmin WHERE sym=? AND date<?", (sym, keep))
    finally:
        con.close()
    win = set(dates[:days]) if days else set(dates)  # 本次只聚合要的天数，更老的继续留在库里
    return [tuple(r) for r in rows if r[0] in win]


def _hkmin_agg(d, pts, step):
    # 单日 1 分钟点 → 分钟K：桶不跳午休；开=上一分钟价（当日首桶用昨收），高/低覆盖开收与桶内极值，量=桶内合计
    out, prev = [], (pts[0][3] or pts[0][1])
    for seg in ([p for p in pts if p[0] <= "1200"], [p for p in pts if p[0] > "1200"]):
        n = len(seg)
        if not n:
            continue
        # 分桶以「间隔」计：首点是开盘快照，之后每分钟一个间隔。首桶 = step 个间隔（step+1 个点），
        # 余数落在段尾的短桶（如上午 09:30~12:00 × 60 分 = [61,60,30] 点 → 10:30/11:30/12:00）
        m = n - 1
        k, rem = (m // step, m % step) if m >= step else (0, 0)
        sizes = ([step + 1] + [step] * (k - 1) + ([rem] if rem else [])) if k else [n]
        i = 0
        for size in sizes:
            chunk, i = seg[i:i + size], i + size
            o = prev
            hi, lo = max(o, chunk[0][1]), min(o, chunk[0][1])
            v = 0.0
            for _, price, vol, _pc in chunk:
                hi, lo, v = max(hi, price), min(lo, price), v + vol
            out.append([_hk_bar_ts(d, chunk[-1][0]), o, chunk[-1][1], hi, lo, v])
            prev = chunk[-1][1]  # 下一桶的开盘价 = 本桶末价
    return out


async def _sina_hk_points(symbol):
    # 新浪港股分时：带 prevclose（当日首桶开盘价最准），但当日盘中/刚收盘时可能停在最后一笔分时上
    # 该域有个别 IP 会卡到 15s 才报超时（换个连接就快）：限 6s 重试，别让首次取数卡十几秒
    url, now, txt = SINA_HK_MIN.format(symbol[2:]), time.time(), None
    hit = _CACHE.get(url)
    if hit and now - hit[0] < TTL:
        txt = hit[1]
    for _ in range(3 if txt is None else 0):
        try:
            async with httpx.AsyncClient(timeout=6, headers=UA, trust_env=False) as cli:
                txt = (await cli.get(url)).content.decode("utf-8", errors="replace")
            if len(_CACHE) > 300:
                _CACHE.clear()
            _CACHE[url] = (now, txt)
            break
        except Exception:  # 连接超时/断流：交给腾讯多日分时补点
            txt = None
    pts = []
    if not txt:
        return pts
    try:
        days = (json.loads(txt).get("result") or {}).get("data") or []
    except (ValueError, TypeError):
        return pts
    for day in days:
        if not day:
            continue
        d = str(day[0].get("date", "")).replace("-", "")  # "2026-10-26" → "20261026"
        pc = float(day[0].get("prevclose") or day[0]["price"])  # 当日首根的开盘参考价
        for p in day:
            pts.append((d, p["m"][:5].replace(":", ""), float(p["price"]),
                        float(p["volume"] or 0), pc))  # "09:30:00" → "0930"
    return pts


async def _tencent_hk_points(symbol):
    # 腾讯港股多日分时：无 prevclose（用当日首点价代开盘价），但当日收盘后能补到 16:00 最后一分钟
    try:
        d0 = (await get_json(TENCENT_HK_DAY.format(symbol[2:]))).get("data") or {}
    except HTTPException:
        return []
    pts = []
    for day in (((d0.get(symbol) or {}).get("data") or []) if isinstance(d0, dict) else []):
        raw = (day or {}).get("data") or []
        if not raw:
            continue
        d = str(day.get("date") or "")
        f0 = raw[0].split()  # 点格式 "0930 0.062 0 0.000"：时间 价 量 成交额
        pc = float(f0[1]) if len(f0) > 1 else 0.0
        for s in raw:
            f = s.split()
            if len(f) >= 3:
                pts.append((d, f[0], float(f[1]), float(f[2]), pc))
    return pts


async def _hk_min_points(symbol):
    # 上游 1 分钟点 = 新浪分时 ∪ 腾讯多日分时。两家都只给最近 5 个交易日（无法绕过，要更长历史
    # 只能靠 hkmin.db 逐日累积），但互补：新浪带 prevclose 口径正、腾讯收盘后能补到最后 16:00 那根
    pts = await _sina_hk_points(symbol)
    tx = await _tencent_hk_points(symbol)
    if not pts:
        return tx
    if not tx:
        return pts
    seen, pc = {(p[0], p[1]) for p in pts}, {}
    for d, _hm, _price, _vol, p0 in pts:
        pc.setdefault(d, p0)
    for d, hm, price, vol, p0 in tx:
        if (d, hm) in seen:
            continue
        pc.setdefault(d, p0)
        pts.append((d, hm, price, vol, pc[d]))
    return pts


async def sina_hk_min_kline(symbol, period, need=0):
    # 港股分钟K：上游只给最近 5 个交易日 1 分钟点（新浪与腾讯同口径，东财本机不可达），故
    # ①上游点写进本地归档库 hkmin.db ②再按 need 反推要几天，从库里取出来聚合成 m5/m15/m30/m60。
    # 这样程序每多跑一天，港股分钟K 就多一天历史，根数逐步追平 A 股（首次运行只有 5 天，属上游上限）
    pts = await _hk_min_points(symbol)
    if not pts:
        raise HTTPException(502, "港股分钟K上游无数据（新浪分时与腾讯多日分时均无返回），请稍后重试")
    step = int(period[1:])
    days = min(HK_MIN_KEEP, math.ceil(need / HK_BARS_PER_DAY[period]) + 2) if need else HK_MIN_KEEP
    try:
        arch = await asyncio.to_thread(_hkmin_sync, symbol[2:], pts, days)
    except Exception:  # 归档不可用（只读盘/库被占）不影响出图：退回只用本次上游数据
        arch = []
    if not arch:
        arch = sorted(pts)
    by = {}
    for d, hm, price, vol, pc in arch:
        by.setdefault(d, []).append((hm, price, vol, pc))
    rows = []
    for d in sorted(by):
        rows.extend(_hkmin_agg(d, by[d], step))
    if not rows:
        raise HTTPException(502, "港股分钟K上游无数据，请稍后重试")
    rows.sort(key=lambda r: r[0])
    return rows


_US_ROWS = {}   # usXXX -> (ts, 新浪全历史日K行)：避免每次请求重复解析几百KB
_US_SECID = {}  # usXXX -> 东财 secid（105/106/107.XXX）：跨请求记忆市场号


async def us_daily_rows(symbol):
    now = time.time()
    hit = _US_ROWS.get(symbol)
    if hit and now - hit[0] < TTL:
        return list(hit[1])  # 浅拷贝：调用方会补当日K线，不能污染缓存
    txt = await get_text(f"{SINA_US_DAILY}?symbol={symbol[2:]}&___qn=3")
    try:
        arr = json.loads(txt[txt.index("=(") + 2: txt.rindex("]") + 1])
        rows = [[r["d"], r["o"], r["c"], r["h"], r["l"], r["v"]] for r in arr]  # 统一为 [日期,开,收,高,低,量]
    except (ValueError, KeyError, TypeError):
        raise HTTPException(502, "美股行情源返回异常，请稍后重试")
    if len(_US_ROWS) > 100:
        _US_ROWS.clear()
    _US_ROWS[symbol] = (now, rows)
    return list(rows)


async def us_live_bar(symbol):
    # 新浪日K要等美股收盘后才更新当日线，用腾讯实时报价补一根活动K线
    try:
        txt = await get_text(f"{QT}{symbol}", gbk=True)
    except Exception:
        return None
    if '"' not in txt:
        return None
    f = txt.split('"')[1].split("~")
    if len(f) < 35 or not f[3] or not f[30]:  # 无报价或字段不全
        return None
    price = f[3]
    op = f[5] if f[5] not in ("", "0") else price
    hi = f[33] if f[33] not in ("", "0") else str(max(float(op), float(price)))
    lo = f[34] if f[34] not in ("", "0") else str(min(float(op), float(price)))
    return [f[30][:10], op, price, hi, lo, f[6]]  # [日期,开,现价,高,低,量]


# ---------- 合股/分股事件检测（美股）：新浪日K是不复权全历史，合股日价格跳整数倍、数量反向变 ----------
# 杠杆ETF（如 SOXS）跌到个位数就反向拆分：股价×10、股数÷10、总市值不变。事件 = (生效日, 数量倍数)
_SPLIT_CACHE = {}  # usXXX -> (ts, [(生效日, 数量倍数)])：事件不常变，缓存 10 分钟
_SPLIT_RATIOS = (20.0, 10.0, 5.0, 4.0, 3.0, 2.0, 0.5, 0.25, 0.2, 0.1, 0.05)


def _guess_split_ratio(jump):
    # 跳变比值 = 比例 × (1 + 当日真实涨跌)。合股/分股比例必为整数或简单倒数，真实涨跌容差
    # -15%~+20%；多候选时取对数距离最近者（如 4.75 可能是 1:5 也可能是 1:4）
    best = None
    for r in _SPLIT_RATIOS:
        x = jump / r
        if 0.85 <= x <= 1.20:
            dist = abs(math.log(x))
            if best is None or dist < best[0]:
                best = (dist, r)
    return best[1] if best else None


async def us_split_events(symbol):
    # 从不复权日K找合股/分股生效日：价格整数倍跳变 + 成交量基准同比例反向位移双重验证。
    # 量能是关键判据——真实单日暴涨暴跌（3倍杠杆ETF可±50%）当日放量、后5日均量回落原基准；
    # 合股后股本变小，量基准永久缩到 1/比例，两者不会混淆
    now = time.time()
    hit = _SPLIT_CACHE.get(symbol)
    if hit and now - hit[0] < 600:
        return hit[1]
    rows = await us_daily_rows(symbol)
    events = []
    for i in range(1, len(rows)):
        prev_c, cur_c = float(rows[i - 1][2]), float(rows[i][2])
        if prev_c <= 0:
            continue
        jump = cur_c / prev_c
        if not (jump > 1.8 or jump < 0.55):
            continue
        r = _guess_split_ratio(jump)
        if not r:
            continue
        v0 = float(rows[i - 1][5])
        vday = float(rows[i][5]) / v0 if v0 > 0 else 1.0
        vpre = [float(x[5]) for x in rows[max(0, i - 5):i]]
        vpost = [float(x[5]) for x in rows[i + 1:i + 6]]
        vpost_r = (sum(vpost) / len(vpost)) / (sum(vpre) / len(vpre)) \
            if vpre and vpost and sum(vpre) > 0 else None
        if r >= 2:   # 合股：当日量缩且量基准下移（后5日不足时退化用当日量比，新合股当天即可判）
            ok = vday < 0.6 and (vpost_r is None or vpost_r < 0.7)
        else:        # 分股：量基准永久上移（单日放量脉冲不算，须后5日持续确认）
            ok = vpost_r is not None and vpost_r > 1.4
        if ok:
            events.append((rows[i][0], 1.0 / r))
    if len(_SPLIT_CACHE) > 100:
        _SPLIT_CACHE.clear()
    _SPLIT_CACHE[symbol] = (now, events)
    return events


async def _splits_for(con, scope):
    # 流水表里出现过的美股标的逐个取合股/分股事件（缓存复用；A股K线源本身已前复权、港股暂未
    # 接入事件检测，天然空表）。上游失败降级为空：按无事件重演，不阻断持仓/订单主链路
    syms = [r["symbol"] for r in con.execute(
        f"SELECT DISTINCT symbol FROM {_tbl(scope, 'orders')} WHERE symbol LIKE 'us%'").fetchall()]
    if not syms:
        return {}
    results = await asyncio.gather(*(us_split_events(s) for s in syms), return_exceptions=True)
    return {s: evs for s, evs in zip(syms, results)
            if isinstance(evs, list) and evs}


def resample(rows, period):
    # 美股周/月K由日K本地聚合：开=区间首日开，收=区间末日收，高=最高，低=最低，量=求和
    out = {}
    for r in rows:
        if period == "month":
            key = r[0][:7]
        else:
            y, w, _ = datetime.date.fromisoformat(r[0]).isocalendar()
            key = f"{y}-{w:02d}"
        b = out.get(key)
        if b is None:
            out[key] = list(r)
        else:
            b[2], b[5] = r[2], str(float(b[5]) + float(r[5]))
            if float(r[3]) > float(b[3]):
                b[3] = r[3]
            if float(r[4]) < float(b[4]):
                b[4] = r[4]
    return list(out.values())


async def us_kline(symbol, period, limit, start, end):
    rows = await us_daily_rows(symbol)
    live = await us_live_bar(symbol)
    if live:
        if rows and live[0] == rows[-1][0]:
            rows[-1] = live  # 收盘后新浪已带当日线，用报价刷新最后一根
        elif not rows or live[0] > rows[-1][0]:
            rows.append(live)
    if period in ("week", "month"):
        rows = resample(rows, period)
    ind = indicators([float(r[2]) for r in rows])  # 新浪源给全历史：指标先在全体上算（天然含预热段）
    if start or end:
        s, e = start.replace("-", ""), end.replace("-", "")
        idx = [i for i, r in enumerate(rows) if (not s or r[0].replace("-", "")[:8] >= s) and (not e or r[0].replace("-", "")[:8] <= e)]
        je = [i for i, r in enumerate(rows) if e and r[0].replace("-", "")[:8] <= e]
        tail = rows[max(0, je[-1] - 1):je[-1] + 1] if je else []  # 截止日收盘：不晚于 end 的最后两根，不受 start 约束（区间皆为非交易日时前端仍能取到"当日价"）
        if idx:
            rows, ind = rows[idx[0]:idx[-1] + 1], _ind_slice(ind, idx[0], idx[-1] + 1)
        else:
            rows, ind = [], {}
    else:
        n = len(rows)
        rows = rows[-limit:]
        ind = _ind_slice(ind, n - len(rows), n)
        tail = []
    if period == "day" and len(tail) >= 2:
        # 截止日恰为合股/分股生效日：tail 前一根是不复权旧价，换算到截止日股本基准——卡片涨跌
        # 幅按当天真实涨跌而非跳空虚大（K线图仍画真实历史价，跳空缺口客观存在；周/月K聚合键
        # 无日粒度，不做此修正）。只改收盘一列，前端仅用它做涨跌基准
        try:
            evs = await us_split_events(symbol)
        except Exception:
            evs = []
        for d, m in evs:
            if d == tail[-1][0]:
                tail = [list(tail[0]), list(tail[1])]
                tail[0][2] = str(float(tail[0][2]) / m)
                break
    return {"symbol": symbol, "period": period, "klines": rows, "ind": ind, "tail": tail}


async def us_min_rows(symbol, period):
    # 新浪美股分钟K：一次只给最近约 1023 根（m5 约两周、m15 约两月、m30 约一季、m60 约半年），
    # 不支持翻页。时间戳（美东 09:30~16:00）转成腾讯口径的 12 位紧凑串 YYYYMMDDHHMM，
    # 与 A股/港股分钟K 在 /api/kline 输出里完全一致，前端无需分流处理
    txt = None
    for _ in range(2):  # 新浪偶发连接超时（实测 m30 会超时一次）：重试一次再报 502，不给前端抛 500
        try:
            txt = await get_text(f"{SINA_US_MINK}?symbol={symbol[2:]}&type={US_MIN_TYPE[period]}")
            break
        except Exception:
            continue
    if txt is None:
        raise HTTPException(502, "美股分钟K源连接超时，请稍后重试")
    try:
        arr = json.loads(txt[txt.index("=(") + 2: txt.rindex("]") + 1])
        # 上游时间为 "2026-10-30 15:35:00"：先截到分钟再紧凑化，得 12 位 YYYYMMDDHHMM（丢掉秒）
        rows = [[r["d"][:16].replace("-", "").replace(" ", "").replace(":", ""),
                r["o"], r["c"], r["h"], r["l"], r["v"]] for r in arr]  # 统一为 [时间,开,收,高,低,量]
    except (ValueError, KeyError, TypeError, AttributeError):
        raise HTTPException(502, "美股分钟K源返回异常，请稍后重试")
    if not rows:
        raise HTTPException(502, "美股分钟K源返回空数据（代码有误或该标的已退市），请稍后重试")
    return rows


async def us_min_kline(symbol, period, limit, start, end):
    # 美股分钟K（不复权）：指标同样先在全体取数上算（天然含预热段），再按窗口/区间切片对齐
    raw = await us_min_rows(symbol, period)
    ind = indicators([float(r[2]) for r in raw])
    if start or end:
        s, e = start.replace("-", ""), end.replace("-", "")
        idx = [i for i, r in enumerate(raw) if (not s or r[0][:8] >= s) and (not e or r[0][:8] <= e)]
        if idx:
            rows, ind = raw[idx[0]:idx[-1] + 1], _ind_slice(ind, idx[0], idx[-1] + 1)
        else:
            rows, ind = [], {}
    else:
        n = len(raw)
        rows = raw[-limit:]
        ind = _ind_slice(ind, n - len(rows), n)
    return {"symbol": symbol, "period": period, "klines": rows, "ind": ind, "tail": []}


async def _em_trends(secid):
    url = (f"{EM_TRENDS}?secid={secid}&ndays=1&iscr=0&fields1=f1,f2,f3,f4,f5,f6,f7,f8"
           "&fields2=f51,f52,f53,f54,f55,f56,f57,f58")
    try:
        d = await asyncio.to_thread(_curl_json, url)  # 东财掐 Python 客户端连接，走系统 curl（见 _curl_json 注释）
    except Exception:
        return None
    return d.get("data") or {}


async def us_minute(symbol):
    code = symbol[2:]
    secid = _US_SECID.get(symbol)
    data = await _em_trends(secid) if secid else None
    if not (data or {}).get("trends"):
        for mk in ("105", "106", "107"):  # 市场号未知（纳指/纽交所/美交所），逐一探测
            dd = await _em_trends(f"{mk}.{code}")
            if (dd or {}).get("trends"):
                _US_SECID[symbol] = f"{mk}.{code}"
                data = dd
                break
    tr = (data or {}).get("trends") or []
    if not tr:
        return {"error": "美股分时暂不可用（上游东财限流），可先看日K"}
    pts, cv, ca = [], 0.0, 0.0
    for line in tr:
        a = line.split(",")  # 时间,开,收,高,低,量,额,均价
        cv += float(a[5])
        ca += float(a[6])
        pts.append(f"{a[0][11:13]}{a[0][14:16]} {a[2]} {cv:.0f} {ca:.3f}")  # 与腾讯分时点格式对齐
    return {"symbol": symbol, "quote": ["200", "", code, tr[-1].split(",")[2], str(data.get("prePrice") or "")],
            "date": tr[0][:10], "points": pts}


MA_WARM = 124  # 均线预热根数：MA125 需 125 根，多取 124 根更早历史做预热，窗口/区间首根即出 MA85/MA125


def _ind_slice(ind, i0, i1):
    # ind 各数组与取数序列等长：按 [i0, i1) 切片，保证指标与返回的 klines 逐根对齐
    return {k: v[i0:i1] for k, v in ind.items()}


def ema(vals, p):
    k = 2 / (p + 1)
    out, e = [], vals[0]
    for v in vals:
        e = v * k + e * (1 - k)
        out.append(e)
    return out


def indicators(closes):
    # 均线 MA3/5/10/85/125 + MACD(5,10,3)，与看盘软件口径一致（EMA 从首根种子起步）
    # MA20 是反手页的出场辅线（收盘跌破 MA20 或 MA85 任一即全平），MA10 保留下发供其它页面展示
    if not closes:
        return {}
    n = len(closes)

    def ma(p):
        return [round(sum(closes[i - p + 1 : i + 1]) / p, 4) if i >= p - 1 else None for i in range(n)]

    e5, e10 = ema(closes, 5), ema(closes, 10)
    dif = [a - b for a, b in zip(e5, e10)]
    dea = ema(dif, 3)
    return {
        "ma3": ma(3), "ma5": ma(5), "ma10": ma(10), "ma20": ma(20), "ma85": ma(85), "ma125": ma(125),
        "dif": [round(x, 4) for x in dif],
        "dea": [round(x, 4) for x in dea],
        "macd": [round(2 * (d - e), 4) for d, e in zip(dif, dea)],
    }


WEB_DIR = Path(__file__).resolve().parent.parent / "web"
# 前端库本地化目录（jsdelivr 曾被网络阻断致页面空白，React/ECharts 等 4 个库改由本机托管）
app.mount("/vendor", StaticFiles(directory=WEB_DIR / "vendor"))


@app.get("/")
async def index():
    return FileResponse(WEB_DIR / "index.html", headers={"Cache-Control": "no-store"})


@app.get("/fanshou.html")
async def fanshou_page():
    # 反手页：上下双标的分钟K + 跨标的止损反手回测（strategy.html 已废弃删除）
    return FileResponse(WEB_DIR / "fanshou.html", headers={"Cache-Control": "no-store"})


@app.get("/api/health")
async def health():
    return {"ok": True, "source": "tencent-free"}


@app.get("/api/kline")
async def kline(symbol: str = Query(...), period: str = "day", limit: int = 300,
                start: str = "", end: str = ""):
    hk_days = None  # 仅港股分钟K 赋值：实际覆盖的交易日数（日K/美股分支保持 None）
    if symbol.startswith("us"):
        if period in DAY_PERIODS:
            return await us_kline(symbol, period, limit, start, end)
        if period in MIN_PERIODS:  # 分钟K走新浪 getMinK（腾讯 mkline 不收 us 代码）
            return await us_min_kline(symbol, period, limit, start, end)
        return {"error": f"period 只支持: {sorted(DAY_PERIODS | MIN_PERIODS)}"}
    if period in DAY_PERIODS:
        cnt = 800 if (start or end) else limit + MA_WARM  # 筛选态多取防目标区间落在窗口外；平时多取供均线预热
        try:
            d = (await get_json(f"{BARE}/fqkline/get?param={symbol},{period},,,{cnt},qfq"))["data"][symbol]
            # 前复权键：A股为 qfqday/qfqweek/qfqmonth，港美股无复权用 day/week/month
            key = ("qfq" + period) if ("qfq" + period) in d else period
            raw = d[key]
        except HTTPException:  # 腾讯 ifzq/proxy 全线被 WAF 挑战：逐级降级 东财(curl) → 新浪（A股/港股）
            try:
                raw = await em_kline(symbol, period, cnt)
            except HTTPException:
                if symbol.startswith("hk"):
                    raw = await sina_hk_kline(symbol, period, cnt)
                elif symbol[:2] in ("sh", "sz"):
                    raw = await sina_kline(symbol, period, cnt)
                else:
                    raise
    elif period in MIN_PERIODS:
        raw = None
        if not symbol.startswith("hk"):  # 港股分钟K：腾讯 mkline 返回 param error、东财港股分钟K恒为空，直走新浪聚合，不白撞两轮
            try:
                d0 = (await get_json(f"{BARE}/kline/mkline?param={symbol},{period},,{limit + MA_WARM}")).get("data")  # 分钟K同样多取预热
                d = (d0.get(symbol) or {}) if isinstance(d0, dict) else {}
                raw = d.get(period)
            except HTTPException:
                pass
        if not raw:  # 腾讯分钟K拿不到（被 WAF 拦或该标的本就不支持）：逐级降级 东财(curl) → 新浪（A股）/ 新浪分时聚合（港股）
            if symbol.startswith("hk"):
                raw = await sina_hk_min_kline(symbol, period, limit + MA_WARM)  # 本地归档累积 + 聚合出 m5/m15/m30/m60
                hk_days = len({r[0][:8] for r in raw})  # 实际覆盖交易日数，前端据此提示「根数为何比 A 股少」
            else:
                try:
                    raw = await em_kline(symbol, period, limit + MA_WARM)
                except HTTPException:
                    raw = None
                if not raw and symbol[:2] in ("sh", "sz"):
                    raw = await sina_kline(symbol, period, limit + MA_WARM)
        if not raw:
            return {"error": "该标的暂不支持分钟K（上游各源均无数据）"}
    else:
        return {"error": f"period 只支持: {sorted(DAY_PERIODS | MIN_PERIODS)}"}
    ind = indicators([float(r[2]) for r in raw])  # 指标在全量取数上算（含预热段），再切片对齐返回窗口
    if start or end:
        s, e = start.replace("-", ""), end.replace("-", "")
        idx = [i for i, r in enumerate(raw) if (not s or r[0].replace("-", "")[:8] >= s) and (not e or r[0].replace("-", "")[:8] <= e)]
        rows = raw[idx[0]:idx[-1] + 1] if idx else []
        ind = _ind_slice(ind, idx[0], idx[-1] + 1) if idx else {}
        je = [i for i, r in enumerate(raw) if e and r[0].replace("-", "")[:8] <= e]
        tail = raw[max(0, je[-1] - 1):je[-1] + 1] if je else []  # 截止日收盘：不晚于 end 的最后两根，不受 start 约束（区间皆为非交易日时前端仍能取到"当日价"）
    else:
        n = len(raw)
        rows = raw[-limit:]
        ind = _ind_slice(ind, n - len(rows), n)
        tail = []
    return {
        "symbol": symbol,
        "period": period,
        "klines": rows,
        "ind": ind,
        "tail": tail,
        "hk_min_days": hk_days,  # 仅港股分钟K 有值：上游只给 5 个交易日，本地归档累积后会逐日变大
    }


# ---------- 回测：单票 + 3 根均线 + K线实体箱体（策略类型见 BT_STRATS）----------
# 口径：docs/回测规则口径.md。与 orders/orders_live 完全隔离，只在内存里重演，不落库。
# 买卖只看均线：MA3 上穿 MA5 当根起每根都买，MA3 下穿 MA5（死叉）当根全部清仓；箱体条件不再参与买入，
# 只用于卖出后重新入场的确认：今日实体底高于前一根实体底 + 前一根实体高度的 2/3 才算箱体重新向好
BT_RESUME = 2.0 / 3.0
# 买入硬过滤①：MA3 最多侵入实体底部 20% 高度（实体至少 80% 站在 MA3 上方），且 MA3 必须在实体顶之下
BT_PENET = 0.2
# A 股专用硬止损：当日收盘较前一根收盘跌 4% 及以上（含 4%）→ 全部清仓
BT_DROP = 0.04
# 半份买入（v2.6，2026-10-29）：正常买入被「实体 80% 站上 MA3」挡住、但实体落进 MA3 的部分在 20%~45% 之间的
# 中段回踩——只要这根是红 K、实体顶高于前一根实体顶、MA3 上行且幅度 ≥ 0.1%，就买半份（每次买入额的一半），图上黄色 B
BT_HBODY = 0.45
# 「MA3 趋势向上」的最小斜率：当根 MA3 相对前一根至少涨 0.5%，才算向上（只看高低不算趋势）
BT_UP = 0.005
# 策略类型：两套共用同一份 bt_run 规则，只有取数周期不同
#   day=日k占Ma3（默认，每根=1 个交易日）  m60=1小时k占Ma3（每根=1 小时，A股每天 4 根）
# m60 的根数上限：腾讯分钟K 单次最多约 800 根（超过 800 上游回落到 320 根），故 limit+MA_WARM 卡在 800
BT_STRATS = {"day": {"period": "day", "label": "日k占Ma3", "limit": 800},
             "m60": {"period": "m60", "label": "1小时k占Ma3", "limit": 800 - MA_WARM},
             # 止损反手（Stop-and-Reverse）：5 分钟 K 上按箱体破位反手。上游 m5 单次最多给 480 根，
             # 过量请求会回落到 320 根，故 limit 卡在 660（+MA_WARM=784，实测可稳定返回 660 根 ≈ 14 个交易日）
             "sr5": {"period": "m5", "label": "5分钟k止损反手", "limit": 660}}
# 止损反手可选的 K 线周期 → 取数根数：m5 上游单次上限最低（超 480 会回落 320），故卡 660；其余周期沿用 676（+预热=800）
# m1 同样卡 676（cnt=800 实测稳定返回 800 根）：搭配前端默认近 2 个交易日，A股约 480 根 / 美股约 782 根 / 港股约 664 根
SR_LIMITS = {"m1": 676, "m5": 660, "m15": 676, "m30": 676, "m60": 676,
             # 日K：单一标的日线历史足够长，400 根（+预热 124 = 524）足以盖住前端默认的 180 个交易日区间
             "day": 400}
# ---------- 止损反手 v7（2026-11-02：站上当根买、掉下当根卖，两边各看自己的 MA5/MA85）----------
# 用户口径：快线（MA5）站上慢线（MA85）就买，掉下就卖——两条腿各自适用，不当根不延迟。
# 实测旧 v3「前一根实体一破就反手」= 拿 1 根 K 的噪声当信号，10 个交易日换手 195 次、胜率 30.8%、-20.49%。
# v4 箱体突破、v5 只看 A 的均线、v6 加 0.5% 缓冲 + 连续 2 根确认均已作废：用户要求规则能逐根对上图。
# 买卖都是当根收盘价成交，不做缓冲/确认/冷却，也不加移动止损。
# v10（2026-11-02）：卖点由「跌破 MA85」单线扩为「跌破 MA20 或 MA85」双线并存——收盘价先破哪条就当根全平；
# v12 把辅线由 MA10 改为 MA20（用户：“10 日线不行，换成 20 日线”），MA20 按当前 K 线周期取 20 根。
SR_MA_FAST = 5      # 快线（MA5）
SR_MA_SLOW = 85     # 慢线（MA85）
SR_MA_MID = 20      # 出场辅线（MA20）：与 MA85 并存，持仓侧收盘跌破任意一条即当根全平
SR_MA_BUF = 0.0     # 乖离缓冲：0 = 只看 MA5 与 MA85 的真实相对位置
# v11（2026-11-02 用户口径）：买点再加一道门槛——当根 K 柱「实体底」（开盘与收盘取低者，不含影线）
# 必须严格高于 MA85，即实体要完全长在慢线上方；只靠均线交叉、K 柱还在慢线下方的当根不买。
SR_REQ_BODY = True  # True = 买点要求实体底 > MA85；False = 回退成只看 MA5 > MA85
# v12（2026-11-02 用户口径）：① 出场辅线 MA10 换成 MA20（跌到 MA20 之下就全卖）；
# ② 买回/加仓要求价格「涨到 5 日线以上」——当根收盘价必须高于自己的 MA5。
SR_BUY_ABOVE_FAST = True  # True = 买点/加仓要求当根收盘价 > MA5；False = 不看这一条
# v13（2026-11-02 用户口径）：买点再补一条与卖点对称的门槛——当根收盘价必须在自己的 MA20 之上。
# 用户原话「上升通道一直买，下降到达 20 日线就全卖，且后续一直是下降通道就不买」：
# 原先卖点看 MA20（跌破全平）而买点完全不看 MA20，会出现「跌破 MA20 卖掉 → 下一根反弹站上 MA5、
# 却仍在 MA20 下方又买回」的来回磨损。补上门槛后，收盘在 MA20 下方时一律不买/不加仓，
# 必须等价格重新站回 MA20 之上（通道修复）才恢复逐根 1 万的加仓，卖/买对 MA20 互为镜像。
SR_BUY_ABOVE_MID = True  # True = 买点/加仓要求当根收盘价 > MA20；False = 不看这一条
SR_CONFIRM = 1      # 连续确认根数：1 = 当根就动手
SR_CD = 0           # 两次动作之间的冷却根数：0 = 不冷却，破位当根即可反手
SR_TRAIL = None     # 移动止损倍数：None = 不设止损（卖点只有掉下慢线）
SR_EACH = 10000.0   # 每笔买入金额上限：趋势没坏就每根再加一笔，现金不够一笔就停手
# v14（2026-11-02 用户口径）：页面新增「全额策略」按钮——判据不变，只换资金口径：
# full=True 时买入一次把可用现金全额买入、卖出仍一次性全平，不逐根 1 万加仓（由 /api/backtest 的 full 参数传入）。
SR_FULL = False     # 默认仍为每根 SR_EACH 加仓；页面点「全额策略」时以请求参数 full=true 覆盖
SR_ATR_N = 14       # ATR 周期
SR_COST = 0.0005    # 单边成本（价差+滑点），买卖各扣一次


def bt_lab(v):
    # 分钟K上游给 14 位紧凑串（202610281030），转成 "2026-10-28 10:30"：同一天有多根，
    # 坐标轴/成交明细/买卖点对位都必须到分钟，否则同日的多笔成交会被绘制到第一根上
    s = str(v)
    if len(s) >= 12 and s.isdigit():
        return f"{s[:4]}-{s[4:6]}-{s[6:8]} {s[8:10]}:{s[10:12]}"
    return s[:16] if len(s) > 10 else s[:10]


def bt_box(r):
    # K线箱体 = 实体（开→收）的长方形，上下影线不计
    o, c = float(r[1]), float(r[2])
    return max(o, c), min(o, c), abs(c - o)


def bt_gc(ma3, ma5, i):
    # MA3 金叉 MA5：当根 MA3 由「不高于 MA5」翻到「高于 MA5」，只认上穿的那一根。
    # 金叉是买入区间的起点：当根买第一笔，之后每根继续买，直到 bt_dc 死叉清仓。
    if i <= 0 or i >= len(ma3) or i >= len(ma5):
        return False
    a, b, pa, pb = ma3[i], ma5[i], ma3[i - 1], ma5[i - 1]
    if None in (a, b, pa, pb):
        return False
    return a > b and pa <= pb


def bt_dc(ma3, ma5, i):
    # MA3 死叉 MA5：当根 MA3 由「不低于 MA5」翻到「低于 MA5」，只认下穿的那一根 → 当根收盘价清仓
    if i <= 0 or i >= len(ma3) or i >= len(ma5):
        return False
    a, b, pa, pb = ma3[i], ma5[i], ma3[i - 1], ma5[i - 1]
    if None in (a, b, pa, pb):
        return False
    return a < b and pa >= pb


def bt_run_sr(rows, capital):
    # 止损反手（Stop-and-Reverse）逐根重演：本金拆两条腿各占一半，互不共享现金，任意时刻只持一边
    #   多头腿：收盘价涨破前一根实体顶 → 用一半本金买入；收盘价跌破前一根实体底 → 卖出平掉
    #   空头腿：收盘价跌破前一根实体底 → 用一半本金开空；收盘价涨破前一根实体顶 → 买入平掉
    # 破位/突破一律以当根**收盘价**确认（盘中最高最低触及不算），按收盘价成交、不计手续费。
    # 回测按「可做空」模拟，A 股实盘没有做空通道，这里的空头腿只用于验证规则本身。
    half = float(capital) / 2
    cash_l, qty_l, cost_l = half, 0.0, 0.0  # 多头腿：现金 / 持仓数量 / 开仓价
    cash_s, qty_s, cost_s = half, 0.0, 0.0  # 空头腿：现金 / 空头数量 / 开空价
    trades, nav = [], []
    peak, mdd, win, closed, realized_all = float(capital), 0.0, 0, 0, 0.0
    n_long = n_short = 0
    for i, r in enumerate(rows):
        date = r[0][:16] if len(r[0]) > 10 else r[0][:10]  # 日K 到日，5 分钟K 到分钟
        close = float(r[2])
        if i > 0:  # 区间首根没有「前一根」，等第二根起才开始判定破位
            ptop, pbot, _ph = bt_box(rows[i - 1])
            if close > ptop:  # 涨破前一根实体顶：先平空，再开多
                if qty_s > 0:
                    gain = (cost_s - close) * qty_s
                    trades.append({"date": date, "side": "buy", "leg": "short", "reason": "sc",
                                   "price": round(close, 4), "qty": round(qty_s, 4),
                                   "amount": round(close * qty_s, 2), "realized": round(gain, 2)})
                    cash_s -= close * qty_s
                    qty_s, cost_s = 0.0, 0.0
                    closed += 1
                    realized_all += gain
                    if gain > 0:
                        win += 1
                if qty_l <= 0:
                    q = half / close
                    trades.append({"date": date, "side": "buy", "leg": "long", "reason": "lo",
                                   "price": round(close, 4), "qty": round(q, 4),
                                   "amount": round(half, 2), "realized": None})
                    cash_l -= close * q
                    qty_l, cost_l = q, close
                    n_long += 1
            elif close < pbot:  # 跌破前一根实体底：先平多，再开空
                if qty_l > 0:
                    gain = (close - cost_l) * qty_l
                    trades.append({"date": date, "side": "sell", "leg": "long", "reason": "lc",
                                   "price": round(close, 4), "qty": round(qty_l, 4),
                                   "amount": round(close * qty_l, 2), "realized": round(gain, 2)})
                    cash_l += close * qty_l
                    qty_l, cost_l = 0.0, 0.0
                    closed += 1
                    realized_all += gain
                    if gain > 0:
                        win += 1
                if qty_s <= 0:
                    q = half / close
                    trades.append({"date": date, "side": "sell", "leg": "short", "reason": "so",
                                   "price": round(close, 4), "qty": round(q, 4),
                                   "amount": round(half, 2), "realized": None})
                    cash_s += close * q
                    qty_s, cost_s = q, close
                    n_short += 1
        # 净值 = 多头腿（现金+市值）+ 空头腿（卖空所得现金 - 空头市值）
        equity = cash_l + qty_l * close + cash_s - qty_s * close
        if equity > peak:
            peak = equity
        dd = equity / peak - 1
        if dd < mdd:
            mdd = dd
        nav.append({"date": date, "equity": round(equity, 2), "pct": round((equity / capital - 1) * 100, 4)})
    end_eq = nav[-1]["equity"] if nav else float(capital)
    total = (end_eq / capital - 1) * 100
    days = 0
    if len(rows) >= 2:
        days = (datetime.date.fromisoformat(rows[-1][0][:10]) - datetime.date.fromisoformat(rows[0][0][:10])).days
    years = days / 365
    px = [float(r[2]) for r in rows]
    float_pnl = (px[-1] - cost_l) * qty_l + (cost_s - px[-1]) * qty_s
    return {
        "trades": trades, "nav": nav,
        "stats": {
            "capital": round(float(capital), 2), "end": round(end_eq, 2),
            "total": round(total, 2), "mdd": round(mdd * 100, 2),
            "ann": round((pow(1 + total / 100, 1 / years) - 1) * 100, 2) if years >= 0.25 else None,
            "buys": sum(1 for t in trades if t["side"] == "buy"),
            "sells": sum(1 for t in trades if t["side"] == "sell"),
            "longs": n_long, "shorts": n_short,  # 开多/开空次数（次数口径与 buys/sells 不同）
            "closes": closed,  # 平仓次数（lc + sc，即胜率的分母）
            "win": win, "winrate": round(win / closed * 100, 1) if closed else None,
            "realized": round(realized_all, 2),
            "pnl": round(end_eq - float(capital), 2),   # 本策略盈利金额（期末资金 - 本金）
            "open_qty": round(qty_l, 4), "open_cost": round(cost_l, 4),
            "short_qty": round(qty_s, 4), "short_cost": round(cost_s, 4),
            "float_pnl": round(float_pnl, 2),
            "first": rows[0][0][:10] if rows else "", "last": rows[-1][0][:10] if rows else "",
            "n": len(rows), "days": days,
        },
    }


def sr_atr(rows, n=SR_ATR_N):
    # ATR 与 K 线等长：首 n-1 根用已有根数算（不返回 None，避免开头几十根全被跳过）
    out, trs, pc = [], [], None
    for r in rows:
        hi, lo, c = float(r[3]), float(r[4]), float(r[2])
        tr = hi - lo if pc is None else max(hi - lo, abs(hi - pc), abs(lo - pc))
        trs.append(tr)
        win = trs[-n:]
        out.append(sum(win) / len(win))
        pc = c
    return out


def sr_ma(rows, n):
    # 简单均值；不够 n 根时返回 None（由调用方决定不交易）
    out, s = [], 0.0
    for i, r in enumerate(rows):
        s += float(r[2])
        if i >= n:
            s -= float(rows[i - n][2])
        out.append(s / n if i >= n - 1 else None)
    return out


def bt_run_pair(rows_a, rows_b, sym_a, sym_b, capital,
                ma5_a=None, ma85_a=None, ma5_b=None, ma85_b=None,
                ma20_a=None, ma20_b=None, full=False):
    # 跨标的止损反手（规则见文件头 SR_* 常量）：两条腿各看自己的均线，当根站上就买、跌破就卖。
    #   入场（v11+v12+v13）：A 的 MA5 > MA85、当根实体底 > MA85、收盘价 > MA5、且收盘价 > MA20 → target=A；B 同判；
    #     两边都成立取 MA5/MA85 更大的那边，都不成立空仓。v13 的「收盘 > MA20」与卖点的「收盘 < MA20」互为镜像：
    #     跌破 MA20 卖掉后，价格没重新站回 MA20 上方就一直不买回（下降通道不接），站回去才恢复逐根加仓。
    #     实体底 = min(开盘, 收盘)，不含影线；SR_REQ_BODY/SR_BUY_ABOVE_FAST/SR_BUY_ABOVE_MID 分别可关上述三条门槛。
    #   离场：持仓侧自己的均线失守——收盘价跌破 MA20 或 MA85 任意一条（v12 两条并存，先破哪条算哪条）就当根全平。
    # 买卖各扣 SR_COST 成本；SR_TRAIL 默认 None（不额外设止损）。
    # B 的时间轴与 A 不一定逐根对齐（停牌/上市日不同）：按时间取 B 收盘/ATR/均线，缺根沿用上一根。
    # 资金：两条腿共用一份本金（v8），每笔最多 SR_EACH，趋势没坏就每根再加一笔，卖出一次性全平。
    # full=True（v14，页面「全额策略」按钮）：一次把可用现金全额买入、卖出仍一次性全平，不做每根 1 万的逐根加仓。
    #   全额买满后 cash=0，下一根的入场门禁 cash<=0 自然停手，故不需要额外的“已持仓不加仓”分支。
    cash = float(capital)
    qty_a, cost_a = 0.0, 0.0    # A 腿：持仓数量 / 当前这一轮累计买入本钱（含成本）
    qty_b, cost_b = 0.0, 0.0    # B 腿：同上
    bmap, batr, bma, bbody = {}, {}, {}, {}
    atr_b = sr_atr(rows_b)
    ma5b = ma5_b if (ma5_b and len(ma5_b) == len(rows_b)) else sr_ma(rows_b, SR_MA_FAST)
    ma85b = ma85_b if (ma85_b and len(ma85_b) == len(rows_b)) else sr_ma(rows_b, SR_MA_SLOW)
    ma20b = ma20_b if (ma20_b and len(ma20_b) == len(rows_b)) else sr_ma(rows_b, SR_MA_MID)
    for r, a, f, s, t in zip(rows_b, atr_b, ma5b, ma85b, ma20b):
        k = r[0][:16] if len(r[0]) > 10 else r[0][:10]
        bmap[k] = float(r[2])
        batr[k] = a
        bma[k] = (f, s, t)
        bbody[k] = min(float(r[1]), float(r[2]))   # 实体底：买点要求它高于 B 自己的 MA85
    atr_a = sr_atr(rows_a)
    ma5 = ma5_a if (ma5_a and len(ma5_a) == len(rows_a)) else sr_ma(rows_a, SR_MA_FAST)
    ma85 = ma85_a if (ma85_a and len(ma85_a) == len(rows_a)) else sr_ma(rows_a, SR_MA_SLOW)
    ma20 = ma20_a if (ma20_a and len(ma20_a) == len(rows_a)) else sr_ma(rows_a, SR_MA_MID)
    trades, nav = [], []
    peak, mdd, win, closed, realized_all = float(capital), 0.0, 0, 0, 0.0
    n_a = n_b = 0
    realized_a = realized_b = 0.0
    pos, last_act, ext = None, -10 ** 9, None   # pos=A/B/None；last_act=上一次买或卖的序号；ext=持仓期最有利收盘
    su = fu = 0                            # A / B 各自站在快慢线上方的连续根数
    sold_bar = -1                          # 卖出当根不再买回（下一根再说过）
    last_b = float(rows_b[0][2]) if rows_b else None

    def _mark_after_trade():
        # 净值一律按「本根成交全部落账后」记：原先是先按成交前记一次，卖出当根会漏掉那笔单边成本，
        # 使 pnl_a+pnl_b 与 pnl=end-capital 出现 0.05%×卖出市值的缺口（m5 实测差 4.99 元）。
        e = cash + qty_a * close + qty_b * last_b
        nav[-1]["equity"] = round(e, 2)
        nav[-1]["pct"] = round((e / float(capital) - 1) * 100, 4)

    for i, r in enumerate(rows_a):
        date = r[0][:16] if len(r[0]) > 10 else r[0][:10]
        close = float(r[2])
        if date in bmap:
            last_b = bmap[date]
        if last_b is None:
            continue
        px = close if pos != "B" else last_b
        if pos == "A":
            ext = max(ext, close)
        elif pos == "B":
            ext = min(ext, last_b)
        # 净值 = 现金 + A 腿市值 + B 腿市值（两条腿共用一份现金）
        equity = cash + qty_a * close + qty_b * last_b
        if equity > peak:
            peak = equity
        mdd = min(mdd, equity / peak - 1)
        nav.append({"date": date, "equity": round(equity, 2),
                    "pct": round((equity / float(capital) - 1) * 100, 4)})
        if i >= len(ma5) or ma5[i] is None or i >= len(ma85) or ma85[i] is None:
            continue
        fast, slow = ma5[i], ma85[i]
        mid = ma20[i] if i < len(ma20) else None
        fast_b, slow_b, mid_b = bma.get(date, (None, None, None))
        body_a = min(float(r[1]), close)   # 当根实体底（开、收取低者），影线不计
        su = su + 1 if fast > slow * (1 + SR_MA_BUF) else 0
        # 买点（v11 实体底门槛 + v12 站上 MA5）：均线交叉、实体长在 MA85 上方，且收盘价还要站回 MA5 之上
        up_a = (su >= SR_CONFIRM and (not SR_REQ_BODY or body_a > slow)
                and (not SR_BUY_ABOVE_FAST or close > fast)
                and (not SR_BUY_ABOVE_MID or mid is None or close > mid))
        # v13：收盘在 MA20 下方视为「下降通道未修复」，一律不买——与下方 dn 的 MA20 卖点互为镜像
        # 卖点（用户口径）：K 柱收盘跌破 MA20 或 MA85 任意一条就全卖，不等 MA5 下穿（两条并存、先破哪条算哪条）
        dn_a = close < slow or (mid is not None and close < mid)
        up_b = dn_b = False
        if fast_b and slow_b:
            fu = fu + 1 if fast_b > slow_b * (1 + SR_MA_BUF) else 0
            body_b = bbody.get(date)
            up_b = (fu >= SR_CONFIRM and (not SR_REQ_BODY or (body_b is not None and body_b > slow_b))
                    and (not SR_BUY_ABOVE_FAST or last_b > fast_b)
                    and (not SR_BUY_ABOVE_MID or mid_b is None or last_b > mid_b))
            dn_b = last_b < slow_b or (mid_b is not None and last_b < mid_b)
        # 哪边趋势好买哪边：两边都满足时比快慢线乖离，都不满足时空仓
        target = None
        if up_a and up_b:
            target = "A" if fast / slow >= fast_b / slow_b else "B"
        elif up_a:
            target = "A"
        elif up_b:
            target = "B"
        # ---- 离场：持仓标的自己掉下慢线，或移动止损 ----
        if pos == "A":
            why = ("止损" if SR_TRAIL and close < ext - SR_TRAIL * atr_a[i] else
                   "反手" if dn_a else None)
            if why:
                amt = qty_a * close * (1 - SR_COST)
                cash += amt
                gain = amt - cost_a
                trades.append({"date": date, "sym": sym_a, "side": "sell", "leg": "A", "reason": "lc",
                               "note": why, "price": round(close, 4), "qty": round(qty_a, 4),
                               "amount": round(amt, 2), "realized": round(gain, 2)})
                qty_a, cost_a = 0.0, 0.0
                closed += 1
                realized_all += gain
                realized_a += gain
                win += 1 if gain > 0 else 0
                pos, last_act, ext = None, i, None
                sold_bar = i
        elif pos == "B":
            mtb = batr.get(date) or atr_a[i]
            if SR_TRAIL and last_b > ext + SR_TRAIL * mtb:
                why = "止损"
            elif dn_b:
                why = "反手"
            else:
                why = None
            if why:
                amt = qty_b * last_b * (1 - SR_COST)
                cash += amt
                gain = amt - cost_b
                trades.append({"date": date, "sym": sym_b, "side": "sell", "leg": "B", "reason": "bc",
                               "note": why, "price": round(last_b, 4), "qty": round(qty_b, 4),
                               "amount": round(amt, 2), "realized": round(gain, 2)})
                qty_b, cost_b = 0.0, 0.0
                closed += 1
                realized_all += gain
                realized_b += gain
                win += 1 if gain > 0 else 0
                pos, last_act, ext = None, i, None
                sold_bar = i
        # ---- 入场/加仓（共用现金池：每笔最多 SR_EACH；趋势没坏就下一根再加一笔，换向只在旧仓位已平掉时发生）----
        # full=True 时门槛换成「现金已用完就停」、每笔金额换成全额，其余判据一字不改
        if target is None or (cash <= 0 if full else cash < SR_EACH) or i - last_act < SR_CD or i == sold_bar:
            _mark_after_trade()
            continue
        if pos is not None and pos != target:
            _mark_after_trade()
            continue
        first = pos is None          # 本轮开仓的第一笔（之后同向的都是加仓）
        note = ("全额买入" if full else "站上MA5买入") if first else "仍在MA5上方加仓"
        amt = cash if full else min(SR_EACH, cash)
        cash -= amt
        if target == "A":
            buy_qty = amt * (1 - SR_COST) / close
            qty_a += buy_qty
            cost_a += amt
            trades.append({"date": date, "sym": sym_a, "side": "buy", "leg": "A", "reason": "lo",
                           "note": note, "price": round(close, 4), "qty": round(buy_qty, 4),
                           "amount": round(amt, 2), "realized": None})
            if first:
                pos, last_act, ext = "A", i, close
            n_a += 1
        else:
            buy_qty = amt * (1 - SR_COST) / last_b
            qty_b += buy_qty
            cost_b += amt
            trades.append({"date": date, "sym": sym_b, "side": "buy", "leg": "B", "reason": "bo",
                           "note": note, "price": round(last_b, 4), "qty": round(buy_qty, 4),
                           "amount": round(amt, 2), "realized": None})
            if first:
                pos, last_act, ext = "B", i, last_b
            n_b += 1
        _mark_after_trade()
    end_eq = nav[-1]["equity"] if nav else float(capital)
    total = (end_eq / float(capital) - 1) * 100
    days = 0
    if len(rows_a) >= 2:
        days = (datetime.date.fromisoformat(rows_a[-1][0][:10]) - datetime.date.fromisoformat(rows_a[0][0][:10])).days
    years = days / 365
    px = float(rows_a[-1][2]) if rows_a else float(capital)
    px_b = last_b if last_b is not None else px
    # 每腿盈利 = 该腿已实现盈亏 + 当前持仓浮动（市值 − 本钱）；两腿之和恒等于策略总盈利
    # （v8 两条腿共用一份现金，不再有各腿独立现金，故不能再用“该腿现金+市值−半仓本金”算）
    pnl_a = realized_a + (qty_a * px - cost_a)
    pnl_b = realized_b + (qty_b * px_b - cost_b)
    float_pnl = (qty_a * px - cost_a) + (qty_b * px_b - cost_b)
    return {
        "trades": trades, "nav": nav,
        "stats": {
            "capital": round(float(capital), 2), "end": round(end_eq, 2),
            "total": round(total, 2), "mdd": round(mdd * 100, 2),
            "ann": round((pow(1 + total / 100, 1 / years) - 1) * 100, 2) if years >= 0.25 else None,
            "buys": sum(1 for t in trades if t["side"] == "buy"),
            "sells": closed,
            "longs": n_a, "shorts": n_b,  # 跨标的模式下：A 腿买入次数 / B 腿买入次数
            "flips": n_b,  # 换手次数 = 反手买反向标的的次数（新版是空仓后再进，不会同日互换）
            "closes": closed, "win": win,
            "winrate": round(win / closed * 100, 1) if closed else None,
            "realized": round(realized_all, 2),
            "realized_a": round(realized_a, 2), "realized_b": round(realized_b, 2),
            "pnl": round(end_eq - float(capital), 2),   # 整个策略的盈利金额
            "pnl_a": round(pnl_a, 2), "pnl_b": round(pnl_b, 2),
            "ret_a": round(pnl_a / float(capital) * 100, 2) if capital else None,   # 该腿盈亏占本金的比例
            "ret_b": round(pnl_b / float(capital) * 100, 2) if capital else None,
            "float_a": round(pnl_a - realized_a, 2), "float_b": round(pnl_b - realized_b, 2),
            "open_qty": round(qty_a, 4), "open_cost": round(px if qty_a else 0.0, 4),
            "short_qty": round(qty_b, 4), "short_cost": round(px_b if qty_b else 0.0, 4),
            "float_pnl": round(float_pnl, 2),
            "pair": True, "sym_a": sym_a, "sym_b": sym_b,
            "full": 1 if full else 0,   # 资金口径标识：1=一次全额买入/全额卖出，0=每根 SR_EACH 逐根加仓
            "first": rows_a[0][0][:10] if rows_a else "", "last": rows_a[-1][0][:10] if rows_a else "",
            "n": len(rows_a), "days": days,
        },
    }


def bt_run(rows, ma3, ma5, capital, each, stop_drop=False):
    # 逐日重演。rows=[日期,开,收,高,低,量] 升序，ma3/ma5 与之等长（含预热，区间首根即有值）；
    # stop_drop=True 时（仅 A 股）启用「当日跌 4% 硬止损」
    cash, qty, cost = float(capital), 0.0, 0.0
    trades, nav = [], []
    peak, mdd, win, closed, realized_all = float(capital), 0.0, 0, 0, 0.0
    long_on = False  # 是否处在「金叉之后、死叉之前」的每日买入区间
    half_done = False  # 本轮持仓是否已经做过一次「减半仓」，每次全部清仓后重置
    for i, r in enumerate(rows):
        date = r[0][:16] if len(r[0]) > 10 else r[0][:10]  # 日K 到日，1 小时K 到分钟
        close = float(r[2])
        m = ma3[i] if i < len(ma3) else None
        top, bot, hgt = bt_box(r)
        ptop = pbot = phgt = None
        if i > 0:
            ptop, pbot, phgt = bt_box(rows[i - 1])
        # 卖出：① MA3 死叉 MA5 当根全清；② A 股当日跌 4% 硬止损；③ 弱势走平箱体——今日实体顶未高于前一日、
        # 实体底也未高于前一日（顶没抬、底还更低）。三种清仓都退出买入区间，按后面「重新入场」条件恢复
        sold = ""
        prev_close = float(rows[i - 1][2]) if i > 0 else None
        prev_m = ma3[i - 1] if i > 0 and i - 1 < len(ma3) else None
        if bt_dc(ma3, ma5, i):
            sold = "dc"
        elif qty > 0 and stop_drop and prev_close and close <= prev_close * (1 - BT_DROP):
            sold = "drop"
        elif qty > 0 and ptop is not None and top <= ptop and bot <= pbot:
            sold = "weak"
        if sold:
            long_on = False
            half_done = False
            if qty > 0:
                gain = (close - cost) * qty
                trades.append({"date": date, "side": "sell", "price": round(close, 4),
                               "qty": round(qty, 4), "amount": round(close * qty, 2),
                               "realized": round(gain, 2), "reason": sold})
                cash += close * qty
                qty, cost = 0.0, 0.0
                closed += 1
                realized_all += gain
                if gain > 0:
                    win += 1
        # 减半仓（两种，每轮持仓只减一次）：
        #   ① half：实体已有一半落进 MA3 线内（MA3 ≥ 实体底 + 半个实体高度，且仍在实体顶之下）+ MA3 仍上行；
        #   ② gb：绿 K（收 < 开）+ 实体顶低于前一根实体顶 + 箱体大部分仍在 MA3 上方（MA3 ≤ 实体底 + 20% 实体高度，
        #      含 MA3 在实体底下方）+ MA3 仍上行——价格还没跌到 MA3 但已走弱，先卖掉当前持仓的一半
        half = ""
        if (not sold and not half_done and qty > 0 and m is not None and prev_m is not None
                and m > prev_m):
            if bot + hgt * 0.5 <= m < top:
                half = "half"
            elif (close < float(r[1]) and ptop is not None and top < ptop
                  and m <= bot + BT_PENET * hgt):
                half = "gb"
        if half:
            half_done = True
            hq = qty * 0.5
            gain = (close - cost) * hq
            trades.append({"date": date, "side": "sell", "price": round(close, 4),
                           "qty": round(hq, 4), "amount": round(close * hq, 2),
                           "realized": round(gain, 2), "reason": half})
            cash += close * hq
            qty -= hq
            realized_all += gain
        # 买入：MA3 上穿 MA5 当根起进入买入区间，此后每根都买 each 元（金叉当根记 gc，其后记 add），
        # 现金不足 each 时停手（本金 10 万 ÷ 每次 1 万 = 最多连买 10 根），清仓后再等下一次金叉；卖出当根不买
        gc = (not sold) and bt_gc(ma3, ma5, i)
        # 卖出后重新入场：未死叉（MA3 仍在 MA5 上方）且 MA3 仍上行，同时箱体重新向好——
        # 今日实体底高于「前一根实体底 + 前一根实体高度 × 2/3」。满足即恢复每日买
        resume = (not sold and i > 0 and m is not None and i < len(ma5) and ma5[i] is not None
                  and ma3[i - 1] is not None and m > ma5[i] and m > ma3[i - 1]
                  and pbot is not None and bot > pbot + phgt * BT_RESUME)
        # 区间首根没有「上一根」可比：若首根 MA3 已在 MA5 上方（金叉发生在区间之前），按已在买入区间处理
        was_on = long_on
        if gc or resume or (i == 0 and m is not None and i < len(ma5) and ma5[i] is not None and m > ma5[i]):
            long_on = True
        # 买入硬过滤（金叉/重新入场只是入场开关，实体不合格照样不买）：
        # ① 箱体大部分在 MA3 上方：MA3 ≤ 实体底 + 20% 实体高度，且 MA3 < 实体顶；
        #    且按整根 K 线（最高~最低）算也过半——MA3 不高于（最高+最低）/2
        # ② 今日实体底高于前一根实体底（最底部在抬高）；
        # ③ MA3 必须朝上（当根 MA3 高于前一根），走平或下行不买。任一不满足，当根不买
        hi, lo = float(r[3]), float(r[4])
        body_ok = (m is not None and m <= bot + BT_PENET * hgt and m < top
                   and m <= (hi + lo) / 2)
        lift_ok = pbot is None or bot > pbot
        prev_m2 = prev_m
        # ③ MA3 必须朝上且有斜率：当根 MA3 相对前一根涨幅 ≥ BT_UP（默认 0.5%），
        # 涨幅几乎为 0（如只涨 0.15%）视为走平，与下行一样不买
        up_ok = prev_m2 is None or (m is not None and prev_m2 > 0
                                    and m > prev_m2 and (m / prev_m2 - 1) >= BT_UP)
        # 半份买入（v2.6）：正常买入没过的「中段回踩」——实体落进 MA3 的部分在 20%~45% 之间（40% 判据里那 20%~50% 的空档），
        # 再要求①红 K（收盘 > 开盘，阴线回踩不买）②实体顶高于前一根实体顶 ③MA3 上行且幅度 ≥ 0.5%（趋势朝上明显），
        # 命中则买半份（each/2），reason=hb，图上黄色 B；卖出/减仓当根不买，现金只要求够半份
        hb_ok = (not sold and not half and long_on and cash >= each * 0.5
                 and m is not None and ptop is not None and prev_m is not None
                 and bot + BT_PENET * hgt < m <= bot + BT_HBODY * hgt
                 and close > float(r[1]) and top > ptop
                 and m > prev_m and (m / prev_m - 1) >= BT_UP)
        if not (sold or half) and long_on and body_ok and lift_ok and up_ok and cash >= each:
            add = each / close
            trades.append({"date": date, "side": "buy", "price": round(close, 4),
                           "qty": round(add, 4), "amount": round(each, 2),
                           "realized": None,
                           "reason": "gc" if gc else ("back" if (resume and not was_on) else "add")})
            cash -= each
            cost = (cost * qty + close * add) / (qty + add)
            qty += add
        elif hb_ok:
            add = each * 0.5 / close
            trades.append({"date": date, "side": "buy", "price": round(close, 4),
                           "qty": round(add, 4), "amount": round(each * 0.5, 2),
                           "realized": None, "reason": "hb"})
            cash -= each * 0.5
            cost = (cost * qty + close * add) / (qty + add)
            qty += add
        equity = cash + qty * close  # 净值 = 现金 + 持仓市值（收盘价）
        if equity > peak:
            peak = equity
        dd = equity / peak - 1
        if dd < mdd:
            mdd = dd
        nav.append({"date": date, "equity": round(equity, 2), "pct": round((equity / capital - 1) * 100, 4)})
    end_eq = nav[-1]["equity"] if nav else float(capital)
    total = (end_eq / capital - 1) * 100
    days = 0
    if len(rows) >= 2:
        days = (datetime.date.fromisoformat(rows[-1][0][:10]) - datetime.date.fromisoformat(rows[0][0][:10])).days
    years = days / 365
    px = [float(r[2]) for r in rows]
    return {
        "trades": trades, "nav": nav,
        "stats": {
            "capital": round(float(capital), 2), "end": round(end_eq, 2),
            "total": round(total, 2), "mdd": round(mdd * 100, 2),
            "ann": round((pow(1 + total / 100, 1 / years) - 1) * 100, 2) if years >= 0.25 else None,
            "buys": sum(1 for t in trades if t["side"] == "buy"),
            "sells": closed, "win": win,
            "winrate": round(win / closed * 100, 1) if closed else None,
            "realized": round(realized_all, 2),
            "open_qty": round(qty, 4), "open_cost": round(cost, 4),
            "float_pnl": round((px[-1] - cost) * qty, 2) if qty > 0 else 0.0,
            "first": rows[0][0][:10] if rows else "", "last": rows[-1][0][:10] if rows else "",
            "n": len(rows), "days": days,
        },
    }


@app.get("/api/backtest")
async def backtest(symbol: str = Query(...), start: str = "", end: str = "",
                   capital: float = 100000.0, each: float = 10000.0,
                   name: str = "", save: bool = True, tf: str = "day", p: str = "", pair: str = "",
                   full: bool = False):
    if capital <= 0 or each <= 0:
        return {"error": "本金与单次买入金额需大于 0"}
    st_tf = BT_STRATS.get(tf)
    if not st_tf:
        return {"error": "策略类型只支持 day（日k占Ma3）/ m60（1小时k占Ma3）/ sr5（5分钟k止损反手）"}
    if tf == "sr5" and p:  # 策略页可在 1/5/15/30/60 分钟间切换，这里按传入周期覆盖默认 m5
        if p not in SR_LIMITS:
            return {"error": "止损反手的 K 线周期只支持 m1 / m5 / m15 / m30 / m60 / day"}
        st_tf = {"period": p, "label": f"止损反手（{p}）", "limit": SR_LIMITS[p]}
    d = await kline(symbol, st_tf["period"], st_tf["limit"], start, end)  # 复用 kline 取数口径（含均线预热与区间切片）
    if d.get("error"):
        return d
    rows = d.get("klines") or []
    if st_tf["period"] != "day":  # 分钟K 标签统一到 "YYYY-MM-DD HH:MM"，bt_run 与图面都用它当行标识
        rows = [[bt_lab(r[0])] + list(r[1:]) for r in rows]
    if len(rows) < 3:
        return {"error": "区间内 K 线不足（落在周末/停牌或超出行情源可回溯范围），请换个区间"}
    ind = d.get("ind") or {}
    if tf == "sr5" and pair:  # 跨标的反手：symbol=上方标的（信号源），pair=下方反向标的
        db = await kline(pair, st_tf["period"], st_tf["limit"], start, end)
        rows_b = db.get("klines") or []
        if st_tf["period"] != "day":
            rows_b = [[bt_lab(r[0])] + list(r[1:]) for r in rows_b]
        if len(rows_b) < 3:
            return {"error": f"反向标的 {pair} 在该区间取不到分钟K，请检查代码或换个周期"}
        ind_b = db.get("ind") or {}
        res = bt_run_pair(rows, rows_b, symbol, pair, capital,
                          ind.get("ma5") or [], ind.get("ma85") or [],
                          ind_b.get("ma5") or [], ind_b.get("ma85") or [],
                          ind.get("ma20") or [], ind_b.get("ma20") or [],
                          full=full)   # full=True：页面「全额策略」——一次买满、卖出全平，不逐根加仓
    elif tf == "sr5":  # 单标的旧路径（只能持多/空一边，不买反向标的）；页面跨标的走的是 bt_run_pair
        res = bt_run_sr(rows, capital)
    else:
        res = bt_run(rows, ind.get("ma3") or [], ind.get("ma5") or [], capital, each, _mkt_of(symbol) == "a")
    # 回测用 MA3/MA5 判信号（MA3 金叉 MA5 + 实体占上 MA3），图面另叠 MA3/MA5/MA20/MA85 四条均线供对照
    res.update({"symbol": symbol, "period": st_tf["period"], "tf": tf, "strat": st_tf["label"],
                "pair": pair,
                "klines": rows,
                "ma3": ind.get("ma3") or [],
                "ind": {k: ind.get(k) or [] for k in ("ma3", "ma5", "ma20", "ma85")}})
    if save:  # 每次回测自动落一条记录（供「回测记录」翻阅），落库失败不影响本次结果
        st0 = res.get("stats") or {}
        snap = {"symbol": symbol, "name": name or symbol, "tf": tf, "strat": st_tf["label"],
                "start": start or st0.get("first", ""), "end": end or st0.get("last", ""),
                "capital": float(capital), "each": float(each),
                # 快照只留画图要用的前5列（日期/开/收/高/低），去掉成交量缩体积
                "klines": [[r[0], r[1], r[2], r[3], r[4]] for r in rows],
                "ind": res["ind"], "trades": res["trades"], "nav": res["nav"], "stats": st0}
        try:
            rid, created = _bt_save(snap, json.dumps(snap, ensure_ascii=False))
            res["rec"] = {"id": rid, "created": created}
        except Exception:
            res["rec"] = None
    return res


# ---------- 回测记录：存/取快照供「回测记录」展示（只读自身表，与 orders/orders_live 无关） ----------
def _bt_save(snap, payload):
    # 同参数（策略+标的+区间+本金+每次买入）重复回测覆盖上一条，避免记录列表被重复项刷满
    ts = datetime.datetime.now().strftime(TS_FMT)
    con = _conn()
    try:
        with con:
            old = con.execute(
                "SELECT id FROM backtests WHERE symbol=? AND start=? AND end=? AND capital=? AND each=? AND tf=? "
                "ORDER BY id DESC LIMIT 1",
                (snap["symbol"], snap["start"], snap["end"], snap["capital"], snap["each"],
                 snap.get("tf") or "day")).fetchone()
            if old:
                con.execute("UPDATE backtests SET name=?, tf=?, created=?, payload=? WHERE id=?",
                            (snap["name"], snap.get("tf") or "day", ts, payload, old["id"]))
                return old["id"], ts
            cur = con.execute(
                "INSERT INTO backtests (symbol, name, start, end, capital, each, tf, created, payload) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (snap["symbol"], snap["name"], snap["start"], snap["end"],
                 snap["capital"], snap["each"], snap.get("tf") or "day", ts, payload))
            return cur.lastrowid, ts
    finally:
        con.close()


@app.get("/api/backtests")
async def bt_records(limit: int = 200):
    # 列表只返回摘要（payload 里的 K 线大 JSON 不返回），点开某条再取详情
    con = _conn()
    try:
        rows = con.execute("SELECT id, symbol, name, start, end, capital, each, tf, created, payload "
                           "FROM backtests ORDER BY created DESC, id DESC LIMIT ?",
                           (max(1, min(limit, 500)),)).fetchall()
    finally:
        con.close()
    items = []
    for r in rows:
        try:
            st = (json.loads(r["payload"]) or {}).get("stats") or {}
        except Exception:
            st = {}
        tf = r["tf"] or "day"
        items.append({"id": r["id"], "symbol": r["symbol"], "name": r["name"],
                      "start": r["start"], "end": r["end"], "created": r["created"],
                      "capital": r["capital"], "each": r["each"], "tf": tf,
                      "strat": (BT_STRATS.get(tf) or {}).get("label", BT_STRATS["day"]["label"]),
                      "stats": st})
    return {"items": items}


@app.get("/api/backtests/{rid}")
async def bt_record(rid: int):
    con = _conn()
    try:
        r = con.execute("SELECT id, symbol, name, start, end, capital, each, tf, created, payload "
                        "FROM backtests WHERE id=?", (rid,)).fetchone()
    finally:
        con.close()
    if not r:
        return {"error": "记录不存在（可能已被删除）"}
    try:
        data = json.loads(r["payload"])
    except Exception:
        return {"error": "记录内容已损坏，删掉重新回测即可"}
    ind = data.get("ind") or {}
    if "ma20" not in ind:  # 旧快照只存了 ma125，图面已改画 MA20：用快照收盘价补算（区间内无预热，前 19 根无值）
        cs = [float(x[2]) for x in (data.get("klines") or [])]
        ind["ma20"] = [round(sum(cs[i - 19:i + 1]) / 20, 4) if i >= 19 else None for i in range(len(cs))]
        data["ind"] = ind
    tf = r["tf"] or "day"
    return {"id": r["id"], "symbol": r["symbol"], "name": r["name"], "start": r["start"],
            "end": r["end"], "capital": r["capital"], "each": r["each"],
            "created": r["created"], "tf": tf,
            "strat": (BT_STRATS.get(tf) or {}).get("label", BT_STRATS["day"]["label"]),
            "data": data}


@app.delete("/api/backtests/{rid}")
async def bt_record_del(rid: int):
    con = _conn()
    try:
        with con:
            n = con.execute("DELETE FROM backtests WHERE id=?", (rid,)).rowcount
    finally:
        con.close()
    return {"deleted": n}


@app.get("/api/minute")
async def minute(symbol: str = Query(...)):
    if symbol.startswith("us"):
        return await us_minute(symbol)
    try:
        d = (await get_json(f"{WEB}/minute/query?code={symbol}"))["data"][symbol]
        m = d["data"]  # {"data": ["0930 price vol amount", ...], "date": "..."}
        return {"symbol": symbol, "quote": d.get("qt", {}).get(symbol, []), "date": m["date"], "points": m["data"]}
    except HTTPException:  # 腾讯被 WAF 挑战：降级东财分时（复用美股分时的换算口径，quote[4]=昨收供前端画基准线）
        data = await _em_trends(_em_secid(symbol))
        tr = (data or {}).get("trends") or []
        if not tr:
            raise HTTPException(502, "分时上游均不可用，请稍后刷新")
        pts, cv, ca = [], 0.0, 0.0
        for line in tr:
            a = line.split(",")  # 时间,开,收,高,低,量,额,均价
            cv += float(a[5])
            ca += float(a[6])
            pts.append(f"{a[0][11:13]}{a[0][14:16]} {a[2]} {cv:.0f} {ca:.3f}")
        return {"symbol": symbol,
                "quote": ["200", "", symbol[2:], tr[-1].split(",")[2], str(data.get("prePrice") or "")],
                "date": tr[0][:10], "points": pts}


@app.get("/api/search")
async def search(q: str = Query(...)):
    d = await get_json(f"{EM_SUGGEST}?input={quote(q)}&type=14&count=10&token={EM_TOKEN}")
    rows = (d.get("QuotationCodeTable") or {}).get("Data") or []
    # 东财市场码→腾讯前缀：0=深 1=沪 116=港股；105=纳斯达克 106=纽交所 107=美交所(AMEX/ETF)，统一走 us 前缀
    mkt = {"0": "sz", "1": "sh", "105": "us", "106": "us", "107": "us", "116": "hk"}
    items = []
    for r in rows:
        m, _, code = r.get("QuoteID", "").partition(".")
        if code and m in mkt:
            items.append({"symbol": mkt[m] + code, "name": r.get("Name", ""), "code": code})
    return {"items": items[:8]}


@app.get("/api/cards")
async def cards_get(scope: str = "hist", as_of: str = ""):
    con = _conn()
    _promote_bought_cards(con, scope, _norm_asof(as_of))  # hist 视点驱动/live 每日游标，函数注释有口径分叉
    rows = con.execute(f"SELECT symbol, name, period, pair FROM {_tbl(scope, 'cards')} ORDER BY pos").fetchall()
    con.close()
    return {"cards": [dict(r) for r in rows]}


@app.get("/api/holdings")
async def holdings_get(scope: str = "hist", as_of: str = ""):
    # as_of：历史回放视点（YYYY-MM-DD 或 YYYY-MM-DD HH:MM）；只给日期按当天 23:59 含全天，
    # 用于回看某日的持仓——该日之后下的单不出现（如27号买的仓，26号视点里不存在；
    # 此截断天然保证「检索买入日之前的日期看不到该仓」），买入当天即显示，T+1 只锁卖出按钮；
    # 视点当天的卖出次日才生效（defer_today_sell）：份额/收益照卖前显示，可卖仍扣当日已卖
    a = _norm_asof(as_of)
    con = _conn()
    splits = await _splits_for(con, scope)  # 合股/分股事件：跨过生效日的持仓自动换股本基准（市值不变）
    pos, _, _ = _replay(con, scope=scope, as_of=a, splits=splits, defer_today_sell=True)
    con.close()
    out = []
    for s, p in pos.items():
        if p["shares"] <= 1e-9:
            continue
        # 视点扫描判断：结束日期之前（含当天）没有该股任何买入流水的，直接不返回该股持仓——
        # 例：3-7 买入的个股，视点 3-6 时 orders 里查无买入记录，前端就不显示它的持仓。
        # 截断重演已天然保证此不变量，这里钉成显式判断，防后续改动破坏该口径
        if a is not None and not (p["buy_ts"] and p["buy_ts"] <= a):
            continue
        out.append({"symbol": s, "name": p["name"], "shares": round(p["shares"], 6), "cost": round(p["cost"], 6),
                    "sellable": round(max(0.0, p["shares"] - (0 if _is_t0(s) else p["bought_today"]) - p["sold_today"]), 6),
                    "buy_ts": p["buy_ts"]})
    return {"holdings": out}


@app.get("/api/orders")
async def orders_get(scope: str = "hist", as_of: str = ""):
    # as_of：历史回放视点（同 /api/holdings）——订单列表与已实现收益都只含该时点前的流水：
    # 回看18号之前的视点看不到18号下的单（及之后卖出落的袋）；realized 按视点重演推导，
    # 视点当天的卖出次日才计入已实现（与持仓「卖出次日生效」同口径）
    a = _norm_asof(as_of)
    con = _conn()
    sql = f"SELECT id, symbol, name, side, price, qty, ts FROM {_tbl(scope, 'orders')}"
    args = ()
    if a:
        sql += " WHERE ts <= ?"
        args = (a,)
    rows = con.execute(sql + " ORDER BY ts, id", args).fetchall()
    splits = await _splits_for(con, scope)  # 已实现收益同样要跨事件重演：合股后卖出的成本基准已换算
    _, realized, _ = _replay(con, scope=scope, as_of=a, splits=splits, defer_today_sell=True)
    con.close()
    # 买入行 realized 为 null（收益由持仓浮动盈亏表达）；realized_total 供抽屉「全部收益=已实现+浮动」
    return {"orders": [{**dict(r), "realized": realized.get(r["id"])} for r in rows],
            "realized_total": round(sum(realized.values()), 2)}


@app.post("/api/orders")
async def orders_post(payload: dict, scope: str = "hist"):
    sym = str(payload.get("symbol") or "").strip()
    side = str(payload.get("side") or "").strip().lower()
    try:
        price, qty = float(payload.get("price")), float(payload.get("qty"))
    except (TypeError, ValueError):
        raise HTTPException(400, "price/qty 必须是数字")
    ts = _norm_ts(payload.get("ts")) or datetime.datetime.now().strftime(TS_FMT)
    if ts > datetime.datetime.now().strftime(TS_FMT):
        raise HTTPException(400, "成交时间不能晚于现在（未来时间的单会提前占用可卖份额）")
    if not sym.startswith(("sh", "sz", "hk", "us")):
        raise HTTPException(400, "symbol 前缀只支持 sh/sz/hk/us")
    if side not in ("buy", "sell"):
        raise HTTPException(400, "side 只支持 buy/sell")
    if not (price > 0 and qty > 0):
        raise HTTPException(400, "price/qty 必须大于 0")
    con = _conn()
    name = str(payload.get("name") or "").strip()
    if not name:  # 名称快照：卡片表 → 同代码最近一笔流水 → 代码本身，逐层兜底
        c = con.execute(f"SELECT name FROM {_tbl(scope, 'cards')} WHERE symbol=?", (sym,)).fetchone()
        if c:
            name = c["name"]
        else:
            o = con.execute(f"SELECT name FROM {_tbl(scope, 'orders')} WHERE symbol=? ORDER BY ts DESC, id DESC LIMIT 1",
                             (sym,)).fetchone()
            name = o["name"] if o else sym
    splits = await _splits_for(con, scope)  # 卖出校验/已实现收益按合股换算后的股本基准重演
    if side == "sell":
        # 卖出上限按「该单业务时点 ts」重演校验（hist 气泡 ts=回放结束日期、手填补录历史单同理）：
        # 不能按当前全量持仓算——回看视点时的持仓与现在不同，超视点持仓的卖出会插队卖超
        p = _replay(con, scope=scope, as_of=ts, splits=splits)[0].get(sym) or {"shares": 0.0, "bought_today": 0.0}
        avail = p["shares"] if _is_t0(sym) else p["shares"] - p["bought_today"]
        if qty > avail + 1e-9:
            con.close()
            tip = "" if _is_t0(sym) or p["bought_today"] <= 1e-9 else "（A股当日买入次日可卖）"
            raise HTTPException(400, f"卖出数量超过可卖，{ts[:10]} 时可卖 {avail:g}{tip}")
        old_conflict = _replay(con, scope=scope, splits=splits)[2]  # 存量卖超坏账（正常数据恒 None），供插入后对比
    with con:
        cur = con.execute(f"INSERT INTO {_tbl(scope, 'orders')} (symbol, name, side, price, qty, ts, created_at) "
                          f"VALUES (?, ?, ?, ?, ?, ?, ?)",
                          (sym, name, side, price, qty, ts, datetime.datetime.now().strftime(TS_FMT)))
    oid = cur.lastrowid
    if side == "sell":
        # 时序兜底：落在历史时点的卖出可能挤占其后已录卖出的份额（或自身卖超），插入后全量重演即现冲突——回滚拒单。
        # 与插入前存量冲突比对 id，坏账不误伤新单；买入只会缓解冲突，无需兜底
        conflict = _replay(con, scope=scope, splits=splits)[2]
        if conflict and conflict["id"] != (old_conflict or {"id": None})["id"]:
            with con:
                con.execute(f"DELETE FROM {_tbl(scope, 'orders')} WHERE id=?", (oid,))
            con.close()
            raise HTTPException(400, f"卖出将超过持仓：{conflict['name']} {conflict['ts']} 的卖出将超过当时可卖，请调小数量")
    realized = _replay(con, scope=scope, splits=splits)[1].get(oid)
    con.close()
    return {"ok": True, "order": {"id": oid, "symbol": sym, "name": name, "side": side,
                                  "price": price, "qty": qty, "ts": ts}, "realized": realized}


@app.delete("/api/orders")
async def orders_clear(scope: str = "hist"):
    # 一键清空：hist + live 两张流水表一并删光（用户要求全局清空——曾发生只清 live 后 hist 回放表残留
    # 旧流水、回放视点仍显示旧收益的事故）；持仓清零、收益归 0、可用资金回到本金（皆为流水重演派生，不落库）；
    # 整表连删不会产生卖超坏账，无需 orders_delete 的删除被依赖预检；卡片与流水独立不受影响
    con = _conn()
    n = 0
    with con:
        for t in ("orders", "orders_live"):
            cur = con.execute(f"DELETE FROM {t}")
            n += cur.rowcount
    con.close()
    return {"deleted": n}


@app.delete("/api/orders/{oid}")
async def orders_delete(oid: int, scope: str = "hist"):
    con = _conn()
    if not con.execute(f"SELECT 1 FROM {_tbl(scope, 'orders')} WHERE id=?", (oid,)).fetchone():
        con.close()
        raise HTTPException(404, "记录不存在")
    splits = await _splits_for(con, scope)
    conflict = _replay(con, exclude_id=oid, scope=scope, splits=splits)[2]
    if conflict:  # 删掉这笔买入会让后续某笔卖出卖超：拒删并点名那笔卖出
        con.close()
        raise HTTPException(400, f"删除被依赖：{conflict['name']} {conflict['ts']} 的卖出将超过可卖持仓，请先删除该笔卖出")
    with con:
        con.execute(f"DELETE FROM {_tbl(scope, 'orders')} WHERE id=?", (oid,))
    con.close()
    return {"ok": True}


@app.delete("/api/holdings/{symbol}")
async def holdings_delete(symbol: str, scope: str = "hist"):
    # 持仓无独立表（由流水重演推导），删除=清掉该标的全部买卖流水：买卖一并消失，重演不会
    # 产生卖超坏账；卡片与流水独立不受影响；已实现收益随卖出流水一起消失（不分回放视点，全删）
    con = _conn()
    n = con.execute(f"SELECT COUNT(*) AS c FROM {_tbl(scope, 'orders')} WHERE symbol=?", (symbol,)).fetchone()["c"]
    if not n:
        con.close()
        raise HTTPException(404, "该标的无买卖流水")
    with con:
        con.execute(f"DELETE FROM {_tbl(scope, 'orders')} WHERE symbol=?", (symbol,))
    con.close()
    return {"ok": True, "deleted": n}


@app.get("/api/quotes")
async def quotes(symbols: str = Query(...)):
    # 腾讯批量报价：qt.gtimg.cn 逗号分隔一次取多个标的（GBK 文本），f[3]=现价 f[4]=昨收
    out = {}
    syms = [s.strip() for s in symbols.split(",") if s.strip()]
    for i in range(0, len(syms), 40):  # 分批防 URL 过长
        txt = await get_text(QT + ",".join(syms[i:i + 40]), gbk=True)
        for line in txt.split(";"):
            if "=" not in line or '\"' not in line:
                continue
            var, _, rest = line.partition("=")
            f = rest.split('"')[1].split("~")
            sym = var.strip().removeprefix("v_")
            if len(f) > 4 and f[3] and f[4]:
                try:
                    price, prev = float(f[3]), float(f[4])
                    # 合股/分股日腾讯沿用未调整昨收（自算涨跌会虚大十倍）：官方涨跌幅字段 f[32]
                    # 与自算差超 3% 时按官方值反推有效昨收基准；正常日两者恒一致，零影响
                    pct = float(f[32]) if len(f) > 32 and f[32] else None
                    if pct is not None and prev > 0 and abs(price / prev - 1 - pct / 100) > 0.03:
                        adj = price / (1 + pct / 100)
                        if adj > 0:
                            prev = adj
                    out[sym] = {"price": price, "prev": prev}
                except (ValueError, ZeroDivisionError):
                    pass
    return {"quotes": out}


@app.put("/api/cards")
async def cards_put(payload: dict, scope: str = "hist"):
    rows = []
    for c in payload.get("cards") or []:
        if isinstance(c, dict) and c.get("symbol"):
            rows.append((str(c["symbol"]), str(c.get("name") or ""), str(c.get("period") or "day"),
                         str(c.get("pair") or "")))
    con = _conn()
    with con:  # 一个事务内先清后插：数组顺序即 pos
        con.execute(f"DELETE FROM {_tbl(scope, 'cards')}")
        con.executemany(f"INSERT INTO {_tbl(scope, 'cards')} (pos, symbol, name, period, pair) "
                        f"VALUES (?, ?, ?, ?, ?)",
                        [(i, s, n, p, pr) for i, (s, n, p, pr) in enumerate(rows)])
    con.close()
    return {"ok": True, "count": len(rows)}


@app.delete("/api/cards/{symbol}")
async def cards_delete(symbol: str, scope: str = "hist"):
    # 卡片右上 ✕：后端直接删 cards/cards_live 表里该 symbol 这一行（hist/live 由 scope 映射）；
    # 只删自选卡本身，该标的买卖流水不受影响（持仓/抽屉与卡片是两套独立数据）；
    # 删中间行留下的 pos 空洞不影响 ORDER BY 排序，之后的全量保存或每日前置重排都会重编号
    con = _conn()
    with con:
        cur = con.execute(f"DELETE FROM {_tbl(scope, 'cards')} WHERE symbol=?", (symbol,))
    n = cur.rowcount
    con.close()
    if not n:
        raise HTTPException(404, "卡片不存在")
    return {"ok": True, "deleted": n}


# ---------- 画线：趋势线/水平线（前端渲染成一条折线 series，随缩放自动跟随）----------
DRAW_KINDS = ("trend", "h")


@app.get("/api/drawings")
async def drawings_get(symbol: str = Query(...), period: str = "day"):
    con = _conn()
    rows = con.execute("SELECT id, kind, pts FROM drawings WHERE symbol=? AND period=? ORDER BY id",
                       (symbol, period)).fetchall()
    con.close()
    items = []
    for r in rows:
        try:
            pts = json.loads(r["pts"])
        except ValueError:
            continue  # 坏数据跳过，不让一条线拖垮整张图
        items.append({"id": r["id"], "kind": r["kind"], "pts": pts})
    return {"items": items}


@app.post("/api/drawings")
async def drawings_add(payload: dict):
    symbol = str(payload.get("symbol") or "").strip()
    period = str(payload.get("period") or "day").strip()
    kind = str(payload.get("kind") or "")
    pts = payload.get("pts") or []
    if not symbol or kind not in DRAW_KINDS or not 1 <= len(pts) <= 2:
        raise HTTPException(400, "参数不合法：需 symbol、kind=trend|h、pts 1~2 个点")
    clean = []
    for p in pts:
        try:
            clean.append({"x": str(p.get("x")), "y": round(float(p.get("y")), 4)})
        except (TypeError, ValueError, AttributeError):
            raise HTTPException(400, "pts 需为 [{x: 类目, y: 价格}]")
    con = _conn()
    with con:
        cur = con.execute("INSERT INTO drawings (symbol, period, kind, pts, created) VALUES (?, ?, ?, ?, ?)",
                          (symbol, period, kind, json.dumps(clean, ensure_ascii=False),
                           datetime.datetime.now().strftime(TS_FMT)))
    con.close()
    return {"ok": True, "item": {"id": cur.lastrowid, "kind": kind, "pts": clean}}


@app.put("/api/drawings/{did}")
async def drawings_update(did: int, payload: dict):
    # 端点拖动改线：kind 可从 h 升为 trend（水平线拖一端即变斜线，两端高度可分别调）
    kind = str(payload.get("kind") or "")
    pts = payload.get("pts") or []
    if kind not in DRAW_KINDS or not 1 <= len(pts) <= 2:
        raise HTTPException(400, "参数不合法：需 kind=trend|h、pts 1~2 个点")
    clean = []
    for p in pts:
        try:
            clean.append({"x": str(p.get("x")), "y": round(float(p.get("y")), 4)})
        except (TypeError, ValueError, AttributeError):
            raise HTTPException(400, "pts 需为 [{x: 类目, y: 价格}]")
    con = _conn()
    with con:
        cur = con.execute("UPDATE drawings SET kind=?, pts=? WHERE id=?",
                          (kind, json.dumps(clean, ensure_ascii=False), did))
    con.close()
    if not cur.rowcount:
        raise HTTPException(404, "线不存在")
    return {"ok": True, "item": {"id": did, "kind": kind, "pts": clean}}


@app.delete("/api/drawings/{did}")
async def drawings_delete(did: int):
    con = _conn()
    with con:
        cur = con.execute("DELETE FROM drawings WHERE id=?", (did,))
    con.close()
    if not cur.rowcount:
        raise HTTPException(404, "线不存在")
    return {"ok": True, "deleted": cur.rowcount}


@app.delete("/api/drawings")
async def drawings_clear(symbol: str = Query(...), period: str = "day"):
    con = _conn()
    with con:
        cur = con.execute("DELETE FROM drawings WHERE symbol=? AND period=?", (symbol, period))
    con.close()
    return {"ok": True, "deleted": cur.rowcount}


@app.get("/api/prefs")
async def prefs_get():
    # 全部界面偏好一次返回 {prefs: {key: value}}；空库返回空对象，前端按默认值渲染
    con = _conn()
    rows = con.execute("SELECT key, value FROM prefs").fetchall()
    con.close()
    return {"prefs": {r["key"]: r["value"] for r in rows}}


@app.put("/api/prefs")
async def prefs_put(payload: dict):
    # 增量 upsert：只写传入的键，未传的键不动（「清除」= 写空串，同样落库防旧值复活）；
    # 键限 ascii 字母数字下划线且长度<=32、值长度<=64：只收日期/页签这类短偏好，防误存大对象
    items = []
    for k, v in (payload.get("prefs") or {}).items():
        k, v = str(k).strip(), str(v)
        if k and len(k) <= 32 and len(v) <= 64 and all(c.isascii() and (c.isalnum() or c == "_") for c in k):
            items.append((k, v))
    con = _conn()
    with con:
        con.executemany("INSERT OR REPLACE INTO prefs (key, value) VALUES (?, ?)", items)
    con.close()
    return {"ok": True, "count": len(items)}


@app.get("/api/recent")
async def recent_get(limit: int = 8):
    # 最近搜过的标的（跨浏览器共享，跟库走）：按最近一次选中时间倒序，默认给 8 条
    n = max(1, min(limit, 20))
    con = _conn()
    rows = con.execute("SELECT symbol, name FROM recent_sym ORDER BY ts DESC LIMIT ?", (n,)).fetchall()
    con.close()
    return {"items": [{"symbol": r["symbol"], "name": r["name"]} for r in rows]}


@app.post("/api/recent")
async def recent_post(payload: dict):
    # 选中一只标的就记一条（同代码覆盖时间戳置顶，不产生重复）；只留最近 40 条，防表无限增长
    sym = str(payload.get("symbol") or "").strip()
    name = str(payload.get("name") or "").strip()
    if not sym or len(sym) > 24:
        return {"ok": False}
    con = _conn()
    with con:
        con.execute("INSERT OR REPLACE INTO recent_sym (symbol, name, ts) VALUES (?, ?, ?)",
                    (sym, name[:32], datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
        con.execute("DELETE FROM recent_sym WHERE symbol NOT IN "
                    "(SELECT symbol FROM recent_sym ORDER BY ts DESC LIMIT 40)")
    con.close()
    return {"ok": True}


@app.get("/api/account")
async def account_get(scope: str = "hist", as_of: str = ""):
    # 账户本金与可用资金：可用资金 = 本金 − 累计买入金额 + 累计卖出回笼（次日到账），由 orders 流水重演推导不落库（删单自动重算）
    # as_of：历史回放视点——可用资金同样只算该时点前的流水（18号买的单在17号视点里还没扣钱）
    a = _norm_asof(as_of)
    s = "live" if scope == "live" else "hist"
    con = _conn()
    row = con.execute("SELECT capital FROM account WHERE scope=?", (s,)).fetchone()
    sql = f"SELECT side, price, qty, ts FROM {_tbl(scope, 'orders')}"
    args = ()
    if a:
        sql += " WHERE ts <= ?"
        args = (a,)
    rows = con.execute(sql, args).fetchall()
    con.close()
    capital = row["capital"] if row else 0.0
    # 卖出回笼资金次日到账（与持仓「卖出次日生效」同口径）：视点日卖出的钱不计入，
    # 否则当天份额还在市值里、卖出款又已进现金，总资产会凭空多出一笔
    today = (a or datetime.datetime.now().strftime(TS_FMT))[:10]
    cash = capital - sum(r["price"] * r["qty"] for r in rows if r["side"] == "buy") \
        + sum(r["price"] * r["qty"] for r in rows if r["side"] == "sell" and r["ts"][:10] < today)
    return {"capital": capital, "cash": round(cash, 2)}


@app.put("/api/account")
async def account_put(payload: dict, scope: str = "hist"):
    # 改本金：只写本金值，历史流水不动（可用资金随本金即时变化）
    try:
        capital = float(payload.get("capital"))
    except (TypeError, ValueError):
        raise HTTPException(400, "capital 必须是数字")
    if not capital > 0:
        raise HTTPException(400, "capital 必须大于 0")
    s = "live" if scope == "live" else "hist"
    con = _conn()
    with con:
        con.execute("INSERT OR REPLACE INTO account (scope, capital) VALUES (?, ?)", (s, capital))
    con.close()
    return {"ok": True, "capital": capital}


# ---------- 收盘前 10 分钟「买入信号」桌面提醒 ----------
# 扫两张自选卡表（cards + cards_live）的并集，命中就弹 Windows 右下角通知。判定与回测同一套口径
# （docs/回测规则口径.md）：MA3 在 MA5 上方（金叉后、死叉前的买入区间）当日就该买；
# 盘中拿不到“今日收盘价”的终值，一律用实时报价现价近似；历史只取到
# 昨日为止，不依赖上游是否已出当日 K 线（腾讯日K盘中给不给当日根都能算）。
REMIND_BEFORE = 10  # 分钟：收盘前多久开扫（同时当作窗口长度，服务中途启动也不会漏）
REMIND_CLOSE = {"a": (15, 0), "hk": (16, 0), "us": (16, 0)}  # 各市场收市时刻（当地时区）
MKT_CN = {"a": "A股", "hk": "港股", "us": "美股"}
_REMIND_DONE = {}  # 市场 -> 已提醒的当地日期：同一收盘窗口只弹一次
_REMIND_BEAT = {"n": 0, "at": ""}  # 后台循环心跳：/api/notify/scan 回显，用于确认定时任务真的在跑
TOAST_PS1 = Path(__file__).resolve().parent / "toast.ps1"


def _beijing_now():
    return datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) + datetime.timedelta(hours=8)


def _nth_sunday(y, m, n):
    d0 = datetime.date(y, m, 1)
    return d0 + datetime.timedelta(days=(6 - d0.weekday()) % 7 + 7 * (n - 1))


def _mkt_now(mkt):
    # 该市场当地的当前时间：A股/港股与北京同区；美股按美东时令（夏令时 UTC-4 = 北京-12，冬令时北京-13）
    bj = _beijing_now()
    if mkt != "us":
        return bj
    dst = _nth_sunday(bj.year, 3, 2) <= bj.date() < _nth_sunday(bj.year, 11, 1)  # 3月第二个周日~11月第一个周日
    return bj - datetime.timedelta(hours=12 if dst else 13)


def _mkt_of(sym):
    if sym.startswith(("sh", "sz")):
        return "a"
    if sym.startswith("hk"):
        return "hk"
    if sym.startswith("us"):
        return "us"
    return ""


def _ts_day(s):
    # 腾讯报价时间戳三种写法：A股 20261027161455 / 美股 2026-10-27 16:00:01 / 港股 2026/10/27 16:08:28
    return "".join(c for c in (s or "") if c.isdigit())[:8]


async def _qt_bulk(syms):
    # 批量取现价/今开/时间戳：qt.gtimg.cn 逗号分隔一次拉完，请求数不随卡片数增长
    out = {}
    for i in range(0, len(syms), 40):
        try:
            txt = await get_text(QT + ",".join(syms[i:i + 40]), gbk=True)
        except Exception:
            continue
        for line in txt.split(";"):
            if "=" not in line or '"' not in line:
                continue
            var, _, rest = line.partition("=")
            f = rest.split('"')[1].split("~")
            if len(f) <= 30 or not f[3]:
                continue
            try:
                out[var.strip().removeprefix("v_")] = {
                    "price": float(f[3]),
                    "prev": float(f[4]) if f[4] not in ("", "0") else 0.0,  # 昨收：顶部条算涨幅用
                    "open": float(f[5]) if f[5] not in ("", "0") else 0.0,
                    "day": _ts_day(f[30]),
                }
            except ValueError:
                pass
    return out


async def _signal_of(sym, mkt, q, sem):
    # 单票判定：今日无报价（休市/停牌/上游未更新）直接跳过，不拿昨收冒充今日价
    today = _mkt_now(mkt).date().strftime("%Y%m%d")
    if not q or q["price"] <= 0 or q["day"] != today:
        return None
    async with sem:
        d = await kline(sym, "day", 12, "", "")
    rows = d.get("klines") or []
    hist = [r for r in rows if r[0][:10].replace("-", "") < today]  # 只保留昨日及更早
    if len(hist) < 5:
        return None  # MA3/MA5 最远用到前五根收盘，不够就不判
    px = q["price"]
    c1, c2, c3, c4, c5 = (float(hist[-k][2]) for k in (1, 2, 3, 4, 5))  # 昨收、前二至前五根收盘
    ma3 = (c2 + c1 + px) / 3  # 今日 MA3 = 前两日收盘 + 今日现价
    ma5 = (c4 + c3 + c2 + c1 + px) / 5  # 今日 MA5（同口径用现价代今日收盘）
    if not ma3 > ma5:
        return None  # 3 日线不在 5 日线上方（未金叉或已死叉）：不在买入区间，不提醒
    return {"symbol": sym, "price": px, "ma3": round(ma3, 4), "ma5": round(ma5, 4)}


async def _heat_of(sym, mkt, q, sem):
    # 顶部固定条「优异标的」用：与 _signal_of 完全同一套数据与口径，但每只都返回（含没命中的），
    # 由前端自己排序展示。hit=True 即收盘前提醒会弹出来的那一只。
    today = _mkt_now(mkt).date().strftime("%Y%m%d")
    async with sem:
        d = await kline(sym, "day", 12, "", "")
    rows = d.get("klines") or []
    hist = [r for r in rows if r[0][:10].replace("-", "") < today]
    if len(hist) < 5:
        return None
    live = bool(q and q["price"] > 0 and q["day"] == today)
    px = q["price"] if live else float(hist[-1][2])  # 休市（美股白天等）用最后一根收盘当现价
    prev = (q["prev"] if live else 0) or float(hist[-2][2])
    c1, c2, c3, c4, c5 = (float(hist[-k][2]) for k in (1, 2, 3, 4, 5))
    ma3 = (c2 + c1 + px) / 3
    ma5 = (c4 + c3 + c2 + c1 + px) / 5
    ma3p, ma5p = (c3 + c2 + c1) / 3, (c5 + c4 + c3 + c2 + c1) / 5
    gc = ma3 > ma5 and ma3p <= ma5p
    hit = bool(ma3 > ma5)  # 提醒口径：金叉后每根都买，MA3 在 MA5 上方即命中
    return {"symbol": sym, "price": px, "pct": round((px / prev - 1) * 100, 2) if prev > 0 else 0.0,
            "ma3": round(ma3, 4), "ma5": round(ma5, 4), "gc": bool(gc), "hit": hit,
            # bull=MA3 在 MA5 上方且仍在上行（多头没坏）；休市标的 pct 是最后一根对前一根的涨跌
            "bull": bool(ma3 > ma5 and ma3 > ma3p), "live": live}


@app.get("/api/heat")
async def heat(force: bool = False):
    # 顶部固定条专用：一次扫完两个仓的全部标的，只返回数据、不弹通知（通知通道见 /api/notify/scan）
    # 结果缓存 3 分钟：全量扫描要拉几十次日K（并发卡在 3），上游被 WAF 拦住走降级链时能扫好几分钟；
    # 不缓存时页面每分钟重扫一次，扫描会一层层叠着跑，把浏览器同域名 6 个连接和后端连接池全占满，
    # 卡片取数排到后面，看上去就像整页反复重载
    now = time.time()
    if not force and _HEAT["d"] and now - _HEAT["t"] < HEAT_TTL:
        return _HEAT["d"]
    async with _HEAT_LOCK:  # 并发的重扫合并：后到的等前一次扫完直接拿结果，不叠加第二次全量扫
        if not force and _HEAT["d"] and time.time() - _HEAT["t"] < HEAT_TTL:
            return _HEAT["d"]
        con = _conn()
        rows = con.execute("SELECT symbol, name FROM cards UNION SELECT symbol, name FROM cards_live").fetchall()
        con.close()
        names = {r["symbol"]: r["name"] for r in rows}
        syms = list(names)
        at = _beijing_now().strftime("%H:%M:%S")
        if not syms:
            return {"items": [], "at": at}
        qs = await _qt_bulk(syms)
        sem = asyncio.Semaphore(3)
        # 卡 45 秒：上游被 WAF 拦住时每只票都要走完降级链（东财 curl 单次就到 10 秒），40 只叠起来能拖两分钟，
        # 超时就把已扫完的先上屏（信号条是展示层，缺几只无所谓，下一轮再补齐），不把整个页面拖死
        tasks = [asyncio.create_task(_heat_of(s, _mkt_of(s), qs.get(s), sem)) for s in syms]
        done, pending = await asyncio.wait(tasks, timeout=45)
        for t in pending:
            t.cancel()
        items = []
        for s, t in zip(syms, tasks):
            r = t.result() if t in done and not t.cancelled() and t.exception() is None else None
            if isinstance(r, dict):
                r["name"] = names.get(s, "")
                r["market"] = _mkt_of(s)
                items.append(r)
        out = {"items": items, "at": at}
        _HEAT.update(t=time.time(), d=out)
        return out


async def scan_signals(mkts=None):
    # 扫一遍自选卡，返回 {市场: [命中] }。mkts=None / 空集合 表示全市场
    want = set(REMIND_CLOSE) if not mkts else set(mkts) & set(REMIND_CLOSE)
    con = _conn()
    rows = con.execute("SELECT symbol, name FROM cards UNION SELECT symbol, name FROM cards_live").fetchall()
    con.close()
    names = {r["symbol"]: r["name"] for r in rows}
    syms = [s for s in names if _mkt_of(s) in want]
    if not syms:
        return {}
    qs = await _qt_bulk(syms)
    sem = asyncio.Semaphore(3)  # 限并发：37 卡同时打上游会触发腾讯 WAF
    res = await asyncio.gather(*(_signal_of(s, _mkt_of(s), qs.get(s), sem) for s in syms),
                               return_exceptions=True)
    out = {}
    for s, r in zip(syms, res):
        if isinstance(r, dict):
            r["name"] = names.get(s, "")
            r["market"] = _mkt_of(s)
            out.setdefault(_mkt_of(s), []).append(r)
    return out


def _toast(title, body):
    # 零依赖桌面通知：调 toast.ps1（纯 ASCII，中文走命令行参数）
    try:
        subprocess.Popen(["powershell", "-NoProfile", "-ExecutionPolicy", "Bypass",
                          "-File", str(TOAST_PS1), "-Title", title, "-Body", body],
                         creationflags=0x08000000)  # CREATE_NO_WINDOW：不闪黑框
    except Exception:
        pass


def push_hits(hits, force=False):
    # 命中列表 → 一条通知（最多列 4 只，其余并成“等 N 只”）。force=手动测试时无命中也给回执
    flat = [h for v in hits.values() for h in v]
    if not flat:
        if force:
            _toast("买入信号 · 无命中", "当前没有自选票处在 MA3 上穿 MA5 后的买入区间")
        return 0
    lines = [f"{h['name'] or h['symbol']} {h['price']:.2f}" for h in flat[:4]]
    if len(flat) > 4:
        lines.append(f"等共 {len(flat)} 只")
    _toast(f"买入信号 · {len(flat)} 只", "\n".join(lines))
    return len(flat)


def _remind_due(mkt):
    lt = _mkt_now(mkt)
    if lt.weekday() >= 5:
        return False  # 周末直接跳过（节假日靠“今日无报价”判定）
    ch, cm = REMIND_CLOSE[mkt]
    t0 = ch * 60 + cm - REMIND_BEFORE
    cur = lt.hour * 60 + lt.minute
    return t0 <= cur < t0 + REMIND_BEFORE and _REMIND_DONE.get(mkt) != lt.date().isoformat()


async def _remind_loop():
    # 每 20 秒醒一次：落在某市场的收盘前窗口里就扫该市场并弹通知，弹过当天不再弹
    while True:
        try:
            for mkt in list(REMIND_CLOSE):
                if not _remind_due(mkt):
                    continue
                hits = await scan_signals({mkt})
                _REMIND_DONE[mkt] = _mkt_now(mkt).date().isoformat()
                push_hits(hits)
        except Exception:
            pass  # 提醒属旁路功能，任何异常都不能影响行情主链路
        _REMIND_BEAT["n"] += 1
        _REMIND_BEAT["at"] = _beijing_now().strftime(TS_FMT)
        await asyncio.sleep(20)


@app.on_event("startup")
async def _start_remind_loop():
    asyncio.create_task(_remind_loop())


@app.get("/api/notify/scan")
async def notify_scan(market: str = "all"):
    # 手动测试：立刻跑一遍扫描并弹通知（无命中也会弹一条回执，用于验证通知通道）
    hits = await scan_signals(None if market in ("", "all") else {market})
    n = push_hits(hits, force=True)
    flat = [h for v in hits.values() for h in v]
    return {"count": n, "hits": flat, "heartbeat": dict(_REMIND_BEAT),
            "scanned": list(REMIND_CLOSE if market in ("", "all") else {market} & set(REMIND_CLOSE))}

