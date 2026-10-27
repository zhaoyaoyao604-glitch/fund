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
_ifzq_pref = {"i": 0}
WEB = "https://web.ifzq.gtimg.cn/appstock/app"  # minute（分时）
BARE = "https://web.ifzq.gtimg.cn/appstock/app"  # fqkline（日/周/月K） / mkline（分钟K）
EM_SUGGEST = "https://searchapi.eastmoney.com/api/suggest/get"  # 东财搜索
EM_TOKEN = "D43BF722C8E33BDC906FB84D85E326E8"  # 东财网页端公开 token
QT = "https://qt.gtimg.cn/q="  # 腾讯实时报价（GBK文本），美股补当日活动K线用
SINA_US_DAILY = "https://stock.finance.sina.com.cn/usstock/api/jsonp_v2.php/var%20_t=/US_MinKService.getDailyK"  # 新浪美股日K全历史（不复权）
EM_TRENDS = "https://push2his.eastmoney.com/api/qt/stock/trends2/get"  # 东财分时（美股）
DAY_PERIODS = {"day", "week", "month"}
MIN_PERIODS = {"m5", "m15", "m30", "m60"}
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
    # 界面偏好表（key-value）：所在页/日期筛选等跨浏览器共享的 UI 偏好，跟库走不跟某个浏览器的 localStorage 走
    if not con.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='prefs'").fetchone():
        with con:
            con.execute("CREATE TABLE prefs (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
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
                        "capital REAL NOT NULL, each REAL NOT NULL, created TEXT NOT NULL, payload TEXT NOT NULL)")
            con.execute("CREATE INDEX idx_backtests_key ON backtests (symbol, start, end, capital, each)")
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


def _swap_host(url, host):
    p = urlsplit(url)
    return urlunsplit((p.scheme, host, p.path, p.query, p.fragment))


async def get_json(url, referer=None, timeout=15):
    now = time.time()
    hit = _CACHE.get(url)
    if hit and now - hit[0] < TTL:
        return hit[1]
    headers = UA if not referer else {**UA, "Referer": referer}
    if "ifzq.gtimg.cn" in url:  # 双域回退：优先当前已知可用域，被 WAF 挑战就换另一个
        i = _ifzq_pref["i"]
        hosts = (IFZQ_HOSTS[i], IFZQ_HOSTS[1 - i])
    else:
        hosts = (None,)
    data = None
    async with httpx.AsyncClient(timeout=timeout, headers=headers) as cli:
        for host in hosts:
            u = _swap_host(url, host) if host else url
            try:
                r = await cli.get(u)
                data = json.loads(r.content.decode("utf-8"))  # 上游是 UTF-8，但 header 不标，得强解
            except Exception:  # 连不上 / 501 挑战页 / 非 JSON 都算该域不可用
                data = None
                continue
            if host:
                _ifzq_pref["i"] = IFZQ_HOSTS.index(host)
            break
    if data is None:
        raise HTTPException(502, "上游行情源返回异常（疑似腾讯WAF临时限流），请稍等片刻再刷新")
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
    async with httpx.AsyncClient(timeout=15, headers=UA) as cli:
        r = await cli.get(url)
    data = r.content.decode("gbk", errors="replace") if gbk else r.content.decode("utf-8", errors="replace")
    if len(_CACHE) > 300:
        _CACHE.clear()
    _CACHE[url] = (now, data)
    return data


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


async def _em_trends(secid):
    url = (f"{EM_TRENDS}?secid={secid}&ndays=1&iscr=0&fields1=f1,f2,f3,f4,f5,f6,f7,f8"
           "&fields2=f51,f52,f53,f54,f55,f56,f57,f58")
    try:
        d = await get_json(url, referer="https://quote.eastmoney.com/", timeout=6)
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
    # 均线 MA3/5/85/125 + MACD(5,10,3)，与看盘软件口径一致（EMA 从首根种子起步）
    if not closes:
        return {}
    n = len(closes)

    def ma(p):
        return [round(sum(closes[i - p + 1 : i + 1]) / p, 4) if i >= p - 1 else None for i in range(n)]

    e5, e10 = ema(closes, 5), ema(closes, 10)
    dif = [a - b for a, b in zip(e5, e10)]
    dea = ema(dif, 3)
    return {
        "ma3": ma(3), "ma5": ma(5), "ma85": ma(85), "ma125": ma(125),
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


@app.get("/api/health")
async def health():
    return {"ok": True, "source": "tencent-free"}


@app.get("/api/kline")
async def kline(symbol: str = Query(...), period: str = "day", limit: int = 300,
                start: str = "", end: str = ""):
    if symbol.startswith("us"):
        if period not in DAY_PERIODS:
            return {"error": "美股暂不支持分钟K（5分/30分/60分），可用 分时 / 日K / 周K / 月K"}
        return await us_kline(symbol, period, limit, start, end)
    if period in DAY_PERIODS:
        cnt = 800 if (start or end) else limit + MA_WARM  # 筛选态多取防目标区间落在窗口外；平时多取供均线预热
        d = (await get_json(f"{BARE}/fqkline/get?param={symbol},{period},,,{cnt},qfq"))["data"][symbol]
        # 前复权键：A股为 qfqday/qfqweek/qfqmonth，港美股无复权用 day/week/month
        key = ("qfq" + period) if ("qfq" + period) in d else period
    elif period in MIN_PERIODS:
        d0 = (await get_json(f"{BARE}/kline/mkline?param={symbol},{period},,{limit + MA_WARM}")).get("data")  # 分钟K同样多取预热
        d = (d0.get(symbol) or {}) if isinstance(d0, dict) else {}
        if not d.get(period):
            return {"error": "该标的暂不支持分钟K（腾讯源分钟K仅A股/ETF可用）"}
        key = period
    else:
        return {"error": f"period 只支持: {sorted(DAY_PERIODS | MIN_PERIODS)}"}
    raw = d[key]
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
    }


# ---------- 回测：单票 + 3日线 + K线实体箱体 ----------
# 口径：docs/回测规则口径.md。与 orders/orders_live 完全隔离，只在内存里重演，不落库。
BT_PAR_TOL = 0.2  # 两日箱体「平行」阈值：顶对顶、底对底各自偏移不超过前日实体高度的 20%


def bt_box(r):
    # K线箱体 = 实体（开→收）的长方形，上下影线不计
    o, c = float(r[1]), float(r[2])
    return max(o, c), min(o, c), abs(c - o)


def bt_run(rows, ma3, capital, each):
    # 逐日重演。rows=[日期,开,收,高,低,量] 升序，ma3 与之等长（含预热，区间首根即有值）
    cash, qty, cost = float(capital), 0.0, 0.0
    trades, nav = [], []
    peak, mdd, win, closed, realized_all = float(capital), 0.0, 0, 0, 0.0
    for i, r in enumerate(rows):
        date, close = r[0][:10], float(r[2])
        top, bot, hgt = bt_box(r)
        m = ma3[i] if i < len(ma3) else None
        # 两日箱体平行（顶∩顶、底∩底都只挪了不到前日实体高度的 20%）→ 当日不买也不卖
        par = False
        pbot = ptop = None
        if i > 0:
            ptop, pbot, phgt = bt_box(rows[i - 1])
            tol = phgt * BT_PAR_TOL
            par = abs(top - ptop) <= tol and abs(bot - pbot) <= tol
        # 卖出：① 箱体跌破前一日箱体底 ② 箱体顶部没抬过前一日箱体顶（顶未抬高即不再上行）
        # ③ 实体一半高度掉进 3 日线（实体中点 < MA3）；①③ 为原有口径，②为追加的顶未抬高清仓
        sold = ""
        if qty > 0 and i > 0 and not par:
            if bot < pbot:
                sold = "broke"
            elif top <= ptop:
                sold = "ntop"
            elif m is not None and (top + bot) / 2 < m:
                sold = "ma3"
        if sold:
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
        # 买入：非平行日 + 3 日线在实体底部之下（整根实体站在均线上方）+ 实体底部高于前一日实体底部
        # （箱体在抬高，未抬高即不买）→ 每次买 each 元；卖出当日不再买；区间首根无前日，不施加抬底条件
        elif not par and m is not None and m < bot and (pbot is None or bot > pbot) and cash >= each:
            add = each / close
            trades.append({"date": date, "side": "buy", "price": round(close, 4),
                           "qty": round(add, 4), "amount": round(each, 2),
                           "realized": None, "reason": "ma3"})
            cash -= each
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
                   name: str = "", save: bool = True):
    if capital <= 0 or each <= 0:
        return {"error": "本金与单次买入金额需大于 0"}
    d = await kline(symbol, "day", 800, start, end)  # 复用日K取数口径（含均线预热与区间切片）
    if d.get("error"):
        return d
    rows = d.get("klines") or []
    if len(rows) < 3:
        return {"error": "区间内交易日不足（落在周末/停牌或超出行情源可回溯范围），请换个区间"}
    res = bt_run(rows, (d.get("ind") or {}).get("ma3") or [], capital, each)
    ind = d.get("ind") or {}
    # 回测只用 MA3 判信号，但图面要同时叠 MA3/MA5/MA85/MA125 四条均线供对照
    res.update({"symbol": symbol, "period": "day", "klines": rows,
                "ma3": ind.get("ma3") or [],
                "ind": {k: ind.get(k) or [] for k in ("ma3", "ma5", "ma85", "ma125")}})
    if save:  # 每次回测自动落一条记录（供「回测记录」翻阅），落库失败不影响本次结果
        st0 = res.get("stats") or {}
        snap = {"symbol": symbol, "name": name or symbol,
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
    # 同参数（标的+区间+本金+每次买入）重复回测覆盖上一条，避免记录列表被重复项刷满
    ts = datetime.datetime.now().strftime(TS_FMT)
    con = _conn()
    try:
        with con:
            old = con.execute(
                "SELECT id FROM backtests WHERE symbol=? AND start=? AND end=? AND capital=? AND each=? "
                "ORDER BY id DESC LIMIT 1",
                (snap["symbol"], snap["start"], snap["end"], snap["capital"], snap["each"])).fetchone()
            if old:
                con.execute("UPDATE backtests SET name=?, created=?, payload=? WHERE id=?",
                            (snap["name"], ts, payload, old["id"]))
                return old["id"], ts
            cur = con.execute(
                "INSERT INTO backtests (symbol, name, start, end, capital, each, created, payload) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (snap["symbol"], snap["name"], snap["start"], snap["end"],
                 snap["capital"], snap["each"], ts, payload))
            return cur.lastrowid, ts
    finally:
        con.close()


@app.get("/api/backtests")
async def bt_records(limit: int = 200):
    # 列表只返回摘要（payload 里的 K 线大 JSON 不返回），点开某条再取详情
    con = _conn()
    try:
        rows = con.execute("SELECT id, symbol, name, start, end, capital, each, created, payload "
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
        items.append({"id": r["id"], "symbol": r["symbol"], "name": r["name"],
                      "start": r["start"], "end": r["end"], "created": r["created"],
                      "capital": r["capital"], "each": r["each"], "stats": st})
    return {"items": items}


@app.get("/api/backtests/{rid}")
async def bt_record(rid: int):
    con = _conn()
    try:
        r = con.execute("SELECT id, symbol, name, start, end, capital, each, created, payload "
                        "FROM backtests WHERE id=?", (rid,)).fetchone()
    finally:
        con.close()
    if not r:
        return {"error": "记录不存在（可能已被删除）"}
    try:
        data = json.loads(r["payload"])
    except Exception:
        return {"error": "记录内容已损坏，删掉重新回测即可"}
    return {"id": r["id"], "symbol": r["symbol"], "name": r["name"], "start": r["start"],
            "end": r["end"], "capital": r["capital"], "each": r["each"],
            "created": r["created"], "data": data}


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
    d = (await get_json(f"{WEB}/minute/query?code={symbol}"))["data"][symbol]
    m = d["data"]  # {"data": ["0930 price vol amount", ...], "date": "..."}
    return {"symbol": symbol, "quote": d.get("qt", {}).get(symbol, []), "date": m["date"], "points": m["data"]}


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
    rows = con.execute(f"SELECT symbol, name, period FROM {_tbl(scope, 'cards')} ORDER BY pos").fetchall()
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
            rows.append((str(c["symbol"]), str(c.get("name") or ""), str(c.get("period") or "day")))
    con = _conn()
    with con:  # 一个事务内先清后插：数组顺序即 pos
        con.execute(f"DELETE FROM {_tbl(scope, 'cards')}")
        con.executemany(f"INSERT INTO {_tbl(scope, 'cards')} (pos, symbol, name, period) VALUES (?, ?, ?, ?)",
                        [(i, s, n, p) for i, (s, n, p) in enumerate(rows)])
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
# （docs/回测规则口径.md）：今日实体（开→现价）底部在 MA3 之上，且今日箱体与前日不平行。
# 盘中拿不到“今日收盘价”的终值，一律用实时报价现价近似，再用今开拼出今日实体；历史只取到
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
    if len(hist) < 2:
        return None
    px = q["price"]
    op = q["open"] or float(hist[-1][2])  # 今开缺失（集合竞价前）退化用昨收
    top, bot = max(op, px), min(op, px)
    ptop, pbot, phgt = bt_box(hist[-1])
    tol = phgt * BT_PAR_TOL
    if phgt > 0 and abs(top - ptop) <= tol and abs(bot - pbot) <= tol:
        return None  # 两日箱体平行：回测口径里不买不卖
    if bot <= pbot:
        return None  # 今日实体底未高于昨日实体底（箱体底部没抬高）：不买入，与回测同一口径
    ma3 = (float(hist[-2][2]) + float(hist[-1][2]) + px) / 3  # MA3 = 前两日收盘 + 今日现价
    if ma3 >= bot:
        return None
    return {"symbol": sym, "price": px, "ma3": round(ma3, 4), "bot": round(bot, 4)}


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
            _toast("买入信号 · 无命中", "当前没有自选票满足 MA3 箱体买入条件")
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

