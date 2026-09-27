# 基金看盘后端：FastAPI -> 免费行情源（无 key）
# A股/港股/ETF：腾讯（日/周/月K、分钟K 走裸域 ifzq.gtimg.cn；分时走 web.ifzq.gtimg.cn）
# 美股：日/周/月K 走新浪全历史（不复权）+ 腾讯实时报价补当日活动K线；分时走东财 trends2（上游限流时返回提示）
# 注：web. 子域 fqkline 曾被腾讯WAF挑战（501 JS验证页），故日/周/月K统一走裸域
import datetime
import json
import sqlite3
import time
from pathlib import Path
from urllib.parse import quote

import httpx
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
# web.ifzq 子域会被腾讯WAF风控（501跳转页），裸域全通：fqkline/minute/mkline 一律走裸域
WEB = "https://ifzq.gtimg.cn/appstock/app"  # minute（分时）
BARE = "https://ifzq.gtimg.cn/appstock/app"  # fqkline（日/周/月K） / mkline（分钟K）
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


def _replay(con, exclude_id=None, scope="hist", as_of=None):
    """按 (ts, id) 全量重演流水。返回 (pos, realized, conflict)：
    pos = symbol -> {shares, cost(移动加权均价), bought_today(当日买入的A股份额), buy_ts(建仓首笔买入时间), name}；
    realized = 卖出单id -> 该笔已实现收益 (卖出价-当时均价)*数量；
    conflict = 重演中第一笔卖超的记录（正常数据恒为 None，仅删除预检时出现）。
    as_of（"YYYY-MM-DD HH:MM"）非空时只重演该时点前的流水——历史回放视点：
    回看某日时之后下的单不出现；T+1 的「当日买入」同样按 as_of 的日期判定，回放出的可卖数与当时一致"""
    sql = f"SELECT id, symbol, name, side, price, qty, ts FROM {_tbl(scope, 'orders')}"
    conds, args = [], []
    if as_of:
        conds.append("ts <= ?")
        args.append(as_of)
    if exclude_id is not None:
        conds.append("id != ?")
        args.append(exclude_id)
    if conds:
        sql += " WHERE " + " AND ".join(conds)
    rows = con.execute(sql + " ORDER BY ts, id", tuple(args)).fetchall()
    today = (as_of or datetime.datetime.now().strftime(TS_FMT))[:10]
    pos, realized, conflict = {}, {}, None
    for r in rows:
        p = pos.setdefault(r["symbol"], {"shares": 0.0, "cost": 0.0, "bought_today": 0.0, "buy_ts": None, "name": r["name"]})
        p["name"] = r["name"]
        if r["side"] == "buy":
            if p["shares"] <= 1e-9:  # 建仓（含清仓后重建）记该笔时间；加仓保持首笔买入时间
                p["buy_ts"] = r["ts"]
            total = p["cost"] * p["shares"] + r["price"] * r["qty"]
            p["shares"] += r["qty"]
            p["cost"] = total / p["shares"]
            if not _is_t0(r["symbol"]) and r["ts"][:10] == today:
                p["bought_today"] += r["qty"]  # A股当日买入份额锁定到明日（T+1）
        else:
            avail = p["shares"] if _is_t0(r["symbol"]) else p["shares"] - p["bought_today"]
            if r["qty"] > avail + 1e-9:
                conflict = dict(r)
                break
            realized[r["id"]] = (r["price"] - p["cost"]) * r["qty"]
            p["shares"] -= r["qty"]
            if p["shares"] <= 1e-9:  # 清仓：均价、当日买入与买入时间一并归零
                p["shares"] = p["cost"] = p["bought_today"] = 0.0
                p["buy_ts"] = None
    return pos, realized, conflict


def _promote_bought_cards(con, scope):
    """买入次日卡片前置：每天首次取卡时，把「游标日(含)到昨天」有买入动作的卡片整体提到列表最前并落库，
    多只命中按最后一笔买入动作时间降序（最新买的最前）；同一时刻录入的一批再按业务首笔买入时间升序、
    录入先后升序排——与抽屉持仓列表（重演建仓序）完全一致，抽屉排第一的持仓卡即主页面第一张。
    判定锚 created_at（真实下单时刻）而非业务 ts——hist 回放给历史日期的单，同样按实际操作的第二天前置；
    只前置当前仍持仓的（买入后已清仓的不再置顶）。游标存 promo 表按 scope 各记一行（首次运行只记今天、
    不追溯历史买入）；昨天没打开页面也无妨——下次打开时区间一并覆盖这段时间的新买入，晚开页同样补前置。"""
    today = datetime.datetime.now().strftime("%Y-%m-%d")
    row = con.execute("SELECT last_date FROM promo WHERE scope=?", (scope,)).fetchone()
    last = row["last_date"] if row else ""
    if not last:  # 首次：只建游标，历史买入不触发
        with con:
            con.execute("INSERT OR REPLACE INTO promo (scope, last_date) VALUES (?, ?)", (scope, today))
        return
    if last >= today:
        return  # 今天已处理，幂等跳过
    # 区间 [last, today)：定长时刻串与纯日期串的字典序比较即时间比较；存量单 created_at='' 天然不命中
    rows = con.execute(f"SELECT symbol, MAX(created_at) AS last_buy, MIN(ts) AS first_buy, MAX(id) AS last_id "
                       f"FROM {_tbl(scope, 'orders')} "
                       f"WHERE side='buy' AND created_at >= ? AND created_at < ? GROUP BY symbol "
                       f"ORDER BY last_buy DESC, first_buy ASC, last_id ASC",
                       (last, today)).fetchall()
    hit = []
    if rows:
        held = {s for s, p in _replay(con, scope=scope)[0].items() if p["shares"] > 1e-9}  # 仍持仓才前置
        hit = [r["symbol"] for r in rows if r["symbol"] in held]
    if hit:
        cards = [r["symbol"] for r in
                 con.execute(f"SELECT symbol FROM {_tbl(scope, 'cards')} ORDER BY pos").fetchall()]
        hs, cs = set(hit), set(cards)
        ordered = [s for s in hit if s in cs] + [c for c in cards if c not in hs]  # 不在自选卡的买入流水自然跳过
        with con:
            con.executemany(f"UPDATE {_tbl(scope, 'cards')} SET pos=? WHERE symbol=?", list(enumerate(ordered)))
    with con:
        con.execute("INSERT OR REPLACE INTO promo (scope, last_date) VALUES (?, ?)", (scope, today))


app = FastAPI()
app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"],
)


_CACHE = {}
TTL = 30  # 秒：同 URL 30 秒内走内存。防止刷新页面 37 卡并发连击上游触发腾讯 WAF 风控


async def get_json(url, referer=None, timeout=15):
    now = time.time()
    hit = _CACHE.get(url)
    if hit and now - hit[0] < TTL:
        return hit[1]
    headers = UA if not referer else {**UA, "Referer": referer}
    async with httpx.AsyncClient(timeout=timeout, headers=headers) as cli:
        r = await cli.get(url)
    try:
        data = json.loads(r.content.decode("utf-8"))  # 上游是 UTF-8，但 header 不标，得强解
    except json.JSONDecodeError:
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
async def cards_get(scope: str = "hist"):
    con = _conn()
    _promote_bought_cards(con, scope)  # 买入次日优先前置：每天首次取卡时重排落库（游标内幂等）
    rows = con.execute(f"SELECT symbol, name, period FROM {_tbl(scope, 'cards')} ORDER BY pos").fetchall()
    con.close()
    return {"cards": [dict(r) for r in rows]}


@app.get("/api/holdings")
async def holdings_get(scope: str = "hist", as_of: str = ""):
    # as_of：历史回放视点（YYYY-MM-DD 或 YYYY-MM-DD HH:MM）；只给日期按当天 23:59 含全天，
    # 用于回看某日的持仓——该日之后下的单不出现（如27号买的仓，26号视点里不存在；
    # 此截断天然保证「检索买入日之前的日期看不到该仓」），买入当天即显示，T+1 只锁卖出按钮
    a = _norm_asof(as_of)
    con = _conn()
    pos, _, _ = _replay(con, scope=scope, as_of=a)
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
                    "sellable": round(p["shares"] if _is_t0(s) else p["shares"] - p["bought_today"], 6),
                    "buy_ts": p["buy_ts"]})
    return {"holdings": out}


@app.get("/api/orders")
async def orders_get(scope: str = "hist", as_of: str = ""):
    # as_of：历史回放视点（同 /api/holdings）——订单列表与已实现收益都只含该时点前的流水：
    # 回看18号之前的视点看不到18号下的单（及之后卖出落的袋）；realized 按视点重演推导
    a = _norm_asof(as_of)
    con = _conn()
    sql = f"SELECT id, symbol, name, side, price, qty, ts FROM {_tbl(scope, 'orders')}"
    args = ()
    if a:
        sql += " WHERE ts <= ?"
        args = (a,)
    rows = con.execute(sql + " ORDER BY ts, id", args).fetchall()
    _, realized, _ = _replay(con, scope=scope, as_of=a)
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
    if side == "sell":
        # 卖出上限按「该单业务时点 ts」重演校验（hist 气泡 ts=回放结束日期、手填补录历史单同理）：
        # 不能按当前全量持仓算——回看视点时的持仓与现在不同，超视点持仓的卖出会插队卖超
        p = _replay(con, scope=scope, as_of=ts)[0].get(sym) or {"shares": 0.0, "bought_today": 0.0}
        avail = p["shares"] if _is_t0(sym) else p["shares"] - p["bought_today"]
        if qty > avail + 1e-9:
            con.close()
            tip = "" if _is_t0(sym) or p["bought_today"] <= 1e-9 else "（A股当日买入次日可卖）"
            raise HTTPException(400, f"卖出数量超过可卖，{ts[:10]} 时可卖 {avail:g}{tip}")
        old_conflict = _replay(con, scope=scope)[2]  # 存量卖超坏账（正常数据恒 None），供插入后对比
    with con:
        cur = con.execute(f"INSERT INTO {_tbl(scope, 'orders')} (symbol, name, side, price, qty, ts, created_at) "
                          f"VALUES (?, ?, ?, ?, ?, ?, ?)",
                          (sym, name, side, price, qty, ts, datetime.datetime.now().strftime(TS_FMT)))
    oid = cur.lastrowid
    if side == "sell":
        # 时序兜底：落在历史时点的卖出可能挤占其后已录卖出的份额（或自身卖超），插入后全量重演即现冲突——回滚拒单。
        # 与插入前存量冲突比对 id，坏账不误伤新单；买入只会缓解冲突，无需兜底
        conflict = _replay(con, scope=scope)[2]
        if conflict and conflict["id"] != (old_conflict or {"id": None})["id"]:
            with con:
                con.execute(f"DELETE FROM {_tbl(scope, 'orders')} WHERE id=?", (oid,))
            con.close()
            raise HTTPException(400, f"卖出将超过持仓：{conflict['name']} {conflict['ts']} 的卖出将超过当时可卖，请调小数量")
    realized = _replay(con, scope=scope)[1].get(oid)
    con.close()
    return {"ok": True, "order": {"id": oid, "symbol": sym, "name": name, "side": side,
                                  "price": price, "qty": qty, "ts": ts}, "realized": realized}


@app.delete("/api/orders/{oid}")
async def orders_delete(oid: int, scope: str = "hist"):
    con = _conn()
    if not con.execute(f"SELECT 1 FROM {_tbl(scope, 'orders')} WHERE id=?", (oid,)).fetchone():
        con.close()
        raise HTTPException(404, "记录不存在")
    conflict = _replay(con, exclude_id=oid, scope=scope)[2]
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
                    out[sym] = {"price": float(f[3]), "prev": float(f[4])}
                except ValueError:
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
    # 账户本金与可用资金：可用资金 = 本金 − 累计买入金额 + 累计卖出金额，由 orders 流水重演推导不落库（删单自动重算）
    # as_of：历史回放视点——可用资金同样只算该时点前的流水（18号买的单在17号视点里还没扣钱）
    a = _norm_asof(as_of)
    s = "live" if scope == "live" else "hist"
    con = _conn()
    row = con.execute("SELECT capital FROM account WHERE scope=?", (s,)).fetchone()
    sql = f"SELECT side, price, qty FROM {_tbl(scope, 'orders')}"
    args = ()
    if a:
        sql += " WHERE ts <= ?"
        args = (a,)
    rows = con.execute(sql, args).fetchall()
    con.close()
    capital = row["capital"] if row else 0.0
    cash = capital - sum(r["price"] * r["qty"] for r in rows if r["side"] == "buy") \
        + sum(r["price"] * r["qty"] for r in rows if r["side"] == "sell")
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
