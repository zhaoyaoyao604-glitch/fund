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


def _replay(con, exclude_id=None, scope="hist"):
    """按 (ts, id) 全量重演流水。返回 (pos, realized, conflict)：
    pos = symbol -> {shares, cost(移动加权均价), bought_today(当日买入的A股份额), buy_ts(建仓首笔买入时间), name}；
    realized = 卖出单id -> 该笔已实现收益 (卖出价-当时均价)*数量；
    conflict = 重演中第一笔卖超的记录（正常数据恒为 None，仅删除预检时出现）"""
    sql = f"SELECT id, symbol, name, side, price, qty, ts FROM {_tbl(scope, 'orders')}"
    args = ()
    if exclude_id is not None:
        sql += " WHERE id != ?"
        args = (exclude_id,)
    rows = con.execute(sql + " ORDER BY ts, id", args).fetchall()
    today = datetime.datetime.now().strftime("%Y-%m-%d")
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
    rows = con.execute(f"SELECT symbol, name, period FROM {_tbl(scope, 'cards')} ORDER BY pos").fetchall()
    con.close()
    return {"cards": [dict(r) for r in rows]}


@app.get("/api/holdings")
async def holdings_get(scope: str = "hist"):
    con = _conn()
    pos, _, _ = _replay(con, scope=scope)
    con.close()
    return {"holdings": [
        {"symbol": s, "name": p["name"], "shares": round(p["shares"], 6), "cost": round(p["cost"], 6),
         "sellable": round(p["shares"] if _is_t0(s) else p["shares"] - p["bought_today"], 6),
         "buy_ts": p["buy_ts"]}
        for s, p in pos.items() if p["shares"] > 1e-9
    ]}


@app.get("/api/orders")
async def orders_get(scope: str = "hist"):
    con = _conn()
    rows = con.execute(f"SELECT id, symbol, name, side, price, qty, ts FROM {_tbl(scope, 'orders')} ORDER BY ts, id").fetchall()
    _, realized, _ = _replay(con, scope=scope)
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
        p = _replay(con, scope=scope)[0].get(sym) or {"shares": 0.0, "bought_today": 0.0}
        avail = p["shares"] if _is_t0(sym) else p["shares"] - p["bought_today"]
        if qty > avail + 1e-9:
            con.close()
            tip = "" if _is_t0(sym) or p["bought_today"] <= 1e-9 else "（A股当日买入次日可卖）"
            raise HTTPException(400, f"卖出数量超过可卖，当前可卖 {avail:g}{tip}")
    with con:
        cur = con.execute(f"INSERT INTO {_tbl(scope, 'orders')} (symbol, name, side, price, qty, ts) VALUES (?, ?, ?, ?, ?, ?)",
                          (sym, name, side, price, qty, ts))
    oid = cur.lastrowid
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
