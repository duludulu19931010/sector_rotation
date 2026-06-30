import argparse
import json
import logging
import sqlite3
import sys
import time
import urllib3
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from datetime import date, datetime, timedelta
from io import StringIO
from pathlib import Path

import pandas as pd
import requests

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ── 路徑 ──────────────────────────────────────────────────────────────
ROOT      = Path(__file__).resolve().parent
DB_FILE   = ROOT / "db" / "market.db"
DATA_DIR  = ROOT / "docs" / "assets" / "data"
INPUT_DIR = ROOT / "input"
GROUP_CSV = INPUT_DIR / "group.csv"
LOG_FILE  = ROOT / "pipeline.log"

TODAY   = date.today().strftime("%Y-%m-%d")
TODAY_8 = date.today().strftime("%Y%m%d")

TWSE_HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
                "Accept": "application/json", "Referer": "https://www.twse.com.tw/"}
TPEX_HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
                "Accept": "application/json", "Referer": "https://www.tpex.org.tw/"}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout),
              logging.FileHandler(str(LOG_FILE), encoding="utf-8")],
)
log = logging.getLogger("twflow")

# ── DB ────────────────────────────────────────────────────────────────
DDL = """
CREATE TABLE IF NOT EXISTS daily (
    date         TEXT NOT NULL,
    code         TEXT NOT NULL,
    market       TEXT NOT NULL DEFAULT '',
    name         TEXT NOT NULL DEFAULT '',
    close_price  REAL    NOT NULL DEFAULT 0,
    trade_volume INTEGER NOT NULL DEFAULT 0,
    trade_value  INTEGER NOT NULL DEFAULT 0,
    avg_price    REAL    NOT NULL DEFAULT 0,
    inst_net     INTEGER NOT NULL DEFAULT 0,
    inst_value   REAL    NOT NULL DEFAULT 0,
    net_yi       REAL    NOT NULL DEFAULT 0,
    PRIMARY KEY (date, code)
);
CREATE INDEX IF NOT EXISTS ix_daily_date ON daily(date);
CREATE INDEX IF NOT EXISTS ix_daily_code ON daily(code);
"""


@contextmanager
def _db():
    DB_FILE.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(str(DB_FILE))
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA synchronous=NORMAL")
    try:
        yield con
        con.commit()
    except Exception:
        con.rollback()
        raise
    finally:
        con.close()


def db_init():
    with _db() as c:
        c.executescript(DDL)
        existing = {r[1] for r in c.execute("PRAGMA table_info(daily)")}
        for col, sql in [
            ("avg_price",  "ALTER TABLE daily ADD COLUMN avg_price  REAL NOT NULL DEFAULT 0"),
            ("inst_value", "ALTER TABLE daily ADD COLUMN inst_value REAL NOT NULL DEFAULT 0"),
        ]:
            if col not in existing:
                c.execute(sql)
                log.info(f"DB migrated: added {col}")
    log.info(f"DB ready: {DB_FILE}")


def db_dates(n: int = 30) -> list[str]:
    with _db() as c:
        rows = c.execute(
            "SELECT date, COUNT(*) AS cnt FROM daily GROUP BY date ORDER BY date DESC"
        ).fetchall()
    if not rows:
        return []
    max_cnt = max(r["cnt"] for r in rows)
    thr = max(50, int(max_cnt * 0.5))
    return [r["date"] for r in rows if r["cnt"] >= thr][:n]


def db_load(days: int = 25) -> pd.DataFrame:
    dates = db_dates(days)
    if not dates:
        return pd.DataFrame()
    ph = ",".join("?" * len(dates))
    with _db() as c:
        df = pd.read_sql_query(
            f"SELECT date,code,market,name,close_price,"
            f"trade_volume,trade_value,avg_price,inst_net,inst_value,net_yi "
            f"FROM daily WHERE date IN ({ph}) ORDER BY date",
            c, params=dates
        )
    log.info(f"DB load: {len(df)} rows, {df['date'].nunique()} dates")
    return df


def db_save(df: pd.DataFrame) -> int:
    if df is None or df.empty:
        return 0
    rows = []
    for _, r in df.iterrows():
        vol  = _int(r.get("trade_volume", 0))
        val  = _int(r.get("trade_value",  0))
        inst = _int(r.get("inst_net",     0))
        avg  = _flt(r.get("avg_price")) or (val / vol if vol else 0.0)
        ival = _flt(r.get("inst_value")) or round(inst * avg / 1e8, 6)
        rows.append((
            r["date"], str(r["code"]).zfill(4), r.get("market", ""), r.get("name", ""),
            _flt(r.get("close_price")), vol, val, round(avg, 2), inst, ival, ival,
        ))
    with _db() as c:
        c.executemany("""
            INSERT OR REPLACE INTO daily
            (date,code,market,name,close_price,trade_volume,trade_value,avg_price,inst_net,inst_value,net_yi)
            VALUES (?,?,?,?,?,?,?,?,?,?,?)
        """, rows)
    from collections import Counter
    for d, n in sorted(Counter(r[0] for r in rows).items()):
        log.info(f"Saved {n} rows for {d}")
    return len(rows)


# ── 工具 ──────────────────────────────────────────────────────────────
def _flt(v) -> float:
    try:    return float(str(v).replace(",", "").strip())
    except: return 0.0


def _int(v) -> int:
    try:    return int(float(str(v).replace(",", "").strip()))
    except: return 0


def _get(url, params=None, headers=None, verify=True, retries=3, delay=2.0):
    h = headers or TWSE_HEADERS
    for i in range(retries):
        try:
            r = requests.get(url, params=params, headers=h, timeout=30, verify=verify)
            r.raise_for_status()
            if not r.text.strip():
                raise ValueError("Empty response")
            d = r.json()
            if isinstance(d, (dict, list)):
                return d
        except Exception as e:
            log.warning(f"GET [{i+1}/{retries}] {url} — {e}")
            if i < retries - 1:
                time.sleep(delay * (i + 1))
    return None


def _roc(s: str) -> str:
    """民國年 → 西元，支援 '1150612'(7碼) 和 '115/06/12'"""
    s = str(s).strip()
    if "/" in s:
        try:
            y, m, d = s.split("/")
            return f"{int(y)+1911}-{int(m):02d}-{int(d):02d}"
        except Exception:
            return ""
    if len(s) == 7 and s.isdigit():
        return f"{int(s[:3])+1911}-{s[3:5]}-{s[5:7]}"
    return ""


# ── TWSE 今日 ─────────────────────────────────────────────────────────
def fetch_twse_today() -> tuple[str, dict]:
    """openapi STOCK_DAY_ALL（只有最新交易日）→ (date, {code:{name,close,volume,value}})"""
    data = _get("https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL")
    if not isinstance(data, list) or len(data) < 100:
        raise RuntimeError(f"TWSE STOCK_DAY_ALL failed: {len(data) if data else 0} rows")
    trade_date = _roc(data[0].get("Date", "")) or TODAY
    result = {}
    for item in data:
        code = str(item.get("Code", "")).strip().zfill(4)
        if not code.strip("0"):
            continue
        result[code] = {
            "name":   item.get("Name", ""),
            "close":  _flt(item.get("ClosingPrice")),
            "volume": _int(item.get("TradeVolume")),
            "value":  _int(item.get("TradeValue")),
        }
    log.info(f"TWSE today: {len(result)} stocks, date={trade_date}")
    return trade_date, result


def fetch_t86(date8: str) -> dict[str, int]:
    """T86?date= → {code: inst_net_shares}（支援歷史）"""
    data = _get(
        "https://www.twse.com.tw/rwd/zh/fund/T86",
        params={"response": "json", "date": date8, "selectType": "ALL"},
    )
    if not isinstance(data, dict) or data.get("stat") != "OK":
        return {}
    result = {}
    for row in data.get("data", []):
        if len(row) < 2:
            continue
        result[str(row[0]).strip().zfill(4)] = _int(row[-1])
    log.info(f"T86 {date8}: {len(result)} entries")
    return result


# ── TWSE 歷史（個股逐月 STOCK_DAY）─────────────────────────────────────
def fetch_twse_stock_month(code: str, month8: str) -> dict[str, dict]:
    """
    STOCK_DAY?stockNo=&date=YYYYMM01 → {date: {close,volume,value}}
    fields: [日期, 成交股數(1), 成交金額(2), 開盤, 最高, 最低, 收盤價(6), ...]
    """
    data = _get(
        "https://www.twse.com.tw/exchangeReport/STOCK_DAY",
        params={"response": "json", "date": month8, "stockNo": code},
        retries=2, delay=1.0,
    )
    if not isinstance(data, dict) or data.get("stat") != "OK":
        return {}
    out = {}
    for row in data.get("data", []):
        if len(row) < 7:
            continue
        dd = _roc(row[0])
        if not dd:
            continue
        out[dd] = {
            "volume": _int(row[1]),
            "value":  _int(row[2]),
            "close":  _flt(row[6]),
        }
    return out


def _twse_hist_one(code: str, months: list[str]) -> tuple[str, dict]:
    merged = {}
    for m8 in months:
        merged.update(fetch_twse_stock_month(code, m8))
        time.sleep(0.3)
    return code, merged


def fetch_twse_history(codes: list[str], dates: list[str], workers: int = 6) -> pd.DataFrame:
    """
    逐股逐月補抓 TWSE 歷史成交（含真均價），再配 T86 三大法人
    codes: TWSE 代號清單；dates: 需要的交易日（決定要抓哪幾個月 + 配哪幾天 T86）
    """
    if not codes or not dates:
        return pd.DataFrame()

    months = sorted({d.replace("-", "")[:6] + "01" for d in dates})
    log.info(f"TWSE history: {len(codes)} codes × {len(months)} months (parallel={workers})")

    price_hist: dict[str, dict] = {}
    done = 0
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futs = {pool.submit(_twse_hist_one, c, months): c for c in codes}
        for fut in as_completed(futs):
            code = futs[fut]
            try:
                _, merged = fut.result()
                price_hist[code] = merged
            except Exception as e:
                log.warning(f"TWSE hist {code}: {e}")
                price_hist[code] = {}
            done += 1
            if done % 200 == 0:
                log.info(f"  TWSE history progress: {done}/{len(codes)}")

    # T86 逐日（只抓需要的交易日）
    t86_cache = {}
    for dd in sorted(set(dates)):
        t86_cache[dd] = fetch_t86(dd.replace("-", ""))
        time.sleep(0.3)

    want = set(dates)
    rows = []
    for code, day_data in price_hist.items():
        for dd, p in day_data.items():
            if dd not in want:
                continue
            avg  = p["value"] / p["volume"] if p["volume"] else 0.0
            inst = t86_cache.get(dd, {}).get(code, 0)
            ival = round(inst * avg / 1e8, 6)
            rows.append({
                "date": dd, "code": code, "market": "TWSE", "name": "",
                "close_price": p["close"], "trade_volume": p["volume"],
                "trade_value": p["value"], "avg_price": round(avg, 2),
                "inst_net": inst, "inst_value": ival, "net_yi": ival,
            })
    df = pd.DataFrame(rows)
    log.info(f"TWSE history: {len(df)} rows, {df['date'].nunique() if not df.empty else 0} dates")
    return df


# ── TPEx（新版 www API，支援歷史日期）─────────────────────────────────
def fetch_tpex_quotes(date_str: str = None) -> tuple[str, dict]:
    """
    afterTrading/dailyQuotes?date=YYYY/MM/DD → (date, {code:{name,close,volume,value}})
    data 欄位: [代號(0),名稱(1),收盤(2),...,成交股數(8),成交金額(9)]
    """
    d = date_str or date.today().strftime("%Y/%m/%d")
    data = _get(
        "https://www.tpex.org.tw/www/zh-tw/afterTrading/dailyQuotes",
        params={"date": d, "id": "", "response": "json"},
        headers=TPEX_HEADERS, verify=False,
    )
    if not isinstance(data, dict) or "tables" not in data:
        raise RuntimeError(f"TPEx quotes failed for {d}")
    tables = data.get("tables", [])
    if not tables or not tables[0].get("data"):
        log.warning(f"TPEx quotes empty for {d}")
        return "", {}
    raw = data.get("date", "")
    trade_date = f"{raw[:4]}-{raw[4:6]}-{raw[6:8]}" if len(raw) == 8 else TODAY
    result = {}
    for row in tables[0]["data"]:
        if len(row) < 10:
            continue
        code  = str(row[0]).strip().zfill(4)
        close = _flt(row[2])
        if not code.strip("0") or close <= 0:
            continue
        result[code] = {
            "name":   str(row[1]).strip(),
            "close":  close,
            "volume": _int(row[8]),
            "value":  _int(row[9]),
        }
    log.info(f"TPEx quotes {trade_date}: {len(result)} stocks")
    return trade_date, result


def fetch_tpex_inst(date_str: str = None) -> dict[str, int]:
    """
    insti/dailyTrade?date=YYYY/MM/DD → {code: inst_net}
    最後一欄(row[-1]) = 三大法人買賣超股數合計
    """
    d = date_str or date.today().strftime("%Y/%m/%d")
    data = _get(
        "https://www.tpex.org.tw/www/zh-tw/insti/dailyTrade",
        params={"type": "Daily", "sect": "AL", "date": d, "id": "", "response": "json"},
        headers=TPEX_HEADERS, verify=False,
    )
    if not isinstance(data, dict) or "tables" not in data:
        raise RuntimeError(f"TPEx inst failed for {d}")
    tables = data.get("tables", [])
    if not tables or not tables[0].get("data"):
        log.warning(f"TPEx inst empty for {d}")
        return {}
    result = {}
    for row in tables[0]["data"]:
        if len(row) < 3:
            continue
        code = str(row[0]).strip().zfill(4)
        if not code.strip("0"):
            continue
        result[code] = _int(row[-1])
    log.info(f"TPEx inst: {len(result)} stocks")
    return result


def fetch_tpex_history(dates: list[str]) -> pd.DataFrame:
    """逐日補抓 TPEx 歷史（行情 + 三大法人）"""
    if not dates:
        return pd.DataFrame()
    log.info(f"TPEx history: {len(dates)} dates")
    rows = []
    for i, dd in enumerate(sorted(dates)):
        d_slash  = dd.replace("-", "/")
        _, price = fetch_tpex_quotes(d_slash)
        inst     = fetch_tpex_inst(d_slash)
        if not price:
            time.sleep(0.8)
            continue
        for code, p in price.items():
            avg  = p["value"] / p["volume"] if p["volume"] else 0.0
            net  = inst.get(code, 0)
            ival = round(net * avg / 1e8, 6)
            rows.append({
                "date": dd, "code": code, "market": "TPEx", "name": p["name"],
                "close_price": p["close"], "trade_volume": p["volume"],
                "trade_value": p["value"], "avg_price": round(avg, 2),
                "inst_net": net, "inst_value": ival, "net_yi": ival,
            })
        if i < len(dates) - 1:
            time.sleep(0.5)
    df = pd.DataFrame(rows)
    log.info(f"TPEx history: {len(df)} rows, {df['date'].nunique() if not df.empty else 0} dates")
    return df


# ── 今日抓取 ──────────────────────────────────────────────────────────
def fetch_today() -> pd.DataFrame:
    with ThreadPoolExecutor(max_workers=3) as pool:
        f_tw = pool.submit(fetch_twse_today)
        f_tp = pool.submit(fetch_tpex_quotes)
        f_ti = pool.submit(fetch_tpex_inst)
        twse_date, r_tw = f_tw.result()
        tpex_date, r_tp = f_tp.result()
        r_ti = f_ti.result()

    # T86 先試今日，有則以今日為 TWSE 日期
    t86_today = fetch_t86(TODAY_8)
    if t86_today:
        twse_date = TODAY
        r_ti_tw   = t86_today
        log.info(f"T86 {TODAY}: {len(t86_today)} entries (today available)")
    else:
        r_ti_tw = fetch_t86(twse_date.replace("-", ""))
        log.info(f"T86 {twse_date}: STOCK_DAY_ALL date (T86 not yet available)")

    tpex_date = tpex_date or TODAY

    rows = []
    for code, p in r_tw.items():
        avg  = p["value"] / p["volume"] if p["volume"] else 0.0
        inst = r_ti_tw.get(code, 0)
        ival = round(inst * avg / 1e8, 6)
        rows.append({"date": twse_date, "code": code, "market": "TWSE", "name": p["name"],
                     "close_price": p["close"], "trade_volume": p["volume"],
                     "trade_value": p["value"], "avg_price": round(avg, 2),
                     "inst_net": inst, "inst_value": ival, "net_yi": ival})
    for code, p in r_tp.items():
        avg  = p["value"] / p["volume"] if p["volume"] else 0.0
        inst = r_ti.get(code, 0)
        ival = round(inst * avg / 1e8, 6)
        rows.append({"date": tpex_date, "code": code, "market": "TPEx", "name": p["name"],
                     "close_price": p["close"], "trade_volume": p["volume"],
                     "trade_value": p["value"], "avg_price": round(avg, 2),
                     "inst_net": inst, "inst_value": ival, "net_yi": ival})

    df = pd.DataFrame(rows)
    tw_n = sum(1 for r in rows if r["market"] == "TWSE")
    tp_n = sum(1 for r in rows if r["market"] == "TPEx")
    log.info(f"Today: {len(df)} stocks (TWSE={tw_n}[{twse_date}], TPEx={tp_n}[{tpex_date}])")
    return df


# ── yfinance 收盤價 ────────────────────────────────────────────────────
def fetch_close_yfinance(codes_market: dict[str, str], days: int = 35) -> pd.DataFrame:
    import yfinance as yf

    def supported(code):
        if not code.isdigit():
            return False
        if len(code) == 4:
            return True
        if len(code) == 6 and not code.startswith("7"):
            return True
        return False

    tickers = {}
    for code, market in codes_market.items():
        if not supported(code):
            continue
        suffix = ".TW" if market == "TWSE" else ".TWO"
        tickers[f"{code}{suffix}"] = code

    skipped = len(codes_market) - len(tickers)
    if skipped:
        log.info(f"yfinance: skipped {skipped} unsupported codes")
    if not tickers:
        return pd.DataFrame()

    tl      = list(tickers.keys())
    bs      = 200
    batches = [tl[i:i+bs] for i in range(0, len(tl), bs)]
    log.info(f"yfinance: {len(tickers)} tickers → {len(batches)} batches ({days}d)")

    all_rows = []
    for i, batch in enumerate(batches):
        try:
            raw = yf.download(batch, period=f"{days}d", auto_adjust=True, progress=False, threads=True)
            if raw.empty:
                continue
            close = raw["Close"] if "Close" in raw.columns else raw.get("Adj Close", pd.DataFrame())
            if isinstance(close, pd.Series):
                close = close.to_frame(name=batch[0])
            for ticker in batch:
                if ticker not in close.columns:
                    continue
                code = tickers[ticker]
                for dt, price in close[ticker].dropna().items():
                    if price > 0:
                        all_rows.append({"date": str(dt)[:10], "code": code, "close_price": float(price)})
            log.info(f"yfinance batch {i+1}/{len(batches)}: done")
        except Exception as e:
            log.warning(f"yfinance batch {i+1}/{len(batches)} failed: {e}")
        if i < len(batches) - 1:
            time.sleep(3)

    df = pd.DataFrame(all_rows)
    if not df.empty:
        log.info(f"yfinance: {len(df)} records, {df['date'].nunique()} dates, "
                 f"{df['code'].nunique()} codes, range={df['date'].min()} ~ {df['date'].max()}")
    return df


# ── 族群清單 ──────────────────────────────────────────────────────────
def load_groups() -> tuple[dict[str, list[str]], dict[str, str]]:
    text = open(GROUP_CSV, "rb").read().decode("cp950", errors="replace")
    df   = pd.read_csv(StringIO(text), header=None)
    groups: dict[str, list[str]] = {}
    names:  dict[str, str]       = {}
    for col in range(0, df.shape[1], 2):
        if col + 1 >= df.shape[1]:
            break
        gname = str(df.iloc[0, col]).strip()
        if not gname or gname == "nan":
            continue
        for row in range(2, len(df)):
            code = str(df.iloc[row, col]).strip()
            name = str(df.iloc[row, col + 1]).strip()
            if not code or code == "nan":
                continue
            c4 = code.zfill(4)
            groups.setdefault(gname, []).append(c4)
            if name and name != "nan":
                names[c4] = name
    log.info(f"Groups: {len(groups)}, {sum(len(v) for v in groups.values())} stocks, names: {len(names)}")
    return groups, names


# ── 計算 ──────────────────────────────────────────────────────────────
def compute(hist_df, yf_close_df, groups, name_map):
    if hist_df.empty:
        return [], {}, {}

    df = hist_df.copy()
    df["code"] = df["code"].astype(str).str.zfill(4)
    df["date"] = df["date"].astype(str)
    all_dates  = sorted(df["date"].unique())
    log.info(f"DB dates: {all_dates[0]} ~ {all_dates[-1]} ({len(all_dates)} days)")

    if not yf_close_df.empty:
        yf = yf_close_df.copy()
        yf["code"] = yf["code"].astype(str).str.zfill(4)
        close_pv    = yf.pivot_table(index="code", columns="date", values="close_price", aggfunc="first")
        close_dates = sorted(close_pv.columns)
        log.info(f"yfinance dates: {close_dates[0]} ~ {close_dates[-1]} ({len(close_dates)} days)")
    else:
        close_pv    = df.pivot_table(index="code", columns="date", values="close_price", aggfunc="first")
        close_dates = sorted(close_pv.columns)
        log.warning("No yfinance data, using DB close_price")

    latest    = close_dates[-1]
    close_now = close_pv[latest].dropna()

    d1  = close_dates[-2]  if len(close_dates) >= 2  else None
    d2  = close_dates[-3]  if len(close_dates) >= 3  else None
    d3  = close_dates[-4]  if len(close_dates) >= 4  else None
    d6  = close_dates[-6]  if len(close_dates) >= 6  else None
    d21 = close_dates[-21] if len(close_dates) >= 21 else None
    log.info(f"latest={latest}, chg_1d base={d1}, chg_5d base={d6}, chg_20d base={d21}")

    def pct(base):
        if not base or base not in close_pv.columns:
            return pd.Series(0.0, index=close_now.index)
        b     = close_pv[base]
        c     = close_now.reindex(b.index)
        valid = b.notna() & (b > 0.01) & c.notna() & (c > 0.01)
        s     = pd.Series(0.0, index=b.index)
        s[valid] = ((c[valid] - b[valid]) / b[valid] * 100).round(2)
        return s

    chg_1d  = pct(d1)
    chg_p1  = pct(d2)
    chg_p2  = pct(d3)
    chg_5d  = pct(d6)
    chg_20d = pct(d21)

    net_pv   = df.pivot_table(index="code", columns="date", values="net_yi", aggfunc="first")
    db_last  = all_dates[-1]
    db_prev  = all_dates[-2] if len(all_dates) >= 2 else None
    last5    = all_dates[-5:]
    last20   = all_dates[-20:]
    net_1d   = net_pv[db_last].fillna(0.0) if db_last in net_pv.columns else pd.Series(0.0, index=net_pv.index)
    net_prev = net_pv[db_prev].fillna(0.0) if db_prev and db_prev in net_pv.columns else pd.Series(0.0, index=net_pv.index)
    net_5d   = net_pv[[c for c in last5  if c in net_pv.columns]].sum(axis=1).fillna(0.0)
    net_20d  = net_pv[[c for c in last20 if c in net_pv.columns]].sum(axis=1).fillna(0.0)
    log.info(f"net_5d: min={round(float(net_5d.min()),2)}, max={round(float(net_5d.max()),2)}, nonzero={int((net_5d != 0).sum())}")

    net_days = {f"net_d{i+1}": net_pv[d].fillna(0.0) if d in net_pv.columns else pd.Series(0.0, index=net_pv.index)
                for i, d in enumerate(reversed(last5))}

    chg_bases = [close_dates[-(i+2)] for i in range(5)] if len(close_dates) >= 6 else []
    chg_days = {}
    for i, base in enumerate(chg_bases):
        key    = f"chg_d{i+1}"
        target = close_dates[-(i+1)]
        if base in close_pv.columns and target in close_pv.columns:
            b = close_pv[base]; c_ = close_pv[target]
            chg_days[key] = ((c_ - b) / b.where(b > 0.01) * 100).fillna(0.0)
        else:
            chg_days[key] = pd.Series(0.0, index=net_pv.index)

    def safe(v):
        import math
        f = float(v) if v is not None else 0.0
        return 0.0 if (math.isnan(f) or math.isinf(f)) else f

    db_names = df[df["date"] == db_last].set_index("code")["name"].to_dict()
    get_name = lambda c: name_map.get(c) or db_names.get(c, "")

    records, details, all_map = [], {}, {}

    for gname, raw_codes in groups.items():
        codes = [c.zfill(4) for c in raw_codes if c.zfill(4) in close_now.index]
        stocks = []
        for c in codes:
            c1  = safe(chg_1d.get(c,  0)); c1  = 0.0 if abs(c1)  > 11  else c1
            cp1 = safe(chg_p1.get(c,  0)); cp1 = 0.0 if abs(cp1) > 11  else cp1
            cp2 = safe(chg_p2.get(c,  0)); cp2 = 0.0 if abs(cp2) > 11  else cp2
            c5  = safe(chg_5d.get(c,  0)); c5  = 0.0 if abs(c5)  > 60  else c5
            c20 = safe(chg_20d.get(c, 0)); c20 = 0.0 if abs(c20) > 200 else c20
            nd  = {k: round(safe(v.get(c, 0)), 4) for k, v in net_days.items()}
            cd  = {k: round(safe(v.get(c, 0)), 2) for k, v in chg_days.items()}
            s = {"code": c, "name": get_name(c), "close": round(float(close_now.get(c, 0)), 2),
                 "net_1d": round(safe(net_1d.get(c, 0)), 4), "net_prev": round(safe(net_prev.get(c, 0)), 4),
                 "net_5d": round(safe(net_5d.get(c, 0)), 4), "net_20d": round(safe(net_20d.get(c, 0)), 4),
                 "chg_1d": round(c1, 2), "chg_prev1": round(cp1, 2), "chg_prev2": round(cp2, 2),
                 "chg_5d": round(c5, 2), "chg_20d": round(c20, 2), **nd, **cd}
            stocks.append(s)
            if c not in all_map:
                all_map[c] = s
        stocks.sort(key=lambda x: x["net_1d"], reverse=True)
        details[gname] = stocks

        if not codes:
            records.append({"g": gname, "cnt": len(raw_codes), "matched": 0,
                            "net_1d": 0.0, "net_prev": 0.0, "net_5d": 0.0, "net_20d": 0.0,
                            "chg_1d": 0.0, "chg_5d": 0.0, "chg_20d": 0.0, "label": "觀望"})
            continue

        g1     = round(sum(s["net_1d"]   for s in stocks), 3)
        g_prev = round(sum(s["net_prev"] for s in stocks), 3)
        g5     = round(sum(s["net_5d"]   for s in stocks), 3)
        g20    = round(sum(s["net_20d"]  for s in stocks), 3)

        def gavg(key):
            vals = [s[key] for s in stocks if s[key] != 0.0]
            return round(sum(vals) / len(vals), 2) if vals else 0.0

        gc1  = gavg("chg_1d");    gc1  = 0.0 if abs(gc1)  > 11  else gc1
        gcp1 = gavg("chg_prev1"); gcp1 = 0.0 if abs(gcp1) > 11  else gcp1
        gcp2 = gavg("chg_prev2"); gcp2 = 0.0 if abs(gcp2) > 11  else gcp2
        gc5  = gavg("chg_5d");    gc5  = 0.0 if abs(gc5)  > 60  else gc5
        gc20 = gavg("chg_20d");   gc20 = 0.0 if abs(gc20) > 200 else gc20
        if abs(g1)  > 1000:  g1  = 0.0
        if abs(g5)  > 5000:  g5  = 0.0
        if abs(g20) > 20000: g20 = 0.0

        g5_avg   = g5 / 5 if g5 else 0.0
        flow_pos = g1 > 0 and g_prev > 0 and g1 > g5_avg and g_prev > g5_avg
        flow_neg = g1 < 0 and g_prev < 0 and g1 < g5_avg and g_prev < g5_avg
        all3_pos = gc1 > 0 and gcp1 > 0 and gcp2 > 0

        if   flow_pos and all3_pos and gc5 > 0:       label = "主力"
        elif flow_pos and (not all3_pos or gc5 <= 0): label = "輪動"
        elif flow_neg:                                 label = "退潮"
        else:                                          label = "觀望"

        records.append({"g": gname, "cnt": len(raw_codes), "matched": len(codes),
                        "net_1d": g1, "net_prev": g_prev, "net_5d": g5, "net_20d": g20,
                        "chg_1d": gc1, "chg_5d": gc5, "chg_20d": gc20, "label": label})

    lc = {l: sum(1 for r in records if r["label"] == l) for l in ["主力","輪動","退潮","觀望"]}
    log.info(f"Groups: {len(records)} | " + " | ".join(f"{l}={n}" for l, n in lc.items()))
    return records, details, all_map


# ── 輸出 ──────────────────────────────────────────────────────────────
def _sanitize(obj):
    import math
    if isinstance(obj, float):
        return 0.0 if (math.isnan(obj) or math.isinf(obj)) else obj
    if isinstance(obj, dict):  return {k: _sanitize(v) for k, v in obj.items()}
    if isinstance(obj, list):  return [_sanitize(v) for v in obj]
    return obj


def _jdump(fname, data):
    with open(DATA_DIR / fname, "w", encoding="utf-8") as f:
        json.dump(_sanitize(data), f, ensure_ascii=False, separators=(",", ":"))


def export_json(records, details, all_stocks, trade_date):
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    attach = lambda lst: [{**r, "stocks": details.get(r["g"], [])} for r in lst]

    bubble = [{**r, "x": r["net_5d"], "y": r["net_1d"],
               "size": max(10, min(72, abs(r["net_5d"]) * 2.8 + 12)),
               "stocks": details.get(r["g"], [])} for r in records]

    def fp(r):
        avg = r["net_5d"] / 5 if r["net_5d"] else 0.0
        return r["net_1d"] > 0 and r["net_prev"] > 0 and r["net_1d"] > avg and r["net_prev"] > avg

    def p3le(r, t):
        return r["chg_1d"] <= t or r.get("chg_prev1", 0) <= t or r.get("chg_prev2", 0) <= t

    inflow = attach(sorted([r for r in records if fp(r) and p3le(r, 5.0) and r["chg_5d"] <= 10.0],
                           key=lambda x: x["net_5d"], reverse=True))
    stealth = attach(sorted([r for r in records if r["net_1d"] > 0 and r["net_prev"] > 0
                             and r["net_5d"] < 0 and p3le(r, 5.0) and r["chg_5d"] <= 10.0],
                            key=lambda x: x["net_5d"]))

    _jdump("bubble_data.json",          bubble)
    _jdump("inflow_low_gain.json",      inflow)
    _jdump("stealth_accumulation.json", stealth)
    _jdump("group_stats.json",          [{k: v for k, v in r.items() if k != "stocks"} for r in records])
    _jdump("stock_screener.json",       list(all_stocks.values()))
    _jdump("metadata.json", {
        "last_updated":    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "trade_date":      trade_date, "groups": len(records),
        "inflow":          len(inflow), "stealth": len(stealth),
        "screener_stocks": len(all_stocks),
    })
    log.info(f"JSON: bubble={len(bubble)}, inflow={len(inflow)}, stealth={len(stealth)}, screener={len(all_stocks)}")


def export_csv():
    with _db() as c:
        df = pd.read_sql_query(
            """SELECT date AS 日期, code AS 代號, market AS 市場, name AS 名稱,
                      close_price AS 收盤價, trade_volume AS 成交總股數,
                      trade_value AS 成交總金額, avg_price AS 成交均價,
                      inst_net AS 三大法人買賣超股數, inst_value AS 三大法人買賣超金額億
               FROM daily ORDER BY date, market, code""", c)
    if df.empty:
        return
    out = DATA_DIR / "market_data.csv"
    df.to_csv(out, index=False, encoding="utf-8-sig")
    log.info(f"CSV: {out} ({len(df):,} rows, {df['日期'].nunique()} dates, {df['代號'].nunique()} codes)")


# ── 主程式 ────────────────────────────────────────────────────────────
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--force",          action="store_true")
    ap.add_argument("--dry-run",        action="store_true")
    ap.add_argument("--reset-history",  action="store_true")
    ap.add_argument("--purge-bad-data", action="store_true")
    args = ap.parse_args()

    log.info("=" * 60)
    log.info(f"TW$FLOW  {datetime.now()}  system_date={TODAY}")
    if args.force:          log.info("*** FORCE ***")
    if args.dry_run:        log.info("*** DRY-RUN ***")
    if args.reset_history:  log.info("*** RESET-HISTORY ***")
    if args.purge_bad_data: log.info("*** PURGE-BAD-DATA ***")
    log.info("=" * 60)

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    db_init()
    groups, name_map = load_groups()

    if args.purge_bad_data and not args.dry_run:
        with _db() as c:
            n = c.execute("SELECT COUNT(*) FROM daily WHERE trade_value=0").fetchone()[0]
            if n:
                c.execute("DELETE FROM daily WHERE trade_value=0")
                log.info(f"Purge: deleted {n} bad rows")

    if not args.dry_run:
        # 今日資料
        today_df = fetch_today()
        if today_df.empty:
            log.error("Today fetch empty, aborting")
            return
        for market in ["TWSE", "TPEx"]:
            mdf = today_df[today_df["market"] == market]
            if mdf.empty:
                continue
            mdate = mdf["date"].iloc[0]
            with _db() as c:
                exists = c.execute(
                    "SELECT COUNT(*) FROM daily WHERE date=? AND market=? AND inst_net!=0",
                    (mdate, market)).fetchone()[0]
            if not args.force and exists > 0:
                log.info(f"[CACHE] {market} {mdate} already in DB")
            else:
                db_save(mdf)

        # 歷史補抓（用 yfinance 交易日曆）
        with _db() as c:
            twse_done = {r[0] for r in c.execute("SELECT DISTINCT date FROM daily WHERE market='TWSE'").fetchall()}
            tpex_done = {r[0] for r in c.execute("SELECT DISTINCT date FROM daily WHERE market='TPEx'").fetchall()}

        if len(twse_done) < 21 or len(tpex_done) < 21 or args.reset_history:
            try:
                import yfinance as yf
                cal = yf.download("2330.TW", period="35d", auto_adjust=True, progress=False)
                yf_dates = {str(d)[:10] for d in cal.index} if not cal.empty else set()
            except Exception as e:
                log.warning(f"yfinance calendar: {e}")
                yf_dates = set()
            recent = sorted(yf_dates)[-25:]

            # TWSE 歷史（個股逐月）
            twse_missing = sorted(set(recent) - twse_done)
            if twse_missing:
                today_db = db_load(days=1)
                twse_codes = sorted(today_db[today_db["market"] == "TWSE"]["code"].astype(str).str.zfill(4).unique()) \
                             if not today_db.empty else []
                log.info(f"TWSE missing {len(twse_missing)} dates, {len(twse_codes)} codes")
                if twse_codes:
                    hdf = fetch_twse_history(twse_codes, twse_missing)
                    if not hdf.empty:
                        db_save(hdf)

            # TPEx 歷史（每日全市場）
            tpex_missing = sorted(set(recent) - tpex_done)
            if tpex_missing:
                log.info(f"TPEx missing {len(tpex_missing)} dates")
                tdf = fetch_tpex_history(tpex_missing)
                if not tdf.empty:
                    db_save(tdf)

    hist = db_load(days=25)
    if hist.empty:
        log.error("No data in DB")
        return

    codes_market = dict(zip(hist["code"].astype(str).str.zfill(4), hist["market"]))
    yf_close = fetch_close_yfinance(codes_market)

    records, details, all_stocks = compute(hist, yf_close, groups, name_map)
    export_json(records, details, all_stocks, hist["date"].max())
    export_csv()

    with _db() as c:
        total  = c.execute("SELECT COUNT(*) FROM daily").fetchone()[0]
        latest = c.execute("SELECT MAX(date) FROM daily").fetchone()[0]
    log.info(f"DB: {total} rows, latest={latest}")
    log.info("Pipeline complete")


if __name__ == "__main__":
    main()
