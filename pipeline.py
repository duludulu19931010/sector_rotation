#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
TW$FLOW pipeline.py
====================
每日從 TWSE / TPEx 官方 API 抓取資料，計算族群資金流向，輸出 JSON 供前端使用。

資料來源（全部官方免費 API）：
  三大法人  https://www.twse.com.tw/rwd/zh/fund/T86
  上市收盤  https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL
  上櫃收盤  https://www.tpex.org.tw/openapi/v1/tpex/exchangeReport/daily_close_quotes

用法：
  python pipeline.py              每日正常執行
  python pipeline.py --force      忽略快取，強制重新抓取
  python pipeline.py --skip-etf   跳過 ETF（ETF API 白天不穩定）
"""

import argparse
import json
import logging
import sqlite3
import sys
import time
import urllib3
from contextlib import contextmanager
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
import numpy as np
import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ─────────────────────────────────────────────────────────
#  設定
# ─────────────────────────────────────────────────────────
ROOT        = Path(__file__).resolve().parent
DB_FILE     = ROOT / "db" / "market.db"
DATA_DIR    = ROOT / "docs" / "assets" / "data"
GROUP_CSV   = ROOT / "input" / "Group.csv"
STOCKS_CSV  = ROOT / "input" / "stock_list.csv"
LOG_FILE    = ROOT / "pipeline.log"
TODAY       = date.today().strftime("%Y-%m-%d")
TODAY_8     = date.today().strftime("%Y%m%d")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(str(LOG_FILE), encoding="utf-8"),
    ],
)
log = logging.getLogger("twflow")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept":     "application/json, text/plain, */*",
    "Referer":    "https://www.twse.com.tw/",
}


# ─────────────────────────────────────────────────────────
#  HTTP 工具
# ─────────────────────────────────────────────────────────
def _get(url: str, params: dict = None, verify: bool = True,
         retries: int = 3, delay: float = 2.0):
    """HTTP GET，回傳 dict/list 或 None。空 body 視為失敗。"""
    for attempt in range(retries):
        try:
            r = requests.get(url, params=params, headers=HEADERS,
                             timeout=30, verify=verify)
            r.raise_for_status()
            if not r.text.strip():
                raise ValueError("Empty response body")
            data = r.json()
            if isinstance(data, (dict, list)):
                return data
            return None
        except Exception as e:
            log.warning(f"  GET [{attempt+1}/{retries}] {url} — {e}")
            if attempt < retries - 1:
                time.sleep(delay * (attempt + 1))
    return None


# ─────────────────────────────────────────────────────────
#  資料庫
# ─────────────────────────────────────────────────────────
DDL = """
CREATE TABLE IF NOT EXISTS inst (
    date  TEXT NOT NULL,
    code  TEXT NOT NULL,
    name  TEXT DEFAULT '',
    f_net INTEGER DEFAULT 0,
    t_net INTEGER DEFAULT 0,
    d_net INTEGER DEFAULT 0,
    total INTEGER DEFAULT 0,
    PRIMARY KEY (date, code)
);
CREATE TABLE IF NOT EXISTS price (
    date   TEXT NOT NULL,
    code   TEXT NOT NULL,
    name   TEXT DEFAULT '',
    market TEXT DEFAULT '',
    close  REAL DEFAULT 0,
    open   REAL DEFAULT 0,
    high   REAL DEFAULT 0,
    low    REAL DEFAULT 0,
    vol    INTEGER DEFAULT 0,
    PRIMARY KEY (date, code)
);
CREATE INDEX IF NOT EXISTS ix_inst_date ON inst(date);
CREATE INDEX IF NOT EXISTS ix_price_date ON price(date);
"""


@contextmanager
def db_conn():
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
    with db_conn() as c:
        c.executescript(DDL)
    log.info(f"DB ready: {DB_FILE}")


def db_inst_dates(n: int = 25) -> list[str]:
    """回傳 inst 表中有完整資料的日期（自適應閾值：最大筆數的 50%）"""
    with db_conn() as c:
        rows = c.execute(
            "SELECT date, COUNT(*) AS cnt FROM inst GROUP BY date ORDER BY date DESC"
        ).fetchall()
    if not rows:
        return []
    max_cnt = max(r["cnt"] for r in rows)
    thr = max(50, max_cnt * 0.5)   # 最低 50，不寫死 500
    return [r["date"] for r in rows if r["cnt"] >= thr][:n]


def db_price_dates() -> list[str]:
    with db_conn() as c:
        rows = c.execute(
            "SELECT date, COUNT(*) AS cnt FROM price GROUP BY date ORDER BY date DESC LIMIT 10"
        ).fetchall()
    if not rows:
        return []
    max_cnt = max(r["cnt"] for r in rows)
    thr = max(50, max_cnt * 0.5)   # 最低 50
    return [r["date"] for r in rows if r["cnt"] >= thr]


def db_has_inst(d: str) -> bool:
    dates = db_inst_dates(1)
    return bool(dates) and dates[0] == _fmt(d)


def db_has_price(d: str) -> bool:
    dates = db_price_dates()
    return _fmt(d) in dates[:1]


def db_load_inst(days: int = 20) -> pd.DataFrame:
    dates = db_inst_dates(days)
    if not dates:
        return pd.DataFrame()
    ph = ",".join("?" * len(dates))
    with db_conn() as c:
        df = pd.read_sql_query(
            f"SELECT date,code,name,f_net,t_net,d_net,total FROM inst WHERE date IN ({ph})",
            c, params=dates
        )
    log.info(f"  Loaded inst: {len(df)} rows, {df['date'].nunique()} dates")
    return df


def db_load_price(d: str = None) -> pd.DataFrame:
    with db_conn() as c:
        if d:
            df = pd.read_sql_query(
                "SELECT date,code,name,market,close,open,high,low,vol FROM price WHERE date=?",
                c, params=(_fmt(d),)
            )
        else:
            df = pd.read_sql_query(
                "SELECT date,code,name,market,close,open,high,low,vol FROM price "
                "WHERE date=(SELECT MAX(date) FROM price)", c
            )
    log.info(f"  Loaded price: {len(df)} rows")
    return df


def db_load_price_history(days: int = 7) -> pd.DataFrame:
    """
    讀取近 days 個有資料的交易日的收盤價（用於計算漲跌幅）
    只取 date, code, close 三欄，節省記憶體
    """
    dates = db_price_dates()
    if not dates:
        return pd.DataFrame()
    recent = dates[:days]   # db_price_dates 已按日期倒序排列
    ph = ",".join("?" * len(recent))
    with db_conn() as c:
        df = pd.read_sql_query(
            f"SELECT date, code, close FROM price WHERE date IN ({ph}) ORDER BY date",
            c, params=recent
        )
    if not df.empty:
        df["code"] = df["code"].astype(str).str.zfill(4)
        log.info(f"  Loaded price history: {len(df)} rows, dates={sorted(df['date'].unique())}")
    return df


def db_save_inst(df: pd.DataFrame) -> int:
    if df is None or df.empty:
        return 0
    rows = [(
        _fmt(r["date"]), str(r["code"]).zfill(4),
        str(r.get("name","") or ""),
        _int(r.get("f_net")), _int(r.get("t_net")),
        _int(r.get("d_net")), _int(r.get("total")),
    ) for _, r in df.iterrows()]
    with db_conn() as c:
        c.executemany(
            "INSERT OR REPLACE INTO inst (date,code,name,f_net,t_net,d_net,total) "
            "VALUES (?,?,?,?,?,?,?)", rows
        )
    # log per date
    from collections import Counter
    counts = Counter(r[0] for r in rows)
    for dt, cnt in sorted(counts.items()):
        log.info(f"  Saved inst {dt}: {cnt} rows")
    return len(rows)


def db_save_price(df: pd.DataFrame, d: str) -> int:
    if df is None or df.empty:
        return 0
    td = _fmt(d)
    rows = [(
        td, str(r["code"]).zfill(4),
        str(r.get("name","") or ""),
        str(r.get("market","") or ""),
        _flt(r.get("close")), _flt(r.get("open")),
        _flt(r.get("high")),  _flt(r.get("low")),
        _int(r.get("vol")),
    ) for _, r in df.iterrows()]
    with db_conn() as c:
        c.executemany(
            "INSERT OR REPLACE INTO price (date,code,name,market,close,open,high,low,vol) "
            "VALUES (?,?,?,?,?,?,?,?,?)", rows
        )
    log.info(f"  Saved price {td}: {len(rows)} rows")
    return len(rows)


# ─────────────────────────────────────────────────────────
#  TWSE / TPEx 爬蟲
# ─────────────────────────────────────────────────────────
def fetch_t86(date8: str) -> pd.DataFrame:
    """
    TWSE T86 三大法人買賣超（上市股）
    欄位索引：0=代號 1=名稱 4=外資淨 7=投信淨 10=自營(自行)淨 13=自營(避險)淨 14=三大合計
    單位：張（1張=1000股）
    億元換算：張數 × 收盤價 × 1000 ÷ 1e8
    """
    data = _get("https://www.twse.com.tw/rwd/zh/fund/T86",
                params={"response": "json", "date": date8, "selectType": "ALL"})
    if not data or not isinstance(data, dict) or data.get("stat") != "OK":
        return pd.DataFrame()
    rows = []
    for row in data.get("data", []):
        if len(row) < 15:
            continue
        rows.append({
            "date":  datetime.strptime(date8, "%Y%m%d").strftime("%Y-%m-%d"),
            "code":  str(row[0]).strip().zfill(4),
            "name":  str(row[1]).strip(),
            "f_net": _int(row[4]),    # 外資淨買超（張）
            "t_net": _int(row[7]),    # 投信淨買超（張）
            "d_net": _int(row[10]) + _int(row[13]),  # 自營合計（張）
            "total": _int(row[14]),   # 三大法人合計（張）
        })
    df = pd.DataFrame(rows)
    log.info(f"  T86(TWSE) {date8}: {len(df)} rows")
    return df


def fetch_tpex_inst(date8: str) -> pd.DataFrame:
    """
    TPEx 上櫃三大法人買賣超
    端點：GET /tpex_mainboard_institutional_investors
    或   GET /tpex/fund/daily_institutional_buying_selling

    TPEx API 回傳欄位（依 OpenAPI 文件）：
      代號、名稱、外資買進、外資賣出、外資淨買超、
      投信買進、投信賣出、投信淨買超、
      自營買進、自營賣出、自營淨買超、三大法人淨買超
    單位：張（1張=1000股），與 TWSE T86 相同
    """
    dd = datetime.strptime(date8, "%Y%m%d").strftime("%Y-%m-%d")

    for ep in [
        "https://www.tpex.org.tw/openapi/v1/tpex_mainboard_institutional_investors",
        "https://www.tpex.org.tw/openapi/v1/tpex/fund/daily_institutional_buying_selling",
    ]:
        data = _get(ep, verify=False, retries=2, delay=1.0)
        if not data or not isinstance(data, list) or len(data) < 5:
            continue

        rows = []
        for item in data:
            # 嘗試不同的欄位名稱（TPEx API 欄位名稱因版本有差異）
            code = str(item.get("SecuritiesCompanyCode",
                        item.get("Code", item.get("code", "")))).strip()
            if not code or not code.isdigit():
                continue
            # 三大法人合計淨買超（張）
            # 可能的欄位名：NetBuy, NetBuySell, ThreeInstitutionalInvestorsNet, net_buy
            total = (_int(item.get("NetBuy",
                     item.get("NetBuySell",
                     item.get("ThreeInstitutionalInvestorsNet",
                     item.get("net_buy", 0))))))
            # 也可能是分開計算
            if total == 0:
                f = _int(item.get("ForeignNetBuy",   item.get("foreign_net", 0)))
                t = _int(item.get("TrustNetBuy",     item.get("trust_net",   0)))
                d = _int(item.get("DealerNetBuy",    item.get("dealer_net",  0)))
                total = f + t + d
            rows.append({
                "date":  dd,
                "code":  code.zfill(4),
                "name":  str(item.get("CompanyName",
                              item.get("Name", item.get("name", "")))).strip(),
                "f_net": _int(item.get("ForeignNetBuy",   item.get("foreign_net", 0))),
                "t_net": _int(item.get("TrustNetBuy",     item.get("trust_net",   0))),
                "d_net": _int(item.get("DealerNetBuy",    item.get("dealer_net",  0))),
                "total": total,
            })
        if rows:
            df = pd.DataFrame(rows)
            log.info(f"  T86(TPEx) {date8}: {len(df)} rows (from {ep.split('/')[-1]})")
            return df

    log.warning(f"  TPEx inst {date8}: all endpoints failed or empty")
    return pd.DataFrame()


def fetch_inst_multiday(days: int = 20, skip_dates: set = None) -> pd.DataFrame:
    """
    抓近 days 個交易日的三大法人資料（TWSE 上市 + TPEx 上櫃合併）
    已在 DB 的日期自動跳過

    TWSE T86：上市股，欄位 14 = 三大合計（張）
    TPEx 法人：上櫃股，三大合計（張），與 TWSE 同單位
    """
    skip_dates = skip_dates or set()
    frames = []
    collected = 0
    offset = 0
    today = datetime.today()

    while collected < days and offset < days * 2 + 20:
        d = today - timedelta(days=offset)
        offset += 1
        if d.weekday() >= 5:
            continue
        dd = d.strftime("%Y-%m-%d")
        d8 = d.strftime("%Y%m%d")
        if dd in skip_dates:
            log.info(f"  SKIP inst {dd} (already in DB)")
            collected += 1
            continue

        # TWSE 上市
        twse_df = fetch_t86(d8)

        # TPEx 上櫃（每次都嘗試，失敗不阻斷）
        tpex_df = fetch_tpex_inst(d8)

        if twse_df.empty and tpex_df.empty:
            log.warning(f"  No inst data for {dd}, skipping")
            continue

        day_frames = [f for f in [twse_df, tpex_df] if not f.empty]
        combined = pd.concat(day_frames, ignore_index=True).drop_duplicates(subset=["code"])
        frames.append(combined)
        collected += 1
        time.sleep(0.5)

    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def fetch_twse_price(trade_date: str) -> pd.DataFrame:
    """上市收盤價，trade_date = YYYY-MM-DD"""
    data = _get("https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL")
    if not data or not isinstance(data, list):
        return pd.DataFrame()
    rows = []
    for item in data:
        code = str(item.get("Code", "")).strip()
        if not code:
            continue
        rows.append({
            "date":   trade_date,
            "code":   code.zfill(4),
            "name":   item.get("Name", ""),
            "market": "TWSE",
            "close":  _flt(item.get("ClosingPrice")),
            "open":   _flt(item.get("OpeningPrice")),
            "high":   _flt(item.get("HighestPrice")),
            "low":    _flt(item.get("LowestPrice")),
            "vol":    _int(item.get("TradeVolume")),
        })
    df = pd.DataFrame(rows)
    log.info(f"  TWSE price: {len(df)} rows")
    return df


def fetch_tpex_price(trade_date: str) -> pd.DataFrame:
    """
    上櫃收盤價（TPEx OpenAPI，verify=False 因為 TPEx 憑證問題）
    端點依序嘗試新舊版本，TradeVolume 是股數不是張數，僅作記錄用
    """
    for ep in [
        "https://www.tpex.org.tw/openapi/v1/tpex_mainboard_daily_close_quotes",
        "https://www.tpex.org.tw/openapi/v1/tpex_mainboard_quotes",
        "https://www.tpex.org.tw/openapi/v1/tpex/exchangeReport/daily_close_quotes",
        "https://www.tpex.org.tw/openapi/v1/tpex/exchangeReport/companies_regular_trading_statistics",
    ]:
        data = _get(ep, verify=False, retries=2, delay=1.5)
        if not data or not isinstance(data, list) or len(data) < 5:
            continue
        rows = []
        for item in data:
            code = str(item.get("SecuritiesCompanyCode",
                        item.get("Code", item.get("code","")))).strip()
            if not code:
                continue
            close = _flt(item.get("Close",
                          item.get("ClosingPrice",
                          item.get("close", 0))))
            if close <= 0:
                continue
            rows.append({
                "date":   trade_date,
                "code":   code.zfill(4),
                "name":   item.get("CompanyName", item.get("Name", item.get("name",""))),
                "market": "TPEx",
                "close":  close,
                "open":   _flt(item.get("Open",  item.get("OpeningPrice",  0))),
                "high":   _flt(item.get("High",  item.get("HighestPrice",  0))),
                "low":    _flt(item.get("Low",   item.get("LowestPrice",   0))),
                "vol":    _int(item.get("TradeVolume", 0)),  # 股數，非張數
            })
        if rows:
            df = pd.DataFrame(rows)
            log.info(f"  TPEx price: {len(df)} rows (from {ep.split('/')[-1]})")
            return df
    log.warning("  TPEx price: all endpoints failed")
    return pd.DataFrame()


def fetch_all_prices(trade_date: str) -> pd.DataFrame:
    twse = fetch_twse_price(trade_date)
    tpex = fetch_tpex_price(trade_date)
    frames = [f for f in [twse, tpex] if not f.empty]
    if not frames:
        return pd.DataFrame()
    df = pd.concat(frames, ignore_index=True).drop_duplicates(subset=["code"])
    log.info(f"  All prices: {len(df)} rows")
    return df


# ─────────────────────────────────────────────────────────
#  輸入檔案讀取
# ─────────────────────────────────────────────────────────
def load_groups() -> dict[str, list[str]]:
    """讀取 input/Group.csv（Big5 編碼）"""
    raw = GROUP_CSV.read_bytes()
    try:
        text = raw.decode("big5")
    except UnicodeDecodeError:
        text = raw.decode("cp950", errors="replace")
    lines = text.strip().replace("\r\n", "\n").replace("\r", "\n").split("\n")
    headers = [h.strip() for h in lines[0].split(",")]
    groups = {h: [] for h in headers if h}
    for line in lines[1:]:
        cols = line.split(",")
        for i, h in enumerate(headers):
            if h and i < len(cols) and cols[i].strip():
                groups[h].append(cols[i].strip())
    total = sum(len(v) for v in groups.values())
    log.info(f"Groups: {len(groups)} 族群, {total} 個股對應")
    return groups


def load_stock_names() -> dict[str, str]:
    """讀取 input/stock_list.csv（CP950），回傳 {代號: 名稱}"""
    raw = STOCKS_CSV.read_bytes()
    try:
        text = raw.decode("cp950")
    except UnicodeDecodeError:
        text = raw.decode("big5", errors="replace")
    result = {}
    for line in text.strip().replace("\r\n", "\n").split("\n")[1:]:
        parts = line.strip().split(",")
        if len(parts) >= 2:
            code = parts[0].strip().zfill(4)
            name = parts[1].strip()
            if code and name:
                result[code] = name
    log.info(f"Stock names: {len(result)} 筆")
    return result


# ─────────────────────────────────────────────────────────
#  計算核心
# ─────────────────────────────────────────────────────────
def compute(inst_df: pd.DataFrame,
            price_df: pd.DataFrame,
            groups: dict[str, list[str]],
            name_map: dict[str, str]) -> tuple[list[dict], dict[str, list[dict]]]:
    """
    計算族群資金流向指標

    inst_df   = 近 20 日三大法人資料（DB 讀取）
    price_df  = 今日收盤價（STOCK_DAY_ALL 回傳，含 date 欄位）
    漲跌幅用  db_load_price_history() 取歷史收盤價計算

    回傳：
      records   族群層級 list[dict]
      details   {族群名: [個股 dict, ...]}
    """
    if inst_df.empty or price_df.empty:
        log.error("inst 或 price 資料為空，無法計算")
        return [], {}

    inst  = inst_df.copy()
    price = price_df.copy()
    inst["code"]  = inst["code"].astype(str).str.zfill(4)
    price["code"] = price["code"].astype(str).str.zfill(4)

    inst_dates = sorted(inst["date"].unique())
    log.info(f"  inst 日期範圍: {inst_dates[0]} ~ {inst_dates[-1]} ({len(inst_dates)} 天)")

    # ── 今日收盤價 ───────────────────────────────────────
    latest_price_date = sorted(price["date"].unique())[-1]
    p_now  = price[price["date"] == latest_price_date].set_index("code")
    close  = p_now["close"]
    log.info(f"  price 最新日: {latest_price_date}, {len(close)} stocks")

    # ── 歷史收盤價（從 DB 取，用於計算漲跌幅）───────────
    # 每天 step_price 都會把當日收盤存進 DB，所以 DB 有多天歷史
    price_hist = db_load_price_history(days=7)

    if price_hist.empty:
        log.warning("  DB 無歷史收盤價，漲跌幅為 0（明日起正常計算）")
        # 今日當基準，算出來全是 0，但不崩潰
        price_hist = price[["date","code","close"]].copy()

    hist_dates = sorted(price_hist["date"].unique())
    log.info(f"  price history dates: {hist_dates}")

    # 五日前 / 昨日
    p5_date = hist_dates[-5] if len(hist_dates) >= 5 else hist_dates[0]
    p1_date = hist_dates[-2] if len(hist_dates) >= 2 else hist_dates[0]
    p5 = price_hist[price_hist["date"] == p5_date].set_index("code")["close"]
    p1 = price_hist[price_hist["date"] == p1_date].set_index("code")["close"]
    log.info(f"  chg_1d 基準: {p1_date}, chg_5d 基準: {p5_date}")

    # ── inst 聚合 ────────────────────────────────────────
    last5 = set(inst_dates[-5:])
    latest_inst_date = inst_dates[-1]

    agg_1d  = inst[inst["date"] == latest_inst_date].groupby("code")["total"].sum()
    agg_5d  = inst[inst["date"].isin(last5)].groupby("code")["total"].sum()
    agg_20d = inst.groupby("code")["total"].sum()

    codes = close.index

    # 億元換算：張數 × 收盤價 × 1000股 ÷ 1e8
    def to_yi(agg: pd.Series, close_s: pd.Series) -> pd.Series:
        return (agg.reindex(close_s.index, fill_value=0) * close_s * 1000 / 1e8).round(4)

    net_1d  = to_yi(agg_1d,  close)
    net_5d  = to_yi(agg_5d,  close)
    net_20d = to_yi(agg_20d, close)

    # 漲跌幅：(今收 - 基準收) / 基準收 × 100
    def pct_chg(base: pd.Series) -> pd.Series:
        b = base.reindex(codes, fill_value=np.nan)
        valid = b.notna() & (b > 0)
        result = pd.Series(np.nan, index=codes)
        result[valid] = ((close[valid] - b[valid]) / b[valid] * 100).round(2)
        return result

    chg_1d = pct_chg(p1)
    chg_5d = pct_chg(p5)

    api_names = p_now["name"].to_dict()
    def get_name(code: str) -> str:
        return name_map.get(code, "") or api_names.get(code, "")

    # ── 族群彙總 ─────────────────────────────────────────
    records: list[dict] = []
    details: dict[str, list[dict]] = {}

    for gname, raw_codes in groups.items():
        codes_set = {str(c).zfill(4) for c in raw_codes}
        matched   = [c for c in codes_set if c in close.index]

        stocks = []
        for c in matched:
            c1d = float(chg_1d.get(c, np.nan))
            c5d = float(chg_5d.get(c, np.nan))
            if np.isnan(c1d) or abs(c1d) > 11: c1d = 0.0   # 台股漲跌停 ±10%
            if np.isnan(c5d): c5d = 0.0
            stocks.append({
                "code":    c,
                "name":    get_name(c),
                "close":   round(float(close.get(c, 0)), 2),
                "net_1d":  round(float(net_1d.get(c, 0)), 4),
                "net_5d":  round(float(net_5d.get(c, 0)), 4),
                "net_20d": round(float(net_20d.get(c, 0)), 4),
                "chg_1d":  round(c1d, 2),
                "chg_5d":  round(c5d, 2),
            })
        stocks.sort(key=lambda x: x["net_1d"], reverse=True)
        details[gname] = stocks

        if not matched:
            records.append({
                "g": gname, "cnt": len(raw_codes), "matched": 0,
                "net_1d": 0.0, "net_5d": 0.0, "net_20d": 0.0,
                "chg_1d": 0.0, "chg_5d": 0.0, "label": "觀望",
            })
            continue

        def gmean(s: pd.Series, idx: list) -> float:
            vals = s.reindex(idx).dropna()
            return round(float(vals.mean()), 2) if not vals.empty else 0.0

        g_net_1d  = round(float(net_1d.reindex(matched, fill_value=0).sum()),  3)
        g_net_5d  = round(float(net_5d.reindex(matched, fill_value=0).sum()),  3)
        g_net_20d = round(float(net_20d.reindex(matched, fill_value=0).sum()), 3)
        g_chg_1d  = gmean(chg_1d, matched)
        g_chg_5d  = gmean(chg_5d, matched)

        if   g_net_5d >  2 and g_chg_5d > 1:  label = "主力"
        elif g_net_5d >  0 and g_chg_5d <= 1: label = "輪動"
        elif g_net_5d < -2:                    label = "退潮"
        else:                                  label = "觀望"

        records.append({
            "g":       gname,
            "cnt":     len(raw_codes),
            "matched": len(matched),
            "net_1d":  g_net_1d,
            "net_5d":  g_net_5d,
            "net_20d": g_net_20d,
            "chg_1d":  g_chg_1d,
            "chg_5d":  g_chg_5d,
            "label":   label,
        })

    log.info(f"  Groups: {len(records)}, "
             f"主力={sum(1 for r in records if r['label']=='主力')}, "
             f"輪動={sum(1 for r in records if r['label']=='輪動')}, "
             f"退潮={sum(1 for r in records if r['label']=='退潮')}, "
             f"觀望={sum(1 for r in records if r['label']=='觀望')}")
    return records, details

def export_json(records: list[dict], details: dict[str, list[dict]]) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    def attach(filtered):
        return [{**r, "stocks": details.get(r["g"], [])} for r in filtered]

    # 泡泡圖：全部族群
    bubble = [{
        **r,
        "x":    r["net_5d"],   # X 軸：五日淨買超（億）
        "y":    r["net_1d"],   # Y 軸：今日淨買超（億）
        "size": max(10, min(72, abs(r["net_5d"]) * 2.8 + 12)),
        "stocks": details.get(r["g"], []),
    } for r in records]

    # 篩選一：流入低漲幅
    inflow = attach(sorted(
        [r for r in records if r["net_5d"] > 0 and r["chg_5d"] < 10],
        key=lambda x: x["net_5d"], reverse=True
    ))

    # 篩選二：偷偷布局（今日買超 且 五日買超 且 五日仍跌）
    stealth = attach(sorted(
        [r for r in records if r["net_1d"] > 0 and r["net_5d"] > 0 and r["chg_5d"] < 0],
        key=lambda x: x["net_5d"], reverse=True
    ))

    _jdump("bubble_data.json",          bubble)
    _jdump("inflow_low_gain.json",       inflow)
    _jdump("stealth_accumulation.json",  stealth)
    _jdump("group_stats.json",          [{k: v for k, v in r.items() if k != "stocks"}
                                          for r in records])
    _jdump("metadata.json", {
        "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "trade_date":   TODAY,
        "inst_rows":    len(records),
    })

    log.info(f"  JSON: bubble={len(bubble)}, inflow={len(inflow)}, stealth={len(stealth)}")


def _jdump(fname: str, data):
    with open(DATA_DIR / fname, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


# ─────────────────────────────────────────────────────────
#  Pipeline Steps
# ─────────────────────────────────────────────────────────
def step_inst(force: bool) -> pd.DataFrame:
    log.info("=== Step 2: 三大法人資料 ===")
    cached = set(db_inst_dates(25))

    if not force and db_has_inst(TODAY):
        log.info("  [CACHE] 今日資料已在 DB")
        return db_load_inst(days=20)

    # 抓新資料（skip 已在 DB 的日期）
    new_df = fetch_inst_multiday(days=20, skip_dates=cached if not force else set())

    if not new_df.empty:
        # 只存 DB 尚未有的日期
        new_only = new_df[~new_df["date"].isin(cached)] if not force else new_df
        if not new_only.empty:
            db_save_inst(new_only)

    # 從 DB 讀取最新 20 日（無論今日 API 是否有資料都能繼續）
    result = db_load_inst(days=20)
    if result.empty:
        log.error("  DB 無 inst 資料，pipeline 無法繼續")
    return result


def step_price(force: bool) -> pd.DataFrame:
    log.info("=== Step 3: 收盤價 ===")

    if not force and db_has_price(TODAY):
        log.info("  [CACHE] 今日收盤價已在 DB")
        return db_load_price(TODAY)

    df = fetch_all_prices(TODAY)

    if not df.empty:
        db_save_price(df, TODAY)
        return df

    # API 失敗（收盤前或 TPEx 不穩），用 DB 最新日
    fallback = db_load_price()
    if not fallback.empty:
        latest = fallback["date"].max() if "date" in fallback.columns else "?"
        log.warning(f"  收盤價 API 失敗，使用 DB 最新日 ({latest})")
    else:
        log.error("  無任何收盤價資料")
    return fallback


def step_compute(groups: dict, name_map: dict,
                 inst_df: pd.DataFrame, price_df: pd.DataFrame):
    log.info("=== Step 4: 計算族群指標 ===")
    if inst_df.empty or price_df.empty:
        log.error("  資料不足，跳過計算")
        return
    records, details = compute(inst_df, price_df, groups, name_map)
    if records:
        export_json(records, details)
    else:
        log.error("  計算結果為空")


# ─────────────────────────────────────────────────────────
#  型別轉換工具
# ─────────────────────────────────────────────────────────
def _fmt(d) -> str:
    s = str(d).strip()
    if len(s) == 8 and s.isdigit():
        return f"{s[:4]}-{s[4:6]}-{s[6:8]}"
    return s[:10].replace("/", "-")

def _int(v) -> int:
    try:    return int(str(v).replace(",", "").strip())
    except: return 0

def _flt(v) -> float:
    try:    return float(str(v).replace(",", "").strip())
    except: return 0.0


# ─────────────────────────────────────────────────────────
#  Main
# ─────────────────────────────────────────────────────────
def main():
    ap = argparse.ArgumentParser(description="TW$FLOW Daily Pipeline")
    ap.add_argument("--force",      action="store_true", help="忽略快取，強制重新抓取")
    ap.add_argument("--skip-etf",   action="store_true", help="跳過 ETF（預設跳過）")
    ap.add_argument("--dry-run",    action="store_true", help="只重算 JSON，不呼叫 API")
    args = ap.parse_args()

    log.info("=" * 60)
    log.info(f"TW$FLOW  {datetime.now()}  trade_date={TODAY}")
    if args.force:   log.info("  *** FORCE MODE ***")
    if args.dry_run: log.info("  *** DRY-RUN MODE ***")
    log.info("=" * 60)

    # 初始化
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    db_init()

    # Step 1: 讀取設定檔
    log.info("=== Step 1: 讀取設定檔 ===")
    groups   = load_groups()
    name_map = load_stock_names()

    if args.dry_run:
        # 只從 DB 重算，不呼叫任何 API
        inst_df  = db_load_inst(days=20)
        price_df = db_load_price()
    else:
        inst_df  = step_inst(force=args.force)
        price_df = step_price(force=args.force)

    step_compute(groups, name_map, inst_df, price_df)

    log.info("Pipeline complete ✓")

    # DB 摘要
    with db_conn() as c:
        ni = c.execute("SELECT COUNT(*) FROM inst").fetchone()[0]
        np_ = c.execute("SELECT COUNT(*) FROM price").fetchone()[0]
        li = c.execute("SELECT MAX(date) FROM inst").fetchone()[0]
        lp = c.execute("SELECT MAX(date) FROM price").fetchone()[0]
    log.info(f"DB: inst={ni} rows (latest={li}), price={np_} rows (latest={lp})")


if __name__ == "__main__":
    main()
