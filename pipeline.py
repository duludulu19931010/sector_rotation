#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
TW$FLOW pipeline.py
====================
每日從 TWSE / TPEx 官方 API 抓取資料，計算族群資金流向，輸出 JSON 供前端使用。

資料來源（全部官方免費 API，不忽略任何一個）：
  TWSE 上市三大法人  https://www.twse.com.tw/rwd/zh/fund/T86
  TPEx 上櫃三大法人  https://www.tpex.org.tw/openapi/v1/...
  TWSE 上市收盤價   https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL
  TPEx 上櫃收盤價   https://www.tpex.org.tw/openapi/v1/...

速度優化：
  1. TWSE + TPEx 並行抓取（concurrent.futures.ThreadPoolExecutor）
  2. DB 批次寫入（executemany，一次 commit）
  3. 快取判斷：已有當日資料直接讀 DB，跳過 API
  4. 只儲存 DB 尚未有的日期，不重複寫入

資料庫（db/market.db）存在 GitHub，每次 push 更新：
  inst  表：三大法人逐日買賣超（上市 + 上櫃）
  price 表：逐日收盤價（上市 + 上櫃）

用法：
  python pipeline.py            每日正常執行
  python pipeline.py --force    忽略快取，強制重新抓取
  python pipeline.py --dry-run  只從 DB 重算 JSON，不呼叫任何 API
"""

import argparse
import json
import logging
import sqlite3
import sys
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from datetime import date, datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ─────────────────────────────────────────────────────────
#  設定
# ─────────────────────────────────────────────────────────
ROOT       = Path(__file__).resolve().parent
DB_FILE    = ROOT / "db" / "market.db"
DATA_DIR   = ROOT / "docs" / "assets" / "data"
GROUP_CSV  = ROOT / "input" / "Group.csv"
STOCKS_CSV = ROOT / "input" / "stock_list.csv"
LOG_FILE   = ROOT / "pipeline.log"

TODAY   = date.today().strftime("%Y-%m-%d")
TODAY_8 = date.today().strftime("%Y%m%d")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(str(LOG_FILE), encoding="utf-8"),
    ],
)
log = logging.getLogger("twflow")

_HDR_TWSE = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept":     "application/json, text/plain, */*",
    "Referer":    "https://www.twse.com.tw/",
}
_HDR_TPEX = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept":     "application/json, text/plain, */*",
    "Referer":    "https://www.tpex.org.tw/",
}


# ─────────────────────────────────────────────────────────
#  HTTP 工具
# ─────────────────────────────────────────────────────────
def _get(url: str, params: dict = None, verify: bool = True,
         headers: dict = None, retries: int = 3, delay: float = 2.0):
    """HTTP GET，回傳 dict/list 或 None。空 body / 非 JSON 視為失敗。"""
    hdrs = headers or _HDR_TWSE
    for attempt in range(retries):
        try:
            r = requests.get(url, params=params, headers=hdrs,
                             timeout=30, verify=verify)
            r.raise_for_status()
            if not r.text.strip():
                raise ValueError("Empty response body")
            data = r.json()
            if isinstance(data, (dict, list)):
                return data
            return None
        except Exception as e:
            log.warning(f"  GET [{attempt+1}/{retries}] {url.split('/')[-1]} — {e}")
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
CREATE INDEX IF NOT EXISTS ix_inst_date  ON inst(date);
CREATE INDEX IF NOT EXISTS ix_price_date ON price(date);
CREATE INDEX IF NOT EXISTS ix_price_code ON price(code);
"""


@contextmanager
def _con():
    DB_FILE.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(str(DB_FILE))
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA synchronous=NORMAL")
    con.execute("PRAGMA cache_size=-64000")   # 64 MB page cache
    try:
        yield con
        con.commit()
    except Exception:
        con.rollback()
        raise
    finally:
        con.close()


def db_init():
    with _con() as c:
        c.executescript(DDL)
    log.info(f"DB: {DB_FILE}")


def db_inst_dates(n: int = 25) -> list[str]:
    """回傳有完整三大法人資料的日期（自適應閾值）"""
    with _con() as c:
        rows = c.execute(
            "SELECT date, COUNT(*) AS cnt FROM inst GROUP BY date ORDER BY date DESC"
        ).fetchall()
    if not rows:
        return []
    max_cnt = max(r["cnt"] for r in rows)
    thr     = max(50, max_cnt * 0.5)
    return [r["date"] for r in rows if r["cnt"] >= thr][:n]


def db_price_dates(n: int = 10) -> list[str]:
    """回傳有完整收盤價資料的日期（自適應閾值）"""
    with _con() as c:
        rows = c.execute(
            "SELECT date, COUNT(*) AS cnt FROM price GROUP BY date ORDER BY date DESC LIMIT 30"
        ).fetchall()
    if not rows:
        return []
    max_cnt = max(r["cnt"] for r in rows)
    thr     = max(50, max_cnt * 0.5)
    return [r["date"] for r in rows if r["cnt"] >= thr][:n]


def db_has_inst_today() -> bool:
    dates = db_inst_dates(1)
    return bool(dates) and dates[0] == TODAY


def db_has_price_today() -> bool:
    return TODAY in db_price_dates(1)


def db_load_inst(days: int = 20) -> pd.DataFrame:
    dates = db_inst_dates(days)
    if not dates:
        return pd.DataFrame()
    ph = ",".join("?" * len(dates))
    with _con() as c:
        df = pd.read_sql_query(
            f"SELECT date,code,name,f_net,t_net,d_net,total "
            f"FROM inst WHERE date IN ({ph}) ORDER BY date,code",
            c, params=dates
        )
    log.info(f"  [DB] inst: {len(df)} rows / {df['date'].nunique()} dates")
    return df


def db_load_price(d: str = None) -> pd.DataFrame:
    """讀指定日（預設最新日）收盤價"""
    with _con() as c:
        if d:
            df = pd.read_sql_query(
                "SELECT date,code,name,market,close FROM price WHERE date=?",
                c, params=(_fmt(d),)
            )
        else:
            df = pd.read_sql_query(
                "SELECT date,code,name,market,close FROM price "
                "WHERE date=(SELECT MAX(date) FROM price)", c
            )
    log.info(f"  [DB] price: {len(df)} rows")
    return df


def db_load_price_history(days: int = 7) -> pd.DataFrame:
    """讀近 N 個交易日收盤價（用於計算漲跌幅）"""
    dates = db_price_dates(days)
    if not dates:
        return pd.DataFrame()
    ph = ",".join("?" * len(dates))
    with _con() as c:
        df = pd.read_sql_query(
            f"SELECT date, code, close FROM price WHERE date IN ({ph}) ORDER BY date",
            c, params=dates
        )
    if not df.empty:
        df["code"] = df["code"].astype(str).str.zfill(4)
    return df


def db_save_inst(df: pd.DataFrame) -> int:
    """批次寫入三大法人資料（INSERT OR REPLACE）"""
    if df is None or df.empty:
        return 0
    rows = [
        (_fmt(r["date"]), str(r["code"]).zfill(4), str(r.get("name","") or ""),
         _int(r.get("f_net")), _int(r.get("t_net")),
         _int(r.get("d_net")), _int(r.get("total")))
        for _, r in df.iterrows()
    ]
    with _con() as c:
        c.executemany(
            "INSERT OR REPLACE INTO inst (date,code,name,f_net,t_net,d_net,total) "
            "VALUES (?,?,?,?,?,?,?)", rows
        )
    counts = Counter(r[0] for r in rows)
    for dt, cnt in sorted(counts.items()):
        log.info(f"  [DB] inst saved {dt}: {cnt} rows")
    return len(rows)


def db_save_price(df: pd.DataFrame, d: str) -> int:
    """批次寫入收盤價"""
    if df is None or df.empty:
        return 0
    td = _fmt(d)
    rows = [
        (td, str(r["code"]).zfill(4), str(r.get("name","") or ""),
         str(r.get("market","") or ""),
         _flt(r.get("close")), _flt(r.get("open")),
         _flt(r.get("high")),  _flt(r.get("low")),
         _int(r.get("vol")))
        for _, r in df.iterrows()
    ]
    with _con() as c:
        c.executemany(
            "INSERT OR REPLACE INTO price (date,code,name,market,close,open,high,low,vol) "
            "VALUES (?,?,?,?,?,?,?,?,?)", rows
        )
    log.info(f"  [DB] price saved {td}: {len(rows)} rows")
    return len(rows)


# ─────────────────────────────────────────────────────────
#  爬蟲 — TWSE（上市）
# ─────────────────────────────────────────────────────────
def _twse_inst_one(date8: str) -> pd.DataFrame:
    """
    TWSE T86 上市三大法人（單日）
    欄位 14 = 三大合計淨買超（張，1張=1000股）
    """
    data = _get("https://www.twse.com.tw/rwd/zh/fund/T86",
                params={"response":"json","date":date8,"selectType":"ALL"},
                headers=_HDR_TWSE)
    if not data or not isinstance(data, dict) or data.get("stat") != "OK":
        return pd.DataFrame()
    dd   = datetime.strptime(date8, "%Y%m%d").strftime("%Y-%m-%d")
    rows = []
    for row in data.get("data", []):
        if len(row) < 15:
            continue
        rows.append({
            "date":  dd,
            "code":  str(row[0]).strip().zfill(4),
            "name":  str(row[1]).strip(),
            "f_net": _int(row[4]),
            "t_net": _int(row[7]),
            "d_net": _int(row[10]) + _int(row[13]),
            "total": _int(row[14]),
        })
    df = pd.DataFrame(rows)
    if not df.empty:
        log.info(f"  TWSE inst {date8}: {len(df)} rows")
    return df


def _twse_price() -> pd.DataFrame:
    """TWSE 上市收盤價（全市場，TradeVolume=股數僅作記錄）"""
    data = _get("https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL",
                headers=_HDR_TWSE)
    if not data or not isinstance(data, list):
        return pd.DataFrame()
    rows = []
    for item in data:
        code = str(item.get("Code","")).strip()
        if not code:
            continue
        close = _flt(item.get("ClosingPrice"))
        if close <= 0:
            continue
        rows.append({
            "date":   TODAY,
            "code":   code.zfill(4),
            "name":   item.get("Name",""),
            "market": "TWSE",
            "close":  close,
            "open":   _flt(item.get("OpeningPrice")),
            "high":   _flt(item.get("HighestPrice")),
            "low":    _flt(item.get("LowestPrice")),
            "vol":    _int(item.get("TradeVolume")),   # 股數，非張數
        })
    df = pd.DataFrame(rows)
    log.info(f"  TWSE price: {len(df)} rows")
    return df


# ─────────────────────────────────────────────────────────
#  爬蟲 — TPEx（上櫃）
# ─────────────────────────────────────────────────────────

# TPEx 法人 API 端點（依序嘗試）
_TPEX_INST_EPS = [
    "https://www.tpex.org.tw/openapi/v1/tpex_mainboard_institutional_investors",
    "https://www.tpex.org.tw/openapi/v1/tpex/fund/daily_institutional_buying_selling",
]

# TPEx 收盤價 API 端點（依序嘗試）
_TPEX_PRICE_EPS = [
    "https://www.tpex.org.tw/openapi/v1/tpex_mainboard_daily_close_quotes",
    "https://www.tpex.org.tw/openapi/v1/tpex_mainboard_quotes",
    "https://www.tpex.org.tw/openapi/v1/tpex/exchangeReport/daily_close_quotes",
    "https://www.tpex.org.tw/openapi/v1/tpex/exchangeReport/companies_regular_trading_statistics",
]


def _tpex_inst_one(date8: str) -> pd.DataFrame:
    """
    TPEx 上櫃三大法人（單日）
    單位：張，與 TWSE T86 相同

    TPEx API 欄位名稱可能因版本而異，嘗試多種欄位名
    """
    dd = datetime.strptime(date8, "%Y%m%d").strftime("%Y-%m-%d")
    for ep in _TPEX_INST_EPS:
        data = _get(ep, verify=False, headers=_HDR_TPEX, retries=2, delay=1.0)
        if not data or not isinstance(data, list) or len(data) < 5:
            continue
        rows = []
        for item in data:
            code = str(item.get("SecuritiesCompanyCode",
                        item.get("Code", item.get("code","")))).strip()
            if not code or not code.isdigit():
                continue

            # 外資淨買超
            f = _int(item.get("ForeignInvestorsNetBuySell",
                    item.get("ForeignNetBuy",
                    item.get("foreign_net", 0))))
            # 投信淨買超
            t = _int(item.get("InvestmentTrustNetBuySell",
                    item.get("TrustNetBuy",
                    item.get("trust_net", 0))))
            # 自營淨買超
            d = _int(item.get("DealersNetBuySell",
                    item.get("DealerNetBuy",
                    item.get("dealer_net", 0))))
            # 三大合計
            total = _int(item.get("TotalNetBuySell",
                         item.get("NetBuy",
                         item.get("NetBuySell",
                         item.get("ThreeInstitutionalInvestorsNet", 0)))))
            if total == 0:
                total = f + t + d

            rows.append({
                "date":  dd,
                "code":  code.zfill(4),
                "name":  str(item.get("CompanyName",
                              item.get("Name", item.get("name","")))).strip(),
                "f_net": f,
                "t_net": t,
                "d_net": d,
                "total": total,
            })
        if rows:
            df = pd.DataFrame(rows)
            log.info(f"  TPEx inst {date8}: {len(df)} rows")
            return df
    log.warning(f"  TPEx inst {date8}: all endpoints failed")
    return pd.DataFrame()


def _tpex_price() -> pd.DataFrame:
    """TPEx 上櫃收盤價（verify=False 因憑證問題）"""
    for ep in _TPEX_PRICE_EPS:
        data = _get(ep, verify=False, headers=_HDR_TPEX, retries=2, delay=1.5)
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
                "date":   TODAY,
                "code":   code.zfill(4),
                "name":   item.get("CompanyName",
                          item.get("Name", item.get("name",""))),
                "market": "TPEx",
                "close":  close,
                "open":   _flt(item.get("Open",  item.get("OpeningPrice",  0))),
                "high":   _flt(item.get("High",  item.get("HighestPrice",  0))),
                "low":    _flt(item.get("Low",   item.get("LowestPrice",   0))),
                "vol":    _int(item.get("TradeVolume", 0)),
            })
        if rows:
            df = pd.DataFrame(rows)
            log.info(f"  TPEx price: {len(df)} rows ({ep.split('/')[-1]})")
            return df
    log.warning("  TPEx price: all endpoints failed")
    return pd.DataFrame()


# ─────────────────────────────────────────────────────────
#  並行抓取（TWSE + TPEx 同時進行）
# ─────────────────────────────────────────────────────────
def fetch_inst_parallel(days: int = 20,
                        skip_dates: set = None) -> pd.DataFrame:
    """
    並行抓取 TWSE + TPEx 三大法人（每一天的 TWSE/TPEx 同時發出請求）
    skip_dates: 已在 DB 的日期，直接跳過

    速度：原本 20天 × 2個來源 × 串行 ≈ 60秒
          並行後 ≈ 20秒（受 API 速率限制）
    """
    skip_dates = skip_dates or set()
    frames: list[pd.DataFrame] = []
    collected = 0
    offset    = 0
    today     = datetime.today()

    # 建立待抓日期清單
    target_dates: list[tuple[str, str]] = []
    while collected < days and offset < days * 2 + 20:
        d  = today - timedelta(days=offset)
        offset += 1
        if d.weekday() >= 5:
            continue
        dd = d.strftime("%Y-%m-%d")
        d8 = d.strftime("%Y%m%d")
        if dd in skip_dates:
            log.info(f"  SKIP {dd} (in DB)")
            collected += 1
            continue
        target_dates.append((dd, d8))
        collected += 1

    if not target_dates:
        return pd.DataFrame()

    log.info(f"  Fetching {len(target_dates)} dates in parallel...")

    def fetch_day(dd_d8: tuple) -> pd.DataFrame:
        dd, d8 = dd_d8
        # 同一天 TWSE + TPEx 並行
        with ThreadPoolExecutor(max_workers=2) as ex:
            f_twse = ex.submit(_twse_inst_one, d8)
            f_tpex = ex.submit(_tpex_inst_one, d8)
            twse_df = f_twse.result()
            tpex_df = f_tpex.result()
        parts = [f for f in [twse_df, tpex_df] if not f.empty]
        if not parts:
            return pd.DataFrame()
        return pd.concat(parts, ignore_index=True).drop_duplicates(subset=["code"])

    # 外層：多天並行（最多 4 個日期同時）
    # 注意：TWSE 有速率限制（每 5 秒 3 個請求），max_workers=4 是安全值
    with ThreadPoolExecutor(max_workers=4) as ex:
        futures = {ex.submit(fetch_day, item): item[0] for item in target_dates}
        for fut in as_completed(futures):
            dd = futures[fut]
            try:
                df = fut.result()
                if not df.empty:
                    frames.append(df)
            except Exception as e:
                log.warning(f"  fetch_day {dd} failed: {e}")
        # 每批次間稍等，避免觸發速率限制
        time.sleep(0.3)

    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def fetch_prices_parallel() -> pd.DataFrame:
    """
    並行抓取 TWSE + TPEx 收盤價
    兩個來源同時發出請求，約節省一半時間
    """
    log.info("  Fetching TWSE + TPEx prices in parallel...")
    with ThreadPoolExecutor(max_workers=2) as ex:
        f_twse = ex.submit(_twse_price)
        f_tpex = ex.submit(_tpex_price)
        twse = f_twse.result()
        tpex = f_tpex.result()

    parts = [f for f in [twse, tpex] if not f.empty]
    if not parts:
        return pd.DataFrame()

    df = pd.concat(parts, ignore_index=True).drop_duplicates(subset=["code"])
    log.info(f"  All prices: {len(df)} rows (TWSE={len(twse)}, TPEx={len(tpex)})")
    return df


# ─────────────────────────────────────────────────────────
#  輸入檔案
# ─────────────────────────────────────────────────────────
def load_groups() -> dict[str, list[str]]:
    raw = GROUP_CSV.read_bytes()
    try:    text = raw.decode("big5")
    except: text = raw.decode("cp950", errors="replace")
    lines   = text.strip().replace("\r\n", "\n").replace("\r", "\n").split("\n")
    headers = [h.strip() for h in lines[0].split(",")]
    groups  = {h: [] for h in headers if h}
    for line in lines[1:]:
        cols = line.split(",")
        for i, h in enumerate(headers):
            if h and i < len(cols) and cols[i].strip():
                groups[h].append(cols[i].strip())
    log.info(f"Groups: {len(groups)} 族群, {sum(len(v) for v in groups.values())} 個股")
    return groups


def load_stock_names() -> dict[str, str]:
    raw = STOCKS_CSV.read_bytes()
    try:    text = raw.decode("cp950")
    except: text = raw.decode("big5", errors="replace")
    result = {}
    for line in text.strip().replace("\r\n", "\n").split("\n")[1:]:
        p = line.strip().split(",")
        if len(p) >= 2 and p[0].strip() and p[1].strip():
            result[p[0].strip().zfill(4)] = p[1].strip()
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
    計算族群資金流向

    單位說明：
      T86 欄位 14 = 三大合計淨買超（張）
      億元換算 = 張數 × 收盤價 × 1000股/張 ÷ 1e8
      STOCK_DAY_ALL TradeVolume = 股數（僅作成交量記錄，不用於換算）
    """
    if inst_df.empty or price_df.empty:
        log.error("資料不足，無法計算")
        return [], {}

    inst  = inst_df.copy()
    price = price_df.copy()
    inst["code"]  = inst["code"].astype(str).str.zfill(4)
    price["code"] = price["code"].astype(str).str.zfill(4)

    inst_dates = sorted(inst["date"].unique())
    log.info(f"  inst: {inst_dates[0]} ~ {inst_dates[-1]} ({len(inst_dates)} days)")

    # ── 今日收盤價 ──
    latest_date = sorted(price["date"].unique())[-1]
    p_now = price[price["date"] == latest_date].set_index("code")
    close = p_now["close"].astype(float)
    log.info(f"  price: {latest_date}, {len(close)} stocks")

    # ── 歷史收盤（漲跌幅用）──
    price_hist = db_load_price_history(days=7)
    if price_hist.empty:
        log.warning("  無歷史收盤，漲跌幅設 0（明日起正常）")
        price_hist = price[["date","code","close"]].copy()

    hist_dates = sorted(price_hist["date"].unique())
    p5_date = hist_dates[-5] if len(hist_dates) >= 5 else hist_dates[0]
    p1_date = hist_dates[-2] if len(hist_dates) >= 2 else hist_dates[0]
    p5 = price_hist[price_hist["date"] == p5_date].set_index("code")["close"].astype(float)
    p1 = price_hist[price_hist["date"] == p1_date].set_index("code")["close"].astype(float)
    log.info(f"  chg_1d 基準: {p1_date}, chg_5d 基準: {p5_date}")

    # ── inst 聚合（張數）──
    last5            = set(inst_dates[-5:])
    latest_inst_date = inst_dates[-1]
    agg_1d  = inst[inst["date"] == latest_inst_date].groupby("code")["total"].sum()
    agg_5d  = inst[inst["date"].isin(last5)].groupby("code")["total"].sum()
    agg_20d = inst.groupby("code")["total"].sum()

    # ── 億元換算（只用 close 做乘法，不用 vol）──
    def yi(agg: pd.Series) -> pd.Series:
        return (agg.reindex(close.index, fill_value=0) * close * 1000 / 1e8).round(4)

    net_1d  = yi(agg_1d)
    net_5d  = yi(agg_5d)
    net_20d = yi(agg_20d)

    # ── 漲跌幅 ──
    def pct(base: pd.Series) -> pd.Series:
        b     = base.reindex(close.index, fill_value=np.nan).astype(float)
        valid = b.notna() & (b > 0)
        r     = pd.Series(np.nan, index=close.index)
        r[valid] = ((close[valid] - b[valid]) / b[valid] * 100).round(2)
        return r

    chg_1d = pct(p1)
    chg_5d = pct(p5)

    api_nm = p_now["name"].to_dict()
    def nm(code: str) -> str:
        return name_map.get(code, "") or api_nm.get(code, "")

    # ── 族群 ──
    records: list[dict] = []
    details: dict[str, list[dict]] = {}

    for gname, raw_codes in groups.items():
        codes_set = {str(c).zfill(4) for c in raw_codes}
        matched   = [c for c in codes_set if c in close.index]

        stocks = []
        for c in matched:
            c1d = float(chg_1d.get(c, np.nan))
            c5d = float(chg_5d.get(c, np.nan))
            if np.isnan(c1d) or abs(c1d) > 11: c1d = 0.0
            if np.isnan(c5d): c5d = 0.0
            stocks.append({
                "code":    c,
                "name":    nm(c),
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
                "g":gname,"cnt":len(raw_codes),"matched":0,
                "net_1d":0.0,"net_5d":0.0,"net_20d":0.0,
                "chg_1d":0.0,"chg_5d":0.0,"label":"觀望",
            })
            continue

        def gmean(s, idx):
            v = s.reindex(idx).dropna()
            return round(float(v.mean()), 2) if not v.empty else 0.0

        g1  = round(float(net_1d.reindex(matched,  fill_value=0).sum()), 3)
        g5  = round(float(net_5d.reindex(matched,  fill_value=0).sum()), 3)
        g20 = round(float(net_20d.reindex(matched, fill_value=0).sum()), 3)
        gc1 = gmean(chg_1d, matched)
        gc5 = gmean(chg_5d, matched)

        if   g5 >  2 and gc5 > 1:  label = "主力"
        elif g5 >  0 and gc5 <= 1: label = "輪動"
        elif g5 < -2:              label = "退潮"
        else:                      label = "觀望"

        records.append({
            "g":gname,"cnt":len(raw_codes),"matched":len(matched),
            "net_1d":g1,"net_5d":g5,"net_20d":g20,
            "chg_1d":gc1,"chg_5d":gc5,"label":label,
        })

    log.info(f"  Groups {len(records)}: "
             + ", ".join(f"{l}={sum(1 for r in records if r['label']==l)}"
                         for l in ["主力","輪動","退潮","觀望"]))
    return records, details


# ─────────────────────────────────────────────────────────
#  JSON 輸出
# ─────────────────────────────────────────────────────────
def export_json(records: list[dict], details: dict[str, list[dict]]) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    attach = lambda rows: [{**r, "stocks": details.get(r["g"], [])} for r in rows]

    bubble = [{
        **r,
        "x":    r["net_5d"],
        "y":    r["net_1d"],
        "size": max(10, min(72, abs(r["net_5d"]) * 2.8 + 12)),
        "stocks": details.get(r["g"], []),
    } for r in records]

    inflow = attach(sorted(
        [r for r in records if r["net_5d"] > 0 and r["chg_5d"] < 10],
        key=lambda x: x["net_5d"], reverse=True))

    stealth = attach(sorted(
        [r for r in records if r["net_1d"] > 0 and r["net_5d"] > 0 and r["chg_5d"] < 0],
        key=lambda x: x["net_5d"], reverse=True))

    _jdump("bubble_data.json",          bubble)
    _jdump("inflow_low_gain.json",       inflow)
    _jdump("stealth_accumulation.json",  stealth)
    _jdump("group_stats.json",          [{k:v for k,v in r.items() if k!="stocks"} for r in records])
    _jdump("metadata.json", {
        "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "trade_date":   TODAY,
        "groups":       len(records),
        "inflow":       len(inflow),
        "stealth":      len(stealth),
    })
    log.info(f"  JSON: bubble={len(bubble)}, inflow={len(inflow)}, stealth={len(stealth)}")


def _jdump(fname: str, data):
    with open(DATA_DIR / fname, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


# ─────────────────────────────────────────────────────────
#  Pipeline
# ─────────────────────────────────────────────────────────
def step_inst(force: bool) -> pd.DataFrame:
    log.info("=== Step 2: 三大法人（TWSE上市 + TPEx上櫃）===")
    cached = set(db_inst_dates(25))

    if not force and db_has_inst_today():
        log.info("  [CACHE] 今日資料已在 DB")
        return db_load_inst(days=20)

    # 並行抓取，已在 DB 的日期自動跳過
    new_df = fetch_inst_parallel(
        days=20,
        skip_dates=cached if not force else set()
    )

    if not new_df.empty:
        # 只儲存 DB 尚未有的日期（不重複寫入）
        new_only = new_df[~new_df["date"].isin(cached)] if not force else new_df
        if not new_only.empty:
            db_save_inst(new_only)

    result = db_load_inst(days=20)
    if result.empty:
        log.error("  DB 無 inst 資料，pipeline 無法繼續")
    return result


def step_price(force: bool) -> pd.DataFrame:
    log.info("=== Step 3: 收盤價（TWSE上市 + TPEx上櫃）===")

    if not force and db_has_price_today():
        log.info("  [CACHE] 今日收盤已在 DB")
        return db_load_price(TODAY)

    # 並行抓取 TWSE + TPEx
    df = fetch_prices_parallel()

    if not df.empty:
        db_save_price(df, TODAY)
        return df

    # 收盤前或 API 失敗，用 DB 最新資料繼續計算
    fallback = db_load_price()
    if not fallback.empty:
        log.warning(f"  API 失敗，使用 DB 最新日 ({fallback['date'].max()})")
    else:
        log.error("  無任何收盤價資料")
    return fallback


def step_compute(groups: dict, name_map: dict,
                 inst_df: pd.DataFrame, price_df: pd.DataFrame):
    log.info("=== Step 4: 計算 ===")
    if inst_df.empty or price_df.empty:
        log.error("  資料不足")
        return
    records, details = compute(inst_df, price_df, groups, name_map)
    if records:
        export_json(records, details)


# ─────────────────────────────────────────────────────────
#  工具
# ─────────────────────────────────────────────────────────
def _fmt(d) -> str:
    s = str(d).strip()
    if len(s) == 8 and s.isdigit():
        return f"{s[:4]}-{s[4:6]}-{s[6:8]}"
    return s[:10].replace("/", "-")

def _int(v) -> int:
    try:    return int(str(v).replace(",","").strip())
    except: return 0

def _flt(v) -> float:
    try:    return float(str(v).replace(",","").strip())
    except: return 0.0


# ─────────────────────────────────────────────────────────
#  Main
# ─────────────────────────────────────────────────────────
def main():
    ap = argparse.ArgumentParser(description="TW$FLOW Daily Pipeline")
    ap.add_argument("--force",    action="store_true", help="忽略快取，強制重新抓取")
    ap.add_argument("--dry-run",  action="store_true", help="只從 DB 重算 JSON")
    args = ap.parse_args()

    t0 = time.time()
    log.info("=" * 60)
    log.info(f"TW$FLOW  {datetime.now()}  trade_date={TODAY}")
    if args.force:   log.info("  *** FORCE ***")
    if args.dry_run: log.info("  *** DRY-RUN ***")
    log.info("=" * 60)

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    db_init()

    log.info("=== Step 1: 讀取設定 ===")
    groups   = load_groups()
    name_map = load_stock_names()

    if args.dry_run:
        inst_df  = db_load_inst(days=20)
        price_df = db_load_price()
    else:
        inst_df  = step_inst(force=args.force)
        price_df = step_price(force=args.force)

    step_compute(groups, name_map, inst_df, price_df)

    # DB 狀態
    with _con() as c:
        ni  = c.execute("SELECT COUNT(*) FROM inst").fetchone()[0]
        np_ = c.execute("SELECT COUNT(*) FROM price").fetchone()[0]
        li  = c.execute("SELECT MAX(date) FROM inst").fetchone()[0]
        lp  = c.execute("SELECT MAX(date) FROM price").fetchone()[0]

    elapsed = time.time() - t0
    log.info(f"DB: inst={ni} rows (latest={li}), price={np_} rows (latest={lp})")
    log.info(f"Pipeline complete ✓  elapsed={elapsed:.1f}s")


if __name__ == "__main__":
    main()
