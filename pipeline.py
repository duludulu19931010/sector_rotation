#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
TW$FLOW pipeline.py  ─  每日資料更新主程式
================================================
資料來源（全部官方免費 API，無需 key）：

  上市收盤 + 三大法人
    個股每日行情  GET https://www.twse.com.tw/exchangeReport/STOCK_DAY
                  params: response=json, date=YYYYMMDD, stockNo=代號
                  回傳: [日期, 成交股數, 成交金額, 開盤價, 最高價, 最低價, 收盤價, 漲跌, 成交筆數]
    全市場行情    GET https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL
                  回傳: [{Code,Name,TradeVolume,TradeValue,OpeningPrice,...,ClosingPrice,...}]
    三大法人 T86  GET https://www.twse.com.tw/rwd/zh/fund/T86
                  params: response=json, date=YYYYMMDD, selectType=ALL
                  回傳: data[i][0]=代號, [1]=名稱, [4]=外資淨, [7]=投信淨, [14]=三大合計（張）

  上櫃收盤 + 三大法人
    個股行情      GET https://www.tpex.org.tw/openapi/v1/tpex_mainboard_quotes
                  回傳: [{SecuritiesCompanyCode, CompanyName, Close, Open, High, Low,
                         TradeVolume, TradeValue, ...}]
    三大法人      GET https://www.tpex.org.tw/openapi/v1/tpex_mainboard_institutional_investors
                  回傳: [{SecuritiesCompanyCode, CompanyName,
                         ForeignInvestorsNetBuySell, DealersNetBuySell,
                         InvestmentTrustNetBuySell, TotalNetBuySell, ...}]

指標定義：
  今日淨買賣超（億）= (成交股數 / 成交金額) × 三大法人總買賣超張數 × 1000
  五日淨買賣超（億）= 最新五筆今日淨買賣超之和
  二十日淨買賣超（億）= 最新二十筆今日淨買賣超之和
  今日漲跌幅（%）= (今日收盤價 - 前一交易日收盤價) / 前一交易日收盤價 × 100
  五日漲跌幅（%）= (今日收盤價 - 前六交易日收盤價) / 前六交易日收盤價 × 100

用法：
  python pipeline.py              正常執行（有快取自動跳過 API）
  python pipeline.py --force      強制重抓（忽略快取）
  python pipeline.py --dry-run    只從 DB 重算 JSON
"""

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
from pathlib import Path

import numpy as np
import pandas as pd
import requests

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ─── 路徑設定 ────────────────────────────────────────────
ROOT       = Path(__file__).resolve().parent
DB_FILE    = ROOT / "db" / "market.db"
DATA_DIR   = ROOT / "docs" / "assets" / "data"
GROUP_CSV  = ROOT / "input" / "Group.csv"
NAMES_CSV  = ROOT / "input" / "stock_list.csv"
LOG_FILE   = ROOT / "pipeline.log"
TODAY      = date.today().strftime("%Y-%m-%d")    # YYYY-MM-DD
TODAY_8    = date.today().strftime("%Y%m%d")      # YYYYMMDD（給 TWSE API）

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
         retries: int = 3, delay: float = 1.5, timeout: int = 25):
    """HTTP GET，回傳 dict/list 或 None。空 body 視為失敗。"""
    for i in range(retries):
        try:
            r = requests.get(url, params=params, headers=HEADERS,
                             timeout=timeout, verify=verify)
            r.raise_for_status()
            if not r.text.strip():
                raise ValueError("Empty response body")
            data = r.json()
            if isinstance(data, (dict, list)):
                return data
        except Exception as e:
            log.warning(f"  GET [{i+1}/{retries}] {url} — {e}")
            if i < retries - 1:
                time.sleep(delay * (i + 1))
    return None


# ─────────────────────────────────────────────────────────
#  資料庫
# ─────────────────────────────────────────────────────────
DDL = """
-- 每日個股資料（含三大法人）
-- 一筆 = 一個代號一個交易日
CREATE TABLE IF NOT EXISTS daily (
    date          TEXT NOT NULL,     -- YYYY-MM-DD
    code          TEXT NOT NULL,     -- 4位代號
    market        TEXT NOT NULL,     -- TWSE / TPEx
    name          TEXT DEFAULT '',
    open_price    REAL DEFAULT 0,
    close_price   REAL DEFAULT 0,
    high_price    REAL DEFAULT 0,
    low_price     REAL DEFAULT 0,
    trade_volume  INTEGER DEFAULT 0, -- 成交股數
    trade_value   INTEGER DEFAULT 0, -- 成交金額（元）
    inst_net      INTEGER DEFAULT 0, -- 三大法人淨買超（張）
    net_yi        REAL DEFAULT 0,    -- 今日淨買賣超（億）= (volume/value)*inst_net*1000
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
    con.execute("PRAGMA cache_size=-32000")
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
    log.info(f"DB ready: {DB_FILE}")


def db_trade_dates(n: int = 30) -> list[str]:
    """回傳 DB 中有完整資料的最近 n 個交易日（自適應閾值）"""
    with _db() as c:
        rows = c.execute(
            "SELECT date, COUNT(*) AS cnt FROM daily GROUP BY date ORDER BY date DESC"
        ).fetchall()
    if not rows:
        return []
    max_cnt = max(r["cnt"] for r in rows)
    thr = max(50, int(max_cnt * 0.5))
    return [r["date"] for r in rows if r["cnt"] >= thr][:n]


def db_has_today() -> bool:
    dates = db_trade_dates(1)
    return bool(dates) and dates[0] == TODAY


def db_load(days: int = 21) -> pd.DataFrame:
    """讀取近 days 個完整交易日的全部資料"""
    dates = db_trade_dates(days)
    if not dates:
        return pd.DataFrame()
    ph = ",".join("?" * len(dates))
    with _db() as c:
        df = pd.read_sql_query(
            f"SELECT date,code,market,name,open_price,close_price,"
            f"trade_volume,trade_value,inst_net,net_yi "
            f"FROM daily WHERE date IN ({ph}) ORDER BY date",
            c, params=dates
        )
    log.info(f"  DB load: {len(df)} rows, {df['date'].nunique()} dates")
    return df


def db_save(df: pd.DataFrame) -> int:
    if df is None or df.empty:
        return 0
    rows = [(
        r["date"], str(r["code"]).zfill(4), r.get("market",""),
        r.get("name",""),
        _flt(r.get("open_price")),  _flt(r.get("close_price")),
        _flt(r.get("high_price")),  _flt(r.get("low_price")),
        _int(r.get("trade_volume")),_int(r.get("trade_value")),
        _int(r.get("inst_net")),    _flt(r.get("net_yi")),
    ) for _, r in df.iterrows()]
    with _db() as c:
        c.executemany("""
            INSERT OR REPLACE INTO daily
            (date,code,market,name,open_price,close_price,high_price,low_price,
             trade_volume,trade_value,inst_net,net_yi)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
        """, rows)
    # 統計各日筆數
    from collections import Counter
    cnt = Counter(r[0] for r in rows)
    for d, n in sorted(cnt.items()):
        log.info(f"  Saved {n} rows for {d}")
    return len(rows)


# ─────────────────────────────────────────────────────────
#  TWSE 爬蟲
# ─────────────────────────────────────────────────────────
def twse_t86(date8: str) -> dict[str, int]:
    """
    T86 三大法人買賣超
    回傳: {代號: 三大合計淨買超張數}
    欄位索引 14 = 三大法人合計淨買超（張）
    """
    data = _get("https://www.twse.com.tw/rwd/zh/fund/T86",
                params={"response": "json", "date": date8, "selectType": "ALL"})
    if not isinstance(data, dict) or data.get("stat") != "OK":
        return {}
    result = {}
    for row in data.get("data", []):
        if len(row) >= 15:
            code = str(row[0]).strip().zfill(4)
            result[code] = _int(row[14])
    log.info(f"  T86(TWSE) {date8}: {len(result)} stocks")
    return result


def twse_day_all() -> dict[str, dict]:
    """
    STOCK_DAY_ALL 全市場當日收盤行情
    回傳: {代號: {name, open, close, high, low, volume, value}}
    """
    data = _get("https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL")
    if not isinstance(data, list):
        return {}
    result = {}
    for item in data:
        code = str(item.get("Code", "")).strip().zfill(4)
        if not code.strip("0"):
            continue
        result[code] = {
            "name":   item.get("Name", ""),
            "open":   _flt(item.get("OpeningPrice")),
            "close":  _flt(item.get("ClosingPrice")),
            "high":   _flt(item.get("HighestPrice")),
            "low":    _flt(item.get("LowestPrice")),
            "volume": _int(item.get("TradeVolume")),   # 股數
            "value":  _int(item.get("TradeValue")),    # 元
        }
    log.info(f"  STOCK_DAY_ALL: {len(result)} stocks")
    return result


# ─────────────────────────────────────────────────────────
#  TPEx 爬蟲
# ─────────────────────────────────────────────────────────
def tpex_quotes() -> dict[str, dict]:
    """
    tpex_mainboard_quotes 上櫃收盤行情
    回傳: {代號: {name, open, close, high, low, volume, value}}
    """
    for ep in [
        "https://www.tpex.org.tw/openapi/v1/tpex_mainboard_quotes",
        "https://www.tpex.org.tw/openapi/v1/tpex_mainboard_daily_close_quotes",
    ]:
        data = _get(ep, verify=False, retries=2, delay=1.0)
        if not isinstance(data, list) or len(data) < 5:
            continue
        result = {}
        for item in data:
            code = str(item.get("SecuritiesCompanyCode",
                        item.get("Code", ""))).strip().zfill(4)
            if not code.strip("0"):
                continue
            close = _flt(item.get("Close", item.get("ClosingPrice", 0)))
            if close <= 0:
                continue
            result[code] = {
                "name":   item.get("CompanyName", item.get("Name", "")),
                "open":   _flt(item.get("Open",  item.get("OpeningPrice", 0))),
                "close":  close,
                "high":   _flt(item.get("High",  item.get("HighestPrice", 0))),
                "low":    _flt(item.get("Low",   item.get("LowestPrice",  0))),
                "volume": _int(item.get("TradeVolume", 0)),   # 股數
                "value":  _int(item.get("TradeValue",  0)),   # 元
            }
        if result:
            log.info(f"  TPEx quotes: {len(result)} stocks (from {ep.split('/')[-1]})")
            return result
    log.warning("  TPEx quotes: all endpoints failed")
    return {}


def tpex_inst() -> dict[str, int]:
    """
    tpex_mainboard_institutional_investors 上櫃三大法人
    回傳: {代號: 三大合計淨買超張數}
    """
    for ep in [
        "https://www.tpex.org.tw/openapi/v1/tpex_mainboard_institutional_investors",
        "https://www.tpex.org.tw/openapi/v1/tpex/fund/daily_institutional_buying_selling",
    ]:
        data = _get(ep, verify=False, retries=2, delay=1.0)
        if not isinstance(data, list) or len(data) < 5:
            continue
        result = {}
        for item in data:
            code = str(item.get("SecuritiesCompanyCode",
                        item.get("Code", ""))).strip().zfill(4)
            if not code.strip("0"):
                continue
            # 三大合計欄位名稱因版本不同
            total = _int(item.get("TotalNetBuySell",
                          item.get("NetBuySell",
                          item.get("ThreeInstitutionsNetBuySell", 0))))
            if total == 0:
                f = _int(item.get("ForeignInvestorsNetBuySell",  0))
                t = _int(item.get("InvestmentTrustNetBuySell",   0))
                d = _int(item.get("DealersNetBuySell",           0))
                total = f + t + d
            result[code] = total
        if result:
            log.info(f"  TPEx inst: {len(result)} stocks (from {ep.split('/')[-1]})")
            return result
    log.warning("  TPEx inst: all endpoints failed")
    return {}


# ─────────────────────────────────────────────────────────
#  並行抓取今日資料
# ─────────────────────────────────────────────────────────
def fetch_today_parallel() -> pd.DataFrame:
    """
    並行抓取 TWSE 和 TPEx 的行情 + 三大法人（4 個 API 同時跑）
    回傳今日資料 DataFrame
    """
    with ThreadPoolExecutor(max_workers=4) as pool:
        f_twse_price = pool.submit(twse_day_all)
        f_twse_inst  = pool.submit(twse_t86, TODAY_8)
        f_tpex_price = pool.submit(tpex_quotes)
        f_tpex_inst  = pool.submit(tpex_inst)

    twse_price = f_twse_price.result()
    twse_inst  = f_twse_inst.result()
    tpex_price = f_tpex_price.result()
    tpex_inst  = f_tpex_inst.result()

    log.info(f"  Parallel fetch done: TWSE {len(twse_price)} price / {len(twse_inst)} inst | "
             f"TPEx {len(tpex_price)} price / {len(tpex_inst)} inst")

    rows = []

    # TWSE 上市
    for code, p in twse_price.items():
        inst_net = twse_inst.get(code, 0)
        net_yi   = _calc_net_yi(p["volume"], p["value"], inst_net)
        rows.append({
            "date": TODAY, "code": code, "market": "TWSE",
            "name": p["name"],
            "open_price": p["open"],   "close_price": p["close"],
            "high_price": p["high"],   "low_price":   p["low"],
            "trade_volume": p["volume"], "trade_value": p["value"],
            "inst_net": inst_net, "net_yi": net_yi,
        })

    # TPEx 上櫃
    for code, p in tpex_price.items():
        inst_net = tpex_inst.get(code, 0)
        net_yi   = _calc_net_yi(p["volume"], p["value"], inst_net)
        rows.append({
            "date": TODAY, "code": code, "market": "TPEx",
            "name": p["name"],
            "open_price": p["open"],   "close_price": p["close"],
            "high_price": p["high"],   "low_price":   p["low"],
            "trade_volume": p["volume"], "trade_value": p["value"],
            "inst_net": inst_net, "net_yi": net_yi,
        })

    df = pd.DataFrame(rows)
    log.info(f"  Today combined: {len(df)} stocks "
             f"(TWSE={sum(1 for r in rows if r['market']=='TWSE')}, "
             f"TPEx={sum(1 for r in rows if r['market']=='TPEx')})")
    return df


def _calc_net_yi(volume: int, value: int, inst_net: int) -> float:
    """
    今日淨買賣超（億）= (成交股數 / 成交金額) × 三大法人總買賣超張數 × 1000
    若成交金額為 0 則回傳 0
    """
    if value == 0 or volume == 0:
        return 0.0
    # 成交股數/成交金額 = 1/均價（股/元），乘以張數*1000 = 億元
    return round((volume / value) * inst_net * 1000, 6)


# ─────────────────────────────────────────────────────────
#  補抓歷史資料（首次執行）
# ─────────────────────────────────────────────────────────
def fetch_history_parallel(need_dates: list[str]) -> pd.DataFrame:
    """
    補抓 DB 中缺少的歷史交易日資料
    TWSE T86 + STOCK_DAY_ALL 並行，但需逐日順序
    """
    if not need_dates:
        return pd.DataFrame()

    all_frames = []
    log.info(f"  Fetching history for {len(need_dates)} dates...")

    for dd in need_dates:
        d8 = dd.replace("-", "")
        # T86 和 TWSE day-all 並行，TPEx 也並行
        with ThreadPoolExecutor(max_workers=3) as pool:
            f_t86       = pool.submit(twse_t86, d8)
            f_tpex_p    = pool.submit(tpex_quotes)       # TPEx 只有當日
            f_tpex_i    = pool.submit(tpex_inst)

        t86      = f_t86.result()
        tpex_p   = f_tpex_p.result()
        tpex_i   = f_tpex_i.result()

        # TWSE 行情用 STOCK_DAY_ALL（只有當日，歷史要一支一支抓）
        # 這裡簡化：若是今日直接用全市場，歷史日就只補 T86 和用 DB 已有收盤
        if dd == TODAY:
            twse_p = twse_day_all()
        else:
            # 歷史日的 TWSE 行情從 STOCK_DAY_ALL 取不到，改用 DB 現有資料
            # 這裡不補歷史收盤，只補今日
            twse_p = {}

        rows = []
        for code, inst_net in t86.items():
            p = twse_p.get(code, {})
            net_yi = _calc_net_yi(p.get("volume",0), p.get("value",0), inst_net)
            rows.append({
                "date": dd, "code": code, "market": "TWSE",
                "name": p.get("name",""),
                "open_price": p.get("open",0),   "close_price": p.get("close",0),
                "high_price": p.get("high",0),   "low_price":   p.get("low",0),
                "trade_volume": p.get("volume",0),"trade_value": p.get("value",0),
                "inst_net": inst_net, "net_yi": net_yi,
            })
        for code, inst_net in tpex_i.items():
            p = tpex_p.get(code, {})
            net_yi = _calc_net_yi(p.get("volume",0), p.get("value",0), inst_net)
            rows.append({
                "date": dd, "code": code, "market": "TPEx",
                "name": p.get("name",""),
                "open_price": p.get("open",0),   "close_price": p.get("close",0),
                "high_price": p.get("high",0),   "low_price":   p.get("low",0),
                "trade_volume": p.get("volume",0),"trade_value": p.get("value",0),
                "inst_net": inst_net, "net_yi": net_yi,
            })
        if rows:
            all_frames.append(pd.DataFrame(rows))
        time.sleep(0.5)  # 避免被限速

    return pd.concat(all_frames, ignore_index=True) if all_frames else pd.DataFrame()


# ─────────────────────────────────────────────────────────
#  計算指標
# ─────────────────────────────────────────────────────────
def compute(hist_df: pd.DataFrame,
            groups: dict[str, list[str]],
            name_map: dict[str, str]) -> tuple[list[dict], dict[str, list[dict]]]:
    """
    從 DB 歷史資料計算族群指標

    hist_df 需含欄位：date, code, market, name, close_price, net_yi

    今日淨買賣超（億）= hist_df 最新日的 net_yi
    五日淨買賣超（億）= 最新五筆 net_yi 之和
    二十日淨買賣超（億）= 最新二十筆 net_yi 之和
    今日漲跌幅（%）= (今收 - 前一日收) / 前一日收 × 100
    五日漲跌幅（%）= (今收 - 前六日收) / 前六日收 × 100
    """
    if hist_df.empty:
        return [], {}

    df = hist_df.copy()
    df["code"] = df["code"].astype(str).str.zfill(4)
    df["date"] = df["date"].astype(str)

    all_dates = sorted(df["date"].unique())
    latest    = all_dates[-1]
    log.info(f"  Dates: {all_dates[0]} ~ {latest} ({len(all_dates)} days)")

    # ── 漲跌幅基準日 ───────────────────────────────────────
    # 今日漲跌幅：前一個交易日
    d_prev1 = all_dates[-2] if len(all_dates) >= 2 else all_dates[-1]
    # 五日漲跌幅：前六個交易日（即 latest 往前數第 6 個）
    d_prev6 = all_dates[-6] if len(all_dates) >= 6 else all_dates[0]
    log.info(f"  chg_1d base: {d_prev1}, chg_5d base: {d_prev6}")

    # ── pivot：code × date → close, net_yi ─────────────────
    close_pv = df.pivot_table(index="code", columns="date",
                               values="close_price", aggfunc="first")
    net_pv   = df.pivot_table(index="code", columns="date",
                               values="net_yi", aggfunc="first")

    # 最新收盤
    close_now  = close_pv[latest].dropna() if latest in close_pv.columns else pd.Series(dtype=float)

    # 漲跌幅
    def pct(base_date: str) -> pd.Series:
        if base_date not in close_pv.columns:
            return pd.Series(0.0, index=close_now.index)
        base = close_pv[base_date]
        c = close_now.reindex(base.index)
        valid = base.notna() & (base > 0) & c.notna()
        s = pd.Series(0.0, index=base.index)
        s[valid] = ((c[valid] - base[valid]) / base[valid] * 100).round(2)
        return s

    chg_1d = pct(d_prev1)
    chg_5d = pct(d_prev6)

    # 各期 net_yi 加總
    last5  = all_dates[-5:]
    last20 = all_dates[-20:]
    net_1d  = net_pv[latest]                   if latest in net_pv.columns else pd.Series(dtype=float)
    net_5d  = net_pv[[c for c in last5  if c in net_pv.columns]].sum(axis=1)
    net_20d = net_pv[[c for c in last20 if c in net_pv.columns]].sum(axis=1)

    # 名稱：優先 stock_list.csv，再 DB 名稱
    db_names = df[df["date"] == latest].set_index("code")["name"].to_dict()
    def name(code: str) -> str:
        return name_map.get(code, "") or db_names.get(code, "")

    # ── 族群彙總 ──────────────────────────────────────────
    records:list[dict] = []
    details:dict[str,list[dict]] = {}

    for gname, raw_codes in groups.items():
        codes = [str(c).zfill(4) for c in raw_codes
                 if str(c).zfill(4) in close_now.index]

        stocks = []
        for c in codes:
            c1 = float(chg_1d.get(c, 0) or 0)
            if abs(c1) > 11: c1 = 0.0  # 台股漲跌停 ±10%
            stocks.append({
                "code":    c,
                "name":    name(c),
                "close":   round(float(close_now.get(c, 0)), 2),
                "net_1d":  round(float(net_1d.get(c,  0) or 0), 4),
                "net_5d":  round(float(net_5d.get(c,  0) or 0), 4),
                "net_20d": round(float(net_20d.get(c, 0) or 0), 4),
                "chg_1d":  round(c1, 2),
                "chg_5d":  round(float(chg_5d.get(c, 0) or 0), 2),
            })
        stocks.sort(key=lambda x: x["net_1d"], reverse=True)
        details[gname] = stocks

        if not codes:
            records.append(_empty(gname, len(raw_codes)))
            continue

        def _s(s: pd.Series) -> float:
            v = s.reindex(codes)
            return float(v.dropna().sum())
        def _m(s: pd.Series) -> float:
            v = s.reindex(codes)
            vals = v.dropna()
            return round(float(vals.mean()), 2) if not vals.empty else 0.0

        g1  = round(_s(net_1d),  3)
        g5  = round(_s(net_5d),  3)
        g20 = round(_s(net_20d), 3)
        gc1 = _m(chg_1d)
        gc5 = _m(chg_5d)

        if   g5 >  2 and gc5 > 1:  label = "主力"
        elif g5 >  0 and gc5 <= 1: label = "輪動"
        elif g5 < -2:              label = "退潮"
        else:                      label = "觀望"

        records.append({
            "g": gname, "cnt": len(raw_codes), "matched": len(codes),
            "net_1d": g1, "net_5d": g5, "net_20d": g20,
            "chg_1d": gc1, "chg_5d": gc5, "label": label,
        })

    log.info(f"  Groups: {len(records)} | "
             + " | ".join(f"{l}={sum(1 for r in records if r['label']==l)}"
                          for l in ["主力","輪動","退潮","觀望"]))
    return records, details


def _empty(gname: str, cnt: int) -> dict:
    return {"g": gname, "cnt": cnt, "matched": 0,
            "net_1d":0.0,"net_5d":0.0,"net_20d":0.0,
            "chg_1d":0.0,"chg_5d":0.0,"label":"觀望"}


# ─────────────────────────────────────────────────────────
#  JSON 輸出
# ─────────────────────────────────────────────────────────
def export_json(records, details, trade_date: str = TODAY) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    bubble = [{
        **r,
        "x":      r["net_5d"],
        "y":      r["net_1d"],
        "size":   max(10, min(72, abs(r["net_5d"]) * 2.8 + 12)),
        "stocks": details.get(r["g"], []),
    } for r in records]

    def attach(filt):
        return [{**r, "stocks": details.get(r["g"],[])} for r in filt]

    inflow  = attach(sorted([r for r in records if r["net_5d"]>0 and r["chg_5d"]<10],
                             key=lambda x: x["net_5d"], reverse=True))
    stealth = attach(sorted([r for r in records
                              if r["net_1d"]>0 and r["net_5d"]>0 and r["chg_5d"]<0],
                             key=lambda x: x["net_5d"], reverse=True))

    _jdump("bubble_data.json",         bubble)
    _jdump("inflow_low_gain.json",     inflow)
    _jdump("stealth_accumulation.json",stealth)
    _jdump("group_stats.json",         [{k:v for k,v in r.items() if k!="stocks"}
                                         for r in records])
    _jdump("metadata.json", {
        "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "trade_date":   trade_date,
        "groups":       len(records),
        "inflow":       len(inflow),
        "stealth":      len(stealth),
    })
    log.info(f"  JSON: bubble={len(bubble)}, inflow={len(inflow)}, stealth={len(stealth)}")


def _jdump(fname: str, data):
    with open(DATA_DIR / fname, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


# ─────────────────────────────────────────────────────────
#  輸入檔
# ─────────────────────────────────────────────────────────
def load_groups() -> dict[str, list[str]]:
    raw = GROUP_CSV.read_bytes()
    text = raw.decode("big5" if b"\xb0\xea" in raw else "cp950", errors="replace")
    lines = text.strip().replace("\r\n","\n").replace("\r","\n").split("\n")
    headers = [h.strip() for h in lines[0].split(",")]
    g = {h:[] for h in headers if h}
    for line in lines[1:]:
        cols = line.split(",")
        for i,h in enumerate(headers):
            if h and i<len(cols) and cols[i].strip():
                g[h].append(cols[i].strip())
    log.info(f"Groups: {len(g)} 族群, {sum(len(v) for v in g.values())} 個股")
    return g


def load_names() -> dict[str, str]:
    raw = NAMES_CSV.read_bytes()
    text = raw.decode("cp950", errors="replace")
    nm = {}
    for line in text.strip().replace("\r\n","\n").split("\n")[1:]:
        p = line.strip().split(",")
        if len(p)>=2 and p[0].strip() and p[1].strip():
            nm[p[0].strip().zfill(4)] = p[1].strip()
    log.info(f"Stock names: {len(nm)}")
    return nm


# ─────────────────────────────────────────────────────────
#  型別轉換
# ─────────────────────────────────────────────────────────
def _flt(v) -> float:
    try:    return float(str(v).replace(",","").strip())
    except: return 0.0

def _int(v) -> int:
    try:    return int(str(v).replace(",","").strip())
    except: return 0


# ─────────────────────────────────────────────────────────
#  Main
# ─────────────────────────────────────────────────────────
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--force",    action="store_true")
    ap.add_argument("--dry-run",  action="store_true")
    args = ap.parse_args()

    log.info("="*60)
    log.info(f"TW$FLOW  {datetime.now()}  trade_date={TODAY}")
    if args.force:   log.info("  *** FORCE ***")
    if args.dry_run: log.info("  *** DRY-RUN ***")
    log.info("="*60)

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    db_init()

    # ── Step 1：讀設定檔 ──────────────────────────────────
    log.info("=== Step 1: Load config ===")
    groups   = load_groups()
    name_map = load_names()

    if not args.dry_run:
        # ── Step 2：確認今日資料是否已在 DB ────────────────
        log.info("=== Step 2: Check cache ===")
        if not args.force and db_has_today():
            log.info("  [CACHE] Today already in DB, skipping API calls")
        else:
            # ── Step 3：並行抓取今日資料 ─────────────────
            log.info("=== Step 3: Fetch today (parallel) ===")
            today_df = fetch_today_parallel()

            if not today_df.empty:
                db_save(today_df)
            else:
                log.warning("  No data fetched today (收盤前？或 API 暫時不穩定）")
                # 若 DB 已有近期資料，仍可繼續計算

        # ── Step 4：補抓歷史（首次執行）──────────────────
        have_dates = db_trade_dates(25)
        log.info(f"=== Step 4: History check ({len(have_dates)} dates in DB) ===")

        if len(have_dates) < 21:
            # 算出近 30 個工作日，找出 DB 缺少的
            need = []
            d = date.today()
            while len(need) < 21 and len(need) + len(have_dates) < 25:
                if d.weekday() < 5:
                    ds = d.strftime("%Y-%m-%d")
                    if ds not in have_dates:
                        need.append(ds)
                d -= timedelta(days=1)
            if need:
                log.info(f"  Need to fetch {len(need)} historical dates")
                hist_df = fetch_history_parallel(sorted(need))
                if not hist_df.empty:
                    db_save(hist_df)
            else:
                log.info("  No missing historical dates")

    # ── Step 5：計算並輸出 ───────────────────────────────
    log.info("=== Step 5: Compute & export ===")
    hist = db_load(days=21)
    if hist.empty:
        log.error("  No data in DB, cannot compute")
        return

    records, details = compute(hist, groups, name_map)
    trade_date = hist["date"].max()
    export_json(records, details, trade_date)

    # DB 摘要
    with _db() as c:
        total = c.execute("SELECT COUNT(*) FROM daily").fetchone()[0]
        latest_date = c.execute("SELECT MAX(date) FROM daily").fetchone()[0]
    log.info(f"DB: {total} rows, latest={latest_date}")
    log.info("Pipeline complete ✓")


if __name__ == "__main__":
    main()
