"""
src/scrapers/twse.py
TWSE / TPEx 官方 API 爬蟲

資料來源（全為公開 API，無需 key）：
  三大法人: https://www.twse.com.tw/rwd/zh/fund/T86
  上市收盤: https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL
  上櫃收盤: https://www.tpex.org.tw/openapi/v1/tpex/exchangeReport/daily_close_quotes
           （TPEx 憑證缺 Subject Key Identifier → verify=False）
"""

import logging
import time
from datetime import datetime, timedelta

import pandas as pd
import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logger = logging.getLogger(__name__)

_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept":     "application/json, text/plain, */*",
    "Referer":    "https://www.twse.com.tw/",
}


# ── HTTP helper ───────────────────────────────────────

def _get(url: str, params: dict = None, verify: bool = True,
         retries: int = 3, delay: float = 1.5) -> dict | list | None:
    for attempt in range(retries):
        try:
            r = requests.get(url, params=params, headers=_HEADERS,
                             timeout=25, verify=verify)
            r.raise_for_status()
            data = r.json()
            if isinstance(data, (dict, list)):
                return data
            return None
        except Exception as e:
            logger.warning(f"GET attempt {attempt+1}/{retries} failed: {url} — {e}")
            if attempt < retries - 1:
                time.sleep(delay * (attempt + 1))
    return None


# ── 三大法人 T86 ─────────────────────────────────────

def fetch_t86(date_str: str) -> pd.DataFrame:
    """
    抓取單日三大法人買賣超
    date_str: YYYYMMDD

    T86 欄位索引（TWSE 官方）：
      0: 股票代號  1: 股票名稱
      2: 外資買進  3: 外資賣出  4: 外資淨買超
      5: 投信買進  6: 投信賣出  7: 投信淨買超
      8: 自營買進（自行）  9: 自營賣出（自行）  10: 自營淨買超（自行）
      11: 自營買進（避險） 12: 自營賣出（避險） 13: 自營淨買超（避險）
      14: 三大法人合計淨買超
    """
    url    = "https://www.twse.com.tw/rwd/zh/fund/T86"
    params = {"response": "json", "date": date_str, "selectType": "ALL"}
    data   = _get(url, params)

    if not data or not isinstance(data, dict) or data.get("stat") != "OK":
        return pd.DataFrame()

    rows = []
    for row in data.get("data", []):
        if len(row) < 15:
            continue
        rows.append({
            "code":        str(row[0]).strip().zfill(4),
            "name":        str(row[1]).strip(),
            "foreign_net": _to_int(row[4]),
            "trust_net":   _to_int(row[7]),
            "dealer_net":  _to_int(row[10]) + _to_int(row[13]),
            "total_net":   _to_int(row[14]),
            "trade_date":  date_str,
        })

    df = pd.DataFrame(rows)
    logger.info(f"T86 {date_str}: {len(df)} rows")
    return df


def fetch_t86_multi(days: int = 5,
                    cached_dates: set = None) -> pd.DataFrame:
    """
    抓近 days 個交易日的 T86
    cached_dates: 已在 DB 的日期集合（跳過不重抓）
    """
    cached_dates = cached_dates or set()
    frames = []
    collected = 0
    offset    = 0
    today     = datetime.today()
    max_offset = days * 2 + 15   # 涵蓋假日緩衝

    while collected < days and offset < max_offset:
        d = today - timedelta(days=offset)
        offset += 1
        if d.weekday() >= 5:        # 週末跳過
            continue
        ds = d.strftime("%Y%m%d")
        dd = d.strftime("%Y-%m-%d")
        if dd in cached_dates:
            logger.info(f"[SKIP] T86 {dd} already in DB")
            collected += 1
            continue
        df = fetch_t86(ds)
        if not df.empty:
            df["trade_date"] = dd   # 統一存 YYYY-MM-DD
            frames.append(df)
            collected += 1
        time.sleep(0.3)

    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


# ── 收盤價 ────────────────────────────────────────────

def fetch_twse_prices() -> pd.DataFrame:
    """上市全市場收盤（TWSE OpenAPI）"""
    data = _get("https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL")
    if not data or not isinstance(data, list):
        return pd.DataFrame()
    rows = []
    for item in data:
        code = str(item.get("Code", "")).strip()
        if not code:
            continue
        rows.append({
            "code":        code.zfill(4),
            "name":        item.get("Name", ""),
            "market":      "TWSE",
            "close_price": _to_flt(item.get("ClosingPrice")),
            "open_price":  _to_flt(item.get("OpeningPrice")),
            "high_price":  _to_flt(item.get("HighestPrice")),
            "low_price":   _to_flt(item.get("LowestPrice")),
            "volume":      _to_int(item.get("TradeVolume")),
        })
    df = pd.DataFrame(rows)
    logger.info(f"TWSE prices: {len(df)} rows")
    return df


def fetch_tpex_prices() -> pd.DataFrame:
    """上櫃全市場收盤（TPEx OpenAPI，verify=False）"""
    data = _get(
        "https://www.tpex.org.tw/openapi/v1/tpex/exchangeReport/daily_close_quotes",
        verify=False
    )
    if not data or not isinstance(data, list):
        return pd.DataFrame()
    rows = []
    for item in data:
        code = str(item.get("SecuritiesCompanyCode", item.get("Code", ""))).strip()
        if not code:
            continue
        rows.append({
            "code":        code.zfill(4),
            "name":        item.get("CompanyName", item.get("Name", "")),
            "market":      "TPEx",
            "close_price": _to_flt(item.get("Close",    item.get("ClosingPrice"))),
            "open_price":  _to_flt(item.get("Open",     item.get("OpeningPrice"))),
            "high_price":  _to_flt(item.get("High",     item.get("HighestPrice"))),
            "low_price":   _to_flt(item.get("Low",      item.get("LowestPrice"))),
            "volume":      _to_int(item.get("TradeVolume")),
        })
    df = pd.DataFrame(rows)
    logger.info(f"TPEx prices: {len(df)} rows")
    return df


def fetch_all_prices() -> pd.DataFrame:
    twse = fetch_twse_prices()
    tpex = fetch_tpex_prices()
    combined = pd.concat([twse, tpex], ignore_index=True)
    combined = combined.drop_duplicates(subset=["code"])
    logger.info(f"All prices: {len(combined)} rows (TWSE={len(twse)}, TPEx={len(tpex)})")
    return combined


# ── 型別轉換 ──────────────────────────────────────────

def _to_int(v) -> int:
    try:    return int(str(v).replace(",", "").strip())
    except: return 0

def _to_flt(v) -> float:
    try:    return float(str(v).replace(",", "").strip())
    except: return 0.0
