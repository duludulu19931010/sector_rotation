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

ROOT      = Path(__file__).resolve().parent
DB_FILE   = ROOT / "db" / "market.db"
DATA_DIR  = ROOT / "docs" / "assets" / "data"
INPUT_DIR = ROOT / "input"
GROUP_CSV = INPUT_DIR / "Group.csv"
NAMES_CSV = INPUT_DIR / "stock_list.csv"
LOG_FILE  = ROOT / "pipeline.log"
TODAY     = date.today().strftime("%Y-%m-%d")
TODAY_8   = date.today().strftime("%Y%m%d")

TWSE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept":     "application/json, text/plain, */*",
    "Referer":    "https://www.twse.com.tw/",
}
TPEX_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept":     "application/json, text/plain, */*",
    "Referer":    "https://www.tpex.org.tw/",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(str(LOG_FILE), encoding="utf-8"),
    ],
)
log = logging.getLogger("twflow")

DDL = """
CREATE TABLE IF NOT EXISTS daily (
    date          TEXT NOT NULL,
    code          TEXT NOT NULL,
    market        TEXT NOT NULL,
    name          TEXT DEFAULT '',
    close_price   REAL DEFAULT 0,
    trade_volume  INTEGER DEFAULT 0,
    trade_value   INTEGER DEFAULT 0,
    inst_net      INTEGER DEFAULT 0,
    net_yi        REAL DEFAULT 0,
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


def db_has_today() -> bool:
    dates = db_dates(1)
    if not dates or dates[0] != TODAY:
        return False
    with _db() as c:
        n = c.execute(
            "SELECT COUNT(*) FROM daily WHERE date=? AND market='TWSE' AND inst_net != 0",
            (TODAY,)
        ).fetchone()[0]
    if n == 0:
        log.info("Today's data exists but TWSE inst_net all 0, will retry")
        return False
    return True


def db_load(days: int = 25) -> pd.DataFrame:
    dates = db_dates(days)
    if not dates:
        return pd.DataFrame()
    ph = ",".join("?" * len(dates))
    with _db() as c:
        df = pd.read_sql_query(
            f"SELECT date,code,market,name,close_price,"
            f"trade_volume,trade_value,inst_net,net_yi "
            f"FROM daily WHERE date IN ({ph}) ORDER BY date",
            c, params=dates
        )
    log.info(f"DB load: {len(df)} rows, {df['date'].nunique()} dates")
    return df


def db_save(df: pd.DataFrame) -> int:
    if df is None or df.empty:
        return 0
    rows = [(
        r["date"], str(r["code"]).zfill(4), r.get("market", ""),
        r.get("name", ""),
        _flt(r.get("close_price")),
        _int(r.get("trade_volume")), _int(r.get("trade_value")),
        _int(r.get("inst_net")),     _flt(r.get("net_yi")),
    ) for _, r in df.iterrows()]
    with _db() as c:
        c.executemany("""
            INSERT OR REPLACE INTO daily
            (date,code,market,name,close_price,
             trade_volume,trade_value,inst_net,net_yi)
            VALUES (?,?,?,?,?,?,?,?,?)
        """, rows)
    from collections import Counter
    for d, n in sorted(Counter(r[0] for r in rows).items()):
        log.info(f"Saved {n} rows for {d}")
    return len(rows)


def db_incomplete_dates() -> list[str]:
    with _db() as c:
        rows = c.execute("""
            SELECT date,
                   SUM(CASE WHEN market='TPEx' THEN 1 ELSE 0 END) AS tpex_cnt
            FROM daily
            WHERE date = (SELECT MAX(date) FROM daily)
            GROUP BY date
        """).fetchall()
    return [r["date"] for r in rows if r["tpex_cnt"] == 0]


def _get(url: str, params: dict = None, headers: dict = None,
         verify: bool = True, retries: int = 3, delay: float = 2.0):
    h = headers or TWSE_HEADERS
    for i in range(retries):
        try:
            r = requests.get(url, params=params, headers=h,
                             timeout=30, verify=verify)
            r.raise_for_status()
            if not r.text.strip():
                raise ValueError("Empty response body")
            data = r.json()
            if isinstance(data, (dict, list)):
                return data
        except Exception as e:
            log.warning(f"GET [{i+1}/{retries}] {url} — {e}")
            if i < retries - 1:
                time.sleep(delay * (i + 1))
    return None


def _flt(v) -> float:
    try:    return float(str(v).replace(",", "").strip())
    except: return 0.0


def _int(v) -> int:
    try:    return int(str(v).replace(",", "").strip())
    except: return 0


def _calc_net_yi(volume: int, value: int, inst_net_shares: int) -> float:
    """
    inst_net_shares: 三大法人買賣超股數（T86/TPEx 原始單位皆為「股」）
    net_yi（億）= inst_net_shares × 均價 ÷ 1e8
    均價 = value / volume
    """
    if volume == 0:
        return 0.0
    avg_price = value / volume
    return round(inst_net_shares * avg_price / 1e8, 6)


def fetch_twse_price_today() -> dict[str, dict]:
    data = _get("https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL")
    if not isinstance(data, list) or len(data) < 100:
        raise RuntimeError(f"TWSE STOCK_DAY_ALL failed: got {len(data) if data else 0} rows")
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
    log.info(f"TWSE price (today): {len(result)} stocks")
    return result


def fetch_twse_t86(date8: str) -> dict[str, int]:
    data = _get(
        "https://www.twse.com.tw/rwd/zh/fund/T86",
        params={"response": "json", "date": date8, "selectType": "ALL"},
    )
    if not isinstance(data, dict) or data.get("stat") != "OK":
        return {}

    fields = data.get("fields", [])
    raw_data = data.get("data", [])

    last_field = fields[-1] if fields else ""
    if "三大法人" not in last_field:
        log.warning(f"T86 {date8}: unexpected last field={last_field!r}, "
                     f"fields={fields}")

    result = {}
    for row in raw_data:
        if len(row) < 2:
            continue
        code = str(row[0]).strip().zfill(4)
        result[code] = _int(row[-1])

    log.info(f"T86(TWSE) {date8}: {len(result)} entries "
             f"(last_field={last_field!r})")
    return result


def fetch_twse_stock_month(code: str, date8: str) -> dict[str, dict]:
    """
    個股月歷史（TWSE）
    回傳: {date(YYYY-MM-DD): {close, volume, value}}
    """
    data = _get(
        "https://www.twse.com.tw/exchangeReport/STOCK_DAY",
        params={"response": "json", "date": date8, "stockNo": code},
        retries=2, delay=1.5,
    )
    if not isinstance(data, dict) or data.get("stat") != "OK":
        return {}
    result = {}
    for row in data.get("data", []):
        if len(row) < 9:
            continue
        roc_date = str(row[0]).strip()
        try:
            y, m, d = roc_date.split("/")
            dd = f"{int(y)+1911:04d}-{m}-{d}"
        except Exception:
            continue
        result[dd] = {
            "volume": _int(row[1]),
            "value":  _int(row[2]),
            "close":  _flt(row[6]),
        }
    return result


def fetch_tpex_price_today() -> dict[str, dict]:
    data = _get(
        "https://www.tpex.org.tw/openapi/v1/tpex_mainboard_quotes",
        headers=TPEX_HEADERS, verify=False,
    )
    if not isinstance(data, list) or len(data) < 5:
        raise RuntimeError(f"TPEx tpex_mainboard_quotes failed: got {len(data) if data else 0} rows")
    result = {}
    for item in data:
        code = str(item.get("SecuritiesCompanyCode", "")).strip().zfill(4)
        if not code.strip("0"):
            continue
        close = _flt(item.get("Close", 0))
        if close <= 0:
            continue
        result[code] = {
            "name":   item.get("CompanyName", ""),
            "close":  close,
            "volume": _int(item.get("TradeVolume", 0)),
            "value":  _int(item.get("TradeValue",  0)),
        }
    log.info(f"TPEx price (today): {len(result)} stocks")
    return result


def fetch_tpex_inst() -> dict[str, int]:
    data = _get(
        "https://www.tpex.org.tw/openapi/v1/tpex_3insti_daily_trading",
        headers=TPEX_HEADERS, verify=False,
    )
    if not isinstance(data, list) or len(data) < 5:
        raise RuntimeError(f"TPEx tpex_3insti_daily_trading failed: got {len(data) if data else 0} rows")

    if data:
        log.info(f"TPEx 3insti RAW: keys={list(data[0].keys())}")
        log.info(f"TPEx 3insti RAW: first item={data[0]}")

    result = {}
    for item in data:
        code = str(item.get("SecuritiesCompanyCode", "")).strip().zfill(4)
        if not code.strip("0"):
            continue
        foreign = _int(item.get("ForeignInvestorsBuy",  0)) - _int(item.get("ForeignInvestorsSell",  0))
        trust   = _int(item.get("InvestmentTrustBuy",   0)) - _int(item.get("InvestmentTrustSell",   0))
        dealer  = _int(item.get("DealersBuy",           0)) - _int(item.get("DealersSell",           0))
        total   = foreign + trust + dealer
        if total == 0:
            total = _int(item.get("TotalNetBuySell", item.get("NetBuySell", 0)))
        result[code] = total
    log.info(f"TPEx 3insti: {len(result)} stocks")
    return result


def _fetch_stock_history_one(code: str, market: str, months: list[str]) -> tuple[str, dict[str, dict]]:
    if market == "TPEx":
        return code, {}
    merged = {}
    for m8 in months:
        month_data = fetch_twse_stock_month(code, m8)
        merged.update(month_data)
    return code, merged


def fetch_group_history(group_codes: list[str],
                         code_market: dict[str, str],
                         months: list[str],
                         max_workers: int = 8) -> pd.DataFrame:
    """
    補抓 TWSE 個股近N個月歷史（價量）+ T86歷史（三大法人，全市場逐日）

    TPEx 個股歷史端點（st43_result.php）已隨 TPEx 2024年10月改版失效，
    無可靠官方API，TPEx 資料從今日起逐日累積。

    group_codes: 全部代號（4位）
    code_market: {代號: 'TWSE'/'TPEx'}（今日資料判斷市場）
    months: 要抓的月份列表，格式 YYYYMM01（例如 ['20260501','20260601']）
    max_workers: 並行請求數（避免被限速，預設8）
    """
    twse_codes = [c for c in group_codes if code_market.get(c, "TWSE") == "TWSE"]
    tpex_codes = [c for c in group_codes if code_market.get(c) == "TPEx"]
    log.info(f"Backfilling history: TWSE={len(twse_codes)} codes (individual), "
             f"TPEx={len(tpex_codes)} codes (skipped, accumulate from today) "
             f"x {len(months)} months (parallel={max_workers})")

    price_hist: dict[str, dict[str, dict]] = {}
    done = 0
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {
            pool.submit(_fetch_stock_history_one, code,
                        code_market.get(code, "TWSE"), months): code
            for code in twse_codes
        }
        for fut in as_completed(futures):
            code = futures[fut]
            try:
                _, merged = fut.result()
                price_hist[code] = merged
            except Exception as e:
                log.warning(f"  history fetch failed for {code}: {e}")
                price_hist[code] = {}
            done += 1
            if done % 200 == 0:
                log.info(f"  history fetch progress: {done}/{len(twse_codes)}")

    total_days = sum(len(v) for v in price_hist.values())
    log.info(f"Backfill price done: {len(price_hist)} codes, {total_days} day-records")

    all_hist_dates = sorted({d for v in price_hist.values() for d in v.keys()})
    log.info(f"Backfill date range: {all_hist_dates[0] if all_hist_dates else 'N/A'} "
             f"~ {all_hist_dates[-1] if all_hist_dates else 'N/A'} "
             f"({len(all_hist_dates)} dates)")

    t86_hist: dict[str, dict[str, int]] = {}
    for dd in all_hist_dates:
        d8 = dd.replace("-", "")
        t86_hist[dd] = fetch_twse_t86(d8)
        time.sleep(0.3)

    rows = []
    for code, day_data in price_hist.items():
        for dd, p in day_data.items():
            inst = t86_hist.get(dd, {}).get(code, 0)
            rows.append({
                "date": dd, "code": code, "market": "TWSE",
                "name": "",
                "close_price": p["close"],
                "trade_volume": p["volume"], "trade_value": p["value"],
                "inst_net": inst,
                "net_yi": _calc_net_yi(p["volume"], p["value"], inst),
            })

    df = pd.DataFrame(rows)
    log.info(f"Backfill total: {len(df)} rows (TWSE only)")
    return df


def fetch_today() -> pd.DataFrame:
    with ThreadPoolExecutor(max_workers=4) as pool:
        f_twse_p = pool.submit(fetch_twse_price_today)
        f_twse_i = pool.submit(fetch_twse_t86, TODAY_8)
        f_tpex_p = pool.submit(fetch_tpex_price_today)
        f_tpex_i = pool.submit(fetch_tpex_inst)

        r_twse_p = f_twse_p.result()
        r_twse_i = f_twse_i.result()
        r_tpex_p = f_tpex_p.result()
        r_tpex_i = f_tpex_i.result()

    log.info(f"Parallel done: TWSE {len(r_twse_p)} price/{len(r_twse_i)} inst "
             f"| TPEx {len(r_tpex_p)} price/{len(r_tpex_i)} inst")

    rows = []
    for code, p in r_twse_p.items():
        inst = r_twse_i.get(code, 0)
        rows.append({
            "date": TODAY, "code": code, "market": "TWSE", "name": p["name"],
            "close_price": p["close"],
            "trade_volume": p["volume"], "trade_value": p["value"],
            "inst_net": inst, "net_yi": _calc_net_yi(p["volume"], p["value"], inst),
        })
    for code, p in r_tpex_p.items():
        inst = r_tpex_i.get(code, 0)
        rows.append({
            "date": TODAY, "code": code, "market": "TPEx", "name": p["name"],
            "close_price": p["close"],
            "trade_volume": p["volume"], "trade_value": p["value"],
            "inst_net": inst, "net_yi": _calc_net_yi(p["volume"], p["value"], inst),
        })

    df = pd.DataFrame(rows)
    twse_n = sum(1 for r in rows if r["market"] == "TWSE")
    tpex_n = sum(1 for r in rows if r["market"] == "TPEx")
    log.info(f"Today total: {len(df)} stocks (TWSE={twse_n}, TPEx={tpex_n})")
    return df



def load_close_csv_files() -> pd.DataFrame:
    """
    讀取 input/YYYYMMDD_Data.csv（CP950），取代號和成交收盤價
    回傳 DataFrame: date, code, close_price
    """
    frames = []
    for f in sorted(INPUT_DIR.glob("*_Data.csv")):
        date8 = f.stem.split("_")[0]
        if not (len(date8) == 8 and date8.isdigit()):
            continue
        dd = f"{date8[:4]}-{date8[4:6]}-{date8[6:8]}"
        try:
            raw = f.read_bytes()
            text = raw.decode("cp950", errors="replace")
            from io import StringIO
            df = pd.read_csv(StringIO(text))
            if "代碼" not in df.columns or "成交" not in df.columns:
                log.warning(f"{f.name}: missing required columns, skip")
                continue
            sub = df[["代碼", "成交"]].copy()
            sub.columns = ["code", "close_price"]
            sub["code"] = (
                sub["code"].astype(str)
                .str.strip()
                .str.replace(r"\.0$", "", regex=True)
                .str.zfill(4)
            )
            sub["close_price"] = pd.to_numeric(sub["close_price"], errors="coerce")
            sub["date"] = dd
            sub = sub.dropna(subset=["close_price"])
            sub = sub[sub["code"].str.match(r"^\d{4,6}$")]
            frames.append(sub[["date", "code", "close_price"]])
            log.info(f"Loaded {f.name}: {len(sub)} rows for {dd}")
        except Exception as e:
            log.warning(f"Failed to load {f.name}: {e}")
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def compute(hist_df: pd.DataFrame,
            csv_close_df: pd.DataFrame,
            groups: dict[str, list[str]],
            name_map: dict[str, str]) -> tuple[list[dict], dict[str, list[dict]]]:
    if hist_df.empty:
        return [], {}

    df = hist_df.copy()
    df["code"] = df["code"].astype(str).str.zfill(4)
    df["date"] = df["date"].astype(str)

    all_dates = sorted(df["date"].unique())
    latest    = all_dates[-1]
    log.info(f"Dates: {all_dates[0]} ~ {latest} ({len(all_dates)} days)")

    if not csv_close_df.empty:
        csv = csv_close_df.copy()
        csv["code"] = csv["code"].astype(str).str.zfill(4)
        csv["date"] = csv["date"].astype(str)
        close_pv = csv.pivot_table(index="code", columns="date",
                                    values="close_price", aggfunc="first")
        close_dates = sorted(close_pv.columns)
        log.info(f"CSV close dates: {close_dates}")
    else:
        close_pv = df.pivot_table(index="code", columns="date",
                                   values="close_price", aggfunc="first")
        close_dates = sorted(close_pv.columns)
        log.warning("No CSV close data, falling back to API close_price")

    if latest in close_pv.columns:
        close_now = close_pv[latest].dropna()
    elif close_dates:
        close_now = close_pv[close_dates[-1]].dropna()
        log.warning(f"latest={latest} not in CSV close, using {close_dates[-1]}")
    else:
        close_now = df[df["date"] == latest].set_index("code")["close_price"]

    log.info(f"close_now: {len(close_now)} codes, "
             f"sample={list(close_now.index[:5])}")

    all_group_codes = {str(c).zfill(4) for codes in groups.values() for c in codes}
    matched_total = all_group_codes & set(close_now.index)
    log.info(f"Group codes total={len(all_group_codes)}, "
             f"matched in close_now={len(matched_total)}")
    if len(matched_total) == 0:
        sample_group = sorted(all_group_codes)[:5]
        sample_close = sorted(close_now.index)[:5]
        log.warning(f"ZERO MATCH! group sample={sample_group}, "
                     f"close_now sample={sample_close}")
        if sample_group and sample_close:
            g0, c0 = sample_group[0], sample_close[0]
            log.warning(f"  repr group code: {g0!r} (len={len(g0)}), "
                         f"repr close code: {c0!r} (len={len(c0)})")

    d_prev1  = close_dates[-2]  if len(close_dates) >= 2  else None
    d_prev6  = close_dates[-6]  if len(close_dates) >= 6  else None
    d_prev21 = close_dates[-21] if len(close_dates) >= 21 else None
    log.info(f"chg_1d base: {d_prev1}, chg_5d base: {d_prev6}, chg_20d base: {d_prev21}")

    def pct(base_date: str | None) -> pd.Series:
        if base_date is None or base_date not in close_pv.columns:
            return pd.Series(0.0, index=close_now.index)
        base = close_pv[base_date]
        c    = close_now.reindex(base.index)
        valid = base.notna() & (base > 0.01) & c.notna() & (c > 0.01)
        s = pd.Series(0.0, index=base.index)
        s[valid] = ((c[valid] - base[valid]) / base[valid] * 100).round(2)
        return s

    chg_1d  = pct(d_prev1)
    chg_5d  = pct(d_prev6)
    chg_20d = pct(d_prev21)

    net_pv  = df.pivot_table(index="code", columns="date",
                              values="net_yi", aggfunc="first")
    last5   = all_dates[-5:]
    last20  = all_dates[-20:]
    net_1d  = net_pv[latest] if latest in net_pv.columns else pd.Series(dtype=float)
    net_5d  = net_pv[[c for c in last5  if c in net_pv.columns]].sum(axis=1)
    net_20d = net_pv[[c for c in last20 if c in net_pv.columns]].sum(axis=1)

    log.info(f"net_pv shape={net_pv.shape}, columns={sorted(net_pv.columns)[-6:]}")
    log.info(f"last5={last5}")
    log.info(f"net_5d describe: count={net_5d.count()}, "
             f"min={net_5d.min():.4f}, max={net_5d.max():.4f}, "
             f"nonzero={int((net_5d!=0).sum())}, "
             f">0={int((net_5d>0).sum())}, <-2={int((net_5d<-2).sum())}")

    def clamp_net(s: pd.Series, limit: float) -> pd.Series:
        return s.where(s.abs() <= limit, 0.0)

    net_1d  = clamp_net(net_1d,  1000.0)
    net_5d  = clamp_net(net_5d,  5000.0)
    net_20d = clamp_net(net_20d, 20000.0)

    log.info(f"after clamp: net_5d nonzero={int((net_5d!=0).sum())}, "
             f">0={int((net_5d>0).sum())}, <-2={int((net_5d<-2).sum())}")

    net_codes   = set(net_5d.index)
    close_codes = set(close_now.index)
    overlap = net_codes & close_codes
    log.info(f"net_5d codes={len(net_codes)}, close_now codes={len(close_codes)}, "
             f"overlap={len(overlap)}")
    if len(overlap) < len(net_codes) * 0.5:
        log.warning(f"net_5d sample: {sorted(net_codes)[:5]}")
        log.warning(f"close_now sample: {sorted(close_codes)[:5]}")

    db_names = df[df["date"] == latest].set_index("code")["name"].to_dict()
    def get_name(code: str) -> str:
        return name_map.get(code, "") or db_names.get(code, "")

    records: list[dict] = []
    details: dict[str, list[dict]] = {}

    for gname, raw_codes in groups.items():
        codes = [str(c).zfill(4) for c in raw_codes
                 if str(c).zfill(4) in close_now.index]

        stocks = []
        for c in codes:
            c1  = float(chg_1d.get(c, 0) or 0)
            c5  = float(chg_5d.get(c, 0) or 0)
            c20 = float(chg_20d.get(c, 0) or 0)
            if abs(c1)  > 11:  c1  = 0.0
            if abs(c5)  > 60:  c5  = 0.0
            if abs(c20) > 200: c20 = 0.0
            stocks.append({
                "code":    c,
                "name":    get_name(c),
                "close":   round(float(close_now.get(c, 0)), 2),
                "net_1d":  round(float(net_1d.get(c,  0) or 0), 4),
                "net_5d":  round(float(net_5d.get(c,  0) or 0), 4),
                "net_20d": round(float(net_20d.get(c, 0) or 0), 4),
                "chg_1d":  round(c1,  2),
                "chg_5d":  round(c5,  2),
                "chg_20d": round(c20, 2),
            })
        stocks.sort(key=lambda x: x["net_1d"], reverse=True)
        details[gname] = stocks

        if not codes:
            records.append(_empty(gname, len(raw_codes)))
            continue

        g1  = round(sum(s["net_1d"]  for s in stocks), 3)
        g5  = round(sum(s["net_5d"]  for s in stocks), 3)
        g20 = round(sum(s["net_20d"] for s in stocks), 3)

        chg1_vals  = [s["chg_1d"]  for s in stocks if s["chg_1d"]  != 0.0]
        chg5_vals  = [s["chg_5d"]  for s in stocks if s["chg_5d"]  != 0.0]
        chg20_vals = [s["chg_20d"] for s in stocks if s["chg_20d"] != 0.0]
        gc1  = round(sum(chg1_vals)  / len(chg1_vals),  2) if chg1_vals  else 0.0
        gc5  = round(sum(chg5_vals)  / len(chg5_vals),  2) if chg5_vals  else 0.0
        gc20 = round(sum(chg20_vals) / len(chg20_vals), 2) if chg20_vals else 0.0

        if abs(g1)   > 1000:  g1   = 0.0
        if abs(g5)   > 5000:  g5   = 0.0
        if abs(g20)  > 20000: g20  = 0.0
        if abs(gc1)  > 11:    gc1  = 0.0
        if abs(gc5)  > 60:    gc5  = 0.0
        if abs(gc20) > 200:   gc20 = 0.0

        if   g5 >  2 and gc5 > 1:  label = "主力"
        elif g5 >  0 and gc5 <= 1: label = "輪動"
        elif g5 < -2:              label = "退潮"
        else:                      label = "觀望"

        records.append({
            "g": gname, "cnt": len(raw_codes), "matched": len(codes),
            "net_1d": g1, "net_5d": g5, "net_20d": g20,
            "chg_1d": gc1, "chg_5d": gc5, "chg_20d": gc20, "label": label,
        })

    log.info(f"Groups: {len(records)} | "
             + " | ".join(f"{l}={sum(1 for r in records if r['label']==l)}"
                          for l in ["主力","輪動","退潮","觀望"]))
    return records, details


def _empty(gname: str, cnt: int) -> dict:
    return {"g": gname, "cnt": cnt, "matched": 0,
            "net_1d": 0.0, "net_5d": 0.0, "net_20d": 0.0,
            "chg_1d": 0.0, "chg_5d": 0.0, "chg_20d": 0.0, "label": "觀望"}


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
        return [{**r, "stocks": details.get(r["g"], [])} for r in filt]

    inflow  = attach(sorted([r for r in records if r["net_5d"] > 0 and r["chg_5d"] < 10],
                             key=lambda x: x["net_5d"], reverse=True))
    stealth = attach(sorted([r for r in records
                              if r["net_1d"] > 0 and r["net_5d"] > 0 and r["chg_5d"] < 0],
                             key=lambda x: x["net_5d"], reverse=True))

    _jdump("bubble_data.json",          bubble)
    _jdump("inflow_low_gain.json",      inflow)
    _jdump("stealth_accumulation.json", stealth)
    _jdump("group_stats.json",          [{k: v for k, v in r.items() if k != "stocks"}
                                          for r in records])
    _jdump("metadata.json", {
        "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "trade_date":   trade_date,
        "groups":       len(records),
        "inflow":       len(inflow),
        "stealth":      len(stealth),
    })
    log.info(f"JSON: bubble={len(bubble)}, inflow={len(inflow)}, stealth={len(stealth)}")


def export_csv() -> None:
    hist = db_load(days=999)
    if hist.empty:
        return
    csv_path = DATA_DIR / "market_data.csv"
    hist.to_csv(csv_path, index=False, encoding="utf-8-sig")
    log.info(f"CSV exported: {csv_path} ({len(hist)} rows)")


def _jdump(fname: str, data):
    with open(DATA_DIR / fname, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def load_groups() -> dict[str, list[str]]:
    raw = GROUP_CSV.read_bytes()
    text = raw.decode("big5" if b"\xb0\xea" in raw else "cp950", errors="replace")
    lines = text.strip().replace("\r\n", "\n").replace("\r", "\n").split("\n")
    headers = [h.strip() for h in lines[0].split(",")]
    g = {h: [] for h in headers if h}
    for line in lines[1:]:
        cols = line.split(",")
        for i, h in enumerate(headers):
            if h and i < len(cols) and cols[i].strip():
                g[h].append(cols[i].strip())
    log.info(f"Groups: {len(g)}, {sum(len(v) for v in g.values())} stocks")
    return g


def load_names() -> dict[str, str]:
    raw = NAMES_CSV.read_bytes()
    text = raw.decode("cp950", errors="replace")
    nm = {}
    for line in text.strip().replace("\r\n", "\n").split("\n")[1:]:
        p = line.strip().split(",")
        if len(p) >= 2 and p[0].strip() and p[1].strip():
            nm[p[0].strip().zfill(4)] = p[1].strip()
    log.info(f"Stock names: {len(nm)}")
    return nm


def _months_for_lookback() -> list[str]:
    today = date.today()
    this_month = today.strftime("%Y%m01")
    prev_month_date = today.replace(day=1) - timedelta(days=1)
    prev_month = prev_month_date.strftime("%Y%m01")
    return sorted({prev_month, this_month})


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--force",         action="store_true")
    ap.add_argument("--dry-run",       action="store_true")
    ap.add_argument("--reset-history", action="store_true",
                    help="刪除 DB 中 TPEx 筆數為 0 的最新日期並重抓")
    args = ap.parse_args()

    log.info("=" * 60)
    log.info(f"TW$FLOW  {datetime.now()}  trade_date={TODAY}")
    if args.force:         log.info("*** FORCE ***")
    if args.dry_run:       log.info("*** DRY-RUN ***")
    if args.reset_history: log.info("*** RESET-HISTORY ***")
    log.info("=" * 60)

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    db_init()

    groups   = load_groups()
    name_map = load_names()

    if not args.dry_run:
        incomplete = db_incomplete_dates()
        if incomplete:
            log.info(f"Found {len(incomplete)} incomplete dates: {incomplete}")
            if args.reset_history or args.force:
                with _db() as c:
                    ph = ",".join("?" * len(incomplete))
                    c.execute(f"DELETE FROM daily WHERE date IN ({ph})", incomplete)
                log.info(f"Deleted {len(incomplete)} incomplete dates from DB")
            else:
                log.warning("Run with --reset-history to re-fetch these dates")

        if args.force or not db_has_today():
            today_df = fetch_today()
            if today_df.empty:
                log.error("Today fetch returned empty, aborting")
                return
            db_save(today_df)
        else:
            log.info("[CACHE] Today already in DB")

        existing_dates = set(db_dates(25))
        if len(existing_dates) < 21 or args.reset_history:
            log.info(f"DB has {len(existing_dates)} trade dates (<21), backfilling history")

            today_df_for_market = db_load(days=1)
            code_market = {}
            if not today_df_for_market.empty:
                code_market = dict(zip(
                    today_df_for_market["code"].astype(str).str.zfill(4),
                    today_df_for_market["market"],
                ))

            all_codes = sorted(code_market.keys())
            months = _months_for_lookback()
            log.info(f"Backfill scope: ALL {len(all_codes)} market codes "
                     f"(TWSE={sum(1 for m in code_market.values() if m=='TWSE')}, "
                     f"TPEx={sum(1 for m in code_market.values() if m=='TPEx')}), "
                     f"months={months}")

            hist_df = fetch_group_history(all_codes, code_market, months)
            if not hist_df.empty:
                hist_df = hist_df[~hist_df["date"].isin(existing_dates | {TODAY})]
                if not hist_df.empty:
                    name_lookup = name_map
                    hist_df["name"] = hist_df["code"].map(name_lookup).fillna("")
                    db_save(hist_df)
                else:
                    log.info("Backfill: no new dates to save")

    hist = db_load(days=25)
    if hist.empty:
        log.error("No data in DB")
        return

    csv_close = load_close_csv_files()

    records, details = compute(hist, csv_close, groups, name_map)
    export_json(records, details, hist["date"].max())
    export_csv()

    with _db() as c:
        total = c.execute("SELECT COUNT(*) FROM daily").fetchone()[0]
        latest_date = c.execute("SELECT MAX(date) FROM daily").fetchone()[0]
    log.info(f"DB: {total} rows, latest={latest_date}")
    log.info("Pipeline complete")


if __name__ == "__main__":
    main()
