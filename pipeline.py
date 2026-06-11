import argparse
import json
import logging
import sqlite3
import sys
import time
import urllib3
from concurrent.futures import ThreadPoolExecutor
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
GROUP_CSV = ROOT / "input" / "Group.csv"
NAMES_CSV = ROOT / "input" / "stock_list.csv"
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
    open_price    REAL DEFAULT 0,
    close_price   REAL DEFAULT 0,
    high_price    REAL DEFAULT 0,
    low_price     REAL DEFAULT 0,
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
    return bool(dates) and dates[0] == TODAY


def db_load(days: int = 21) -> pd.DataFrame:
    dates = db_dates(days)
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
    log.info(f"DB load: {len(df)} rows, {df['date'].nunique()} dates")
    return df


def db_save(df: pd.DataFrame) -> int:
    if df is None or df.empty:
        return 0
    rows = [(
        r["date"], str(r["code"]).zfill(4), r.get("market", ""),
        r.get("name", ""),
        _flt(r.get("open_price")),   _flt(r.get("close_price")),
        _flt(r.get("high_price")),   _flt(r.get("low_price")),
        _int(r.get("trade_volume")), _int(r.get("trade_value")),
        _int(r.get("inst_net")),     _flt(r.get("net_yi")),
    ) for _, r in df.iterrows()]
    with _db() as c:
        c.executemany("""
            INSERT OR REPLACE INTO daily
            (date,code,market,name,open_price,close_price,high_price,low_price,
             trade_volume,trade_value,inst_net,net_yi)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
        """, rows)
    from collections import Counter
    for d, n in sorted(Counter(r[0] for r in rows).items()):
        log.info(f"Saved {n} rows for {d}")
    return len(rows)


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


def _to_date(d8: str) -> str:
    return f"{d8[:4]}-{d8[4:6]}-{d8[6:8]}"


def _flt(v) -> float:
    try:    return float(str(v).replace(",", "").strip())
    except: return 0.0


def _int(v) -> int:
    try:    return int(str(v).replace(",", "").strip())
    except: return 0


def _calc_net_yi(volume: int, value: int, inst_net: int) -> float:
    if value == 0 or volume == 0:
        return 0.0
    return round((volume / value) * inst_net * 1000, 6)


def fetch_twse_price_today() -> dict[str, dict]:
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
            "volume": _int(item.get("TradeVolume")),
            "value":  _int(item.get("TradeValue")),
        }
    log.info(f"TWSE price (today): {len(result)} stocks")
    return result


def fetch_twse_price_hist(date8: str) -> dict[str, dict]:
    data = _get(
        "https://www.twse.com.tw/rwd/zh/afterTrading/STOCK_DAY_ALL",
        params={"response": "json", "date": date8},
    )
    if not isinstance(data, dict) or data.get("stat") != "OK":
        return {}
    fields = data.get("fields", [])
    result = {}
    for row in data.get("data", []):
        if len(row) < 9:
            continue
        code = str(row[0]).strip().zfill(4)
        if not code.strip("0"):
            continue
        result[code] = {
            "name":   str(row[1]).strip(),
            "volume": _int(row[2]),
            "value":  _int(row[4]),
            "open":   _flt(row[5]),
            "high":   _flt(row[6]),
            "low":    _flt(row[7]),
            "close":  _flt(row[8]),
        }
    log.info(f"TWSE price (hist) {date8}: {len(result)} stocks")
    return result


def fetch_twse_t86(date8: str) -> dict[str, int]:
    data = _get(
        "https://www.twse.com.tw/rwd/zh/fund/T86",
        params={"response": "json", "date": date8, "selectType": "ALL"},
    )
    if not isinstance(data, dict) or data.get("stat") != "OK":
        return {}
    result = {}
    for row in data.get("data", []):
        if len(row) >= 15:
            code = str(row[0]).strip().zfill(4)
            result[code] = _int(row[14])
    log.info(f"T86(TWSE) {date8}: {len(result)} stocks")
    return result


def fetch_tpex_price_today() -> dict[str, dict]:
    data = _get(
        "https://www.tpex.org.tw/openapi/v1/tpex_mainboard_quotes",
        headers=TPEX_HEADERS, verify=False,
    )
    if not isinstance(data, list) or len(data) < 5:
        log.warning("TPEx price: no data")
        return {}
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
            "open":   _flt(item.get("Open",  0)),
            "close":  close,
            "high":   _flt(item.get("High",  0)),
            "low":    _flt(item.get("Low",   0)),
            "volume": _int(item.get("TradeVolume", 0)),
            "value":  _int(item.get("TradeValue",  0)),
        }
    log.info(f"TPEx price (today): {len(result)} stocks")
    return result


def fetch_tpex_price_hist(date8: str) -> dict[str, dict]:
    yy = int(date8[:4]) - 1911
    mm = date8[4:6]
    dd = date8[6:8]
    data = _get(
        "https://www.tpex.org.tw/web/stock/aftertrading/daily_close_quotes/stk_quote_result.php",
        params={"l": "zh-tw", "d": f"{yy}/{mm}/{dd}", "se": "EW"},
        headers=TPEX_HEADERS, verify=False,
    )
    if not isinstance(data, dict):
        log.warning(f"TPEx price (hist) {date8}: unexpected response type")
        return {}
    aa = data.get("aaData", [])
    if not aa:
        log.warning(f"TPEx price (hist) {date8}: aaData empty")
        return {}
    result = {}
    for row in aa:
        if len(row) < 9:
            continue
        code = str(row[0]).strip().zfill(4)
        if not code.strip("0"):
            continue
        result[code] = {
            "name":   str(row[1]).strip(),
            "close":  _flt(row[2]),
            "open":   _flt(row[4]),
            "high":   _flt(row[5]),
            "low":    _flt(row[6]),
            "volume": _int(row[8]),
            "value":  _int(row[9]) if len(row) > 9 else 0,
        }
    log.info(f"TPEx price (hist) {date8}: {len(result)} stocks")
    return result


def fetch_tpex_inst() -> dict[str, int]:
    data = _get(
        "https://www.tpex.org.tw/openapi/v1/tpex_3insti_daily_trading",
        headers=TPEX_HEADERS, verify=False,
    )
    if not isinstance(data, list) or len(data) < 5:
        log.warning("TPEx 3insti: no data")
        return {}
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


def build_day_df(trade_date: str,
                 twse_price: dict, twse_inst: dict,
                 tpex_price: dict, tpex_inst: dict) -> pd.DataFrame:
    rows = []
    for code, p in twse_price.items():
        inst = twse_inst.get(code, 0)
        rows.append({
            "date": trade_date, "code": code, "market": "TWSE", "name": p["name"],
            "open_price": p["open"],   "close_price": p["close"],
            "high_price": p["high"],   "low_price":   p["low"],
            "trade_volume": p["volume"], "trade_value": p["value"],
            "inst_net": inst, "net_yi": _calc_net_yi(p["volume"], p["value"], inst),
        })
    for code, p in tpex_price.items():
        inst = tpex_inst.get(code, 0)
        rows.append({
            "date": trade_date, "code": code, "market": "TPEx", "name": p["name"],
            "open_price": p["open"],   "close_price": p["close"],
            "high_price": p["high"],   "low_price":   p["low"],
            "trade_volume": p["volume"], "trade_value": p["value"],
            "inst_net": inst, "net_yi": _calc_net_yi(p["volume"], p["value"], inst),
        })
    return pd.DataFrame(rows)


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
    return build_day_df(TODAY, r_twse_p, r_twse_i, r_tpex_p, r_tpex_i)


def fetch_history(missing_dates: list[str]) -> pd.DataFrame:
    frames = []
    for dd in missing_dates:
        d8 = dd.replace("-", "")
        twse_p = fetch_twse_price_hist(d8)
        twse_i = fetch_twse_t86(d8)
        tpex_p = fetch_tpex_price_hist(d8)
        tpex_i = {}
        day_df = build_day_df(dd, twse_p, twse_i, tpex_p, tpex_i)
        if not day_df.empty:
            frames.append(day_df)
        time.sleep(0.5)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def get_recent_trade_dates(n: int = 21) -> list[str]:
    dates = []
    d = date.today()
    while len(dates) < n:
        if d.weekday() < 5:
            dates.append(d.strftime("%Y-%m-%d"))
        d -= timedelta(days=1)
    dates.sort()
    return dates


def compute(hist_df: pd.DataFrame,
            groups: dict[str, list[str]],
            name_map: dict[str, str]) -> tuple[list[dict], dict[str, list[dict]]]:
    if hist_df.empty:
        return [], {}

    df = hist_df.copy()
    df["code"] = df["code"].astype(str).str.zfill(4)
    df["date"] = df["date"].astype(str)

    all_dates = sorted(df["date"].unique())
    latest    = all_dates[-1]
    d_prev1   = all_dates[-2] if len(all_dates) >= 2 else all_dates[-1]
    d_prev6   = all_dates[-6] if len(all_dates) >= 6 else all_dates[0]

    log.info(f"Dates: {all_dates[0]} ~ {latest} ({len(all_dates)} days)")
    log.info(f"chg_1d base: {d_prev1}, chg_5d base: {d_prev6}")

    close_pv = df.pivot_table(index="code", columns="date",
                               values="close_price", aggfunc="first")
    net_pv   = df.pivot_table(index="code", columns="date",
                               values="net_yi", aggfunc="first")

    close_now = close_pv[latest].dropna() if latest in close_pv.columns else pd.Series(dtype=float)

    def pct(base_date: str) -> pd.Series:
        if base_date not in close_pv.columns or base_date == latest:
            return pd.Series(0.0, index=close_now.index)
        base = close_pv[base_date]
        c    = close_now.reindex(base.index)
        valid = base.notna() & (base > 0) & c.notna()
        s = pd.Series(0.0, index=base.index)
        s[valid] = ((c[valid] - base[valid]) / base[valid] * 100).round(2)
        return s

    chg_1d = pct(d_prev1)
    chg_5d = pct(d_prev6)

    last5  = all_dates[-5:]
    last20 = all_dates[-20:]
    net_1d  = net_pv[latest] if latest in net_pv.columns else pd.Series(dtype=float)
    net_5d  = net_pv[[c for c in last5  if c in net_pv.columns]].sum(axis=1)
    net_20d = net_pv[[c for c in last20 if c in net_pv.columns]].sum(axis=1)

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
            c1 = float(chg_1d.get(c, 0) or 0)
            if abs(c1) > 11:
                c1 = 0.0
            stocks.append({
                "code":    c,
                "name":    get_name(c),
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
            return float(s.reindex(codes).dropna().sum())
        def _m(s: pd.Series) -> float:
            v = s.reindex(codes).dropna()
            return round(float(v.mean()), 2) if not v.empty else 0.0

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

    log.info(f"Groups: {len(records)} | "
             + " | ".join(f"{l}={sum(1 for r in records if r['label']==l)}"
                          for l in ["主力","輪動","退潮","觀望"]))
    return records, details


def _empty(gname: str, cnt: int) -> dict:
    return {"g": gname, "cnt": cnt, "matched": 0,
            "net_1d": 0.0, "net_5d": 0.0, "net_20d": 0.0,
            "chg_1d": 0.0, "chg_5d": 0.0, "label": "觀望"}


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


def db_incomplete_dates() -> list[str]:
    with _db() as c:
        rows = c.execute("""
            SELECT date,
                   SUM(CASE WHEN market='TPEx' THEN 1 ELSE 0 END) AS tpex_cnt
            FROM daily
            GROUP BY date
            ORDER BY date DESC
        """).fetchall()
    return [r["date"] for r in rows if r["tpex_cnt"] == 0]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--force",         action="store_true")
    ap.add_argument("--dry-run",       action="store_true")
    ap.add_argument("--reset-history", action="store_true",
                    help="刪除 DB 中 TPEx 筆數為 0 的歷史日期並重抓")
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
            log.info(f"Found {len(incomplete)} dates with TPEx=0: {incomplete}")
            if args.reset_history or args.force:
                with _db() as c:
                    ph = ",".join("?" * len(incomplete))
                    c.execute(f"DELETE FROM daily WHERE date IN ({ph})", incomplete)
                log.info(f"Deleted {len(incomplete)} incomplete dates from DB")
            else:
                log.warning("Run with --reset-history to re-fetch these dates")

        have = set(db_dates(25))
        need = get_recent_trade_dates(21)
        missing = [d for d in need if d not in have and d != TODAY]

        if missing:
            log.info(f"Fetching {len(missing)} historical dates: {missing[0]} ~ {missing[-1]}")
            hist_df = fetch_history(missing)
            if not hist_df.empty:
                db_save(hist_df)

        if args.force or not db_has_today():
            today_df = fetch_today()
            if not today_df.empty:
                db_save(today_df)
            else:
                log.warning("No data fetched today (API may not be available yet)")
        else:
            log.info("[CACHE] Today already in DB")

    hist = db_load(days=21)
    if hist.empty:
        log.error("No data in DB")
        return

    records, details = compute(hist, groups, name_map)
    export_json(records, details, hist["date"].max())
    export_csv()

    with _db() as c:
        total = c.execute("SELECT COUNT(*) FROM daily").fetchone()[0]
        latest_date = c.execute("SELECT MAX(date) FROM daily").fetchone()[0]
    log.info(f"DB: {total} rows, latest={latest_date}")
    log.info("Pipeline complete")


if __name__ == "__main__":
    main()
