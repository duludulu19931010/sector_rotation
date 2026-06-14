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

ROOT      = Path(__file__).resolve().parent
DB_FILE          = ROOT / "db" / "market.db"
DATA_DIR         = ROOT / "docs" / "assets" / "data"
INPUT_DIR        = ROOT / "input"
XQ_DIR           = INPUT_DIR / "XQ"
TPEX_DIR         = INPUT_DIR / "TPEx"
GROUP_CSV        = INPUT_DIR / "group.csv"
LOG_FILE         = ROOT / "pipeline.log"

TWSE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept":     "application/json, text/plain, */*",
    "Referer":    "https://www.twse.com.tw/",
}
TPEX_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
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

TODAY   = date.today().strftime("%Y-%m-%d")
TODAY_8 = date.today().strftime("%Y%m%d")

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

MIGRATE_SQL = """
ALTER TABLE daily ADD COLUMN avg_price  REAL NOT NULL DEFAULT 0;
ALTER TABLE daily ADD COLUMN inst_value REAL NOT NULL DEFAULT 0;
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
        existing_cols = {row[1] for row in c.execute("PRAGMA table_info(daily)")}
        for col, sql in [("avg_price",  "ALTER TABLE daily ADD COLUMN avg_price  REAL NOT NULL DEFAULT 0"),
                         ("inst_value", "ALTER TABLE daily ADD COLUMN inst_value REAL NOT NULL DEFAULT 0")]:
            if col not in existing_cols:
                c.execute(sql)
                log.info(f"DB migrated: added column {col}")
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


def db_has_trade_date(trade_date: str) -> bool:
    dates = db_dates(1)
    if not dates or dates[0] != trade_date:
        return False
    with _db() as c:
        n = c.execute(
            "SELECT COUNT(*) FROM daily WHERE date=? AND market='TWSE' AND inst_net != 0",
            (trade_date,)
        ).fetchone()[0]
    if n == 0:
        log.info(f"Trade date {trade_date} exists but TWSE inst_net all 0, will retry")
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
        vol   = _int(r.get("trade_volume"))
        val   = _int(r.get("trade_value"))
        inst  = _int(r.get("inst_net"))
        avg   = _flt(r.get("avg_price"))   if "avg_price"   in r.index and _flt(r.get("avg_price"))   != 0 else _calc_avg_price(vol, val)
        ival  = _flt(r.get("inst_value"))  if "inst_value"  in r.index and _flt(r.get("inst_value"))  != 0 else _calc_inst_value(inst, avg)
        netyi = _flt(r.get("net_yi"))      if "net_yi"      in r.index and _flt(r.get("net_yi"))      != 0 else _calc_net_yi(vol, val, inst)
        rows.append((
            r["date"], str(r["code"]).zfill(4), r.get("market", ""),
            r.get("name", ""),
            _flt(r.get("close_price")),
            vol, val, avg, inst, ival, netyi,
        ))
    with _db() as c:
        c.executemany("""
            INSERT OR REPLACE INTO daily
            (date,code,market,name,close_price,
             trade_volume,trade_value,avg_price,inst_net,inst_value,net_yi)
            VALUES (?,?,?,?,?,?,?,?,?,?,?)
        """, rows)
    from collections import Counter
    for d, n in sorted(Counter(r[0] for r in rows).items()):
        log.info(f"Saved {n} rows for {d}")
    return len(rows)


def _flt(v) -> float:
    try:    return float(str(v).replace(",", "").strip())
    except: return 0.0


def _int(v) -> int:
    try:    return int(float(str(v).replace(",", "").strip()))
    except: return 0


def _calc_avg_price(volume: int, value: int) -> float:
    if volume == 0:
        return 0.0
    return round(value / volume, 2)


def _calc_inst_value(inst_net: int, avg_price: float) -> float:
    return round(inst_net * avg_price / 1e8, 6)


def _calc_net_yi(volume: int, value: int, inst_net: int) -> float:
    if volume == 0:
        return 0.0
    avg = value / volume
    return round(inst_net * avg / 1e8, 6)


def _get(url: str, params: dict = None, headers: dict = None,
         verify: bool = True, retries: int = 3, delay: float = 2.0):
    h = headers or TWSE_HEADERS
    for i in range(retries):
        try:
            r = requests.get(url, params=params, headers=h, timeout=30, verify=verify)
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


def _parse_twse_date(roc_date8: str) -> str:
    try:
        s = str(roc_date8).strip()
        if len(s) == 7:
            y = int(s[:3]) + 1911
            return f"{y}-{s[3:5]}-{s[5:7]}"
    except Exception:
        pass
    return ""


def fetch_twse_price_today() -> tuple[str, dict]:
    data = _get("https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL")
    if not isinstance(data, list) or len(data) < 100:
        raise RuntimeError(f"TWSE STOCK_DAY_ALL failed: got {len(data) if data else 0} rows")
    trade_date = _parse_twse_date(data[0].get("Date", "")) if data else ""
    if not trade_date:
        trade_date = TODAY
        log.warning(f"TWSE: cannot parse Date, using TODAY={TODAY}")
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
    log.info(f"TWSE price (today): {len(result)} stocks, trade_date={trade_date}")
    return trade_date, result


def fetch_twse_t86(date8: str) -> dict[str, int]:
    data = _get(
        "https://www.twse.com.tw/rwd/zh/fund/T86",
        params={"response": "json", "date": date8, "selectType": "ALL"},
    )
    if not isinstance(data, dict) or data.get("stat") != "OK":
        return {}
    fields   = data.get("fields", [])
    raw_data = data.get("data", [])
    last_field = fields[-1] if fields else ""
    if "三大法人" not in last_field:
        log.warning(f"T86 {date8}: unexpected last field={last_field!r}")
    result = {}
    for row in raw_data:
        if len(row) < 2:
            continue
        code = str(row[0]).strip().zfill(4)
        result[code] = _int(row[-1])
    log.info(f"T86(TWSE) {date8}: {len(result)} entries (last_field={last_field!r})")
    return result


def fetch_twse_stock_month(code: str, date8: str) -> dict[str, dict]:
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
        }
    return result


def fetch_tpex_price_today() -> tuple[str, dict]:
    data = _get(
        "https://www.tpex.org.tw/openapi/v1/tpex_mainboard_quotes",
        headers=TPEX_HEADERS, verify=False,
    )
    if not isinstance(data, list) or len(data) < 5:
        raise RuntimeError(f"TPEx quotes failed: got {len(data) if data else 0} rows")
    trade_date = _parse_twse_date(data[0].get("Date", "")) if data else ""
    if not trade_date:
        trade_date = TODAY
        log.warning(f"TPEx: cannot parse Date, using TODAY={TODAY}")
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
            "volume": _int(item.get("TradingShares",     0)),
            "value":  _int(item.get("TransactionAmount", 0)),
        }
    log.info(f"TPEx price (today): {len(result)} stocks, trade_date={trade_date}")
    return trade_date, result


def fetch_tpex_inst() -> dict[str, int]:
    data = _get(
        "https://www.tpex.org.tw/openapi/v1/tpex_3insti_daily_trading",
        headers=TPEX_HEADERS, verify=False,
    )
    if not isinstance(data, list) or len(data) < 5:
        raise RuntimeError(f"TPEx 3insti failed: got {len(data) if data else 0} rows")
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


def fetch_today() -> pd.DataFrame:
    with ThreadPoolExecutor(max_workers=3) as pool:
        f_twse_p = pool.submit(fetch_twse_price_today)
        f_tpex_p = pool.submit(fetch_tpex_price_today)
        f_tpex_i = pool.submit(fetch_tpex_inst)
        twse_date, r_twse_p = f_twse_p.result()
        tpex_date, r_tpex_p = f_tpex_p.result()
        r_tpex_i = f_tpex_i.result()

    trade_date = twse_date or tpex_date or TODAY
    if trade_date != TODAY:
        log.info(f"API trade_date={trade_date} (system date={TODAY})")

    r_twse_i = fetch_twse_t86(trade_date.replace("-", ""))
    log.info(f"Parallel done: TWSE {len(r_twse_p)} price/{len(r_twse_i)} inst "
             f"| TPEx {len(r_tpex_p)} price/{len(r_tpex_i)} inst")

    rows = []
    for code, p in r_twse_p.items():
        inst  = r_twse_i.get(code, 0)
        avg   = _calc_avg_price(p["volume"], p["value"])
        rows.append({
            "date": trade_date, "code": code, "market": "TWSE", "name": p["name"],
            "close_price":  p["close"],
            "trade_volume": p["volume"], "trade_value": p["value"],
            "avg_price":    avg,
            "inst_net":     inst,
            "inst_value":   _calc_inst_value(inst, avg),
            "net_yi":       _calc_net_yi(p["volume"], p["value"], inst),
        })
    for code, p in r_tpex_p.items():
        inst  = r_tpex_i.get(code, 0)
        avg   = _calc_avg_price(p["volume"], p["value"])
        rows.append({
            "date": trade_date, "code": code, "market": "TPEx", "name": p["name"],
            "close_price":  p["close"],
            "trade_volume": p["volume"], "trade_value": p["value"],
            "avg_price":    avg,
            "inst_net":     inst,
            "inst_value":   _calc_inst_value(inst, avg),
            "net_yi":       _calc_net_yi(p["volume"], p["value"], inst),
        })

    df = pd.DataFrame(rows)
    log.info(f"Today total: {len(df)} stocks (TWSE={sum(1 for r in rows if r['market']=='TWSE')}, "
             f"TPEx={sum(1 for r in rows if r['market']=='TPEx')}), trade_date={trade_date}")
    return df


def _fetch_twse_history_one(code: str, months: list[str]) -> tuple[str, dict]:
    merged = {}
    for m8 in months:
        merged.update(fetch_twse_stock_month(code, m8))
    return code, merged


def fetch_twse_history(twse_codes: list[str], months: list[str],
                        max_workers: int = 8) -> pd.DataFrame:
    log.info(f"TWSE history backfill: {len(twse_codes)} codes x {len(months)} months (parallel={max_workers})")
    price_hist: dict[str, dict] = {}
    done = 0
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {pool.submit(_fetch_twse_history_one, code, months): code for code in twse_codes}
        for fut in as_completed(futures):
            code = futures[fut]
            try:
                _, merged = fut.result()
                price_hist[code] = merged
            except Exception as e:
                log.warning(f"history fetch failed {code}: {e}")
                price_hist[code] = {}
            done += 1
            if done % 200 == 0:
                log.info(f"  TWSE history progress: {done}/{len(twse_codes)}")

    all_dates = sorted({d for v in price_hist.values() for d in v.keys()})
    log.info(f"TWSE history dates: {all_dates[0] if all_dates else 'N/A'} ~ "
             f"{all_dates[-1] if all_dates else 'N/A'} ({len(all_dates)} dates)")

    t86: dict[str, dict] = {}
    for dd in all_dates:
        t86[dd] = fetch_twse_t86(dd.replace("-", ""))
        time.sleep(0.3)

    rows = []
    for code, day_data in price_hist.items():
        for dd, p in day_data.items():
            inst  = t86.get(dd, {}).get(code, 0)
            avg   = _calc_avg_price(p["volume"], p["value"])
            rows.append({
                "date": dd, "code": code, "market": "TWSE", "name": "",
                "close_price":  0,
                "trade_volume": p["volume"], "trade_value": p["value"],
                "avg_price":    avg,
                "inst_net":     inst,
                "inst_value":   _calc_inst_value(inst, avg),
                "net_yi":       _calc_net_yi(p["volume"], p["value"], inst),
            })
    df = pd.DataFrame(rows)
    log.info(f"TWSE history total: {len(df)} rows")
    return df


def _months_for_lookback() -> list[str]:
    today = date.today()
    this_month = today.strftime("%Y%m01")
    prev = today.replace(day=1) - timedelta(days=1)
    return sorted({prev.strftime("%Y%m01"), this_month})


def load_groups() -> tuple[dict[str, list[str]], dict[str, str]]:
    """
    解析 input/group.csv（cp950 編碼）
    格式：
      row 0 : 族群名稱（奇數欄空白）
      row 1 : 代號, 名稱, 代號, 名稱, ...（標題，跳過）
      row 2+: 個股代號與名稱交替排列
    回傳 (groups, name_map)
    """
    raw  = open(GROUP_CSV, 'rb').read()
    text = raw.decode('cp950', errors='replace')
    df   = pd.read_csv(StringIO(text), header=None)

    groups:   dict[str, list[str]] = {}
    name_map: dict[str, str]       = {}

    for col_idx in range(0, df.shape[1], 2):
        if col_idx + 1 >= df.shape[1]:
            break
        gname = str(df.iloc[0, col_idx]).strip()
        if not gname or gname == 'nan':
            continue
        for row_idx in range(2, len(df)):
            code = str(df.iloc[row_idx, col_idx]).strip()
            name = str(df.iloc[row_idx, col_idx + 1]).strip()
            if not code or code == 'nan':
                continue
            c4 = code.zfill(4)
            groups.setdefault(gname, []).append(c4)
            if name and name != 'nan':
                name_map[c4] = name

    log.info(f"Groups: {len(groups)}, "
             f"{sum(len(v) for v in groups.values())} stocks, "
             f"names: {len(name_map)}")
    return groups, name_map


def load_xq_csv_files() -> pd.DataFrame:
    """
    input/XQ/YYYYMMDD_Data.csv → date, code, close_price
    支援 utf-8-sig / cp950
    """
    frames = []
    xq_dir = XQ_DIR
    if not xq_dir.exists():
        xq_dir = INPUT_DIR
    for f in sorted(xq_dir.glob("*_Data.csv")):
        date8 = f.stem.split("_")[0]
        if not (len(date8) == 8 and date8.isdigit()):
            continue
        dd = f"{date8[:4]}-{date8[4:6]}-{date8[6:8]}"
        try:
            raw = f.read_bytes()
            text = None
            for enc in ("utf-8-sig", "utf-8", "cp950", "big5"):
                try:
                    text = raw.decode(enc)
                    break
                except (UnicodeDecodeError, LookupError):
                    continue
            if text is None:
                text = raw.decode("cp950", errors="replace")
            df = pd.read_csv(StringIO(text))
            if "代碼" not in df.columns or "成交" not in df.columns:
                log.warning(f"{f.name}: missing required columns (代碼/成交), skip")
                continue
            sub = df[["代碼", "成交"]].copy()
            sub.columns = ["code", "close_price"]
            sub["code"] = (
                sub["code"].astype(str).str.strip()
                .str.replace(r"\.0$", "", regex=True)
                .str.zfill(4)
            )
            sub["close_price"] = pd.to_numeric(sub["close_price"], errors="coerce")
            sub["date"] = dd
            sub = sub.dropna(subset=["close_price"])
            sub = sub[sub["code"].str.match(r"^\d{4,6}$")]
            frames.append(sub[["date", "code", "close_price"]])
            log.info(f"XQ {f.name}: {len(sub)} rows for {dd}")
        except Exception as e:
            log.warning(f"Failed to load {f.name}: {e}")
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def load_tpex_csv_files() -> pd.DataFrame:
    """
    input/TPEx/TPEx_YYYYMMDD.csv → date, code, close_price, trade_volume, trade_value
    格式：前2行標題，第3行起為資料（big5編碼）
    欄位：代號, 名稱, 收盤, 漲跌, 開盤, 最高, 最低, 均價, 成交股數, 成交金額(元), ...
    """
    frames = []
    if not TPEX_DIR.exists():
        return pd.DataFrame()
    for f in sorted(TPEX_DIR.glob("TPEx_*.csv")):
        stem = f.stem.replace("TPEx_", "")
        if not (len(stem) == 8 and stem.isdigit()):
            continue
        dd = f"{stem[:4]}-{stem[4:6]}-{stem[6:8]}"
        try:
            raw = f.read_bytes()
            text = raw.decode('big5', errors='replace')
            lines = text.replace('\r\n', '\n').replace('\r', '\n').split('\n')
            data_text = '\n'.join(lines[2:])
            df = pd.read_csv(StringIO(data_text))
            needed = {'代號', '收盤', '成交股數', '成交金額(元)'}
            if not needed.issubset(df.columns):
                log.warning(f"{f.name}: missing columns {needed - set(df.columns)}, skip")
                continue
            df['代號'] = df['代號'].astype(str).str.strip().str.zfill(4)
            df['close_price']  = pd.to_numeric(df['收盤'].astype(str).str.replace(',',''), errors='coerce')
            df['trade_volume'] = pd.to_numeric(df['成交股數'].astype(str).str.replace(',',''), errors='coerce').fillna(0).astype(int)
            df['trade_value']  = pd.to_numeric(df['成交金額(元)'].astype(str).str.replace(',',''), errors='coerce').fillna(0).astype(int)
            df['avg_price']    = df.apply(lambda r: _calc_avg_price(int(r['trade_volume']), int(r['trade_value'])), axis=1)
            df['inst_net']     = 0
            df['inst_value']   = 0.0
            df['net_yi']       = 0.0
            df['name']  = df['名稱'].astype(str).str.strip() if '名稱' in df.columns else ''
            df['date']  = dd
            df['code']  = df['代號']
            df['market'] = 'TPEx'
            sub = df[['date','code','market','name','close_price',
                       'trade_volume','trade_value','avg_price',
                       'inst_net','inst_value','net_yi']].copy()
            sub = sub.dropna(subset=['close_price'])
            sub = sub[sub['code'].str.match(r'^\d{4,6}$')]
            frames.append(sub)
            log.info(f"TPEx CSV {f.name}: {len(sub)} rows for {dd}")
        except Exception as e:
            log.warning(f"Failed to load {f.name}: {e}")
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def load_tpex_dealer_csv_files() -> dict[str, dict[str, int]]:
    """
    input/TPExDealer/TPExDealer_YYYYMMDD.csv → {date: {code: inst_net}}
    格式：cp950，第1行說明文字，第2行欄位名，第3行起資料
    取「三大法人買賣超股數合計」欄（最後一欄）
    """
    result: dict[str, dict[str, int]] = {}
    if not TPEX_DEALER_DIR.exists():
        return result
    for f in sorted(TPEX_DEALER_DIR.glob("TPExDealer_*.csv")):
        stem = f.stem.replace("TPExDealer_", "")
        if not (len(stem) == 8 and stem.isdigit()):
            continue
        dd = f"{stem[:4]}-{stem[4:6]}-{stem[6:8]}"
        try:
            raw  = f.read_bytes()
            text = raw.decode('cp950', errors='replace')
            lines = text.replace('\r\n', '\n').replace('\r', '\n').split('\n')
            data_text = '\n'.join(lines[1:])
            df = pd.read_csv(StringIO(data_text))
            inst_col = '三大法人買賣超股數合計'
            if inst_col not in df.columns or '代號' not in df.columns:
                log.warning(f"{f.name}: missing required columns, skip")
                continue
            day_map: dict[str, int] = {}
            for _, row in df.iterrows():
                code = str(row['代號']).strip().zfill(4)
                if not code.strip('0'):
                    continue
                val = str(row[inst_col]).replace(',', '').strip()
                try:
                    day_map[code] = int(float(val))
                except (ValueError, TypeError):
                    pass
            result[dd] = day_map
            log.info(f"TPExDealer {f.name}: {len(day_map)} stocks for {dd}")
        except Exception as e:
            log.warning(f"Failed to load {f.name}: {e}")
    return result


def compute(hist_df: pd.DataFrame,
            xq_close_df: pd.DataFrame,
            groups: dict[str, list[str]],
            name_map: dict[str, str]) -> tuple[list[dict], dict[str, list[dict]]]:
    if hist_df.empty:
        return [], {}

    df = hist_df.copy()
    df["code"] = df["code"].astype(str).str.zfill(4)
    df["date"] = df["date"].astype(str)

    all_dates = sorted(df["date"].unique())
    latest    = all_dates[-1]
    log.info(f"Dates in DB: {all_dates[0]} ~ {latest} ({len(all_dates)} days)")

    if not xq_close_df.empty:
        xq = xq_close_df.copy()
        xq["code"] = xq["code"].astype(str).str.zfill(4)
        xq["date"] = xq["date"].astype(str)
        close_pv    = xq.pivot_table(index="code", columns="date", values="close_price", aggfunc="first")
        close_dates = sorted(close_pv.columns)
        log.info(f"XQ close dates: {close_dates[:3]}...{close_dates[-2:] if len(close_dates)>2 else ''}")
    else:
        close_pv    = df.pivot_table(index="code", columns="date", values="close_price", aggfunc="first")
        close_dates = sorted(close_pv.columns)
        log.warning("No XQ close data, using API close_price fallback")

    if latest in close_pv.columns:
        close_now = close_pv[latest].dropna()
    elif close_dates:
        close_now = close_pv[close_dates[-1]].dropna()
        log.warning(f"latest={latest} not in XQ close, using {close_dates[-1]}")
    else:
        close_now = df[df["date"] == latest].set_index("code")["close_price"]

    all_group_codes = {str(c).zfill(4) for codes in groups.values() for c in codes}
    matched = all_group_codes & set(close_now.index)
    log.info(f"Group codes={len(all_group_codes)}, matched in close_now={len(matched)}")

    d_prev1  = close_dates[-2]  if len(close_dates) >= 2  else None
    d_prev6  = close_dates[-6]  if len(close_dates) >= 6  else None
    d_prev21 = close_dates[-21] if len(close_dates) >= 21 else None
    log.info(f"chg_1d base={d_prev1}, chg_5d base={d_prev6}, chg_20d base={d_prev21}")

    def pct(base_date) -> pd.Series:
        if base_date is None or base_date not in close_pv.columns:
            return pd.Series(0.0, index=close_now.index)
        base  = close_pv[base_date]
        c     = close_now.reindex(base.index)
        valid = base.notna() & (base > 0.01) & c.notna() & (c > 0.01)
        s     = pd.Series(0.0, index=base.index)
        s[valid] = ((c[valid] - base[valid]) / base[valid] * 100).round(2)
        return s

    chg_1d  = pct(d_prev1)
    chg_5d  = pct(d_prev6)
    chg_20d = pct(d_prev21)

    net_pv  = df.pivot_table(index="code", columns="date", values="net_yi", aggfunc="first")
    last5   = all_dates[-5:]
    last20  = all_dates[-20:]
    net_1d  = net_pv[latest]             if latest          in net_pv.columns else pd.Series(dtype=float)
    net_prev = net_pv[all_dates[-2]]     if len(all_dates) >= 2 and all_dates[-2] in net_pv.columns else pd.Series(dtype=float)
    net_5d  = net_pv[[c for c in last5  if c in net_pv.columns]].sum(axis=1)
    net_20d = net_pv[[c for c in last20 if c in net_pv.columns]].sum(axis=1)

    log.info(f"net_pv shape={net_pv.shape}, last5={last5}")
    nonzero_5d = int((net_5d != 0).sum())
    log.info(f"net_5d: min={round(float(net_5d.min()),2)}, max={round(float(net_5d.max()),2)}, nonzero={nonzero_5d}")

    def clamp(s: pd.Series, limit: float) -> pd.Series:
        return s.where(s.abs() <= limit, 0.0)

    net_1d  = clamp(net_1d,  1000.0)
    net_5d  = clamp(net_5d,  5000.0)
    net_20d = clamp(net_20d, 20000.0)

    db_names = df[df["date"] == latest].set_index("code")["name"].to_dict()
    def get_name(code: str) -> str:
        return name_map.get(code, "") or db_names.get(code, "")

    records: list[dict] = []
    details: dict[str, list[dict]] = {}

    for gname, raw_codes in groups.items():
        codes = [str(c).zfill(4) for c in raw_codes if str(c).zfill(4) in close_now.index]

        stocks = []
        for c in codes:
            c1  = float(chg_1d.get(c,  0) or 0)
            c5  = float(chg_5d.get(c,  0) or 0)
            c20 = float(chg_20d.get(c, 0) or 0)
            if abs(c1)  > 11:  c1  = 0.0
            if abs(c5)  > 60:  c5  = 0.0
            if abs(c20) > 200: c20 = 0.0
            stocks.append({
                "code":     c,
                "name":     get_name(c),
                "close":    round(float(close_now.get(c, 0)), 2),
                "net_1d":   round(float(net_1d.get(c,   0) or 0), 4),
                "net_prev": round(float(net_prev.get(c,  0) or 0), 4),
                "net_5d":   round(float(net_5d.get(c,   0) or 0), 4),
                "net_20d":  round(float(net_20d.get(c,  0) or 0), 4),
                "chg_1d":   round(c1,  2),
                "chg_5d":   round(c5,  2),
                "chg_20d":  round(c20, 2),
            })
        stocks.sort(key=lambda x: x["net_1d"], reverse=True)
        details[gname] = stocks

        if not codes:
            records.append(_empty(gname, len(raw_codes)))
            continue

        g1    = round(sum(s["net_1d"]   for s in stocks), 3)
        g_prev= round(sum(s["net_prev"] for s in stocks), 3)
        g5    = round(sum(s["net_5d"]   for s in stocks), 3)
        g20   = round(sum(s["net_20d"]  for s in stocks), 3)

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

        g5_avg = g5 / 5 if g5 != 0 else 0.0

        # 資金加速度（主力）：三個條件同時成立
        #   1. 最後交易日淨買超 > 前一日淨買超
        #   2. 最後交易日與前一日淨買超皆 > 0
        #   3. 最後交易日與前一日淨買超皆 > 五日平均
        is_accelerating = (
            g1 > g_prev
            and g1 > 0 and g_prev > 0
            and g1 > g5_avg and g_prev > g5_avg
        )

        if   is_accelerating:          label = "主力"
        elif g5 > 0 and not is_accelerating: label = "輪動"
        elif g5 < -2:                  label = "退潮"
        else:                          label = "觀望"

        records.append({
            "g": gname, "cnt": len(raw_codes), "matched": len(codes),
            "net_1d": g1, "net_5d": g5, "net_20d": g20,
            "chg_1d": gc1, "chg_5d": gc5, "chg_20d": gc20, "label": label,
        })

    log.info(f"Groups: {len(records)} | " +
             " | ".join(f"{l}={sum(1 for r in records if r['label']==l)}"
                        for l in ["主力","輪動","退潮","觀望"]))
    return records, details


def _empty(gname: str, cnt: int) -> dict:
    return {"g": gname, "cnt": cnt, "matched": 0,
            "net_1d": 0.0, "net_5d": 0.0, "net_20d": 0.0,
            "chg_1d": 0.0, "chg_5d": 0.0, "chg_20d": 0.0, "label": "觀望"}


def export_json(records: list[dict], details: dict, trade_date: str) -> None:
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
    _jdump("group_stats.json",          [{k: v for k, v in r.items() if k != "stocks"} for r in records])
    _jdump("metadata.json", {
        "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "trade_date":   trade_date,
        "groups":       len(records),
        "inflow":       len(inflow),
        "stealth":      len(stealth),
    })
    log.info(f"JSON: bubble={len(bubble)}, inflow={len(inflow)}, stealth={len(stealth)}")


def export_csv() -> None:
    """
    匯出 DB 全部資料為 CSV，欄位用中文命名，方便直接用 Excel 閱讀。
    儲存至 docs/assets/data/market_data.csv，隨每日更新推送到 GitHub。
    """
    with _db() as c:
        df = pd.read_sql_query(
            """
            SELECT
                date         AS 日期,
                code         AS 代號,
                market       AS 市場,
                name         AS 名稱,
                close_price  AS 收盤價,
                trade_volume AS 成交總股數,
                trade_value  AS 成交總金額,
                avg_price    AS 成交均價,
                inst_net     AS 三大法人買賣超股數,
                inst_value   AS 三大法人買賣超金額億
            FROM daily
            ORDER BY date, market, code
            """,
            c
        )
    if df.empty:
        log.warning("export_csv: no data")
        return
    out = DATA_DIR / "market_data.csv"
    df.to_csv(out, index=False, encoding="utf-8-sig")
    log.info(f"CSV exported: {out} ({len(df):,} rows, "
             f"{df['日期'].nunique()} dates, {df['代號'].nunique()} codes)")


def _jdump(fname: str, data):
    with open(DATA_DIR / fname, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--force",          action="store_true", help="強制重抓今日")
    ap.add_argument("--dry-run",        action="store_true", help="只用現有DB+XQ重算，不打API")
    ap.add_argument("--reset-history",  action="store_true", help="強制重新補抓TWSE歷史")
    ap.add_argument("--purge-bad-data", action="store_true", help="清除trade_value=0的污染資料並重新補抓")
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
            log.info(f"Purge: found {n} rows with trade_value=0")
            if n > 0:
                c.execute("DELETE FROM daily WHERE trade_value=0")
                log.info(f"Purge: deleted {n} bad rows")
        args.reset_history = True

    if not args.dry_run:
        latest_in_db = (db_dates(1) or [None])[0]

        if args.force or not (latest_in_db and db_has_trade_date(latest_in_db)):
            today_df = fetch_today()
            if today_df.empty:
                log.error("Today fetch returned empty, aborting")
                return
            actual_trade_date = today_df["date"].iloc[0]
            if not args.force and db_has_trade_date(actual_trade_date):
                log.info(f"[CACHE] Trade date {actual_trade_date} already in DB")
            else:
                db_save(today_df)
        else:
            log.info(f"[CACHE] Latest trade date {latest_in_db} already in DB")

        existing_dates = set(db_dates(25))
        if len(existing_dates) < 21 or args.reset_history:
            log.info(f"DB has {len(existing_dates)} trade dates, backfilling TWSE history")
            today_db = db_load(days=1)
            twse_codes = sorted(
                today_db[today_db["market"] == "TWSE"]["code"].astype(str).str.zfill(4).unique()
            ) if not today_db.empty else []
            months = _months_for_lookback()
            log.info(f"Backfill: {len(twse_codes)} TWSE codes, months={months}")
            hist_df = fetch_twse_history(twse_codes, months)
            if not hist_df.empty:
                hist_df = hist_df[~hist_df["date"].isin(existing_dates)]
                if not hist_df.empty:
                    db_save(hist_df)

    hist = db_load(days=25)
    if hist.empty:
        log.error("No data in DB")
        return

    tpex_csv = load_tpex_csv_files()
    if not tpex_csv.empty:
        with _db() as c:
            tpex_done = {r[0] for r in c.execute(
                "SELECT DISTINCT date FROM daily WHERE market='TPEx'"
            ).fetchall()}
        new_tpex = tpex_csv[~tpex_csv["date"].isin(tpex_done)].copy()
        if not new_tpex.empty:
            log.info(f"Saving {len(new_tpex)} TPEx CSV rows for {new_tpex['date'].nunique()} new dates")
            db_save(new_tpex)
            hist = db_load(days=25)

    tpex_dealer = load_tpex_dealer_csv_files()
    if tpex_dealer:
        inserted = updated = 0
        with _db() as c:
            for dd, inst_map in tpex_dealer.items():
                for code, inst in inst_map.items():
                    if not inst:
                        continue
                    row = c.execute(
                        "SELECT trade_volume, trade_value, name FROM daily "
                        "WHERE date=? AND code=? AND market='TPEx'",
                        (dd, code)
                    ).fetchone()
                    if row:
                        vol, val = row[0], row[1]
                        avg  = _calc_avg_price(vol, val)
                        ival = _calc_inst_value(inst, avg)
                        nyi  = _calc_net_yi(vol, val, inst)
                        c.execute(
                            "UPDATE daily SET inst_net=?, inst_value=?, net_yi=? "
                            "WHERE date=? AND code=? AND market='TPEx'",
                            (inst, ival, nyi, dd, code)
                        )
                        updated += 1
                    else:
                        name = name_map.get(code, "")
                        c.execute("""
                            INSERT OR IGNORE INTO daily
                            (date,code,market,name,close_price,trade_volume,trade_value,
                             avg_price,inst_net,inst_value,net_yi)
                            VALUES (?,?,?,?,0,0,0,0,?,?,?)
                        """, (dd, code, "TPEx", name, inst, 0.0, 0.0))
                        inserted += 1
        log.info(f"TPExDealer: updated={updated}, inserted={inserted} rows "
                 f"across {len(tpex_dealer)} dates")
        if inserted or updated:
            hist = db_load(days=25)

    xq_close = load_xq_csv_files()
    records, details = compute(hist, xq_close, groups, name_map)
    export_json(records, details, hist["date"].max())
    export_csv()

    with _db() as c:
        total  = c.execute("SELECT COUNT(*) FROM daily").fetchone()[0]
        latest = c.execute("SELECT MAX(date) FROM daily").fetchone()[0]
    log.info(f"DB: {total} rows, latest={latest}")
    log.info("Pipeline complete")


if __name__ == "__main__":
    main()
