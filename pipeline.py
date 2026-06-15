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
TPEX_DIR         = INPUT_DIR / "TPEx"
TPEX_DEALER_DIR  = INPUT_DIR / "TPExDealer"
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


def fetch_close_yfinance(codes_market: dict[str, str], days: int = 30) -> pd.DataFrame:
    """
    用 yfinance 批次抓取全市場收盤價歷史。
    TWSE 上市：代號.TW，TPEx 上櫃：代號.TWO
    分批下載避免 Yahoo Finance 限速（每批 200 支，批次間休息 2 秒）
    """
    import yfinance as yf

    def is_yf_supported(code: str) -> bool:
        if not code.isdigit():
            return False
        if len(code) == 4:
            return True
        if len(code) == 6 and not code.startswith("7"):
            return True
        return False

    twse_tickers = {f"{c}.TW":  c for c, m in codes_market.items()
                    if m == "TWSE" and is_yf_supported(c)}
    tpex_tickers = {f"{c}.TWO": c for c, m in codes_market.items()
                    if m == "TPEx" and is_yf_supported(c)}
    all_tickers  = {**twse_tickers, **tpex_tickers}

    skipped = len(codes_market) - len(all_tickers)
    if skipped:
        log.info(f"yfinance: skipped {skipped} unsupported codes (ETN/non-numeric)")

    if not all_tickers:
        return pd.DataFrame()

    ticker_list = list(all_tickers.keys())
    batch_size  = 200
    batches     = [ticker_list[i:i+batch_size] for i in range(0, len(ticker_list), batch_size)]
    log.info(f"yfinance: {len(ticker_list)} tickers → {len(batches)} batches x {batch_size} ({days}d)")

    all_rows = []
    for i, batch in enumerate(batches):
        try:
            raw = yf.download(
                batch,
                period=f"{days}d",
                auto_adjust=True,
                progress=False,
                threads=True,
            )
            if raw.empty:
                log.warning(f"yfinance batch {i+1}/{len(batches)}: no data")
                continue

            try:
                close = raw["Close"] if "Close" in raw.columns else raw["Adj Close"]
            except Exception:
                log.warning(f"yfinance batch {i+1}/{len(batches)}: cannot find Close column")
                continue

            # 單 ticker 時 close 是 Series，多 ticker 時是 DataFrame
            if isinstance(close, pd.Series):
                close = close.to_frame(name=batch[0])

            for ticker in batch:
                if ticker not in close.columns:
                    continue
                series = close[ticker].dropna()
                code = all_tickers[ticker]
                for dt, price in series.items():
                    date_str = dt.strftime("%Y-%m-%d") if hasattr(dt, "strftime") else str(dt)[:10]
                    if price > 0:
                        all_rows.append({"date": date_str, "code": code, "close_price": float(price)})

            log.info(f"yfinance batch {i+1}/{len(batches)}: done ({len(batch)} tickers)")

        except Exception as e:
            log.warning(f"yfinance batch {i+1}/{len(batches)} failed: {e}")

        if i < len(batches) - 1:
            time.sleep(3)

    df = pd.DataFrame(all_rows)
    if df.empty:
        log.warning("yfinance: no data returned across all batches")
        return df

    log.info(f"yfinance total: {len(df)} records, "
             f"{df['date'].nunique()} dates, "
             f"{df['code'].nunique()} codes, "
             f"range={df['date'].min()} ~ {df['date'].max()}")
    return df


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

    # T86 先試今日（TODAY_8），若有資料代表 TWSE 三大法人已揭露
    # 若無資料，退回 STOCK_DAY_ALL 的日期
    t86_today = fetch_twse_t86(TODAY_8)
    if t86_today:
        twse_inst_date = TODAY
        r_twse_i = t86_today
        log.info(f"T86 has today's data ({TODAY}), using as TWSE inst date")
    else:
        twse_inst_date = twse_date
        r_twse_i = fetch_twse_t86(twse_date.replace("-", ""))
        log.info(f"T86 no data for today, using STOCK_DAY_ALL date={twse_date}")

    # TWSE 收盤價：STOCK_DAY_ALL 若已更新到今日則用今日，否則用 API 日期
    # 當 T86 有今日資料但 STOCK_DAY_ALL 還沒更新，TWSE 收盤價仍用 API 日期
    # （收盤價晚揭露，三大法人已先揭露的情況）
    if twse_inst_date == TODAY and twse_date != TODAY:
        log.info(f"TWSE price still at {twse_date}, inst already at {twse_inst_date}")

    tpex_date = tpex_date or TODAY

    log.info(f"Parallel done: TWSE price {len(r_twse_p)}/{twse_date}, inst {len(r_twse_i)}/{twse_inst_date} "
             f"| TPEx {len(r_tpex_p)}/{tpex_date} price/{len(r_tpex_i)} inst")

    rows = []
    for code, p in r_twse_p.items():
        inst  = r_twse_i.get(code, 0)
        avg   = _calc_avg_price(p["volume"], p["value"])
        rows.append({
            "date": twse_date, "code": code, "market": "TWSE", "name": p["name"],
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
            "date": tpex_date, "code": code, "market": "TPEx", "name": p["name"],
            "close_price":  p["close"],
            "trade_volume": p["volume"], "trade_value": p["value"],
            "avg_price":    avg,
            "inst_net":     inst,
            "inst_value":   _calc_inst_value(inst, avg),
            "net_yi":       _calc_net_yi(p["volume"], p["value"], inst),
        })

    df = pd.DataFrame(rows)
    log.info(f"Today total: {len(df)} stocks "
             f"(TWSE={sum(1 for r in rows if r['market']=='TWSE')} [{twse_date}], "
             f"TPEx={sum(1 for r in rows if r['market']=='TPEx')} [{tpex_date}])")
    return df


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
            yf_close_df: pd.DataFrame,
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

    if not yf_close_df.empty:
        yf = yf_close_df.copy()
        yf["code"] = yf["code"].astype(str).str.zfill(4)
        yf["date"] = yf["date"].astype(str)
        close_pv    = yf.pivot_table(index="code", columns="date", values="close_price", aggfunc="first")
        close_dates = sorted(close_pv.columns)
        log.info(f"yfinance close dates: {close_dates[0]} ~ {close_dates[-1]} ({len(close_dates)} days)")
    else:
        close_pv    = df.pivot_table(index="code", columns="date", values="close_price", aggfunc="first")
        close_dates = sorted(close_pv.columns)
        log.warning("No yfinance close data, using API close_price fallback")

    # 漲跌幅序列：用 yfinance 日期（不與 DB 取交集）
    # 買賣超序列：用 DB 的日期
    trade_dates  = close_dates
    latest_trade = trade_dates[-1]
    log.info(f"yfinance trade dates: {trade_dates[0]} ~ {latest_trade} ({len(trade_dates)} days)")

    if latest_trade in close_pv.columns:
        close_now = close_pv[latest_trade].dropna()
    else:
        close_now = df[df["date"] == latest_trade].set_index("code")["close_price"]

    all_group_codes = {str(c).zfill(4) for codes in groups.values() for c in codes}
    matched = all_group_codes & set(close_now.index)
    log.info(f"Group codes={len(all_group_codes)}, matched in close_now={len(matched)}")

    # 漲跌幅基準日（用 XQ 日期序列）
    d_prev1  = trade_dates[-2]  if len(trade_dates) >= 2  else None   # 前一個交易日
    d_prev2  = trade_dates[-3]  if len(trade_dates) >= 3  else None   # 前二個交易日
    d_prev3  = trade_dates[-4]  if len(trade_dates) >= 4  else None   # 前三個交易日
    d_prev6  = trade_dates[-6]  if len(trade_dates) >= 6  else None   # 五日前
    d_prev21 = trade_dates[-21] if len(trade_dates) >= 21 else None   # 二十日前
    log.info(f"latest_trade={latest_trade}, chg_1d base={d_prev1}, chg_5d base={d_prev6}, chg_20d base={d_prev21}")

    # 買賣超序列：用 DB 日期（最新5/20個交易日）
    net_last5  = all_dates[-5:]
    net_last20 = all_dates[-20:]
    net_latest = all_dates[-1]   # DB 最新交易日（可能與 XQ 最新日不同）
    net_prev_d = all_dates[-2] if len(all_dates) >= 2 else None

    def pct(base_date, now_date=None) -> pd.Series:
        """計算漲跌幅：(now - base) / base × 100"""
        now_col = close_pv[now_date] if now_date and now_date in close_pv.columns else close_now
        if base_date is None or base_date not in close_pv.columns:
            return pd.Series(0.0, index=now_col.index)
        base  = close_pv[base_date]
        c     = now_col.reindex(base.index)
        valid = base.notna() & (base > 0.01) & c.notna() & (c > 0.01)
        s     = pd.Series(0.0, index=base.index)
        s[valid] = ((c[valid] - base[valid]) / base[valid] * 100).round(2)
        return s

    chg_1d    = pct(d_prev1)                    # 最新交易日單日漲跌幅
    chg_prev1 = pct(d_prev2, d_prev1)           # 倒數第2個交易日單日漲跌幅
    chg_prev2 = pct(d_prev3, d_prev2)           # 倒數第3個交易日單日漲跌幅
    chg_5d    = pct(d_prev6)                    # 五日漲跌幅
    chg_20d   = pct(d_prev21)                   # 二十日漲跌幅

    # 淨買賣超：也用統一的 trade_dates 決定最後 5/20 天
    net_pv  = df.pivot_table(index="code", columns="date", values="net_yi", aggfunc="first")
    net_1d   = net_pv[net_latest].fillna(0.0)  if net_latest  in net_pv.columns else pd.Series(0.0, index=net_pv.index)
    net_prev = net_pv[net_prev_d].fillna(0.0)  if net_prev_d  in net_pv.columns else pd.Series(0.0, index=net_pv.index)
    net_5d   = net_pv[[c for c in net_last5  if c in net_pv.columns]].sum(axis=1).fillna(0.0)
    net_20d  = net_pv[[c for c in net_last20 if c in net_pv.columns]].sum(axis=1).fillna(0.0)

    log.info(f"net_pv shape={net_pv.shape}, net_last5={net_last5}")
    nonzero_5d = int((net_5d != 0).sum())
    log.info(f"net_5d: min={round(float(net_5d.min()),2)}, max={round(float(net_5d.max()),2)}, nonzero={nonzero_5d}")

    def clamp(s: pd.Series, limit: float) -> pd.Series:
        return s.where(s.abs() <= limit, 0.0)

    net_1d  = clamp(net_1d,  1000.0)
    net_5d  = clamp(net_5d,  5000.0)
    net_20d = clamp(net_20d, 20000.0)

    db_names = df[df["date"] == net_latest].set_index("code")["name"].to_dict()
    def get_name(code: str) -> str:
        return name_map.get(code, "") or db_names.get(code, "")

    records: list[dict] = []
    details: dict[str, list[dict]] = {}

    for gname, raw_codes in groups.items():
        codes = [str(c).zfill(4) for c in raw_codes if str(c).zfill(4) in close_now.index]

        stocks = []
        for c in codes:
            c1    = float(chg_1d.get(c,    0) or 0)
            cp1   = float(chg_prev1.get(c, 0) or 0)
            cp2   = float(chg_prev2.get(c, 0) or 0)
            c5    = float(chg_5d.get(c,    0) or 0)
            c20   = float(chg_20d.get(c,   0) or 0)
            if abs(c1)  > 11:  c1  = 0.0
            if abs(cp1) > 11:  cp1 = 0.0
            if abs(cp2) > 11:  cp2 = 0.0
            if abs(c5)  > 60:  c5  = 0.0
            if abs(c20) > 200: c20 = 0.0
            stocks.append({
                "code":       c,
                "name":       get_name(c),
                "close":      round(float(close_now.get(c, 0)), 2),
                "net_1d":     round(float(net_1d.get(c,    0) or 0), 4),
                "net_prev":   round(float(net_prev.get(c,  0) or 0), 4),
                "net_5d":     round(float(net_5d.get(c,    0) or 0), 4),
                "net_20d":    round(float(net_20d.get(c,   0) or 0), 4),
                "chg_1d":     round(c1,  2),
                "chg_prev1":  round(cp1, 2),
                "chg_prev2":  round(cp2, 2),
                "chg_5d":     round(c5,  2),
                "chg_20d":    round(c20, 2),
            })
        stocks.sort(key=lambda x: x["net_1d"], reverse=True)
        details[gname] = stocks

        if not codes:
            records.append(_empty(gname, len(raw_codes)))
            continue

        g1     = round(sum(s["net_1d"]   for s in stocks), 3)
        g_prev = round(sum(s["net_prev"] for s in stocks), 3)
        g5     = round(sum(s["net_5d"]   for s in stocks), 3)
        g20    = round(sum(s["net_20d"]  for s in stocks), 3)

        chg1_vals   = [s["chg_1d"]    for s in stocks if s["chg_1d"]    != 0.0]
        chgp1_vals  = [s["chg_prev1"] for s in stocks if s["chg_prev1"] != 0.0]
        chgp2_vals  = [s["chg_prev2"] for s in stocks if s["chg_prev2"] != 0.0]
        chg5_vals   = [s["chg_5d"]    for s in stocks if s["chg_5d"]    != 0.0]
        chg20_vals  = [s["chg_20d"]   for s in stocks if s["chg_20d"]   != 0.0]

        gc1   = round(sum(chg1_vals)  / len(chg1_vals),  2) if chg1_vals  else 0.0
        gcp1  = round(sum(chgp1_vals) / len(chgp1_vals), 2) if chgp1_vals else 0.0
        gcp2  = round(sum(chgp2_vals) / len(chgp2_vals), 2) if chgp2_vals else 0.0
        gc5   = round(sum(chg5_vals)  / len(chg5_vals),  2) if chg5_vals  else 0.0
        gc20  = round(sum(chg20_vals) / len(chg20_vals), 2) if chg20_vals else 0.0

        if abs(g1)   > 1000:  g1   = 0.0
        if abs(g5)   > 5000:  g5   = 0.0
        if abs(g20)  > 20000: g20  = 0.0
        if abs(gc1)  > 11:    gc1  = 0.0
        if abs(gcp1) > 11:    gcp1 = 0.0
        if abs(gcp2) > 11:    gcp2 = 0.0
        if abs(gc5)  > 60:    gc5  = 0.0
        if abs(gc20) > 200:   gc20 = 0.0

        g5_avg = g5 / 5 if g5 != 0 else 0.0

        # ── 資金條件 ──────────────────────────────────────────────
        # 正向：最新兩日淨買超 > 0 且皆 > 五日平均
        flow_pos = (g1 > 0 and g_prev > 0
                    and g1 > g5_avg and g_prev > g5_avg)
        # 負向：最新兩日淨買超 < 0 且皆 < 五日平均
        flow_neg = (g1 < 0 and g_prev < 0
                    and g1 < g5_avg and g_prev < g5_avg)

        # ── 價格條件 ──────────────────────────────────────────────
        # 最新三個交易日單日漲跌幅全部 > 0
        price_all3_pos = (gc1 > 0 and gcp1 > 0 and gcp2 > 0)
        # 最新三個交易日單日漲跌幅任一 <= 0
        price_any3_le0 = not price_all3_pos
        # 五日總漲跌幅
        chg5_pos = gc5 > 0

        # ── 標籤 ──────────────────────────────────────────────────
        # 主力：資金正向 + 最新三日全漲 + 五日總漲
        if   flow_pos and price_all3_pos and chg5_pos:
            label = "主力"
        # 輪動：資金正向，但近三日有跌或五日未漲（排除主力）
        elif flow_pos and (price_any3_le0 or not chg5_pos):
            label = "輪動"
        # 退潮：資金負向
        elif flow_neg:
            label = "退潮"
        # 觀望：其餘
        else:
            label = "觀望"

        records.append({
            "g": gname, "cnt": len(raw_codes), "matched": len(codes),
            "net_1d": g1, "net_prev": g_prev, "net_5d": g5, "net_20d": g20,
            "chg_1d": gc1, "chg_prev1": gcp1, "chg_prev2": gcp2,
            "chg_5d": gc5, "chg_20d": gc20, "label": label,
        })

    log.info(f"Groups: {len(records)} | " +
             " | ".join(f"{l}={sum(1 for r in records if r['label']==l)}"
                        for l in ["主力","輪動","退潮","觀望"]))
    return records, details


def _empty(gname: str, cnt: int) -> dict:
    return {"g": gname, "cnt": cnt, "matched": 0,
            "net_1d": 0.0, "net_prev": 0.0, "net_5d": 0.0, "net_20d": 0.0,
            "chg_1d": 0.0, "chg_prev1": 0.0, "chg_prev2": 0.0,
            "chg_5d": 0.0, "chg_20d": 0.0, "label": "觀望"}


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

    def flow_pos(r):
        """資金正向：最新兩日淨買超 > 0 且皆 > 五日平均"""
        avg = r["net_5d"] / 5 if r["net_5d"] != 0 else 0.0
        return (r["net_1d"] > 0 and r["net_prev"] > 0
                and r["net_1d"] > avg and r["net_prev"] > avg)

    def price_any3_le(r, threshold):
        """最近三個交易日單日漲跌幅任一天 <= threshold"""
        return (r["chg_1d"] <= threshold
                or r["chg_prev1"] <= threshold
                or r["chg_prev2"] <= threshold)

    # ── 流入低漲幅 ────────────────────────────────────────────────
    # 資金正向（最新兩日淨買超 > 0 且 > 五日平均）
    # 最近三日單日漲跌幅任一 <= 5%
    # 五日總漲跌幅 <= 10%
    inflow = attach(sorted(
        [r for r in records
         if flow_pos(r)
         and price_any3_le(r, 5.0)
         and r["chg_5d"] <= 10.0],
        key=lambda x: x["net_5d"], reverse=True
    ))

    # ── 偷偷佈局 ──────────────────────────────────────────────────
    # 最新兩日淨買超 > 0（但不需要 > 五日平均）
    # 五日淨買超總和 < 0（整體仍在流出）
    # 最近三日單日漲跌幅任一 <= 5%
    # 五日總漲跌幅 <= 10%
    stealth = attach(sorted(
        [r for r in records
         if r["net_1d"] > 0 and r["net_prev"] > 0
         and r["net_5d"] < 0
         and price_any3_le(r, 5.0)
         and r["chg_5d"] <= 10.0],
        key=lambda x: x["net_5d"], reverse=False
    ))

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
    import math

    def sanitize(obj):
        if isinstance(obj, float):
            return 0.0 if (math.isnan(obj) or math.isinf(obj)) else obj
        if isinstance(obj, dict):
            return {k: sanitize(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [sanitize(v) for v in obj]
        return obj

    with open(DATA_DIR / fname, "w", encoding="utf-8") as f:
        json.dump(sanitize(data), f, ensure_ascii=False, separators=(",", ":"))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--force",          action="store_true", help="強制重抓今日")
    ap.add_argument("--dry-run",        action="store_true", help="只用現有DB，不打API")
    ap.add_argument("--purge-bad-data", action="store_true", help="清除trade_value=0的污染資料並重新補抓")
    args = ap.parse_args()

    log.info("=" * 60)
    log.info(f"TW$FLOW  {datetime.now()}  system_date={TODAY}")
    if args.force:          log.info("*** FORCE ***")
    if args.dry_run:        log.info("*** DRY-RUN ***")
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

    if not args.dry_run:
        latest_in_db = (db_dates(1) or [None])[0]

        today_df = fetch_today()
        if today_df.empty:
            log.error("Today fetch returned empty, aborting")
            return

        # TWSE 和 TPEx 可能日期不同，分別判斷是否需要存入
        to_save = []
        for market in ["TWSE", "TPEx"]:
            mdf = today_df[today_df["market"] == market]
            if mdf.empty:
                continue
            mdate = mdf["date"].iloc[0]
            with _db() as c:
                exists = c.execute(
                    "SELECT COUNT(*) FROM daily WHERE date=? AND market=? AND inst_net!=0",
                    (mdate, market)
                ).fetchone()[0]
            if not args.force and exists > 0:
                log.info(f"[CACHE] {market} {mdate} already in DB")
            else:
                log.info(f"Saving {market} {mdate} ({len(mdf)} rows)")
                to_save.append(mdf)

        if to_save:
            db_save(pd.concat(to_save, ignore_index=True))

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

    # ── yfinance 收盤價歷史（漲跌幅計算用）────────────────────────
    hist = db_load(days=25)  # reload after TPExDealer updates
    codes_market = {}
    if not hist.empty:
        codes_market = dict(zip(
            hist["code"].astype(str).str.zfill(4),
            hist["market"]
        ))
    yf_close = fetch_close_yfinance(codes_market, days=35)

    records, details = compute(hist, yf_close, groups, name_map)
    export_json(records, details, hist["date"].max())
    export_csv()

    with _db() as c:
        total  = c.execute("SELECT COUNT(*) FROM daily").fetchone()[0]
        latest = c.execute("SELECT MAX(date) FROM daily").fetchone()[0]
    log.info(f"DB: {total} rows, latest={latest}")
    log.info("Pipeline complete")


if __name__ == "__main__":
    main()
