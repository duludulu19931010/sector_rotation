import argparse
import json
import logging
import os
import sqlite3
import sys
import time
from contextlib import contextmanager
from datetime import date, datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import requests

ROOT      = Path(__file__).resolve().parent
DB_FILE   = ROOT / "db" / "market.db"
DATA_DIR  = ROOT / "docs" / "assets" / "data"
GROUP_CSV = ROOT / "input" / "Group.csv"
NAMES_CSV = ROOT / "input" / "stock_list.csv"
LOG_FILE  = ROOT / "pipeline.log"
TODAY     = date.today().strftime("%Y-%m-%d")

FINMIND_URL = "https://api.finmindtrade.com/api/v4/data"

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


def db_trade_dates(n: int = 30) -> list[str]:
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
    log.info(f"DB load: {len(df)} rows, {df['date'].nunique()} dates")
    return df


def db_save(df: pd.DataFrame) -> int:
    if df is None or df.empty:
        return 0
    rows = [(
        r["date"], str(r["code"]).zfill(4), r.get("market", ""),
        r.get("name", ""),
        _flt(r.get("open_price")),  _flt(r.get("close_price")),
        _flt(r.get("high_price")),  _flt(r.get("low_price")),
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
    cnt = Counter(r[0] for r in rows)
    for d, n in sorted(cnt.items()):
        log.info(f"Saved {n} rows for {d}")
    return len(rows)


def _finmind_token() -> str:
    token = os.environ.get("FINMIND_TOKEN", "")
    if not token:
        log.error("FINMIND_TOKEN is not set. Set it with: $env:FINMIND_TOKEN='your_token'")
        log.error("Get a free token at https://finmindtrade.com")
        raise SystemExit(1)
    return token


def _fm_get(dataset: str, start_date: str, end_date: str,
            stock_id: str = None, retries: int = 3) -> pd.DataFrame:
    token = _finmind_token()
    params = {
        "dataset":    dataset,
        "start_date": start_date,
        "end_date":   end_date,
    }
    if stock_id:
        params["data_id"] = stock_id
    headers = {"Authorization": f"Bearer {token}"}

    for i in range(retries):
        try:
            r = requests.get(FINMIND_URL, params=params, headers=headers, timeout=60)
            if r.status_code == 400:
                log.error(f"FinMind 400 Bad Request: {r.text[:200]}")
                log.error(f"  dataset={dataset}, start={start_date}, end={end_date}")
                return pd.DataFrame()
            r.raise_for_status()
            body = r.json()
            if body.get("status") != 200:
                msg = body.get("msg", "unknown error")
                log.warning(f"FinMind {dataset}: {msg}")
                if "over" in msg.lower() or "limit" in msg.lower():
                    log.warning("Rate limit hit, waiting 60s")
                    time.sleep(60)
                    continue
                return pd.DataFrame()
            data = body.get("data", [])
            if not data:
                log.warning(f"FinMind {dataset}: empty data for {start_date}~{end_date}")
                return pd.DataFrame()
            df = pd.DataFrame(data)
            log.info(f"FinMind {dataset}: {len(df)} rows ({start_date} ~ {end_date})")
            return df
        except SystemExit:
            raise
        except Exception as e:
            log.warning(f"FinMind GET [{i+1}/{retries}] {dataset}: {e}")
            if i < retries - 1:
                time.sleep(3 * (i + 1))
    return pd.DataFrame()


def fetch_price_range(start_date: str, end_date: str) -> pd.DataFrame:
    df = _fm_get("TaiwanStockPrice", start_date, end_date)
    if df.empty:
        return pd.DataFrame()
    df = df.rename(columns={
        "stock_id":       "code",
        "Trading_Volume": "trade_volume",
        "Trading_money":  "trade_value",
        "open":           "open_price",
        "close":          "close_price",
        "max":            "high_price",
        "min":            "low_price",
    })
    df["code"] = df["code"].astype(str).str.zfill(4)
    df["market"] = "TWSE"
    keep = ["date","code","market","trade_volume","trade_value",
            "open_price","close_price","high_price","low_price"]
    df = df[[c for c in keep if c in df.columns]]
    return df


def fetch_inst_range(start_date: str, end_date: str) -> pd.DataFrame:
    df = _fm_get("TaiwanStockInstitutionalInvestorsBuySell", start_date, end_date)
    if df.empty:
        return pd.DataFrame()
    df["code"] = df["stock_id"].astype(str).str.zfill(4)
    df["net"] = _int_series(df["buy"]) - _int_series(df["sell"])
    inst_net = (
        df.groupby(["date", "code"])["net"]
        .sum()
        .reset_index()
        .rename(columns={"net": "inst_net"})
    )
    return inst_net


def _int_series(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s.astype(str).str.replace(",", ""), errors="coerce").fillna(0).astype(int)


def _calc_net_yi(volume: int, value: int, inst_net: int) -> float:
    if value == 0 or volume == 0:
        return 0.0
    return round((volume / value) * inst_net * 1000, 6)


def fetch_and_merge(start_date: str, end_date: str) -> pd.DataFrame:
    price_df = fetch_price_range(start_date, end_date)
    if price_df.empty:
        log.error("No price data from FinMind")
        return pd.DataFrame()

    inst_df = fetch_inst_range(start_date, end_date)

    if not inst_df.empty:
        merged = price_df.merge(inst_df, on=["date", "code"], how="left")
        merged["inst_net"] = merged["inst_net"].fillna(0).astype(int)
    else:
        log.warning("No institutional data, inst_net set to 0")
        merged = price_df.copy()
        merged["inst_net"] = 0

    for col in ["trade_volume", "trade_value"]:
        if col in merged.columns:
            merged[col] = pd.to_numeric(
                merged[col].astype(str).str.replace(",", ""), errors="coerce"
            ).fillna(0).astype(int)

    merged["net_yi"] = merged.apply(
        lambda r: _calc_net_yi(
            int(r.get("trade_volume", 0)),
            int(r.get("trade_value", 0)),
            int(r.get("inst_net", 0)),
        ), axis=1
    )

    needed = ["date","code","market","open_price","close_price",
              "high_price","low_price","trade_volume","trade_value","inst_net","net_yi"]
    for col in needed:
        if col not in merged.columns:
            merged[col] = 0
    merged = merged[needed]

    log.info(f"Merged: {len(merged)} rows, {merged['date'].nunique()} dates")
    return merged


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

    last5  = all_dates[-5:]
    last20 = all_dates[-20:]
    net_1d  = net_pv[latest] if latest in net_pv.columns else pd.Series(dtype=float)
    net_5d  = net_pv[[c for c in last5  if c in net_pv.columns]].sum(axis=1)
    net_20d = net_pv[[c for c in last20 if c in net_pv.columns]].sum(axis=1)

    db_names = df[df["date"] == latest].set_index("code")["name"].to_dict()

    def name(code: str) -> str:
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


def _jdump(fname: str, data):
    with open(DATA_DIR / fname, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def _flt(v) -> float:
    try:    return float(str(v).replace(",", "").strip())
    except: return 0.0


def _int(v) -> int:
    try:    return int(str(v).replace(",", "").strip())
    except: return 0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--force",   action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    log.info("=" * 60)
    log.info(f"TW$FLOW  {datetime.now()}  trade_date={TODAY}")
    if args.force:   log.info("*** FORCE ***")
    if args.dry_run: log.info("*** DRY-RUN ***")
    log.info("=" * 60)

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    db_init()

    groups   = load_groups()
    name_map = load_names()

    if not args.dry_run:
        if not args.force and db_has_today():
            log.info("[CACHE] Today already in DB")
        else:
            have_dates = set(db_trade_dates(25))
            need_days  = 21

            target_dates = []
            d = date.today()
            while len(target_dates) < need_days:
                if d.weekday() < 5:
                    target_dates.append(d.strftime("%Y-%m-%d"))
                d -= timedelta(days=1)
            target_dates.sort()

            missing = [d for d in target_dates if d not in have_dates]

            if missing:
                start = missing[0]
                end   = missing[-1]
                log.info(f"Fetching {len(missing)} missing dates: {start} ~ {end}")
                new_df = fetch_and_merge(start, end)
                if not new_df.empty:
                    save_df = new_df[new_df["date"].isin(missing)]
                    db_save(save_df)
                else:
                    log.warning("No new data fetched")
            else:
                log.info("All target dates already in DB")

    hist = db_load(days=21)
    if hist.empty:
        log.error("No data in DB, cannot compute")
        return

    records, details = compute(hist, groups, name_map)
    trade_date = hist["date"].max()
    export_json(records, details, trade_date)

    with _db() as c:
        total = c.execute("SELECT COUNT(*) FROM daily").fetchone()[0]
        latest_date = c.execute("SELECT MAX(date) FROM daily").fetchone()[0]
    log.info(f"DB: {total} rows, latest={latest_date}")
    log.info("Pipeline complete")


if __name__ == "__main__":
    main()
