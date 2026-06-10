"""
src/db/store.py
SQLite 資料庫 — 唯一真相來源

設計原則：
  1. trade_date 一律存 YYYY-MM-DD（8 位 YYYYMMDD 在寫入時自動轉換）
  2. INSERT OR REPLACE — 同日重跑安全
  3. WAL mode — 讀寫並發
  4. 所有對外函式回傳 pandas DataFrame 或 list[dict]，不暴露 sqlite3 物件
"""

import sqlite3
import logging
from contextlib import contextmanager
from datetime import date, datetime
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

_DB: Path | None = None   # 由 init() 設定


def init(db_path: str | Path) -> None:
    global _DB
    _DB = Path(db_path)
    _DB.parent.mkdir(parents=True, exist_ok=True)
    with _conn() as c:
        c.executescript(_DDL)
        # 遷移：舊版 group_daily 沒有 net_20d 欄位
        cols = [r["name"] for r in c.execute("PRAGMA table_info(group_daily)")]
        if "net_20d" not in cols:
            c.execute("ALTER TABLE group_daily ADD COLUMN net_20d REAL DEFAULT 0")
            logger.info("Migrated: added net_20d to group_daily")
    logger.info(f"DB ready: {_DB}")


@contextmanager
def _conn():
    assert _DB is not None, "call db.init() first"
    con = sqlite3.connect(str(_DB))
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


# ── DDL ──────────────────────────────────────────────

_DDL = """
CREATE TABLE IF NOT EXISTS institutional_flow (
    trade_date  TEXT NOT NULL,          -- YYYY-MM-DD
    code        TEXT NOT NULL,          -- 股票代號（4位）
    name        TEXT,
    foreign_net INTEGER DEFAULT 0,      -- 外資淨買超（張）
    trust_net   INTEGER DEFAULT 0,      -- 投信淨買超（張）
    dealer_net  INTEGER DEFAULT 0,      -- 自營淨買超（張）
    total_net   INTEGER DEFAULT 0,      -- 三大法人合計（張）
    PRIMARY KEY (trade_date, code)
);

CREATE TABLE IF NOT EXISTS stock_prices (
    trade_date  TEXT NOT NULL,
    code        TEXT NOT NULL,
    name        TEXT,
    market      TEXT,                   -- TWSE / TPEx
    close_price REAL,
    open_price  REAL,
    high_price  REAL,
    low_price   REAL,
    volume      INTEGER,
    PRIMARY KEY (trade_date, code)
);

CREATE TABLE IF NOT EXISTS group_daily (
    trade_date      TEXT NOT NULL,
    group_name      TEXT NOT NULL,
    stock_count     INTEGER,
    matched         INTEGER,
    net_5d          REAL,   -- 五日淨買超（億）X軸
    net_1d          REAL,   -- 今日淨買超（億）Y軸 = 資金加速度
    net_20d         REAL,   -- 近20日累計淨買超（億）
    change_5d_pct   REAL,
    change_1d_pct   REAL,
    label           TEXT,
    PRIMARY KEY (trade_date, group_name)
);

CREATE TABLE IF NOT EXISTS etf_holdings (
    trade_date   TEXT NOT NULL,
    etf_code     TEXT NOT NULL,
    stock_code   TEXT NOT NULL,
    stock_name   TEXT,
    shares       INTEGER,               -- 持股張數
    market_value REAL,
    weight_pct   REAL,                  -- 佔基金比例 %
    chg_shares   INTEGER DEFAULT 0,     -- 較前日增減張數
    chg_pct      REAL    DEFAULT 0,     -- 較前日增減比率 %
    PRIMARY KEY (trade_date, etf_code, stock_code)
);

CREATE INDEX IF NOT EXISTS ix_if_date  ON institutional_flow(trade_date);
CREATE INDEX IF NOT EXISTS ix_sp_date  ON stock_prices(trade_date);
CREATE INDEX IF NOT EXISTS ix_gd_date  ON group_daily(trade_date);
CREATE INDEX IF NOT EXISTS ix_eh_date  ON etf_holdings(trade_date, etf_code);
"""


# ── 日期工具 ──────────────────────────────────────────

def _d(v) -> str:
    """任意格式 → YYYY-MM-DD"""
    s = str(v).strip()
    if len(s) == 8 and s.isdigit():
        return f"{s[:4]}-{s[4:6]}-{s[6:8]}"
    return s[:10].replace("/", "-")


def today_str() -> str:
    return date.today().strftime("%Y-%m-%d")


# ── 快取查詢 ──────────────────────────────────────────

def has_institutional(trade_date: str) -> bool:
    """今日三大法人是否已入庫（與歷史最大筆數相比 >= 50% 視為完整）"""
    td = _d(trade_date)
    with _conn() as c:
        n = c.execute(
            "SELECT COUNT(*) FROM institutional_flow WHERE trade_date=?", (td,)
        ).fetchone()[0]
        mx = c.execute(
            "SELECT MAX(cnt) FROM (SELECT COUNT(*) AS cnt FROM institutional_flow GROUP BY trade_date)"
        ).fetchone()[0] or 0
    threshold = max(100, mx * 0.5)
    if n >= threshold and n > 0:
        logger.info(f"[DB-CACHE] institutional {td}: {n} rows")
        return True
    return False


def has_prices(trade_date: str) -> bool:
    """今日收盤價是否已入庫（自適應閾值）"""
    td = _d(trade_date)
    with _conn() as c:
        n = c.execute(
            "SELECT COUNT(*) FROM stock_prices WHERE trade_date=?", (td,)
        ).fetchone()[0]
        mx_row = c.execute(
            "SELECT MAX(cnt) FROM (SELECT COUNT(*) AS cnt FROM stock_prices GROUP BY trade_date)"
        ).fetchone()[0]
    mx = mx_row or 0
    threshold = max(100, mx * 0.5) if mx else 500
    if n >= threshold and n > 0:
        logger.info(f"[DB-CACHE] prices {td}: {n} rows")
        return True
    return False


def has_group_daily(trade_date: str) -> bool:
    td = _d(trade_date)
    with _conn() as c:
        n = c.execute(
            "SELECT COUNT(*) FROM group_daily WHERE trade_date=?", (td,)
        ).fetchone()[0]
    return n >= 50


def has_etf_holdings(etf_code: str, trade_date: str) -> bool:
    td = _d(trade_date)
    with _conn() as c:
        n = c.execute(
            "SELECT COUNT(*) FROM etf_holdings WHERE etf_code=? AND trade_date=?",
            (etf_code, td)
        ).fetchone()[0]
    return n > 0


# ── 讀取 ─────────────────────────────────────────────

def get_institutional_dates(n: int = 10) -> list[str]:
    """
    回傳有完整資料的最近 n 個交易日
    完整定義：該日筆數 >= 全部日期最大筆數的 50%（自適應，不寫死閾值）
    """
    with _conn() as c:
        rows = c.execute("""
            SELECT trade_date, COUNT(*) AS cnt FROM institutional_flow
            GROUP BY trade_date ORDER BY trade_date DESC
        """).fetchall()
    if not rows:
        return []
    max_cnt = max(r["cnt"] for r in rows)
    threshold = max(100, max_cnt * 0.5)
    return [r["trade_date"] for r in rows if r["cnt"] >= threshold][:n]


def load_institutional(days: int = 5) -> pd.DataFrame:
    """讀近 days 個有完整資料的交易日"""
    dates = get_institutional_dates(days)
    if not dates:
        return pd.DataFrame()
    ph = ",".join("?" * len(dates))
    with _conn() as c:
        df = pd.read_sql_query(
            f"SELECT trade_date,code,name,foreign_net,trust_net,dealer_net,total_net "
            f"FROM institutional_flow WHERE trade_date IN ({ph}) ORDER BY trade_date,code",
            c, params=dates
        )
    logger.info(f"[DB] institutional: {len(df)} rows, dates={dates[:3]}")
    return df


def load_prices(trade_date: str = None) -> pd.DataFrame:
    """讀指定日（預設最新日）收盤價"""
    with _conn() as c:
        if trade_date:
            td = _d(trade_date)
            df = pd.read_sql_query(
                "SELECT * FROM stock_prices WHERE trade_date=?", c, params=(td,)
            )
        else:
            df = pd.read_sql_query(
                "SELECT * FROM stock_prices "
                "WHERE trade_date=(SELECT MAX(trade_date) FROM stock_prices)", c
            )
    logger.info(f"[DB] prices: {len(df)} rows")
    return df


def load_group_daily(trade_date: str = None) -> list[dict]:
    with _conn() as c:
        if trade_date:
            rows = c.execute(
                "SELECT * FROM group_daily WHERE trade_date=? ORDER BY net_5d DESC",
                (_d(trade_date),)
            ).fetchall()
        else:
            rows = c.execute("""
                SELECT * FROM group_daily
                WHERE trade_date=(SELECT MAX(trade_date) FROM group_daily)
                ORDER BY net_5d DESC
            """).fetchall()
    return [dict(r) for r in rows]


def load_etf_holdings(etf_code: str, trade_date: str = None) -> list[dict]:
    with _conn() as c:
        if trade_date:
            rows = c.execute(
                "SELECT * FROM etf_holdings WHERE etf_code=? AND trade_date=? ORDER BY weight_pct DESC",
                (etf_code, _d(trade_date))
            ).fetchall()
        else:
            rows = c.execute(
                "SELECT * FROM etf_holdings WHERE etf_code=? "
                "AND trade_date=(SELECT MAX(trade_date) FROM etf_holdings WHERE etf_code=?) "
                "ORDER BY weight_pct DESC",
                (etf_code, etf_code)
            ).fetchall()
    return [dict(r) for r in rows]


# ── 寫入 ─────────────────────────────────────────────

def save_institutional(df: pd.DataFrame) -> int:
    """
    寫入三大法人資料
    df 需要欄位: trade_date, code, name, foreign_net, trust_net, dealer_net, total_net
    """
    if df is None or df.empty:
        return 0
    rows = []
    for _, r in df.iterrows():
        rows.append((
            _d(r["trade_date"]),
            str(r["code"]).zfill(4),
            str(r.get("name", "") or ""),
            _int(r.get("foreign_net")),
            _int(r.get("trust_net")),
            _int(r.get("dealer_net", r.get("dealer_self_net", 0))),
            _int(r.get("total_net")),
        ))
    with _conn() as c:
        c.executemany(
            "INSERT OR REPLACE INTO institutional_flow "
            "(trade_date,code,name,foreign_net,trust_net,dealer_net,total_net) "
            "VALUES (?,?,?,?,?,?,?)",
            rows
        )
    # log per date
    dates = sorted(set(r[0] for r in rows))
    for td in dates:
        n = sum(1 for r in rows if r[0] == td)
        logger.info(f"[DB] saved {n} institutional rows for {td}")
    return len(rows)


def save_prices(df: pd.DataFrame, trade_date: str) -> int:
    if df is None or df.empty:
        return 0
    td = _d(trade_date)
    rows = [(
        td,
        str(r.get("code", "")).zfill(4),
        str(r.get("name", "") or ""),
        str(r.get("market", "") or ""),
        _flt(r.get("close_price")),
        _flt(r.get("open_price")),
        _flt(r.get("high_price")),
        _flt(r.get("low_price")),
        _int(r.get("volume")),
    ) for _, r in df.iterrows()]
    with _conn() as c:
        c.executemany(
            "INSERT OR REPLACE INTO stock_prices "
            "(trade_date,code,name,market,close_price,open_price,high_price,low_price,volume) "
            "VALUES (?,?,?,?,?,?,?,?,?)",
            rows
        )
    logger.info(f"[DB] saved {len(rows)} prices for {td}")
    return len(rows)


def save_group_daily(records: list[dict], trade_date: str) -> int:
    if not records:
        return 0
    td = _d(trade_date)
    rows = [(
        td,
        r["group_name"],
        _int(r.get("stock_count")),
        _int(r.get("matched")),
        _flt(r.get("net_5d")),
        _flt(r.get("net_1d")),
        _flt(r.get("net_20d")),
        _flt(r.get("change_5d_pct")),
        _flt(r.get("change_1d_pct")),
        r.get("label", ""),
    ) for r in records]
    with _conn() as c:
        c.executemany(
            "INSERT OR REPLACE INTO group_daily "
            "(trade_date,group_name,stock_count,matched,net_5d,net_1d,net_20d,"
            "change_5d_pct,change_1d_pct,label) VALUES (?,?,?,?,?,?,?,?,?,?)",
            rows
        )
    logger.info(f"[DB] saved {len(rows)} group_daily for {td}")
    return len(rows)


def save_etf_holdings(etf_code: str, holdings: list[dict], trade_date: str) -> int:
    if not holdings:
        return 0
    td = _d(trade_date)
    rows = [(
        td, etf_code,
        str(h.get("stock_code", "")).zfill(4),
        str(h.get("stock_name", "") or ""),
        _int(h.get("shares", h.get("shares_k"))),
        _flt(h.get("market_value")),
        _flt(h.get("weight_pct", h.get("ratio"))),
        _int(h.get("chg_shares", h.get("change_shares", 0))),
        _flt(h.get("chg_pct",   h.get("change_pct",   0))),
    ) for h in holdings]
    with _conn() as c:
        c.executemany(
            "INSERT OR REPLACE INTO etf_holdings "
            "(trade_date,etf_code,stock_code,stock_name,shares,market_value,"
            "weight_pct,chg_shares,chg_pct) VALUES (?,?,?,?,?,?,?,?,?)",
            rows
        )
    logger.info(f"[DB] saved {len(rows)} etf_holdings for {etf_code} @ {td}")
    return len(rows)


# ── 摘要 ─────────────────────────────────────────────

def summary() -> dict:
    def _cnt(table):
        with _conn() as c:
            return c.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    def _max(table, col="trade_date"):
        with _conn() as c:
            v = c.execute(f"SELECT MAX({col}) FROM {table}").fetchone()[0]
            return v or "—"
    return {
        "institutional_flow": {"rows": _cnt("institutional_flow"), "latest": _max("institutional_flow")},
        "stock_prices":        {"rows": _cnt("stock_prices"),        "latest": _max("stock_prices")},
        "group_daily":         {"rows": _cnt("group_daily"),         "latest": _max("group_daily")},
        "etf_holdings":        {"rows": _cnt("etf_holdings"),        "latest": _max("etf_holdings")},
    }


# ── 型別轉換工具 ─────────────────────────────────────

def _int(v) -> int:
    try:    return int(str(v).replace(",", "").strip())
    except: return 0

def _flt(v) -> float:
    try:    return float(str(v).replace(",", "").strip())
    except: return 0.0
