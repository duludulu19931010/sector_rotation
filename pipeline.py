#!/usr/bin/env python3
"""
pipeline.py  —  每日資料更新主程式

執行流程：
  1. 讀取 input/Group.csv（族群清單）
  2. 三大法人資料    DB 有今日 → 讀 DB；否則 → 爬 T86 → 存 DB
  3. 收盤價          DB 有今日 → 讀 DB；否則 → 爬 API → 存 DB
  4. 族群計算        DB 有今日 → 從 DB 重算（保留個股明細）→ 輸出 JSON
  5. ETF 清單       TWSE OpenAPI 或 fallback
  6. ETF 持股       DB 有今日 → 讀 DB；否則 → 爬蟲 → 存 DB → 輸出 JSON

選項：
  --force     忽略快取，強制重新爬取所有 API
  --skip-etf  跳過 ETF 部分
  --dry-run   只從 DB 重算 JSON，不呼叫任何 API
"""

import argparse
import json
import logging
import sys
from datetime import date, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / "src"))

import pandas as pd

import src.db.store as db
from src.scrapers.twse import fetch_t86_multi, fetch_all_prices
from src.scrapers.etf  import (
    fetch_active_etf_list, get_top10_by_scale, get_top10_by_popularity,
    fetch_holdings, compute_holdings_change,
)
from src.analysis.compute import (
    load_groups, compute_stock_stats, compute_group_stats, export_json,
)

# ── 設定 ──────────────────────────────────────────────

DB_PATH   = ROOT / "db" / "market.db"
DATA_DIR  = ROOT / "docs" / "assets" / "data"
INPUT_CSV = ROOT / "input" / "Group.csv"
LOG_FILE  = ROOT / "pipeline.log"
TODAY     = date.today().strftime("%Y-%m-%d")
TODAY_8   = date.today().strftime("%Y%m%d")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(str(LOG_FILE), encoding="utf-8"),
    ],
)
log = logging.getLogger("pipeline")


# ══════════════════════════════════════════════════════
#  Steps
# ══════════════════════════════════════════════════════

def step1_groups() -> dict:
    log.info("=== Step 1: Load groups ===")
    g = load_groups(INPUT_CSV)
    log.info(f"  {len(g)} groups, {sum(len(v) for v in g.values())} stock entries")
    return g


def step2_institutional(force: bool = False) -> pd.DataFrame:
    log.info("=== Step 2: Institutional flow (20 days) ===")

    cached_dates = set(db.get_institutional_dates(25))

    if not force and db.has_institutional(TODAY) and len(cached_dates) >= 20:
        log.info("  [CACHE HIT] reading 20 days from DB")
        return db.load_institutional(days=20)

    # 抓 API：近 20 個交易日（已在 DB 的日期自動跳過，首次跑約 20 次 API，之後每天只抓 1 次）
    df = fetch_t86_multi(days=20, cached_dates=cached_dates if not force else set())
    if df.empty and not cached_dates:
        log.warning("  API returned nothing and DB empty")
        return pd.DataFrame()

    new_df = df[~df["trade_date"].isin(cached_dates)] if not force and not df.empty else df
    if not new_df.empty:
        db.save_institutional(new_df)

    return db.load_institutional(days=20)


def step3_prices(force: bool = False) -> pd.DataFrame:
    log.info("=== Step 3: Prices ===")

    if not force and db.has_prices(TODAY):
        log.info("  [CACHE HIT] reading from DB")
        return db.load_prices(TODAY)

    df = fetch_all_prices()
    if df.empty:
        log.warning("  API returned nothing, using DB fallback")
        return db.load_prices()

    db.save_prices(df, TODAY)
    return df


def step4_group_analysis(
    groups: dict,
    inst_df: pd.DataFrame,
    price_df: pd.DataFrame,
    force: bool = False,
) -> bool:
    log.info("=== Step 4: Group analysis ===")

    # 若快取命中但傳入的 df 是空的（dry-run），從 DB 補充
    if inst_df.empty:
        inst_df = db.load_institutional(days=5)
    if price_df.empty:
        price_df = db.load_prices()

    if inst_df.empty or price_df.empty:
        log.error("  No data available, skipping group analysis")
        _write_empty_json()
        return False

    stock_df = compute_stock_stats(inst_df, price_df)
    if stock_df.empty:
        log.error("  compute_stock_stats returned empty")
        _write_empty_json()
        return False

    records, details = compute_group_stats(groups, stock_df)

    # 輸出 JSON（含個股明細）
    summary = export_json(records, details, DATA_DIR)
    log.info(f"  {summary}")

    # 存 DB
    if not db.has_group_daily(TODAY) or force:
        db.save_group_daily(records, TODAY)

    return True


def step5_etf_list() -> tuple[list[dict], list[dict], list[dict]]:
    log.info("=== Step 5: ETF list ===")
    etfs     = fetch_active_etf_list()
    top_mc   = get_top10_by_scale(etfs)
    top_pop  = get_top10_by_popularity(etfs)
    log.info(f"  market_cap top10: {[e['code'] for e in top_mc]}")
    log.info(f"  popular    top10: {[e['code'] for e in top_pop]}")
    return etfs, top_mc, top_pop


def step6_etf_holdings(top_mc: list[dict], top_pop: list[dict]) -> None:
    log.info("=== Step 6: ETF holdings ===")
    all_etfs = {e["code"]: e for e in top_mc + top_pop}

    cache_dir = ROOT / ".cache" / "holdings"
    cache_dir.mkdir(parents=True, exist_ok=True)

    mc_reports  = []
    pop_reports = []

    for code, meta in all_etfs.items():
        # 今日持股
        if db.has_etf_holdings(code, TODAY):
            log.info(f"  [CACHE] {code} holdings in DB")
            curr = db.load_etf_holdings(code, TODAY)
        else:
            curr = fetch_holdings(code, TODAY_8)
            if curr:
                # 取昨日持股計算增減
                prev = db.load_etf_holdings(code)
                if prev and prev[0].get("trade_date", "") != TODAY:
                    curr = compute_holdings_change(curr, prev)
                db.save_etf_holdings(code, curr, TODAY)
            else:
                curr = db.load_etf_holdings(code)   # fallback 到最新 DB 資料

        report = {
            "etf_code":  code,
            "etf_name":  meta.get("name", ""),
            "manager":   meta.get("manager", ""),
            "data_date": TODAY,
            "holdings":  _format_holdings(curr),
        }

        if code in {e["code"] for e in top_mc}:
            mc_reports.append(report)
        if code in {e["code"] for e in top_pop}:
            pop_reports.append(report)

    # 五日流向
    mc_flow  = _compute_5d_flow(top_mc)
    pop_flow = _compute_5d_flow(top_pop)

    _jdump(DATA_DIR / "etf_market_cap_holdings.json",  mc_reports)
    _jdump(DATA_DIR / "etf_popular_holdings.json",     pop_reports)
    _jdump(DATA_DIR / "etf_market_cap_flow_5d.json",   mc_flow)
    _jdump(DATA_DIR / "etf_popular_flow_5d.json",      pop_flow)
    log.info(f"  ETF JSON exported")


def _format_holdings(raw: list[dict]) -> list[dict]:
    result = []
    for i, h in enumerate(sorted(raw, key=lambda x: x.get("weight_pct", x.get("ratio", 0)), reverse=True)):
        result.append({
            "rank":        i + 1,
            "stock_code":  str(h.get("stock_code", h.get("code", ""))).zfill(4),
            "stock_name":  h.get("stock_name", h.get("name", "")),
            "shares":      h.get("shares", h.get("shares_lot", 0)),
            "weight_pct":  h.get("weight_pct", h.get("ratio", 0)),
            "chg_shares":  h.get("chg_shares", h.get("change_shares", 0)),
            "chg_pct":     h.get("chg_pct",   h.get("change_pct",   0)),
        })
    return result


def _compute_5d_flow(etf_list: list[dict]) -> dict:
    """計算近五日增持/減持前十"""
    from collections import defaultdict
    changes: dict[str, dict] = defaultdict(lambda: {"name": "", "increase": 0, "decrease": 0})

    for etf in etf_list:
        code = etf["code"]
        dates = db.get_institutional_dates(6)   # 最多 6 個交易日
        prev_h: dict[str, int] = {}

        for td in reversed(dates):
            curr_rows = db.load_etf_holdings(code, td)
            curr_h = {h["stock_code"]: h["shares"] for h in curr_rows}
            for sc, shares in curr_h.items():
                delta = shares - prev_h.get(sc, 0)
                name  = next((h["stock_name"] for h in curr_rows if h["stock_code"] == sc), "")
                if delta > 0:
                    changes[sc]["name"]     = name
                    changes[sc]["increase"] += delta
                elif delta < 0:
                    changes[sc]["name"]     = name
                    changes[sc]["decrease"] += abs(delta)
            prev_h = curr_h

    top_buy = sorted(
        [{"stock_code": c, "stock_name": v["name"], "total_increase": v["increase"]}
         for c, v in changes.items() if v["increase"] > 0],
        key=lambda x: x["total_increase"], reverse=True
    )[:10]

    top_sell = sorted(
        [{"stock_code": c, "stock_name": v["name"], "total_decrease": v["decrease"]}
         for c, v in changes.items() if v["decrease"] > 0],
        key=lambda x: x["total_decrease"], reverse=True
    )[:10]

    return {"top_buy": top_buy, "top_sell": top_sell}


# ── 工具 ──────────────────────────────────────────────

def _write_empty_json():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    for fname in ["bubble_data.json", "inflow_low_gain.json",
                  "stealth_accumulation.json", "group_stats.json"]:
        p = DATA_DIR / fname
        if not p.exists():
            p.write_text("[]", encoding="utf-8")


def _jdump(path: Path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def _write_metadata():
    _jdump(DATA_DIR / "metadata.json", {
        "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "trade_date":   TODAY,
    })


# ══════════════════════════════════════════════════════
#  Main
# ══════════════════════════════════════════════════════

def main():
    ap = argparse.ArgumentParser(description="TW$FLOW daily pipeline")
    ap.add_argument("--force",      action="store_true", help="忽略快取，強制重新爬取")
    ap.add_argument("--skip-etf",   action="store_true", help="跳過 ETF 部分")
    ap.add_argument("--skip-group", action="store_true", help="跳過族群分析")
    ap.add_argument("--dry-run",    action="store_true", help="只用 DB 資料重算 JSON")
    args = ap.parse_args()

    log.info("=" * 60)
    log.info(f"TW$FLOW pipeline  {datetime.now()}  trade_date={TODAY}")
    if args.force:   log.info("  *** FORCE MODE ***")
    if args.dry_run: log.info("  *** DRY-RUN MODE ***")
    log.info("=" * 60)

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    db.init(DB_PATH)

    groups = step1_groups()

    if not args.skip_group:
        if args.dry_run:
            inst_df  = pd.DataFrame()
            price_df = pd.DataFrame()
        else:
            inst_df  = step2_institutional(force=args.force)
            price_df = step3_prices(force=args.force)
        step4_group_analysis(groups, inst_df, price_df, force=args.force)
    else:
        log.info("=== Step 2-4: skipped ===")

    if not args.skip_etf and not args.dry_run:
        _, top_mc, top_pop = step5_etf_list()
        step6_etf_holdings(top_mc, top_pop)
    else:
        log.info("=== Step 5-6: skipped ===")

    _write_metadata()
    log.info(f"DB: {json.dumps(db.summary(), ensure_ascii=False)}")
    log.info("Pipeline complete ✓")


if __name__ == "__main__":
    main()
