"""
src/analysis/compute.py
族群資金流向計算

指標定義（正式版）：
  net_5d    族群五日淨買超（億）= Σ 個股五日三大法人合計（張）× 收盤價 × 1000 ÷ 1e8
            → 泡泡圖 X 軸：資金出入量（億）

  net_1d    族群今日淨買超（億）= Σ 個股今日三大法人合計（張）× 收盤價 × 1000 ÷ 1e8
            → 泡泡圖 Y 軸：資金加速度（億/天）

  change_5d_pct  族群五日漲幅（%）= 族群個股的收盤價均漲幅
  change_1d_pct  族群今日漲幅（%）

個股明細：
  每個族群都帶 stocks 欄位，包含每檔個股的全部指標
  篩選表展開後顯示這些明細
"""

import json
import logging
from pathlib import Path

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


def load_groups(csv_path: str | Path) -> dict[str, list[str]]:
    """讀取族群 CSV（Big5 編碼）"""
    p = Path(csv_path)
    if not p.exists():
        logger.error(f"Group CSV not found: {p}")
        return {}
    raw = p.read_bytes()
    try:
        text = raw.decode("big5")
    except UnicodeDecodeError:
        text = raw.decode("cp950", errors="replace")

    lines = text.strip().replace("\r\n", "\n").replace("\r", "\n").split("\n")
    headers = [h.strip() for h in lines[0].split(",")]
    groups: dict[str, list[str]] = {h: [] for h in headers if h}
    for line in lines[1:]:
        for i, h in enumerate(headers):
            if not h:
                continue
            cols = line.split(",")
            if i < len(cols) and cols[i].strip():
                groups[h].append(cols[i].strip())
    logger.info(f"Groups: {len(groups)}, total stocks: {sum(len(v) for v in groups.values())}")
    return groups


def compute_stock_stats(
    inst_df: pd.DataFrame,   # institutional_flow（多日）
    price_df: pd.DataFrame,  # stock_prices（最新日）
) -> pd.DataFrame:
    """
    計算每檔個股的統計數據

    回傳 DataFrame 欄位：
      code, name
      total_5d_shares   五日三大法人合計（張）
      total_1d_shares   今日三大法人合計（張）
      close_price       最新收盤價
      close_5d_ago      五日前收盤價
      close_1d_ago      昨日收盤價
      net_5d            五日淨買超（億）
      net_1d            今日淨買超（億）
      change_5d_pct     五日漲幅（%）
      change_1d_pct     今日漲幅（%）
    """
    if inst_df.empty or price_df.empty:
        return pd.DataFrame()

    inst  = inst_df.copy()
    price = price_df.copy()
    inst["code"]  = inst["code"].astype(str).str.zfill(4)
    price["code"] = price["code"].astype(str).str.zfill(4)

    # ── 近 20 日合計（inst 內所有資料）─────────────────
    total_20d = (
        inst.groupby("code")["total_net"]
        .sum().reset_index()
        .rename(columns={"total_net": "total_20d_shares"})
    )

    # ── 五日合計（最近 5 個交易日）─────────────────────
    inst_dates = sorted(inst["trade_date"].unique())
    last5 = inst_dates[-5:] if len(inst_dates) >= 5 else inst_dates
    total_5d = (
        inst[inst["trade_date"].isin(last5)]
        .groupby("code")["total_net"]
        .sum().reset_index()
        .rename(columns={"total_net": "total_5d_shares"})
    )

    # ── 今日（最新日）合計 ────────────────────────────
    latest_inst_date = inst["trade_date"].max()
    total_1d = (
        inst[inst["trade_date"] == latest_inst_date]
        .groupby("code")["total_net"]
        .sum().reset_index()
        .rename(columns={"total_net": "total_1d_shares"})
    )

    # ── 收盤價：最新日 ────────────────────────────────
    dates = sorted(price["trade_date"].unique())
    latest_price = (
        price[price["trade_date"] == dates[-1]]
        [["code", "name", "close_price"]]
        .rename(columns={"close_price": "close_price"})
    )

    # ── 收盤價：五日前 ────────────────────────────────
    d5 = dates[-5] if len(dates) >= 5 else dates[0]
    price_5d = (
        price[price["trade_date"] == d5][["code", "close_price"]]
        .rename(columns={"close_price": "close_5d_ago"})
    )

    # ── 收盤價：昨日 ──────────────────────────────────
    d1 = dates[-2] if len(dates) >= 2 else dates[0]
    price_1d = (
        price[price["trade_date"] == d1][["code", "close_price"]]
        .rename(columns={"close_price": "close_1d_ago"})
    )

    # ── 合併 ─────────────────────────────────────────
    df = total_5d.merge(total_20d, on="code", how="left")
    df = df.merge(total_1d, on="code", how="left")
    df = df.merge(latest_price, on="code", how="left")
    df = df.merge(price_5d,     on="code", how="left")
    df = df.merge(price_1d,     on="code", how="left")

    df["total_1d_shares"]  = df["total_1d_shares"].fillna(0)
    df["total_20d_shares"] = df["total_20d_shares"].fillna(0)
    df["close_price"]  = pd.to_numeric(df["close_price"],  errors="coerce").fillna(0)
    df["close_5d_ago"] = pd.to_numeric(df["close_5d_ago"], errors="coerce")
    df["close_1d_ago"] = pd.to_numeric(df["close_1d_ago"], errors="coerce")

    # ── 億元換算 ─────────────────────────────────────
    df["net_5d"]  = (df["total_5d_shares"]  * df["close_price"] * 1000 / 1e8).round(4)
    df["net_1d"]  = (df["total_1d_shares"]  * df["close_price"] * 1000 / 1e8).round(4)
    df["net_20d"] = (df["total_20d_shares"] * df["close_price"] * 1000 / 1e8).round(4)

    # ── 漲幅 ─────────────────────────────────────────
    df["change_5d_pct"] = np.where(
        df["close_5d_ago"].notna() & (df["close_5d_ago"] > 0),
        ((df["close_price"] - df["close_5d_ago"]) / df["close_5d_ago"] * 100).round(2),
        0.0,
    )
    df["change_1d_pct"] = np.where(
        df["close_1d_ago"].notna() & (df["close_1d_ago"] > 0),
        ((df["close_price"] - df["close_1d_ago"]) / df["close_1d_ago"] * 100).round(2),
        0.0,
    )

    return df


def compute_group_stats(
    groups: dict[str, list[str]],
    stock_df: pd.DataFrame,
) -> tuple[list[dict], dict[str, list[dict]]]:
    """
    彙總到族群層級

    回傳：
      group_records    [{"group_name","net_5d","net_1d","change_5d_pct",...}]
      stock_details    {"族群名": [個股 dict, ...]}
    """
    if stock_df.empty:
        return [], {}

    ss = stock_df.copy()
    ss["code"] = ss["code"].astype(str).str.zfill(4)

    records: list[dict] = []
    details: dict[str, list[dict]] = {}

    for gname, codes in groups.items():
        padded  = [str(c).zfill(4) for c in codes]
        subset  = ss[ss["code"].isin(padded)]

        # 個股明細（不論族群是否入篩選）
        stock_list = []
        for _, s in subset.iterrows():
            stock_list.append({
                "code":          s["code"],
                "name":          s.get("name", ""),
                "close_price":   round(float(s.get("close_price", 0)), 2),
                "net_5d":        round(float(s.get("net_5d", 0)), 4),
                "net_1d":        round(float(s.get("net_1d", 0)), 4),
                "net_20d":       round(float(s.get("net_20d", 0)), 4),
                "change_5d_pct": round(float(s.get("change_5d_pct", 0)), 2),
                "change_1d_pct": round(float(s.get("change_1d_pct", 0)), 2),
            })
        stock_list.sort(key=lambda x: x["net_1d"], reverse=True)
        details[gname] = stock_list

        if subset.empty:
            records.append(_empty_group(gname, len(codes)))
            continue

        net_5d      = float(subset["net_5d"].sum())
        net_1d      = float(subset["net_1d"].sum())
        net_20d     = float(subset["net_20d"].sum())
        change_5d   = float(subset["change_5d_pct"].mean())
        change_1d   = float(subset["change_1d_pct"].mean())

        # 標籤
        if   net_5d > 2  and change_5d > 1:   label = "主力"
        elif net_5d > 0  and change_5d <= 1:   label = "輪動"
        elif net_5d < -2:                      label = "退潮"
        else:                                  label = "觀望"

        records.append({
            "group_name":    gname,
            "stock_count":   len(codes),
            "matched":       int(len(subset)),
            "net_5d":        round(net_5d,    3),   # X 軸
            "net_1d":        round(net_1d,    3),   # Y 軸
            "net_20d":       round(net_20d,   3),   # 近20日累計
            "change_5d_pct": round(change_5d, 2),
            "change_1d_pct": round(change_1d, 2),
            "label":         label,
        })

    return records, details


def _empty_group(name: str, stock_count: int) -> dict:
    return {
        "group_name": name, "stock_count": stock_count, "matched": 0,
        "net_5d": 0.0, "net_1d": 0.0, "net_20d": 0.0,
        "change_5d_pct": 0.0, "change_1d_pct": 0.0, "label": "觀望",
    }


# ── 篩選 ─────────────────────────────────────────────

def screen_inflow_low_gain(records: list[dict]) -> list[dict]:
    """
    資金大量流入但漲幅仍低
    條件：net_5d > 0 AND change_5d_pct < 10
    排序：net_5d 由大到小
    """
    return sorted(
        [r for r in records if r["net_5d"] > 0 and r["change_5d_pct"] < 10],
        key=lambda x: x["net_5d"], reverse=True
    )


def screen_stealth(records: list[dict]) -> list[dict]:
    """
    大盤跌但法人偷偷布局
    條件：net_1d > 0（今日還在買）AND net_5d > 0（五日累積也是買超）AND change_5d_pct < 0（但股價仍跌）
    排序：net_5d 由大到小
    """
    return sorted(
        [r for r in records
         if r["net_1d"] > 0 and r["net_5d"] > 0 and r["change_5d_pct"] < 0],
        key=lambda x: x["net_5d"], reverse=True
    )


# ── 輸出 JSON ─────────────────────────────────────────

def export_json(
    records: list[dict],
    details: dict[str, list[dict]],
    output_dir: str | Path,
) -> dict:
    """
    生成前端所需的所有 JSON

    bubble_data.json          泡泡圖資料（不含個股明細）
    group_stats.json          所有族群統計
    inflow_low_gain.json      篩選結果（含 stocks 個股明細）
    stealth_accumulation.json 篩選結果（含 stocks 個股明細）
    """
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    # bubble（含個股明細，供點擊側欄使用）
    bubble = []
    for r in records:
        bubble.append({
            **r,
            "x":      r["net_5d"],   # X 軸：五日淨買超（億）
            "y":      r["net_1d"],   # Y 軸：今日淨買超（億）
            "size":   max(10, min(72, abs(r["net_5d"]) * 2.8 + 12)),
            "stocks": details.get(r["group_name"], []),
        })

    def attach(filtered: list[dict]) -> list[dict]:
        result = []
        for row in filtered:
            gname = row["group_name"]
            result.append({**row, "stocks": details.get(gname, [])})
        return result

    inflow  = attach(screen_inflow_low_gain(records))
    stealth = attach(screen_stealth(records))

    _jdump(out / "bubble_data.json",          bubble)
    _jdump(out / "group_stats.json",          records)
    _jdump(out / "inflow_low_gain.json",       inflow)
    _jdump(out / "stealth_accumulation.json",  stealth)

    summary = {
        "total":   len(records),
        "inflow":  len(inflow),
        "stealth": len(stealth),
    }
    logger.info(f"JSON exported: {summary}")
    return summary


def _jdump(path: Path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
