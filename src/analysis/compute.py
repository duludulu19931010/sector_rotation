"""
src/analysis/compute.py
族群資金流向計算

指標定義：
  net_5d   族群五日淨買超（億）→ 泡泡圖 X 軸：資金出入量
  net_1d   族群今日淨買超（億）→ 泡泡圖 Y 軸：資金加速度
  net_20d  族群近20日累計淨買超（億）

股票名稱優先順序：
  1. input/stock_list.csv（CP950 編碼，代號,名稱）
  2. T86 API 欄位 1
  3. TWSE/TPEx 收盤價 API Name 欄位
"""

import json
import logging
from pathlib import Path

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# ── 股票名稱對照表 ────────────────────────────────────

_STOCK_NAME_MAP: dict[str, str] = {}


def load_stock_names(csv_path: str | Path) -> dict[str, str]:
    """
    讀取 input/stock_list.csv（CP950 編碼）
    格式：代號,名稱
    回傳：{"0000": "名稱", ...}
    """
    p = Path(csv_path)
    if not p.exists():
        logger.warning(f"stock_list.csv not found: {p}")
        return {}
    try:
        raw = p.read_bytes()
        try:
            text = raw.decode("cp950")
        except UnicodeDecodeError:
            text = raw.decode("big5", errors="replace")
        result = {}
        for line in text.strip().replace("\r\n", "\n").split("\n")[1:]:
            parts = line.strip().split(",")
            if len(parts) >= 2:
                code = parts[0].strip().zfill(4)
                name = parts[1].strip()
                if code and name:
                    result[code] = name
        logger.info(f"stock_list loaded: {len(result)} codes")
        return result
    except Exception as e:
        logger.error(f"Failed to load stock_list.csv: {e}")
        return {}


def init_stock_names(csv_path: str | Path) -> None:
    """在 pipeline 啟動時呼叫一次，載入名稱對照表"""
    global _STOCK_NAME_MAP
    _STOCK_NAME_MAP = load_stock_names(csv_path)


def get_name(code: str, api_name: str = "") -> str:
    """
    取得股票名稱，優先 stock_list.csv，再用 API 回傳的名稱
    """
    code4 = str(code).zfill(4)
    return _STOCK_NAME_MAP.get(code4, "") or api_name or code4


# ── 族群清單 ─────────────────────────────────────────

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


# ── 個股統計計算 ──────────────────────────────────────

def compute_stock_stats(
    inst_df: pd.DataFrame,   # institutional_flow（多日，含 trade_date 欄位）
    price_df: pd.DataFrame,  # stock_prices（含 trade_date 欄位）
) -> pd.DataFrame:
    """
    計算每檔個股的統計數據

    回傳 DataFrame 欄位：
      code, name
      net_5d    五日淨買超（億）
      net_1d    今日淨買超（億）
      net_20d   近20日淨買超（億）
      close_price 最新收盤價
      change_5d_pct  五日漲幅（%）
      change_1d_pct  今日漲幅（%）
    """
    if inst_df.empty or price_df.empty:
        return pd.DataFrame()

    # 確保 trade_date 欄位存在
    if "trade_date" not in inst_df.columns:
        logger.error("inst_df missing 'trade_date' column")
        return pd.DataFrame()
    if "trade_date" not in price_df.columns:
        logger.error("price_df missing 'trade_date' column")
        return pd.DataFrame()

    inst  = inst_df.copy()
    price = price_df.copy()
    inst["code"]  = inst["code"].astype(str).str.zfill(4)
    price["code"] = price["code"].astype(str).str.zfill(4)

    inst_dates  = sorted(inst["trade_date"].unique())
    price_dates = sorted(price["trade_date"].unique())

    last5_dates = inst_dates[-5:]   if len(inst_dates)  >= 5 else inst_dates
    latest_inst = inst_dates[-1]    if inst_dates else None

    # ── 近20日合計（全部 inst 資料）────────────────────
    total_20d = (
        inst.groupby("code")["total_net"]
        .sum().reset_index()
        .rename(columns={"total_net": "total_20d_shares"})
    )

    # ── 近5日合計 ──────────────────────────────────────
    total_5d = (
        inst[inst["trade_date"].isin(last5_dates)]
        .groupby("code")["total_net"]
        .sum().reset_index()
        .rename(columns={"total_net": "total_5d_shares"})
    )

    # ── 今日（最新日）合計 ────────────────────────────
    total_1d = (
        inst[inst["trade_date"] == latest_inst]
        .groupby("code")["total_net"]
        .sum().reset_index()
        .rename(columns={"total_net": "total_1d_shares"})
    ) if latest_inst else pd.DataFrame(columns=["code","total_1d_shares"])

    # ── 最新收盤價（price 最新日）────────────────────
    latest_price_date = price_dates[-1] if price_dates else None
    price_latest = (
        price[price["trade_date"] == latest_price_date]
        [["code", "name", "close_price"]]
    ) if latest_price_date else pd.DataFrame(columns=["code","name","close_price"])

    # ── 五日前收盤價 ──────────────────────────────────
    d5 = price_dates[-5] if len(price_dates) >= 5 else price_dates[0]
    price_5d = (
        price[price["trade_date"] == d5][["code","close_price"]]
        .rename(columns={"close_price": "close_5d_ago"})
    )

    # ── 昨日收盤價 ────────────────────────────────────
    d1 = price_dates[-2] if len(price_dates) >= 2 else price_dates[0]
    price_1d = (
        price[price["trade_date"] == d1][["code","close_price"]]
        .rename(columns={"close_price": "close_1d_ago"})
    )

    # ── 合併 ─────────────────────────────────────────
    df = total_5d.merge(total_20d, on="code", how="outer")
    df = df.merge(total_1d,    on="code", how="left")
    df = df.merge(price_latest, on="code", how="left")
    df = df.merge(price_5d,     on="code", how="left")
    df = df.merge(price_1d,     on="code", how="left")

    df["total_5d_shares"]  = df["total_5d_shares"].fillna(0)
    df["total_20d_shares"] = df["total_20d_shares"].fillna(0)
    df["total_1d_shares"]  = df["total_1d_shares"].fillna(0)
    df["close_price"]  = pd.to_numeric(df["close_price"],  errors="coerce").fillna(0)
    df["close_5d_ago"] = pd.to_numeric(df["close_5d_ago"], errors="coerce")
    df["close_1d_ago"] = pd.to_numeric(df["close_1d_ago"], errors="coerce")

    # ── 名稱：優先 stock_list.csv ────────────────────
    df["api_name"] = df["name"].fillna("")
    df["name"] = df["code"].apply(lambda c: get_name(c, ""))
    # fallback 到 API 名稱
    mask = df["name"] == ""
    df.loc[mask, "name"] = df.loc[mask, "api_name"]
    df.drop(columns=["api_name"], inplace=True)

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


# ── 族群彙總 ─────────────────────────────────────────

def compute_group_stats(
    groups: dict[str, list[str]],
    stock_df: pd.DataFrame,
) -> tuple[list[dict], dict[str, list[dict]]]:
    """彙總個股到族群層級"""
    if stock_df.empty:
        return [], {}

    ss = stock_df.copy()
    ss["code"] = ss["code"].astype(str).str.zfill(4)

    records: list[dict] = []
    details: dict[str, list[dict]] = {}

    for gname, codes in groups.items():
        padded = [str(c).zfill(4) for c in codes]
        subset = ss[ss["code"].isin(padded)]

        # 個股明細
        stock_list = []
        for _, s in subset.iterrows():
            stock_list.append({
                "code":          s["code"],
                "name":          s.get("name", ""),
                "close_price":   round(float(s.get("close_price", 0)), 2),
                "net_5d":        round(float(s.get("net_5d",  0)), 4),
                "net_1d":        round(float(s.get("net_1d",  0)), 4),
                "net_20d":       round(float(s.get("net_20d", 0)), 4),
                "change_5d_pct": round(float(s.get("change_5d_pct", 0)), 2),
                "change_1d_pct": round(float(s.get("change_1d_pct", 0)), 2),
            })
        stock_list.sort(key=lambda x: x["net_1d"], reverse=True)
        details[gname] = stock_list

        if subset.empty:
            records.append(_empty_group(gname, len(codes)))
            continue

        net_5d    = float(subset["net_5d"].sum())
        net_1d    = float(subset["net_1d"].sum())
        net_20d   = float(subset["net_20d"].sum())
        change_5d = float(subset["change_5d_pct"].mean())
        change_1d = float(subset["change_1d_pct"].mean())

        if   net_5d > 2  and change_5d > 1:  label = "主力"
        elif net_5d > 0  and change_5d <= 1: label = "輪動"
        elif net_5d < -2:                    label = "退潮"
        else:                                label = "觀望"

        records.append({
            "group_name":    gname,
            "stock_count":   len(codes),
            "matched":       int(len(subset)),
            "net_5d":        round(net_5d,    3),
            "net_1d":        round(net_1d,    3),
            "net_20d":       round(net_20d,   3),
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
    """net_5d > 0 AND change_5d_pct < 10，依 net_5d 排序"""
    return sorted(
        [r for r in records if r["net_5d"] > 0 and r["change_5d_pct"] < 10],
        key=lambda x: x["net_5d"], reverse=True
    )


def screen_stealth(records: list[dict]) -> list[dict]:
    """net_1d > 0 AND net_5d > 0 AND change_5d_pct < 0，依 net_5d 排序"""
    return sorted(
        [r for r in records if r["net_1d"] > 0 and r["net_5d"] > 0 and r["change_5d_pct"] < 0],
        key=lambda x: x["net_5d"], reverse=True
    )


# ── 輸出 JSON ─────────────────────────────────────────

def export_json(
    records: list[dict],
    details: dict[str, list[dict]],
    output_dir: str | Path,
) -> dict:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    bubble = []
    for r in records:
        bubble.append({
            **r,
            "x":      r["net_5d"],
            "y":      r["net_1d"],
            "size":   max(10, min(72, abs(r["net_5d"]) * 2.8 + 12)),
            "stocks": details.get(r["group_name"], []),
        })

    def attach(filtered: list[dict]) -> list[dict]:
        return [{**row, "stocks": details.get(row["group_name"], [])} for row in filtered]

    inflow  = attach(screen_inflow_low_gain(records))
    stealth = attach(screen_stealth(records))

    _jdump(out / "bubble_data.json",          bubble)
    _jdump(out / "group_stats.json",          records)
    _jdump(out / "inflow_low_gain.json",       inflow)
    _jdump(out / "stealth_accumulation.json",  stealth)

    summary = {"total": len(records), "inflow": len(inflow), "stealth": len(stealth)}
    logger.info(f"JSON exported: {summary}")
    return summary


def _jdump(path: Path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
