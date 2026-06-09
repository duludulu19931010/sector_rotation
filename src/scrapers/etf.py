"""
src/scrapers/etf.py
主動式 ETF 爬蟲

資料來源（依序 fallback）：
  1. TWSE OpenAPI ETF_BASIC_INFO  → ETF 清單
  2. TWSE ETF_FUND API            → 持股明細
  3. MOPS ajax_t203sb04           → 持股明細（HTML parse）
  4. etfinfo.tw                   → 持股明細（HTML parse）
"""

import logging
import re
import time

import requests
import urllib3
from bs4 import BeautifulSoup

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logger = logging.getLogger(__name__)

_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0",
    "Accept":     "application/json, text/html, */*",
    "Accept-Language": "zh-TW,zh;q=0.9",
}


# ── HTTP ─────────────────────────────────────────────

def _get(url, params=None, verify=True, retries=3) -> dict | list | None:
    for i in range(retries):
        try:
            r = requests.get(url, params=params, headers=_HEADERS, timeout=25, verify=verify)
            r.raise_for_status()
            data = r.json()
            return data if isinstance(data, (dict, list)) else None
        except Exception as e:
            logger.warning(f"GET {i+1}/{retries}: {url} — {e}")
            if i < retries - 1:
                time.sleep(1.5 * (i + 1))
    return None


def _post_html(url, form_data, referer="", timeout=30) -> BeautifulSoup | None:
    try:
        r = requests.post(url, data=form_data, headers={
            **_HEADERS,
            "Content-Type": "application/x-www-form-urlencoded",
            "Referer": referer,
        }, timeout=timeout)
        r.raise_for_status()
        return BeautifulSoup(r.text, "lxml")
    except Exception as e:
        logger.warning(f"POST {url} — {e}")
        return None


# ── ETF 清單 ──────────────────────────────────────────

# 台灣主動式 ETF 的代號特徵：末尾為英文字母 A 或 D
_FALLBACK_ETFS = [
    ("00403A","主動統一升級50",  "統一"),
    ("00992A","主動群益科技創新","群益"),
    ("00985A","主動野村台灣50",  "野村"),
    ("00991A","主動復華未來50",  "復華"),
    ("00981A","主動統一台股增長","統一"),
    ("00995A","主動中信台灣卓越","中國信託"),
    ("00994A","主動第一金台股優","第一金"),
    ("00984A","主動安聯台灣高息","安聯"),
    ("00982A","主動群益台灣強棒","群益"),
    ("00987A","主動台新優勢成長","台新"),
    ("00983A","主動中信ARK創新", "中國信託"),
    ("00983D","主動富邦複合收益","富邦"),
    ("00982D","主動富邦動態入息","富邦"),
    ("00984D","主動聯博全球非投","聯博"),
    ("00989A","主動摩根美國科技","摩根"),
    ("00986A","主動台新龍頭成長","台新"),
    ("00401A","主動摩根台灣鑫收","摩根"),
    ("00999A","主動野村臺灣高息","野村"),
    ("00993A","主動安聯台灣",    "安聯"),
    ("00996A","主動兆豐台灣豐收","兆豐"),
    ("00400A","主動國泰動能高息","國泰"),
    ("00990A","主動元大AI新經濟","元大"),
    ("00980A","主動野村臺灣優選","野村"),
    ("00997A","主動群益美國增長","群益"),
    ("00988A","主動統一全球創新","統一"),
    ("00404A","主動聯博動能50",  "聯博"),
]


def fetch_active_etf_list() -> list[dict]:
    """
    回傳主動式 ETF 清單
    [{"code", "name", "manager"}]
    """
    data = _get("https://openapi.twse.com.tw/v1/ETF/ETF_BASIC_INFO")
    if data and isinstance(data, list):
        active = []
        for item in data:
            code = str(item.get("ETFid", item.get("Code", ""))).strip()
            etype = str(item.get("ETFType", ""))
            if code.endswith(("A", "D")) or "主動" in etype:
                active.append({
                    "code":    code,
                    "name":    item.get("ETFName", item.get("Name", "")),
                    "manager": item.get("ManagementCompany", ""),
                })
        if active:
            logger.info(f"TWSE ETF list: {len(active)} active ETFs")
            return active

    # fallback
    logger.warning("Using hardcoded active ETF list")
    return [{"code": c, "name": n, "manager": m} for c, n, m in _FALLBACK_ETFS]


def get_top10_by_scale(etf_list: list[dict]) -> list[dict]:
    """
    依規模排序取前 10（API 有就用，否則取前 10 fallback）
    """
    data = _get("https://openapi.twse.com.tw/v1/ETF/NAV_FUND")
    if data and isinstance(data, list):
        scale_map = {}
        for item in data:
            code = str(item.get("ETFid", item.get("Code", ""))).strip()
            scale = _to_flt(item.get("FundSize", item.get("TotalNetAssets", 0)))
            scale_map[code] = scale
        for e in etf_list:
            e["scale"] = scale_map.get(e["code"], 0)
        return sorted(etf_list, key=lambda x: x.get("scale", 0), reverse=True)[:10]
    return etf_list[:10]


def get_top10_by_popularity(etf_list: list[dict]) -> list[dict]:
    """依受益人數排序取前 10"""
    data = _get("https://openapi.twse.com.tw/v1/ETF/ETF_BENFICIARY")
    if data and isinstance(data, list):
        holders_map = {}
        for item in data:
            code = str(item.get("ETFid", item.get("Code", ""))).strip()
            holders_map[code] = _to_int(item.get("BeneficiaryCount", item.get("Holders", 0)))
        for e in etf_list:
            e["holders"] = holders_map.get(e["code"], 0)
        return sorted(etf_list, key=lambda x: x.get("holders", 0), reverse=True)[:10]
    return etf_list[:10]


# ── ETF 持股 ─────────────────────────────────────────

def fetch_holdings(etf_code: str, date_str: str = None) -> list[dict]:
    """
    依序嘗試三個來源抓持股
    回傳: [{"stock_code","stock_name","shares","market_value","weight_pct"}]
    """
    from datetime import date as _date
    if not date_str:
        date_str = _date.today().strftime("%Y%m%d")

    # 1. TWSE ETF_FUND
    h = _from_twse_etf_fund(etf_code, date_str)
    if h:
        logger.info(f"[ETF-TWSE] {etf_code}: {len(h)} holdings")
        return h

    # 2. MOPS
    h = _from_mops(etf_code)
    if h:
        logger.info(f"[ETF-MOPS] {etf_code}: {len(h)} holdings")
        return h

    # 3. etfinfo.tw
    h = _from_etfinfo(etf_code)
    if h:
        logger.info(f"[ETF-etfinfo] {etf_code}: {len(h)} holdings")
        return h

    logger.warning(f"All sources failed for {etf_code}")
    return []


def _from_twse_etf_fund(etf_code: str, date_str: str) -> list[dict]:
    data = _get(
        "https://www.twse.com.tw/rwd/zh/fund/ETF_FUND",
        params={"response": "json", "fundCo": etf_code, "date": date_str}
    )
    if not isinstance(data, dict) or data.get("stat") != "OK":
        return []
    holdings = []
    for row in data.get("data", []):
        if isinstance(row, list) and len(row) >= 3:
            holdings.append({
                "stock_code":   str(row[0]).strip().zfill(4),
                "stock_name":   str(row[1]).strip(),
                "shares":       _to_int(row[2]),
                "market_value": _to_flt(row[3]) if len(row) > 3 else 0.0,
                "weight_pct":   _to_flt(row[4]) if len(row) > 4 else 0.0,
            })
        elif isinstance(row, dict):
            holdings.append({
                "stock_code":   str(row.get("stockCode", row.get("Code", ""))).strip().zfill(4),
                "stock_name":   str(row.get("stockName", row.get("Name", ""))).strip(),
                "shares":       _to_int(row.get("shares", row.get("SharesThousand", 0))),
                "market_value": _to_flt(row.get("marketValue", 0)),
                "weight_pct":   _to_flt(row.get("ratio", row.get("Ratio", 0))),
            })
    return holdings


def _from_mops(etf_code: str) -> list[dict]:
    soup = _post_html(
        "https://mops.twse.com.tw/mops/web/ajax_t203sb04",
        form_data={
            "encodeURIComponent": "1", "step": "1",
            "firstin": "1", "off": "1",
            "co_id": etf_code, "TYPEK": "sii",
        },
        referer="https://mops.twse.com.tw/mops/web/t203sb04"
    )
    if not soup:
        return []
    return _parse_holdings_table(soup)


def _from_etfinfo(etf_code: str) -> list[dict]:
    try:
        r = requests.get(
            f"https://www.etfinfo.tw/etf/{etf_code}/active",
            headers=_HEADERS, timeout=25
        )
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "lxml")
        return _parse_holdings_table(soup)
    except Exception as e:
        logger.warning(f"etfinfo.tw {etf_code}: {e}")
        return []


def _parse_holdings_table(soup: BeautifulSoup) -> list[dict]:
    """通用 table 解析：找第一個含有效股票代號的表格"""
    for table in soup.find_all("table"):
        holdings = []
        for row in table.find_all("tr")[1:]:
            cells = row.find_all("td")
            if len(cells) < 3:
                continue
            code = cells[0].get_text(strip=True)
            if not re.match(r"^\d{4,6}$", code):
                continue
            holdings.append({
                "stock_code":   code.zfill(4),
                "stock_name":   cells[1].get_text(strip=True) if len(cells) > 1 else "",
                "shares":       _to_int(cells[2].get_text(strip=True)),
                "market_value": _to_flt(cells[3].get_text(strip=True)) if len(cells) > 3 else 0.0,
                "weight_pct":   _to_flt(cells[4].get_text(strip=True)) if len(cells) > 4 else 0.0,
            })
        if holdings:
            return holdings
    return []


def compute_holdings_change(
    current: list[dict], previous: list[dict]
) -> list[dict]:
    """計算持股今日增減"""
    prev = {h["stock_code"]: h["shares"] for h in previous}
    result = []
    for h in current:
        code = h["stock_code"]
        prev_shares = prev.get(code, 0)
        delta = h["shares"] - prev_shares
        pct   = (delta / prev_shares * 100) if prev_shares else (100.0 if delta > 0 else 0.0)
        result.append({**h, "chg_shares": delta, "chg_pct": round(pct, 2)})
    return result


# ── 型別轉換 ──────────────────────────────────────────

def _to_int(v) -> int:
    try:    return int(str(v).replace(",", "").strip())
    except: return 0

def _to_flt(v) -> float:
    try:    return float(str(v).replace(",", "").strip())
    except: return 0.0
