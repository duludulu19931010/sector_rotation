"""
比對 tpex_mainboard_quotes 與 tpex_mainboard_daily_close_quotes 的欄位定義與數值
請在 self-hosted runner（台灣IP）上執行
"""
import sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

import requests
import urllib3
urllib3.disable_warnings()

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://www.tpex.org.tw/",
}

print("=== tpex_mainboard_quotes (目前使用的今日端點) ===")
r1 = requests.get(
    "https://www.tpex.org.tw/openapi/v1/tpex_mainboard_quotes",
    headers=headers, verify=False, timeout=20
)
data1 = r1.json()
print(f"Length: {len(data1)}")
if data1:
    print(f"keys: {list(data1[0].keys())}")
    # 找3105穩懋核對
    for item in data1:
        if item.get("SecuritiesCompanyCode") == "3105":
            print(f"3105 raw: {item}")
            break

print()
print("=== tpex_mainboard_daily_close_quotes (測試出能用的端點) ===")
r2 = requests.get(
    "https://www.tpex.org.tw/openapi/v1/tpex_mainboard_daily_close_quotes",
    headers=headers, verify=False, timeout=20
)
data2 = r2.json()
print(f"Length: {len(data2)}")
for item in data2:
    if item.get("SecuritiesCompanyCode") == "3105":
        print(f"3105 raw: {item}")
        break

print()
print("=== 比對欄位: tpex_mainboard_quotes 是否有 TradeVolume/TradeValue ===")
if data1:
    sample = data1[0]
    print(f"'TradeVolume' in keys: {'TradeVolume' in sample}")
    print(f"'TradeValue' in keys: {'TradeValue' in sample}")
    print(f"'TradingShares' in keys: {'TradingShares' in sample}")
    print(f"'TransactionAmount' in keys: {'TransactionAmount' in sample}")
