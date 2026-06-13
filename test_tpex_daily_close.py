"""
測試 tpex_mainboard_daily_close_quotes 是否支援歷史日期參數 d=YYY/MM/DD
請在 self-hosted runner（台灣IP）上執行此腳本
"""
import sys
import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

import requests
import urllib3
urllib3.disable_warnings()

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://www.tpex.org.tw/",
}

# 測試1: 不帶日期參數 (今日)
print("=== 測試1: 無參數 (應為今日) ===")
r1 = requests.get(
    "https://www.tpex.org.tw/openapi/v1/tpex_mainboard_daily_close_quotes",
    headers=headers, verify=False, timeout=20
)
print(f"Status: {r1.status_code}, Content-Type: {r1.headers.get('Content-Type')}")
try:
    data1 = r1.json()
    print(f"Type: {type(data1)}, Length: {len(data1) if isinstance(data1,(list,dict)) else 'N/A'}")
    if isinstance(data1, list) and data1:
        print(f"First item keys: {list(data1[0].keys())}")
        print(f"First item: {data1[0]}")
except Exception as e:
    print(f"JSON parse failed: {e}")
    print(f"Body[:300]: {r1.text[:300]}")

print()
print("=== 測試2: 帶 d=115/06/12 (今年6/12, 民國115年) ===")
r2 = requests.get(
    "https://www.tpex.org.tw/openapi/v1/tpex_mainboard_daily_close_quotes",
    params={"d": "115/06/12"},
    headers=headers, verify=False, timeout=20
)
print(f"Status: {r2.status_code}, Content-Type: {r2.headers.get('Content-Type')}")
try:
    data2 = r2.json()
    print(f"Type: {type(data2)}, Length: {len(data2) if isinstance(data2,(list,dict)) else 'N/A'}")
    if isinstance(data2, list) and data2:
        print(f"First item: {data2[0]}")
        # 找台積電對應的上櫃股票來核對 (上櫃沒有台積電, 找一個常見上櫃股, 例如3105上詮 or 6488環球晶)
        for item in data2:
            if item.get("SecuritiesCompanyCode") in ("3105","6488","5269","8164"):
                print(f"Sample stock: {item}")
                break
except Exception as e:
    print(f"JSON parse failed: {e}")
    print(f"Body[:300]: {r2.text[:300]}")

print()
print("=== 測試3: 帶 d=115/05/12 (上個月, 跨月測試) ===")
r3 = requests.get(
    "https://www.tpex.org.tw/openapi/v1/tpex_mainboard_daily_close_quotes",
    params={"d": "115/05/12"},
    headers=headers, verify=False, timeout=20
)
print(f"Status: {r3.status_code}, Content-Type: {r3.headers.get('Content-Type')}")
try:
    data3 = r3.json()
    print(f"Type: {type(data3)}, Length: {len(data3) if isinstance(data3,(list,dict)) else 'N/A'}")
    if isinstance(data3, list) and data3:
        print(f"First item: {data3[0]}")
except Exception as e:
    print(f"JSON parse failed: {e}")
    print(f"Body[:300]: {r3.text[:300]}")

print()
print("=== 比對: 測試1(今日) 與 測試2(d=今天日期) 是否相同 ===")
print("如果 data1 == data2，代表 d 參數被忽略，端點只回傳今日資料")
print("如果 data2 != data3，且 data3 的日期確實是 5/12 的資料，代表 d 參數有效")
