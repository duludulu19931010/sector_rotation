"""
探測 TPEx 新版「個股日成交資訊」頁面 (stock-pricing.html) 背後的 API 端點
頁面: https://www.tpex.org.tw/zh-tw/mainboard/trading/info/stock-pricing.html
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
    "Referer": "https://www.tpex.org.tw/zh-tw/mainboard/trading/info/stock-pricing.html",
}

# 新版TPEx常見的後端API命名模式候選
candidates = [
    ("https://www.tpex.org.tw/www/zh-tw/afterTrading/tradingStock", {"code": "3105", "date": "115/06", "id": "", "response": "json"}),
    ("https://www.tpex.org.tw/www/zh-tw/afterTrading/dailyQuotes", {"code": "3105", "date": "2026-06", "response": "json"}),
    ("https://www.tpex.org.tw/web/stock/aftertrading/daily_trading_info/st43_result.php", {"d": "115/06", "stkno": "3105"}),
    ("https://www.tpex.org.tw/www/api/stock/info/dailyClose", {"stkno": "3105", "date": "2026-06"}),
    ("https://wwwapi.tpex.org.tw/web/stock/aftertrading/daily_trading_info/st43_result.php", {"d": "115/06", "stkno": "3105", "l": "zh-tw"}),
    ("https://wwwapi.tpex.org.tw/www/zh-tw/afterTrading/tradingStock", {"code": "3105", "date": "115/06", "response": "json"}),
]

for url, params in candidates:
    print(f"=== {url} ===")
    print(f"params: {params}")
    try:
        r = requests.get(url, params=params, headers=headers, verify=False, timeout=15)
        print(f"Status: {r.status_code}, Content-Type: {r.headers.get('Content-Type')}, Length: {len(r.text)}")
        if r.status_code == 200:
            try:
                data = r.json()
                if isinstance(data, dict):
                    print(f"keys: {list(data.keys())}")
                elif isinstance(data, list):
                    print(f"list length: {len(data)}")
                    if data:
                        print(f"first item: {data[0]}")
            except Exception as e:
                print(f"Not JSON: {r.text[:200]}")
    except Exception as e:
        print(f"Request failed: {e}")
    print()
