"""
測試 TWSE/TPEx 四個頁面背後的 API 是否支援全市場歷史日期查詢
請在 self-hosted runner 上執行
"""
import sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
import requests, json, urllib3
urllib3.disable_warnings()

TWSE_H = {"User-Agent":"Mozilla/5.0","Accept":"application/json","Referer":"https://www.twse.com.tw/"}
TPEX_H = {"User-Agent":"Mozilla/5.0","Accept":"application/json","Referer":"https://www.tpex.org.tw/"}

def test(label, url, params=None, headers=None, verify=True):
    print(f"\n=== {label} ===")
    print(f"URL: {url}")
    print(f"Params: {params}")
    try:
        r = requests.get(url, params=params, headers=headers or TWSE_H,
                        verify=verify, timeout=20)
        print(f"Status: {r.status_code}, Content-Type: {r.headers.get('Content-Type','')[:50]}")
        if r.status_code == 200:
            try:
                d = r.json()
                if isinstance(d, dict):
                    print(f"Keys: {list(d.keys())}")
                    print(f"stat: {d.get('stat')}, total: {d.get('total')}")
                    data = d.get('data') or d.get('aaData') or []
                    if data:
                        print(f"Rows: {len(data)}, first row: {data[0][:5] if isinstance(data[0],list) else list(data[0].items())[:3]}")
                elif isinstance(d, list):
                    print(f"List length: {len(d)}, first: {str(d[0])[:120] if d else 'empty'}")
            except Exception as e:
                print(f"JSON parse failed: {e}")
                print(f"Body[:200]: {r.text[:200]}")
        else:
            print(f"Body[:200]: {r.text[:200]}")
    except Exception as e:
        print(f"Request failed: {e}")

# 1. TWSE 個股日成交 - 全市場版本 (STOCK_DAY_ALL 帶日期參數)
test("TWSE STOCK_DAY_ALL with date",
     "https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL",
     params={"date": "20260612"})

# 2. TWSE 個股日成交 - 新版網頁可能用的端點
test("TWSE MI_INDEX (全市場行情)",
     "https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX",
     params={"response":"json","date":"20260612","type":"ALL"})

# 3. TWSE 個股日成交 - 舊版全市場
test("TWSE STOCK_DAY_ALL_96962 (all stocks by date)",
     "https://www.twse.com.tw/exchangeReport/STOCK_DAY_ALL",
     params={"response":"json","date":"20260612"})

# 4. TWSE T86 已知有效
test("TWSE T86 (三大法人, date=20260612)",
     "https://www.twse.com.tw/rwd/zh/fund/T86",
     params={"response":"json","date":"20260612","selectType":"ALL"})

# 5. TPEx 全市場行情 - 帶日期
test("TPEx tpex_mainboard_daily_close_quotes with d=",
     "https://www.tpex.org.tw/openapi/v1/tpex_mainboard_daily_close_quotes",
     params={"d":"115/06/12"}, headers=TPEX_H, verify=False)

# 6. TPEx 三大法人 - 帶日期
test("TPEx tpex_3insti with d=",
     "https://www.tpex.org.tw/openapi/v1/tpex_3insti_daily_trading",
     params={"d":"115/06/12"}, headers=TPEX_H, verify=False)

# 7. TPEx 新版三大法人頁面
test("TPEx 3insti detail day (new URL pattern)",
     "https://www.tpex.org.tw/openapi/v1/tpex_3insti_details_daily_trading",
     params={"d":"115/06/12"}, headers=TPEX_H, verify=False)

print("\n=== DONE ===")
