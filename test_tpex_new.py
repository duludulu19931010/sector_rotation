"""
測試 TPEx 新版 www API 是否支援歷史日期查詢
請在 self-hosted runner（台灣IP）執行
"""
import sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
import requests, json, urllib3
urllib3.disable_warnings()

H = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://www.tpex.org.tw/",
    "X-Requested-With": "XMLHttpRequest",
}

def test(label, url):
    print(f"\n{'='*60}")
    print(f"=== {label} ===")
    print(f"URL: {url}")
    try:
        r = requests.get(url, headers=H, verify=False, timeout=25)
        print(f"Status: {r.status_code}")
        print(f"Content-Type: {r.headers.get('Content-Type','')[:60]}")
        body = r.text
        print(f"Body length: {len(body)}")

        # 嘗試 JSON 解析
        try:
            d = r.json()
            print(f"--- JSON parsed ---")
            if isinstance(d, dict):
                print(f"Top keys: {list(d.keys())}")
                # 找日期欄位
                for k in ('date','reportDate','rptDate','data','tables','aaData','stat'):
                    if k in d:
                        v = d[k]
                        if isinstance(v, list):
                            print(f"  {k}: list[{len(v)}]")
                            if v:
                                print(f"    first: {str(v[0])[:150]}")
                        else:
                            print(f"  {k}: {str(v)[:100]}")
                # 探測 tables 結構
                if 'tables' in d and isinstance(d['tables'], list) and d['tables']:
                    t0 = d['tables'][0]
                    if isinstance(t0, dict):
                        print(f"  tables[0] keys: {list(t0.keys())}")
                        if 'data' in t0 and t0['data']:
                            print(f"  tables[0].data[{len(t0['data'])}], first row: {str(t0['data'][0])[:150]}")
                        if 'fields' in t0:
                            print(f"  tables[0].fields: {t0['fields']}")
                        if 'date' in t0:
                            print(f"  tables[0].date: {t0['date']}")
            elif isinstance(d, list):
                print(f"List[{len(d)}], first: {str(d[0])[:200]}")
        except Exception as je:
            print(f"--- Not JSON: {je} ---")
            print(f"Body head: {body[:300]}")
    except Exception as e:
        print(f"FAILED: {e}")

# 行情：response=json + 兩個不同日期，比對日期是否真的不同
test("行情 06/30 (json)",
     "https://www.tpex.org.tw/www/zh-tw/afterTrading/dailyQuotes?date=2026/06/30&id=&response=json")
test("行情 06/29 (json)",
     "https://www.tpex.org.tw/www/zh-tw/afterTrading/dailyQuotes?date=2026/06/29&id=&response=json")
test("行情 06/12 (json) 確認歷史",
     "https://www.tpex.org.tw/www/zh-tw/afterTrading/dailyQuotes?date=2026/06/12&id=&response=json")

# 三大法人：response=json + 不同日期
test("法人 06/30 (json)",
     "https://www.tpex.org.tw/www/zh-tw/insti/dailyTrade?type=Daily&sect=AL&date=2026/06/30&id=&response=json")
test("法人 06/29 (json)",
     "https://www.tpex.org.tw/www/zh-tw/insti/dailyTrade?type=Daily&sect=AL&date=2026/06/29&id=&response=json")
test("法人 06/12 (json) 確認歷史",
     "https://www.tpex.org.tw/www/zh-tw/insti/dailyTrade?type=Daily&sect=AL&date=2026/06/12&id=&response=json")

print(f"\n{'='*60}")
print("=== DONE ===")
print("重點確認：")
print("1. 06/30 vs 06/29 vs 06/12 回傳的 date 欄位是否不同（代表 date 參數有效）")
print("2. data/tables 裡的欄位順序（代號/收盤/成交股數/成交金額 的位置）")
print("3. 法人資料的三大法人買賣超合計欄位位置")
