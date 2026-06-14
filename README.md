# TW$FLOW · 族群資金儀表板

台灣股市族群資金流向追蹤系統。每日自動從 TWSE/TPEx 官方 API 抓取資料，搭配 XQ 每日收盤 CSV，計算族群淨買賣超與漲跌幅，輸出互動式泡泡圖至 GitHub Pages。

Self-hosted Runner（台灣本地機器）負責資料抓取與計算，GitHub Pages 負責前端展示。

---

## 每日操作

**你每天只需要做一件事：**

1. 把當日 XQ 匯出的 `YYYYMMDD_Data.csv` 放入 `input/XQ/`
2. GitHub Actions → Run workflow（不勾任何參數）

整個流程 < 1 分鐘。

---

## 資料來源

### 每日自動抓取（官方 API）

| 市場 | 資料項目 | 端點 |
|------|---------|------|
| TWSE | 收盤價、成交總股數、成交總金額 | `openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL` |
| TWSE | 三大法人買賣超股數 | `www.twse.com.tw/rwd/zh/fund/T86?date=YYYYMMDD&selectType=ALL` |
| TPEx | 收盤價、成交總股數、成交總金額 | `tpex.org.tw/openapi/v1/tpex_mainboard_quotes` |
| TPEx | 三大法人買賣超股數 | `tpex.org.tw/openapi/v1/tpex_3insti_daily_trading` |

日期從 API 的 `Date` 欄位解析（民國年7位數），非系統日期，週末/假日執行不會建立假日資料。

T86：欄位數因日期而異（16或19欄），三大法人合計固定為**最後一欄 `row[-1]`**，單位為股。

### 每日手動提供

| 資料 | 路徑 | 說明 |
|------|------|------|
| 收盤價序列 | `input/XQ/YYYYMMDD_Data.csv` | XQ 匯出，需有 `代碼`、`成交` 欄位，支援 utf-8-sig / cp950 |

收盤價序列用於計算漲跌幅（今日/五日/二十日）。成交量、成交金額、三大法人資料均來自官方 API。

### 歷史資料（一次性補充，之後不再需要）

| 資料 | 路徑 | 格式 |
|------|------|------|
| TPEx 過去行情 | `input/TPEx/TPEx_YYYYMMDD.csv` | big5，前2行標題跳過，欄位含`代號`、`收盤`、`成交股數`、`成交金額(元)` |
| TPEx 過去三大法人 | `input/TPExDealer/TPExDealer_YYYYMMDD.csv` | cp950，第1行說明跳過，最後欄為`三大法人買賣超股數合計` |

TPEx 個股歷史 API（`st43_result.php`）已於 2024/10 改版後失效，過去資料請手動放入對應資料夾。今後每日資料由 API 自動累積，不再需要手動提供。

### TWSE 歷史自動補抓（DB 不足 21 個交易日時自動執行）

- 端點：`exchangeReport/STOCK_DAY?date=YYYYMM01&stockNo=代號`（逐股逐月）
- 範圍：當日資料中全部 TWSE 代號（約 1366 支）× 當月 + 上月
- 並行：8 個 parallel requests
- 三大法人：T86 歷史逐日抓取（0.3秒間隔）
- 預估時間：首次執行約 **15~20 分鐘**，之後每日不觸發

---

## 指標定義

### 儲存於 DB 的欄位

| 欄位 | 說明 | 來源 |
|------|------|------|
| `close_price` | 收盤價 | API |
| `trade_volume` | 成交總股數 | API |
| `trade_value` | 成交總金額（元） | API |
| `avg_price` | 成交均價 = `trade_value ÷ trade_volume` | 計算 |
| `inst_net` | 三大法人買賣超總股數 | API |
| `inst_value` | 三大法人買賣超總金額（億）= `inst_net × avg_price ÷ 1e8` | 計算 |

### 計算指標（基於統一交易日序列）

所有指標均以 **XQ CSV 與 DB 的交集日期**作為統一的交易日序列，確保漲跌幅與買賣超使用相同的日期基準。

| 指標 | 定義 |
|------|------|
| 今日漲跌幅（%） | 最後交易日收盤 vs 前一個交易日收盤 |
| 五日漲跌幅（%） | 最後交易日收盤 vs 5個交易日前收盤 |
| 二十日漲跌幅（%） | 最後交易日收盤 vs 20個交易日前收盤 |
| 今日淨買賣超（億） | 最後交易日的 `inst_value` |
| 五日淨買賣超（億） | 最後5個交易日的 `inst_value` 加總 |
| 二十日淨買賣超（億） | 最後20個交易日的 `inst_value` 加總 |

### 族群標籤

| 標籤 | 條件（族群層級） |
|------|----------------|
| **主力** | 今日淨買超 > 前一日淨買超，且兩者皆 > 0，且兩者皆 > 五日平均 |
| **輪動** | 五日淨買超 > 0，但不符合主力條件 |
| **退潮** | 五日淨買超 < -2 億 |
| **觀望** | 其餘 |

泡泡圖：**X 軸 = 五日淨買超（資金出入量）**，**Y 軸 = 今日淨買超（資金加速度）**

---

## 檔案結構

```
├── pipeline.py                        主程式
├── requirements.txt
├── .gitignore
├── README.md
├── input/
│   ├── group.csv                      族群清單（cp950）
│   ├── XQ/
│   │   └── YYYYMMDD_Data.csv          每日 XQ 收盤（utf-8-sig 或 cp950）
│   ├── TPEx/
│   │   └── TPEx_YYYYMMDD.csv          TPEx 歷史行情（big5，一次性補充）
│   └── TPExDealer/
│       └── TPExDealer_YYYYMMDD.csv    TPEx 歷史三大法人（cp950，一次性補充）
├── db/
│   └── market.db                      SQLite（隨每次更新推送至 GitHub）
├── docs/
│   ├── index.html
│   ├── sector.html
│   └── assets/data/
│       ├── bubble_data.json
│       ├── inflow_low_gain.json
│       ├── stealth_accumulation.json
│       ├── group_stats.json
│       ├── metadata.json
│       └── market_data.csv            完整資料匯出（可直接下載）
└── .github/workflows/daily.yml
```

---

## group.csv 格式（cp950）

```
3C通路商,,3D印表機,,IC製造,...      ← row 0：族群名稱（偶數欄）
代號,名稱,代號,名稱,代號,名稱,...   ← row 1：標題（跳過）
2392,正崴,3504,揚明光,2330,台積電,... ← row 2+：個股代號與名稱
```

每兩欄一族群（代號 + 名稱），目前 499 族群、3881 支股票。

---

## market_data.csv

每次執行後自動匯出至 `docs/assets/data/market_data.csv`，包含 DB 全部資料，欄位如下：

| 欄位 | 說明 |
|------|------|
| 日期 | YYYY-MM-DD |
| 代號 | 股票代號（4位） |
| 市場 | TWSE / TPEx |
| 名稱 | 股票名稱 |
| 收盤價 | 元 |
| 成交總股數 | 股 |
| 成交總金額 | 元 |
| 成交均價 | 元 |
| 三大法人買賣超股數 | 股（正=買超，負=賣超） |
| 三大法人買賣超金額億 | 億元 |

下載網址：
```
https://raw.githubusercontent.com/<你的帳號>/sector_rotation/main/docs/assets/data/market_data.csv
```

---

## Workflow 選項

| 參數 | 說明 | 使用時機 |
|------|------|---------|
| `force` | 強制重抓今日（即使 DB 已有） | API 資料異常需要重抓 |
| `reset_history` | 重新補抓 TWSE 個股歷史 | 歷史資料有誤需要重建 |
| `dry_run` | 不打 API，只用現有 DB + XQ 重算 | 測試計算邏輯 |
| `purge_bad_data` | 清除 `trade_value=0` 的污染資料並重抓 | 修復舊版本遺留的壞資料 |

---

## 首次執行 / 重建資料庫

```
1. 在 GitHub 上刪除 db/market.db（點檔案 → 垃圾桶）
2. 把過去的 TPEx CSV 放入 input/TPEx/ 和 input/TPExDealer/
3. Actions → Run workflow（不勾任何參數）
```

首次執行會自動補抓 TWSE 歷史（約 15~20 分鐘），之後每日 < 1 分鐘。

---

## 障礙排除

| 現象 | 可能原因 | 解法 |
|------|---------|------|
| 漲跌幅全部 0 | `input/XQ/` 沒有 CSV 或欄位名稱不符 | 確認 CSV 有 `代碼`、`成交` 欄位 |
| TPEx inst_value 都是 0 | 沒有 TPExDealer CSV | 放入對應日期的 TPExDealer CSV 後重跑 `dry_run` |
| TWSE 補抓很慢（15~20分鐘） | 1366 支股票逐股請求，正常現象 | 僅首次執行，之後不觸發 |
| 頁面顯示「資料載入失敗」 | bubble_data.json 未推送或格式異常 | 看瀏覽器 Console 確認具體錯誤 |
| `database is locked` | 上次執行中斷 | 刪除 `db/market.db-wal`、`db/market.db-shm` |
| API 抓取失敗（RuntimeError） | TWSE/TPEx API 暫時無回應 | 等待後重跑，通常收盤後幾分鐘可用 |

---

## 免責聲明

資料來自 TWSE/TPEx 公開 API 及使用者提供之 CSV，非即時，存在揭露延遲。本專案為個人研究用途，不構成投資建議。
