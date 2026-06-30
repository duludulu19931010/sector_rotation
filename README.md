# TW$FLOW · 族群資金儀表板

台灣股市族群資金流向追蹤系統。每日自動從 TWSE/TPEx 官方 API 抓取成交與三大法人資料，用 yfinance 取得收盤價序列，計算族群淨買賣超與漲跌幅，輸出互動式泡泡圖至 GitHub Pages。

Self-hosted Runner（台灣本地機器）負責資料抓取與計算，GitHub Pages 負責前端展示。

---

## 每日操作

**GitHub Actions → Run workflow（不勾任何參數）**

全部自動完成。不需要手動提供任何檔案。首次建立資料庫請勾選 `reset_history`（見下方）。

---

## 資料來源

| 資料 | 來源 | 日期支援 |
|------|------|---------|
| 收盤價序列（35天） | yfinance (Yahoo Finance) | 完整歷史，盤後約 20 分鐘 |
| TWSE 今日成交股數/金額 | `openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL` | 僅最新交易日 |
| TWSE 歷史成交股數/金額 | `www.twse.com.tw/exchangeReport/STOCK_DAY`（個股逐月） | 支援歷史 |
| TWSE 三大法人買賣超 | `www.twse.com.tw/rwd/zh/fund/T86` | 支援歷史，最後欄 row[-1] |
| TPEx 行情（收盤/成交） | `www.tpex.org.tw/www/zh-tw/afterTrading/dailyQuotes` | 支援歷史 date 參數 |
| TPEx 三大法人買賣超 | `www.tpex.org.tw/www/zh-tw/insti/dailyTrade` | 支援歷史，最後欄 row[-1] |

所有資料皆由 API 自動抓取，**不再需要任何手動 CSV**。

### 端點說明

TWSE 的 `openapi STOCK_DAY_ALL` 只回傳最新交易日（date 參數無效），歷史成交量/金額改用個股逐月端點 `STOCK_DAY?stockNo=&date=YYYYMM01`，回傳每日成交股數、成交金額、收盤價。TPEx 新版 www API 的 date 參數有效，可直接抓任意歷史日期的全市場資料。

---

## 指標定義

### 儲存於 DB 的欄位

| 欄位 | 說明 | 來源 |
|------|------|------|
| `close_price` | 收盤價 | API |
| `trade_volume` | 成交總股數 | API |
| `trade_value` | 成交總金額（元） | API |
| `avg_price` | 成交均價 = trade_value ÷ trade_volume | 計算 |
| `inst_net` | 三大法人買賣超總股數 | API |
| `inst_value` | 三大法人買賣超總金額（億）= inst_net × avg_price ÷ 1e8 | 計算 |

### 漲跌幅（來自 yfinance 收盤價序列）

漲跌幅使用 yfinance 的日期序列，與 DB 無關，確保最新收盤日永遠正確。

| 指標 | 定義 |
|------|------|
| 今日漲跌幅（%） | 最新交易日收盤 vs 前一個交易日收盤 |
| 五日漲跌幅（%） | 最新交易日收盤 vs 最新第6個交易日收盤（涵蓋最新5個交易日） |
| 二十日漲跌幅（%） | 最新交易日收盤 vs 最新第21個交易日收盤（涵蓋最新20個交易日） |

### 淨買賣超（來自 DB）

| 指標 | 定義 |
|------|------|
| 最新交易日淨買賣超（億） | 最新交易日的 inst_value |
| 五日淨買賣超（億） | 最新5個交易日的 inst_value 加總 |
| 二十日淨買賣超（億） | 最新20個交易日的 inst_value 加總 |

---

## 族群標籤定義

泡泡圖 **X 軸 = 五日淨買超（資金出入量）**，**Y 軸 = 今日淨買超（資金加速度）**。

「五日平均」= 五日淨買超 ÷ 5。

### 主力（四個條件全部成立）

1. 最新交易日淨買超 > 0，且前一個交易日淨買超 > 0
2. 最新交易日淨買超 > 五日平均，且前一個交易日淨買超 > 五日平均
3. 最新三個交易日單日漲跌幅**全部 > 0**
4. 最近五個交易日總漲跌幅 > 0

### 輪動（排除主力後，資金正向但漲幅不足）

1. 最新交易日淨買超 > 0，且前一個交易日淨買超 > 0
2. 最新交易日淨買超 > 五日平均，且前一個交易日淨買超 > 五日平均
3. 最新三個交易日單日漲跌幅**任一天 ≤ 0**，**或**五日總漲跌幅 ≤ 0

### 退潮

1. 最新交易日淨買超 < 0，且前一個交易日淨買超 < 0
2. 最新交易日淨買超 < 五日平均，且前一個交易日淨買超 < 五日平均

### 觀望

其餘不符合上述任一定義的族群。

---

## 篩選清單定義

### 流入低漲幅

資金持續流入但股價尚未明顯反映。排序：五日淨買超由大到小。

1. 最新交易日淨買超 > 0，且前一個交易日淨買超 > 0，且皆 > 五日平均
2. 最新三個交易日單日漲跌幅任一天 ≤ 5%
3. 最近五個交易日總漲跌幅 ≤ 10%

### 偷偷佈局

資金悄悄進場，整體仍在流出期，股價尚未反映。排序：五日淨買超由小到大（流出最多在前）。

1. 最新交易日淨買超 > 0，且前一個交易日淨買超 > 0
2. 最近五個交易日淨買超總和 < 0（整體仍在流出）
3. 最新三個交易日單日漲跌幅任一天 ≤ 5%
4. 最近五個交易日總漲跌幅 ≤ 10%

### 選股清單（⚠️ 按鈕觸發）

點擊頁面底部免責聲明的 ⚠️ 開啟。四個策略，個股只歸入符合的最高天數清單（去重）。

| 策略 | 連續買超 | N日總漲幅 | 每日漲幅 |
|------|---------|----------|---------|
| 二日 | 連續 2 日買超 | 2 日總漲幅 < 5% | 每日漲幅 < 5% |
| 三日 | 連續 3 日買超 | 3 日總漲幅 < 10% | 每日漲幅 < 5% |
| 四日 | 連續 4 日買超 | 4 日總漲幅 < 15% | 每日漲幅 < 5% |
| 五日 | 連續 5 日買超 | 5 日總漲幅 < 20% | 每日漲幅 < 5% |

### 合計流入 / 流出

泡泡圖上方的合計流入/流出，以個股為單位去重後加總，**只計算 4 位數且千位為 1-9 的股票**（排除 ETF/ETN）。

---

## 檔案結構

```
├── pipeline.py
├── requirements.txt
├── .gitignore
├── README.md
├── input/
│   └── group.csv                      族群清單（cp950，唯一需維護的檔案）
├── db/
│   └── market.db                      SQLite（每日隨更新推送至 GitHub）
├── docs/
│   ├── index.html
│   ├── sector.html
│   └── assets/data/
│       ├── bubble_data.json
│       ├── inflow_low_gain.json
│       ├── stealth_accumulation.json
│       ├── group_stats.json
│       ├── stock_screener.json
│       ├── metadata.json
│       └── market_data.csv
└── .github/workflows/daily.yml
```

---

## group.csv 格式（cp950）

```
3C通路商,,3D印表機,,IC製造,...         ← row 0：族群名稱（奇數欄空白）
代號,名稱,代號,名稱,代號,名稱,...      ← row 1：標題（跳過）
2392,正崴,3504,揚明光,2330,台積電,...  ← row 2+：代號與名稱交替
```

每兩欄一族群。修改後上傳至 `input/group.csv` 即生效。

---

## market_data.csv

每次執行後自動匯出至 `docs/assets/data/market_data.csv`：

| 欄位 | 說明 |
|------|------|
| 日期 / 代號 / 市場 / 名稱 | 基本資訊 |
| 收盤價 / 成交總股數 / 成交總金額 / 成交均價 | 行情 |
| 三大法人買賣超股數 / 三大法人買賣超金額億 | 法人 |

下載：`https://raw.githubusercontent.com/<帳號>/sector_rotation/main/docs/assets/data/market_data.csv`

---

## Workflow 選項

| 參數 | 說明 | 使用時機 |
|------|------|---------|
| `force` | 強制重抓今日 | API 資料異常需要重抓 |
| `reset_history` | 重新補抓 TWSE + TPEx 近30天歷史 | 首次建立 DB、歷史有誤 |
| `dry_run` | 不打 API，只用現有 DB 重算 | 測試計算邏輯 |
| `purge_bad_data` | 清除 trade_value=0 的污染資料 | 修復舊版遺留壞資料 |

---

## 首次執行 / 重建資料庫

```
1. 確認 input/group.csv 已在 repo
2. Actions → Run workflow → 勾選 reset_history
```

首次執行會逐股逐月補抓 TWSE 歷史（約 1400 支 × 2 個月，**25~45 分鐘**）+ TPEx 每日全市場補抓。之後每天 DB 已有歷史，只抓今日，幾秒完成。

### 為什麼首次較慢

TWSE 沒有「全市場歷史」的單一端點，歷史成交量/金額只能逐股查詢 `STOCK_DAY?stockNo=`。1400 支 × 2 月 ≈ 2800 次請求，受 TWSE 限速約需半小時。這是一次性成本，DB 建立後不再發生。TPEx 有全市場歷史端點，補抓快很多。

---

## 障礙排除

| 現象 | 可能原因 | 解法 |
|------|---------|------|
| 漲跌幅全部 0 | yfinance 抓取失敗或限速 | 稍後重跑，確認 runner 可連 query1.finance.yahoo.com |
| TWSE 歷史補抓很慢 | 個股逐月請求，1400 股需半小時 | 僅首次，之後不觸發 |
| 某些 ETN 代號報錯（$7xxxxx） | yfinance 不支援 ETN | 已自動過濾，不影響 |
| 頁面顯示「載入中」卡住 | sector.html 為舊版或 JSON 未推送 | F12 Console 看錯誤，確認 sector.html 為最新版 |
| `database is locked` | 上次執行中斷 | 刪除 db/market.db-wal 和 db/market.db-shm |
| API 抓取失敗 | TWSE/TPEx API 暫時無回應 | 等待後重跑，通常收盤後幾分鐘可用 |

---

## 免責聲明

資料來自 TWSE/TPEx 公開 API 及 Yahoo Finance（yfinance），非即時，存在揭露延遲。本專案為個人研究用途，不構成任何投資建議。
