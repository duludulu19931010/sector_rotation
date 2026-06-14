# TW$FLOW · 族群資金儀表板

TWSE/TPEx 官方 API + 每日 XQ CSV + TPEx 歷史 CSV → SQLite → GitHub Pages

Self-hosted Runner（台灣本地機器，TPEx 不封鎖）

---

## 資料來源

### 每日自動抓取（官方 API）

| 市場 | 資料 | 端點 |
|------|------|------|
| TWSE | 今日收盤、成交股數、成交金額 | `openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL` |
| TWSE | 三大法人買賣超（股） | `www.twse.com.tw/rwd/zh/fund/T86?date=YYYYMMDD&selectType=ALL` |
| TPEx | 今日收盤、成交股數、成交金額 | `tpex.org.tw/openapi/v1/tpex_mainboard_quotes` |
| TPEx | 三大法人買賣超（股） | `tpex.org.tw/openapi/v1/tpex_3insti_daily_trading` |

T86：欄位數因日期而異（16 或 19 欄），三大法人合計固定為**最後一欄 `row[-1]`**，單位為「股」。

日期從 API `Date` 欄位解析（民國年7位數），非系統日期，週末/假日執行不會建立假日資料。

### 每日手動提供（CSV）

| 資料 | 路徑 | 格式 | 說明 |
|------|------|------|------|
| 收盤價 | `input/XQ/YYYYMMDD_Data.csv` | utf-8-sig 或 cp950 | XQ 匯出，需有 `代碼`、`成交` 欄位 |
| TPEx 歷史行情 | `input/TPEx/TPEx_YYYYMMDD.csv` | big5，前2行標題跳過 | 從 TPEx 官網下載 |
| TPEx 三大法人歷史 | `input/TPExDealer/TPExDealer_YYYYMMDD.csv` | cp950，第1行說明跳過 | 從 TPEx 官網下載，最後欄為「三大法人買賣超股數合計」 |

**TPEx 個股歷史 API（st43_result.php）已於 TPEx 2024/10 改版後失效**。過去資料請手動放入對應資料夾，今後由每日 API 自動累積。

### TWSE 個股歷史（自動補抓，DB 不足 21 個交易日時執行）

- 端點：`exchangeReport/STOCK_DAY?date=YYYYMM01&stockNo=代號`
- 範圍：今日資料中全部 TWSE 代號（約 1366 支）× 當月+上月
- 並行：8 個 parallel requests
- T86 歷史：逐日抓取（0.3秒間隔）
- 預估時間：首次執行 **10~20 分鐘**，之後每日 < 1 分鐘

---

## 指標定義

| 欄位 | 定義 |
|------|------|
| `close_price` | 收盤價 |
| `trade_volume` | 成交總股數 |
| `trade_value` | 成交總金額（元） |
| `avg_price` | 成交均價 = `trade_value ÷ trade_volume` |
| `inst_net` | 三大法人買賣超總股數 |
| `inst_value` | 三大法人買賣超總金額（億） = `inst_net × avg_price ÷ 1e8` |
| `net_yi` | 同 `inst_value`（計算結果相同） |

**漲跌幅計算（來自 XQ CSV 收盤價序列）：**

| 指標 | 公式 |
|------|------|
| 今日漲跌幅（%） | `(今日收盤 − 昨日收盤) ÷ 昨日收盤 × 100` |
| 五日漲跌幅（%） | `(今日收盤 − 五日前收盤) ÷ 五日前收盤 × 100` |
| 二十日漲跌幅（%） | `(今日收盤 − 二十日前收盤) ÷ 二十日前收盤 × 100` |

泡泡圖：**X 軸 = 五日淨買超（資金出入量）**，**Y 軸 = 今日淨買超（資金加速度）**

---

## 檔案結構

```
├── pipeline.py
├── requirements.txt
├── .gitignore
├── README.md
├── input/
│   ├── group.csv                        族群清單（cp950）
│   ├── XQ/
│   │   └── YYYYMMDD_Data.csv             XQ 每日收盤（utf-8-sig 或 cp950）
│   ├── TPEx/
│   │   └── TPEx_YYYYMMDD.csv             TPEx 每日行情（big5）
│   └── TPExDealer/
│       └── TPExDealer_YYYYMMDD.csv        TPEx 三大法人（cp950）
├── db/
│   └── market.db                         SQLite
├── docs/
│   ├── index.html
│   ├── sector.html
│   └── assets/data/
│       ├── bubble_data.json
│       ├── inflow_low_gain.json
│       ├── stealth_accumulation.json
│       ├── group_stats.json
│       └── metadata.json
└── .github/workflows/daily.yml
```

---

## group.csv 格式

```
3C通路商,,3D印表機,,4G通訊設備,,...   ← row 0：族群名稱（偶數欄）
代號,名稱,代號,名稱,代號,名稱,...     ← row 1：標題（跳過）
2392,正崴,3504,揚明光,3596,智易,...   ← row 2+ ：個股代號與名稱
```

cp950 編碼，每兩欄一族群（代號+名稱）。

---

## DB Schema（`daily` 表）

| 欄位 | 型態 | 說明 |
|------|------|------|
| `date` | TEXT | 交易日 YYYY-MM-DD（來自 API Date 欄，非系統日期） |
| `code` | TEXT | 股票代號（4 位） |
| `market` | TEXT | TWSE / TPEx |
| `name` | TEXT | 股票名稱 |
| `close_price` | REAL | 收盤價 |
| `trade_volume` | INTEGER | 成交總股數 |
| `trade_value` | INTEGER | 成交總金額（元） |
| `avg_price` | REAL | 成交均價 |
| `inst_net` | INTEGER | 三大法人買賣超總股數 |
| `inst_value` | REAL | 三大法人買賣超總金額（億） |
| `net_yi` | REAL | 同 inst_value |

**既有 DB 自動遷移：** 若 `avg_price`、`inst_value` 欄位不存在，`db_init()` 自動執行 `ALTER TABLE` 補上。

---

## 每日操作

### 你每天需要做的事

1. 把當日 `YYYYMMDD_Data.csv` 放入 `input/XQ/`
2. Actions → Run workflow（不勾任何參數）

### 提供 TPEx 歷史資料

1. TPEx 官網 → 上櫃股票行情 → 選日期 → 下載 CSV → 命名 `TPEx_YYYYMMDD.csv` → 放入 `input/TPEx/`
2. TPEx 官網 → 三大法人買賣超彙總表 → 選日期 → 下載 CSV → 命名 `TPExDealer_YYYYMMDD.csv` → 放入 `input/TPExDealer/`

### 首次執行 / 重建資料庫

```
1. 刪除 db/market.db（在 GitHub 網頁點檔案 → 垃圾桶）
2. Actions → Run workflow（不勾任何參數）
   → 自動抓今日 API 資料
   → 自動補抓 TWSE 歷史（10~20 分鐘）
   → 讀入 input/TPEx/ 和 input/TPExDealer/ 裡的所有 CSV
```

---

## workflow_dispatch 選項

| 參數 | 說明 |
|------|------|
| `force` | 強制重抓今日（即使 DB 已有今日資料） |
| `reset_history` | 清除並重新補抓 TWSE 個股歷史 |
| `dry_run` | 不打 API，只用現有 DB + XQ 重算 JSON |
| `purge_bad_data` | 清除 trade_value=0 的污染資料並重新補抓 |

---

## 障礙排除

| 現象 | 原因 | 解法 |
|------|------|------|
| `RuntimeError: TWSE STOCK_DAY_ALL failed` | API 今日尚未更新 | 收盤後重跑 |
| `RuntimeError: TPEx quotes failed` | TPEx API 暫時無回應 | 等待後重跑 |
| 漲跌幅全部 0 | `input/XQ/` 沒有 CSV 或欄位名稱不符 | 確認 CSV 有 `代碼`、`成交` 欄位 |
| TPEx inst_net 都是 0 | `input/TPExDealer/` 沒有對應日期的 CSV | 放入 TPExDealer CSV 後重跑 `--dry-run` |
| TWSE 歷史補抓很慢 | 1366 支 × 2 個月逐股請求，正常現象 | 僅首次執行，之後不觸發 |
| `database is locked` | 上次執行中斷 | 刪除 `db/market.db-wal`、`db/market.db-shm` |

---

## 免責聲明

資料來自 TWSE/TPEx 公開 API 及使用者提供之 CSV，非即時，存在揭露延遲。本專案為個人研究用途，不構成投資建議。
