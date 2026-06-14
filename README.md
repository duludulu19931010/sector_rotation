# TW$FLOW · 族群資金儀表板

TWSE/TPEx 官方 API + XQ 每日收盤 CSV + TPEx 歷史 CSV → SQLite → GitHub Pages

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

T86 欄位數因日期而異（16 或 19 欄），三大法人合計固定為 **最後一欄 `row[-1]`**，單位為「股」。

全市場端點若回傳資料量不足（TWSE < 100 / TPEx < 5 筆）直接 raise exception，pipeline 中止，不會存入不完整資料。

### 每日手動提供（CSV）

| 資料 | 路徑 | 說明 |
|------|------|------|
| 收盤價 | `input/XQ/YYYYMMDD_Data.csv` | XQ 匯出，需有 `代碼`、`成交` 欄位，支援 utf-8-sig / cp950 |
| TPEx 歷史（過去） | `input/TPEx/TPEx_YYYYMMDD.csv` | 從 TPEx 官網下載，big5 編碼，前 2 行標題跳過 |

**TPEx 個股歷史 API 端點（st43_result.php）已於 TPEx 2024/10 改版後失效**，過去資料請手動放入 `input/TPEx/`，今後資料由今日 API 每日累積。

### TWSE 個股歷史（自動補抓，首次執行）

DB 不足 21 個交易日時自動觸發：

- 端點：`exchangeReport/STOCK_DAY?date=YYYYMM01&stockNo=代號`
- 範圍：今日資料中全部 TWSE 代號（約 1366 支）× 當月+上月
- 並行：8 個 parallel requests
- 三大法人：T86 歷史逐日抓取（每次 0.3秒間隔）
- 預估時間：首次執行 **10~20 分鐘**，之後每日 < 1 分鐘

---

## 指標定義

| 指標 | 公式 |
|------|------|
| 今日淨買賣超（億） | `三大法人買賣超股數 × 均價 ÷ 1e8`（均價 = 成交金額 ÷ 成交股數） |
| 五日淨買賣超（億） | 最新連續 5 日「今日淨買賣超」之和 |
| 二十日淨買賣超（億） | 最新連續 20 日「今日淨買賣超」之和 |
| 今日漲跌幅（%） | `(今日收盤 − 昨日收盤) ÷ 昨日收盤 × 100`，來自 XQ CSV |
| 五日漲跌幅（%） | `(今日收盤 − 五日前收盤) ÷ 五日前收盤 × 100` |
| 二十日漲跌幅（%） | `(今日收盤 − 二十日前收盤) ÷ 二十日前收盤 × 100` |

**漲跌幅完全來自 XQ CSV 收盤價序列**；成交股數/成交金額/三大法人來自 API，絕不混用 CSV 數值。

泡泡圖：**X 軸 = 五日淨買超（資金出入量）**，**Y 軸 = 今日淨買超（資金加速度）**

### 合理範圍防護

| 項目 | 上限 | 超出時 |
|------|------|--------|
| 今日漲跌幅 | ±11% | 歸零 |
| 五日漲跌幅 | ±60% | 歸零 |
| 二十日漲跌幅 | ±200% | 歸零 |
| 今日淨買賣超 | ±1000億 | 歸零 |
| 五日淨買賣超 | ±5000億 | 歸零 |
| 二十日淨買賣超 | ±20000億 | 歸零 |

族群數字 = 個股明細加總（不再有矛盾）。

---

## 檔案結構

```
├── pipeline.py
├── check_record.py
├── check_db_status.py
├── requirements.txt
├── .gitignore
├── input/
│   ├── group.csv                   族群清單（cp950，寬表格，500+ 族群）
│   ├── stock_list.csv              股票名稱對照
│   ├── XQ/
│   │   └── YYYYMMDD_Data.csv        XQ 每日收盤（utf-8-sig 或 cp950）
│   └── TPEx/
│       └── TPEx_YYYYMMDD.csv        TPEx 歷史行情（big5，前2行標題跳過）
├── db/
│   └── market.db
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

## DB Schema（`daily` 表）

| 欄位 | 說明 |
|------|------|
| `date` | 交易日 YYYY-MM-DD（來自 API 的 Date 欄位，非系統日期） |
| `code` | 股票代號（4 位） |
| `market` | TWSE / TPEx |
| `name` | 股票名稱 |
| `close_price` | 收盤價（API；XQ CSV 在 compute 階段覆蓋） |
| `trade_volume` | 成交股數 |
| `trade_value` | 成交金額（元） |
| `inst_net` | 三大法人買賣超股數（股） |
| `net_yi` | 今日淨買賣超（億） |

---

## 每日操作

### 每天需要做的事

1. 把當日 `YYYYMMDD_Data.csv` 放入 `input/XQ/`
2. Actions → Run workflow（不需要勾任何參數）

### 首次執行 / 重建資料庫

```bash
git rm db/market.db
git commit -m "reset db"
git push
```

Actions → Run workflow（無需勾參數，DB 不存在時自動觸發 TWSE 歷史補抓，約 10~20 分鐘）。

### TPEx 過去歷史

1. 到 TPEx 官網 → 上櫃股票行情 → 選日期 → 下載 CSV
2. 重新命名為 `TPEx_YYYYMMDD.csv` 放入 `input/TPEx/`
3. Actions → Run workflow

---

## workflow_dispatch 選項

| 參數 | 說明 |
|------|------|
| `force` | 強制重抓今日（即使 DB 已有今日資料） |
| `reset_history` | 清除並重新補抓 TWSE 歷史 |
| `dry_run` | 不打 API，只用現有 DB + XQ 重算 JSON |
| `purge_bad_data` | 清除 trade_value=0 的污染資料並重新補抓 |
| `check_code` | 查詢指定股票代號的 DB 記錄（印在 log） |
| `check_db_status` | 診斷各日期的 trade_value/inst_net/net_yi 分布 |
| `run_tpex_new_history_test` | 探測 TPEx 新版個股行情頁面的 API 端點 |

---

## 障礙排除

| 現象 | 原因 | 解法 |
|------|------|------|
| `RuntimeError: TWSE STOCK_DAY_ALL failed` | TWSE API 今日尚未更新或回傳異常 | 確認交易時段後重跑 |
| `RuntimeError: TPEx quotes failed` | TPEx API 暫時無回應 | 等待後重跑 |
| 漲跌幅全部 0 | XQ CSV 不足或未放入 `input/XQ/` | 確認 CSV 放置路徑與欄位名稱 |
| `missing required columns` | XQ CSV 缺少 `代碼` 或 `成交` 欄位 | 確認 XQ 匯出格式 |
| TWSE 歷史補抓很慢 | 約 1366 支 × 2 個月逐股請求，正常現象 | 僅首次執行一次，之後不觸發 |
| TPEx net_20d 需較久才正常 | TPEx 個股歷史無 API，從今日起累積 | 約 21 個交易日後完整（可手動放入 `input/TPEx/` 加速） |
| `database is locked` | 上次執行中斷 | 刪除 `db/market.db-wal`、`db/market.db-shm` |

---

## 免責聲明

資料來自 TWSE/TPEx 公開 API 及使用者提供之 CSV，非即時，存在揭露延遲。本專案為個人研究用途，不構成投資建議。
