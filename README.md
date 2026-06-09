# TW$FLOW · 台股族群資金儀表板

族群板塊輪動泡泡圖（官方 API 真實數據）  
GitHub Actions（Windows Self-hosted Runner）每日自動爬取 → SQLite 累積 → GitHub Pages

---

## 資料是真實的嗎？

**是。所有數據 100% 來自官方免費 API，每次 Workflow 執行即時抓取並計算：**

| 數據 | 來源 | 端點 |
|------|------|------|
| 三大法人買賣超 | TWSE 官方 | `https://www.twse.com.tw/rwd/zh/fund/T86` |
| 上市收盤價 + 股票名稱 | TWSE OpenAPI | `https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL` |
| 上櫃收盤價 + 股票名稱 | TPEx OpenAPI | `https://www.tpex.org.tw/openapi/v1/tpex/exchangeReport/daily_close_quotes` |

股票名稱來自 T86 欄位 1 與收盤價 API 的 Name 欄位，每日與數據一起存入 SQLite。

資料流：`官方 API → SQLite（db/market.db，永久累積）→ 計算 → JSON → 前端`

---

## 指標定義

| 指標 | 公式 | 用途 |
|------|------|------|
| **資金出入量（億）** | Σ 個股近5日三大法人合計（張）× 收盤價 × 1000 ÷ 1e8 | 泡泡圖 **X 軸** |
| **資金加速度（億/天）** | Σ 個股今日三大法人合計（張）× 收盤價 × 1000 ÷ 1e8 | 泡泡圖 **Y 軸** |
| 近20日累計淨買超（億） | 同上，視窗 = 20 個交易日 | 點擊泡泡的詳情側欄 |
| 五日漲跌（%） | (今收 − 5日前收) ÷ 5日前收 × 100，族群取個股平均 | tooltip / 表格 |

### 泡泡圖標籤

| 標籤 | 條件 |
|------|------|
| 主力 | 五日淨買超 > 2億 且 五日漲幅 > 1% |
| 輪動 | 五日淨買超 > 0 且 漲幅 ≤ 1% |
| 退潮 | 五日淨買超 < −2億 |
| 觀望 | 其餘 |

### 篩選條件

- **流入低漲幅**：`net_5d > 0 AND change_5d_pct < 10`，依 net_5d 由大到小
- **偷偷布局**：`net_1d > 0 AND net_5d > 0 AND change_5d_pct < 0`，依 net_5d 由大到小

---

## 前端互動功能

| 功能 | 操作 |
|------|------|
| 泡泡圖縮放 | 滑鼠滾輪放大/縮小，拖曳平移，「重置縮放」按鈕還原 |
| 族群詳情側欄 | **點擊泡泡** → 右側彈出：當日淨買超、近5日淨買超、近20日累計、近5日漲跌 + 個股明細表 |
| 篩選表展開 | 點擊表格列 → 展開該族群個股（代碼/名稱/今日漲跌/今日淨買超/五日漲跌/五日淨買超） |
| 流向過濾 | 「全部 / 流入 / 流出」按鈕切換泡泡圖顯示範圍 |

---

## 資料庫（db/market.db）

每日 pipeline 自動寫入，**永不刪除歷史**：

| 資料表 | 內容 | 主鍵 |
|--------|------|------|
| `institutional_flow` | 每日個股三大法人（外資/投信/自營/合計，張）+ 股票名稱 | (trade_date, code) |
| `stock_prices` | 每日個股 OHLC + 成交量 + 股票名稱 + 市場別 | (trade_date, code) |
| `group_daily` | 每日族群統計（net_1d / net_5d / net_20d / 漲跌幅 / 標籤） | (trade_date, group_name) |
| `etf_holdings` | 每日 ETF 持股快照 | (trade_date, etf_code, stock_code) |

### 快取機制（避免重複爬取）

| 檢查 | 行為 |
|------|------|
| 今日 T86 已入庫 且 DB 已有 ≥20 個交易日 | 跳過 API，直接讀 DB |
| 今日收盤價已入庫 | 跳過 API |
| T86 抓 20 日時 | 已在 DB 的日期自動跳過，**首次執行約 20 次 API 呼叫，之後每天只抓 1 次** |

### 查詢歷史資料

用 [DB Browser for SQLite](https://sqlitebrowser.org/) 開 `db/market.db`，或：

```python
import sys; sys.path.insert(0, 'src')
import src.db.store as db
db.init('db/market.db')
rows = db.load_group_daily('2026-06-09')   # 指定日所有族群
df   = db.load_institutional(days=20)       # 近20日法人資料
```

---

## 資料夾結構

```
├── pipeline.py                 主程式
├── input/Group.csv             族群清單（Big5，58 族群 809 檔）
├── db/market.db                SQLite
├── src/
│   ├── db/store.py             資料庫模組
│   ├── scrapers/twse.py        T86 + 收盤價爬蟲
│   ├── scrapers/etf.py         ETF 爬蟲
│   └── analysis/compute.py     指標計算
├── docs/
│   ├── sector.html             前端
│   └── assets/data/*.json      每日輸出
└── .github/workflows/daily.yml
```

---

## 執行方式

```powershell
python pipeline.py              # 正常（快取命中則跳 API）
python pipeline.py --force      # 強制全部重抓
python pipeline.py --skip-etf   # 跳過 ETF
python pipeline.py --dry-run    # 純用 DB 重算 JSON
```

排程：每週一到五台灣時間 18:30（`.github/workflows/daily.yml`）

---

## Windows 自架 Runner 安裝

1. **確認 Python**：PowerShell 執行 `python --version`（需 3.11+，沒有就裝並加 PATH）
2. **下載 Runner**：repo → Settings → Actions → Runners → New self-hosted runner → Windows x64 → 照頁面指令執行
3. **裝成服務**：
   ```powershell
   cd C:\runners\graphics
   .\svc.cmd install
   .\svc.cmd start
   ```
4. **授權 push**：repo → Settings → Actions → General → Workflow permissions → **Read and write** → Save
5. **首次測試**：`python pipeline.py --skip-etf`

---

## 障礙排除

### `pwsh: command not found`
Workflow 用了 PowerShell 7。確認 `daily.yml` 的 job 有 `defaults: run: shell: powershell`。

### `setup-python: Cannot find python.exe`
Self-hosted runner 不要用 `actions/setup-python`，直接用機器上的 Python（目前的 workflow 已正確）。

### push 403 `Permission denied to github-actions[bot]`
兩個都要做：① Settings → Actions → General → Workflow permissions → Read and write；② workflow job 內有 `permissions: contents: write` 且 checkout 帶 `persist-credentials: true`。

### TPEx SSL `Missing Subject Key Identifier`
TPEx 憑證問題，程式已用 `verify=False` 處理。確認 `src/scrapers/twse.py` 頂部有 `urllib3.disable_warnings(...)`。

### T86 回傳 403
TWSE 對海外 IP 或高頻請求封鎖。確認 runner 在台灣網路環境，且排程在收盤後（18:30 預設）。程式每次 T86 呼叫間隔 0.3 秒。

### `database is locked`
```powershell
Remove-Item db\market.db-wal -ErrorAction SilentlyContinue
Remove-Item db\market.db-shm -ErrorAction SilentlyContinue
```

### Group.csv 亂碼
CSV 必須是 Big5 編碼。Excel 另存時選「CSV（逗號分隔，繁體中文 Big5）」。

### 泡泡圖點不出側欄 / 個股顯示「股XXXX」
舊版 JSON 沒有 `stocks`/`net_20d` 欄位，或股票名稱來自舊資料。執行 `python pipeline.py --force` 重抓重算。

### Pages 沒更新
① Actions 確認 update job 綠色；② `metadata.json` 的 last_updated 是今天；③ Ctrl+Shift+R 清快取。

---

## 免責聲明

資料來自 TWSE/TPEx 公開 API，非即時，存在揭露延遲。本專案為個人研究用途，不構成投資建議。
