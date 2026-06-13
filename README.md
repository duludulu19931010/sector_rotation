# TW$FLOW · 族群資金儀表板

TWSE / TPEx 官方 API（全市場）+ 每日 CSV 收盤價 → SQLite 累積 → GitHub Pages  
Self-hosted Runner（台灣本地機器）

---

## 資料來源

### API（三大法人、成交金額、成交股數 — 全市場一次抓取）

| 市場 | 資料 | 端點 |
|------|------|------|
| TWSE 上市 | 今日收盤、成交股數、成交金額 | `https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL` |
| TWSE 上市 | 歷史收盤、成交股數、成交金額 | `https://www.twse.com.tw/rwd/zh/afterTrading/STOCK_DAY_ALL?date=YYYYMMDD` |
| TWSE 上市 | 三大法人買賣超（T86） | `https://www.twse.com.tw/rwd/zh/fund/T86?date=YYYYMMDD` |
| TPEx 上櫃 | 今日收盤、成交股數、成交金額 | `https://www.tpex.org.tw/openapi/v1/tpex_mainboard_quotes` |
| TPEx 上櫃 | 歷史收盤、成交股數、成交金額 | `https://www.tpex.org.tw/web/stock/aftertrading/daily_close_quotes/stk_quote_result.php` |
| TPEx 上櫃 | 三大法人買賣超 | `https://www.tpex.org.tw/openapi/v1/tpex_3insti_daily_trading` |

**今日全市場抓取（`fetch_twse_price_today`, `fetch_tpex_price_today`, `fetch_tpex_inst`）若回傳資料量過少會直接 raise exception，pipeline 中止，不會存入不完整資料。**

T86 欄位索引：`[0]`代號 `[1]`名稱 `[14]`三大法人合計淨買超（張）  
tpex_3insti_daily_trading：`ForeignInvestorsBuy/Sell`、`InvestmentTrustBuy/Sell`、`DealersBuy/Sell`，程式自行加總

### CSV（收盤價，僅用於漲跌幅計算）

`input/YYYYMMDD_Data.csv`（CP950 編碼），每日手動放入，需包含欄位：

```
代碼,商品,...,成交,...
```

只使用 `代碼` 和 `成交`（收盤價）兩欄。**API 抓到的三大法人資料絕對不會被 CSV 覆蓋——三大法人只來自 API。**

---

## 指標定義

| 指標 | 公式 |
|------|------|
| 今日淨買賣超（億） | `(成交金額 ÷ 成交股數) × 三大法人總買賣超張數 × 1000 ÷ 1e8`<br>= 均價 × 買賣超張數 × 1000股/張 ÷ 1e8 |
| 五日淨買賣超（億） | 最新連續五日「今日淨買賣超」之和 |
| 二十日淨買賣超（億） | 最新連續二十日「今日淨買賣超」之和 |
| 今日漲跌幅（%） | `(今日收盤價 − 昨日收盤價) ÷ 昨日收盤價 × 100` |
| 五日漲跌幅（%） | `(今日收盤價 − 五日前收盤價) ÷ 五日前收盤價 × 100`（往前數第6個交易日） |
| 二十日漲跌幅（%） | `(今日收盤價 − 二十日前收盤價) ÷ 二十日前收盤價 × 100`（往前數第21個交易日） |

漲跌幅一律取自 **CSV 收盤價序列**；若 CSV 不足則 fallback 用 API 收盤價（僅當日）。

泡泡圖 X 軸：五日淨買賣超（資金出入量）  
泡泡圖 Y 軸：今日淨買賣超（資金加速度）

### 合理範圍防護（資料異常自動歸零）

| 項目 | 上限 |
|------|------|
| 今日漲跌幅 | ±11% |
| 五日漲跌幅 | ±60% |
| 二十日漲跌幅 | ±200% |
| 今日淨買賣超 | ±1000億 |
| 五日淨買賣超 | ±5000億 |
| 二十日淨買賣超 | ±20000億 |

族群層級的數值**直接加總個股明細**（已套用上述防護），確保族群數字 = 個股明細加總，不會出現不一致。

---

## 檔案結構

```
├── pipeline.py
├── requirements.txt
├── .gitignore
├── input/
│   ├── Group.csv             族群個股清單（Big5，58 族群）
│   ├── stock_list.csv        股票名稱對照（CP950，1937 筆）
│   └── YYYYMMDD_Data.csv      每日收盤價（CP950，每天手動新增）
├── db/
│   └── market.db              SQLite（三大法人/成交金額/成交股數，每日累積）
├── docs/
│   ├── index.html
│   ├── sector.html
│   └── assets/data/
│       ├── bubble_data.json
│       ├── inflow_low_gain.json
│       ├── stealth_accumulation.json
│       ├── group_stats.json
│       ├── metadata.json
│       └── market_data.csv    DB 全量匯出
└── .github/workflows/daily.yml
```

---

## 執行

### 每日流程

1. 收盤後，把當日 `YYYYMMDD_Data.csv` 放入 `input/`
2. 執行：

```bash
python pipeline.py
```

DB 自動累積三大法人/成交金額/成交股數；CSV 自動讀取所有 `input/*_Data.csv` 提供收盤價序列。

### 首次執行

DB 為空時會自動補抓近 21 個交易日的 TWSE/TPEx 歷史（成交股數/金額/三大法人/收盤），約需 3~5 分鐘。

CSV 也需要對應日期的歷史檔案才能計算五日/二十日漲跌幅——若手上沒有歷史 CSV，可先用 API 收盤價 fallback（精度較低，僅當日有效）。

### 選項

```bash
python pipeline.py                       # 正常（快取則跳過 API）
python pipeline.py --force                # 強制重抓今日
python pipeline.py --reset-history --force  # 清除不完整歷史並重抓
python pipeline.py --dry-run               # 只用現有 DB + CSV 重算 JSON
```

---

## GitHub 設定

### Pages
1. repo → Settings → Pages
2. Source 選 **`GitHub Actions`**

### Runner
self-hosted（台灣本地機器，TPEx 不封鎖）：

```powershell
.\config.cmd --url https://github.com/你的帳號/sector_rotation --token ...
.\svc.cmd install
.\svc.cmd start
```

repo → Settings → Actions → General → Workflow permissions → **Read and write**

---

## 障礙排除

| 錯誤 | 原因 | 解法 |
|------|------|------|
| `RuntimeError: TWSE STOCK_DAY_ALL failed` | API 回應過少/異常 | 確認交易時段、重跑 |
| `RuntimeError: TPEx tpex_mainboard_quotes failed` | TPEx API 暫時無回應 | 等待後重跑 |
| 漲跌幅為 0 | CSV 歷史天數不足 | 累積到 6/21 天後自動正常 |
| 族群數字與個股明細不符 | 不應發生（已修正：族群=個股加總） | 若發生，回報並重跑 `--reset-history --force` |
| `database is locked` | 上次執行中斷 | 刪 `db/market.db-wal`、`db/market.db-shm` |

---

## 免責聲明

資料來自 TWSE/TPEx 公開 API 及使用者提供之 CSV，非即時，存在揭露延遲。本專案為個人研究用途，不構成投資建議。
