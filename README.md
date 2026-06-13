# TW$FLOW · 族群資金儀表板

TWSE / TPEx 官方 API + 每日 CSV 收盤價 → SQLite 累積 → GitHub Pages  
Self-hosted Runner（台灣本地機器）

---

## 資料來源

### 每日資料（今日，全市場一次抓取）

| 市場 | 資料 | 端點 |
|------|------|------|
| TWSE 上市 | 今日收盤、成交股數、成交金額 | `https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL` |
| TWSE 上市 | 三大法人買賣超 | `https://www.twse.com.tw/rwd/zh/fund/T86?date=YYYYMMDD&selectType=ALL` |
| TPEx 上櫃 | 今日收盤、成交股數、成交金額 | `https://www.tpex.org.tw/openapi/v1/tpex_mainboard_quotes` |
| TPEx 上櫃 | 三大法人買賣超 | `https://www.tpex.org.tw/openapi/v1/tpex_3insti_daily_trading` |

**全市場端點若回傳資料量過少（TWSE<100 / TPEx<5）會直接 raise exception，pipeline 中止，不會存入不完整資料。**

T86：欄位數量因日期而不同（16或19欄），**三大法人買賣超股數固定為最後一欄（`row[-1]`）**，已驗證與「外陸資+外資自營商+投信+自營商」各子項加總一致。單位為「股」。

tpex_3insti_daily_trading：`ForeignInvestorsBuy/Sell`、`InvestmentTrustBuy/Sell`、`DealersBuy/Sell`，程式自行加總（單位「股」）。

### 歷史補抓（個股逐月，僅 DB 不足21個交易日時執行一次）

| 市場 | 端點 | 一次回傳範圍 |
|------|------|------|
| TWSE 上市 | `https://www.twse.com.tw/exchangeReport/STOCK_DAY?date=YYYYMM01&stockNo=代號&response=json` | 該股票整月每日收盤/成交股數/成交金額 |
| TPEx 上櫃 | `https://www.tpex.org.tw/web/stock/aftertrading/daily_trading_info/st43_result.php?d=YYY/MM&stkno=代號` | 同上（民國年） |
| TWSE 三大法人歷史 | `T86?date=YYYYMMDD`（逐日，全市場） | 當日全市場三大法人 |

補抓範圍：族群池全部代號（809支）× 當月+上月（涵蓋21個交易日），三大法人取自 T86 歷史逐日抓取後比對代號。

**此端點僅補「收盤價/成交股數/成交金額」，三大法人歷史仍來自 T86（已驗證可靠）。**

### CSV（收盤價，僅用於漲跌幅計算）

`input/YYYYMMDD_Data.csv`（CP950 編碼），每日手動放入，需包含欄位：

```
代碼,商品,...,成交,...
```

只使用 `代碼` 和 `成交`（收盤價）兩欄。**API 抓到的三大法人/成交金額/成交股數絕對不會被 CSV 覆蓋。**

---

## 指標定義

| 指標 | 公式 |
|------|------|
| 今日淨買賣超（億） | `三大法人買賣超股數 × 均價 ÷ 1e8`（均價 = 成交金額 ÷ 成交股數） |
| 五日淨買賣超（億） | 最新連續五日「今日淨買賣超」之和 |
| 二十日淨買賣超（億） | 最新連續二十日「今日淨買賣超」之和 |
| 今日漲跌幅（%） | `(今日收盤價 − 昨日收盤價) ÷ 昨日收盤價 × 100` |
| 五日漲跌幅（%） | `(今日收盤價 − 五日前收盤價) ÷ 五日前收盤價 × 100`（往前數第6個交易日） |
| 二十日漲跌幅（%） | `(今日收盤價 − 二十日前收盤價) ÷ 二十日前收盤價 × 100`（往前數第21個交易日） |

漲跌幅一律取自 **CSV 收盤價序列**；若 CSV 不足則 fallback 用 API 收盤價（僅當日有效）。

### 泡泡圖軸定義

| 軸 | 指標 | 意義 |
|----|------|------|
| **X 軸** | 五日淨買超（億） | 資金出入量（累積方向） |
| **Y 軸** | 今日淨買超（億） | 資金加速度（當日動能） |

圓圈大小：五日淨買超絕對值。

### 合理範圍防護（資料異常自動歸零）

| 項目 | 上限 |
|------|------|
| 今日漲跌幅 | ±11% |
| 五日漲跌幅 | ±60% |
| 二十日漲跌幅 | ±200% |
| 今日淨買賣超 | ±1000億 |
| 五日淨買賣超 | ±5000億 |
| 二十日淨買賣超 | ±20000億 |

族群層級數值**直接加總個股明細**（已套用上述防護），族群數字 = 個股明細加總，不會出現不一致。

---

## 檔案結構

```
├── pipeline.py
├── check_record.py            單筆資料檢索工具
├── requirements.txt
├── .gitignore
├── input/
│   ├── Group.csv               族群個股清單（Big5，58 族群）
│   ├── stock_list.csv          股票名稱對照（CP950，1937 筆）
│   └── YYYYMMDD_Data.csv        每日收盤價（CP950，每天手動新增）
├── db/
│   └── market.db                SQLite（三大法人/成交金額/成交股數/收盤價，每日累積）
├── docs/
│   ├── index.html
│   ├── sector.html
│   └── assets/data/
│       ├── bubble_data.json
│       ├── inflow_low_gain.json
│       ├── stealth_accumulation.json
│       ├── group_stats.json
│       ├── metadata.json
│       └── market_data.csv      DB 全量匯出
└── .github/workflows/daily.yml
```

---

## DB 結構（單表 `daily`）

| 欄位 | 說明 |
|------|------|
| `date` | 交易日 YYYY-MM-DD |
| `code` | 股票代號（4位） |
| `market` | TWSE / TPEx |
| `name` | 股票名稱 |
| `close_price` | 收盤價 |
| `trade_volume` | 成交股數 |
| `trade_value` | 成交金額（元） |
| `inst_net` | 三大法人買賣超股數 |
| `net_yi` | 今日淨買賣超（億） |

---

## 執行

### 每日流程

1. 收盤後，把當日 `YYYYMMDD_Data.csv` 放入 `input/`
2. 執行：

```bash
python pipeline.py
```

### 首次執行 / 重建整個資料庫

```bash
rm db/market.db
rm docs/assets/data/*.json
python pipeline.py
```

執行流程：
1. 抓取**今日**全市場資料（TWSE/TPEx 價量 + 三大法人），存入 DB
2. 偵測 DB 交易日數 < 21 → 觸發**歷史補抓**：
   - 對族群池 809 支股票，逐股呼叫個股月歷史端點（當月+上月）
   - 對應的三大法人改用 T86 逐日歷史
   - 寫入 DB（不覆蓋今日資料）
3. 從 DB 讀取近25個交易日資料 + CSV 收盤價序列，計算指標並輸出 JSON

**首次執行時間：809支 × 2個月 ≈ 1600次個股請求 + 約20次T86歷史請求，預估 15-25 分鐘。** 之後每天執行，DB 已有 ≥21 個交易日，不會再觸發補抓，執行時間 < 1 分鐘。

### 選項

```bash
python pipeline.py                          # 正常（今日已存在則跳過API）
python pipeline.py --force                   # 強制重抓今日
python pipeline.py --reset-history --force   # 清除不完整日期並強制重新補抓歷史
python pipeline.py --dry-run                 # 只用現有 DB + CSV 重算 JSON，不打API
```

### 查詢單筆資料

```bash
python check_record.py 2330              # 查某股票所有日期
python check_record.py 2330 2026-06-12   # 查某股票某日
python check_record.py --date 2026-06-12 # 查某日全市場統計
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

### workflow_dispatch 選項

| 參數 | 說明 |
|------|------|
| `force` | 強制重抓今日 |
| `reset_history` | 清除不完整日期並強制重新補抓歷史 |
| `dry_run` | 不打API，只用現有DB+CSV重算 |
| `check_code` | 查詢指定股票代號的完整記錄（輸出至log） |

---

## 障礙排除

| 錯誤 | 原因 | 解法 |
|------|------|------|
| `RuntimeError: TWSE STOCK_DAY_ALL failed` | 今日API回應過少/異常 | 確認交易時段、重跑 |
| `RuntimeError: TPEx tpex_mainboard_quotes failed` | TPEx API 暫時無回應 | 等待後重跑 |
| 漲跌幅為0 | CSV 歷史天數不足 | 累積到6/21天後自動正常 |
| 族群數字與個股明細不符 | 不應發生（族群=個股加總） | 回報並重跑 `--reset-history --force` |
| `database is locked` | 上次執行中斷 | 刪 `db/market.db-wal`、`db/market.db-shm` |
| 歷史補抓很慢 | 809支×2個月逐股抓取為正常現象 | 僅首次執行需要，之後不再觸發 |

---

## 免責聲明

資料來自 TWSE/TPEx 公開 API 及使用者提供之 CSV，非即時，存在揭露延遲。本專案為個人研究用途，不構成投資建議。
