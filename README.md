# TW$FLOW · 族群資金儀表板

TWSE / TPEx 官方 API 每日更新 → SQLite 累積 → GitHub Pages  
完全使用 GitHub Actions ubuntu-latest，**不需要 Self-hosted Runner**

---

## 資料來源（官方免費 API，無需帳號）

| 市場 | 資料 | 端點 |
|------|------|------|
| TWSE 上市 | 全市場收盤行情 | `https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL` |
| TWSE 上市 | 三大法人買賣超（T86） | `https://www.twse.com.tw/rwd/zh/fund/T86` |
| TPEx 上櫃 | 收盤行情 | `https://www.tpex.org.tw/openapi/v1/tpex_mainboard_quotes` |
| TPEx 上櫃 | 三大法人買賣超 | `https://www.tpex.org.tw/openapi/v1/tpex_3insti_daily_trading` |

TPEx 端點使用 `verify=False`（TPEx 憑證缺 Subject Key Identifier）。

**T86 欄位索引：** `[0]`代號 `[1]`名稱 `[14]`三大法人合計淨買超（張）

**tpex_3insti_daily_trading 欄位：** `SecuritiesCompanyCode`、`ForeignInvestorsBuy/Sell`、`InvestmentTrustBuy/Sell`、`DealersBuy/Sell`，程式自行計算各法人淨買超再加總

---

## 指標定義

| 指標 | 公式 |
|------|------|
| 今日淨買賣超（億） | `(成交股數 ÷ 成交金額) × 三大法人總買賣超張數 × 1000` |
| 五日淨買賣超（億） | 最新五筆今日淨買賣超之和 |
| 二十日淨買賣超（億） | 最新二十筆今日淨買賣超之和 |
| 今日漲跌幅（%） | `(今收 − 前一交易日收) ÷ 前一交易日收 × 100` |
| 五日漲跌幅（%） | `(今收 − 前六交易日收) ÷ 前六交易日收 × 100` |

泡泡圖 X 軸：五日淨買賣超（資金出入量）  
泡泡圖 Y 軸：今日淨買賣超（資金加速度）

---

## 架構

```
├── pipeline.py               主程式
├── requirements.txt
├── input/
│   ├── Group.csv             族群個股清單（Big5，58 族群）
│   └── stock_list.csv        股票名稱對照（CP950，1937 筆）
├── db/market.db              SQLite（每日累積）
├── docs/
│   ├── index.html
│   ├── sector.html           主頁面
│   └── assets/data/          每日 JSON
└── .github/workflows/daily.yml
```

---

## 執行

### 首次手動執行（收盤後 15:30 以後）

```bash
pip install -r requirements.txt
python pipeline.py
```

首次執行只有今日資料，歷史資料會在後續每天累積。

### 選項

```bash
python pipeline.py              # 正常（快取則跳過 API）
python pipeline.py --force      # 強制重抓今日
python pipeline.py --dry-run    # 只用 DB 資料重算 JSON
```

### 自動排程

GitHub Actions 每週一到五 **台灣時間 18:30** 自動執行，結束後 GitHub Pages 自動更新。**不需要任何本地機器。**

---

## GitHub 設定

### Pages
1. repo → Settings → Pages
2. Source 選 **`GitHub Actions`**
3. Save

### Workflow 權限
repo → Settings → Actions → General → Workflow permissions → **Read and write** → Save

---

## 障礙排除

| 錯誤 | 原因 | 解法 |
|------|------|------|
| `Pages 401` | Source 未選 GitHub Actions | Settings → Pages → Source → GitHub Actions |
| TWSE API 空回應 | 非交易日或收盤前執行 | 等 15:30 收盤後再執行 |
| TPEx SSL 警告 | TPEx 憑證問題 | 已設定 `verify=False`，不影響功能 |
| 漲跌幅全為 0 | DB 只有一天資料 | 第二天起正常 |
| `database is locked` | 上次中斷 | 刪 `db/market.db-wal` 和 `db/market.db-shm` |

---

## 免責聲明

資料來自 TWSE/TPEx 公開 API，非即時，存在揭露延遲。本專案為個人研究用途，不構成投資建議。
