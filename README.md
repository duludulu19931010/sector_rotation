# TW$FLOW · 族群資金儀表板

TWSE / TPEx 官方 API 每日更新 → SQLite 累積 → GitHub Pages

---

## 資料來源（全部官方免費 API）

| 市場 | 資料 | API 端點 |
|------|------|---------|
| **TWSE 上市** | 全市場收盤行情 | `https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL` |
| **TWSE 上市** | 三大法人買賣超 | `https://www.twse.com.tw/rwd/zh/fund/T86` |
| **TPEx 上櫃** | 收盤行情 | `https://www.tpex.org.tw/openapi/v1/tpex_mainboard_quotes` |
| **TPEx 上櫃** | 三大法人買賣超 | `https://www.tpex.org.tw/openapi/v1/tpex_mainboard_institutional_investors` |

TWSE T86 欄位索引：`[0]`代號 `[1]`名稱 `[4]`外資淨 `[7]`投信淨 `[10]`自營(自行)淨 `[13]`自營(避險)淨 **`[14]`三大法人合計淨買超（張）**

---

## 指標定義

| 指標 | 公式 |
|------|------|
| **今日淨買賣超（億）** | `(成交股數 ÷ 成交金額) × 三大法人總買賣超張數 × 1000` |
| **五日淨買賣超（億）** | 最新五筆今日淨買賣超之和 |
| **二十日淨買賣超（億）** | 最新二十筆今日淨買賣超之和 |
| **今日漲跌幅（%）** | `(今收 - 前一交易日收) ÷ 前一交易日收 × 100` |
| **五日漲跌幅（%）** | `(今收 - 前六交易日收) ÷ 前六交易日收 × 100` |

### 泡泡圖軸定義

| 軸 | 指標 | 意義 |
|----|------|------|
| **X 軸** | 五日淨買賣超（億） | 資金出入量（累積方向） |
| **Y 軸** | 今日淨買賣超（億） | 資金加速度（當日動能） |

---

## 加速設計

| 優化項目 | 做法 | 效果 |
|----------|------|------|
| 並行 API | `ThreadPoolExecutor(4)` 同時抓 TWSE 行情、TWSE T86、TPEx 行情、TPEx 三大法人 | 原 ~40s → ~12s |
| DB 快取 | 今日資料已存 → 跳過所有 API | 重複執行 ~0.1s |
| 自適應閾值 | `max(50, max_cnt × 50%)` 判斷完整性 | 不寫死筆數上限 |
| pandas pivot | 計算漲跌幅和累積 net_yi 一次完成 | 避免逐行迴圈 |
| 單表設計 | 行情 + 三大法人合一張 `daily` 表 | 無 JOIN 查詢 |

---

## 資料庫設計

`db/market.db`，單表 `daily`：

| 欄位 | 類型 | 說明 |
|------|------|------|
| `date` | TEXT | YYYY-MM-DD |
| `code` | TEXT | 4位代號 |
| `market` | TEXT | TWSE / TPEx |
| `name` | TEXT | 股票名稱 |
| `open_price` | REAL | 開盤價 |
| `close_price` | REAL | 收盤價 |
| `trade_volume` | INT | 成交股數 |
| `trade_value` | INT | 成交金額（元） |
| `inst_net` | INT | 三大法人淨買超（張） |
| `net_yi` | REAL | 今日淨買賣超（億） |

---

## 檔案結構

```
├── pipeline.py          主程式（單一入口）
├── requirements.txt
├── input/
│   ├── Group.csv        族群個股清單（Big5，58族群）
│   └── stock_list.csv   股票名稱對照（CP950，1937筆）
├── db/market.db         SQLite（每日累積）
├── docs/
│   ├── index.html
│   ├── sector.html      主頁面
│   └── assets/data/     每日 JSON
└── .github/workflows/daily.yml
```

---

## 安裝與執行

### 前置需求
- Python 3.11+，加入 PATH
- Windows 自架 GitHub Actions Runner

### 首次執行（收盤後 15:30 以後）

```powershell
pip install -r requirements.txt

# 清除舊資料（如果有）
Remove-Item db\market.db -ErrorAction SilentlyContinue
Remove-Item docs\assets\data\*.json -ErrorAction SilentlyContinue

# 執行（首次約 3 分鐘，抓 21 個交易日歷史）
python pipeline.py

git add -A
git commit -m "data: init"
git push
```

### 每日自動執行

GitHub Actions 排程：週一到五 **台灣時間 18:30**

### 手動選項

```powershell
python pipeline.py              # 正常（快取則跳 API）
python pipeline.py --force      # 強制重抓
python pipeline.py --dry-run    # 只用 DB 重算 JSON
```

---

## GitHub Pages 設定

1. repo → Settings → Pages
2. **Source 必須選 `GitHub Actions`（不是 Deploy from a branch）**
3. Save

---

## Runner 設定

```powershell
# 1. 下載並設定（repo → Settings → Actions → Runners → New self-hosted runner）
.\config.cmd --url https://github.com/... --token ...

# 2. 安裝成服務
.\svc.cmd install
.\svc.cmd start
```

repo → Settings → Actions → General → Workflow permissions → **Read and write** → Save

---

## 障礙排除

| 錯誤 | 原因 | 解法 |
|------|------|------|
| `Pages 401 Requires authentication` | Source 未設為 GitHub Actions | Settings → Pages → Source → GitHub Actions |
| `pwsh: command not found` | 用了 PowerShell 7 | `daily.yml` 加 `shell: powershell` |
| `push 403` | Token 無 push 權限 | Settings → Actions → General → Read and write |
| TPEx API 空 body | 交易時段速率限制 | 等 15:30 後再執行 |
| T86 回傳 403 | 海外 IP 或交易時段限制 | Runner 需在台灣；排程設 18:30 |
| 漲跌幅全為 0 | DB 只有一天資料 | 第二天起正常；首次補歷史自動解決 |
| `database is locked` | 上次中斷 | 刪 `db\market.db-wal` 和 `db\market.db-shm` |

---

## 免責聲明

資料來自 TWSE/TPEx 公開 API，非即時，存在揭露延遲。本專案為個人研究用途，不構成投資建議。
