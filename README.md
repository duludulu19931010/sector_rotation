# TW$FLOW · 台股族群資金儀表板

每日自動從 TWSE / TPEx 官方 API 抓取資料 → SQLite 累積（存 GitHub）→ GitHub Pages 呈現

---

## 資料來源

| 市場 | 資料 | API |
|------|------|-----|
| TWSE 上市 | 三大法人買賣超 | `https://www.twse.com.tw/rwd/zh/fund/T86` |
| TPEx 上櫃 | 三大法人買賣超 | `https://www.tpex.org.tw/openapi/v1/tpex_mainboard_institutional_investors` |
| TWSE 上市 | 收盤價 + 股票名稱 | `https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL` |
| TPEx 上櫃 | 收盤價 + 股票名稱 | `https://www.tpex.org.tw/openapi/v1/tpex_mainboard_daily_close_quotes` |

上市和上櫃都會抓，缺一不可。TPEx 法人 API 若回傳空（白天不穩定），排程設在收盤後 18:30，屆時正常。

---

## 指標定義

| 指標 | 計算方式 | 用途 |
|------|----------|------|
| **資金出入量（億）** | Σ 個股**近5日**三大法人合計（張）× 收盤價 × 1000 ÷ 1e8 | 泡泡圖 X 軸 |
| **資金加速度（億/天）** | Σ 個股**今日**三大法人合計（張）× 收盤價 × 1000 ÷ 1e8 | 泡泡圖 Y 軸 |
| 近20日累計（億） | 同上，視窗 = 20 個交易日 | 側欄詳情 |
| 今日漲跌（%） | (今收 − 昨收) ÷ 昨收 × 100 | 表格、側欄 |
| 五日漲跌（%） | (今收 − 5日前收) ÷ 5日前收 × 100 | 表格、側欄 |

**單位說明：**
- T86 欄位 14（三大法人合計）= **張數**（1張=1000股）
- 收盤價 API 的 TradeVolume = **股數**（僅作成交量記錄，不用於換算）

**篩選邏輯：**
- 流入低漲幅：`近5日淨買超 > 0 且 近5日漲幅 < 10%`
- 偷偷布局：`今日淨買超 > 0 且 近5日淨買超 > 0 且 近5日漲幅 < 0%`

---

## 速度優化

1. **TWSE + TPEx 並行抓取**（`ThreadPoolExecutor`）
   - 收盤價：TWSE + TPEx 同時發請求，節省 ~50% 時間
   - 三大法人：每個交易日的 TWSE + TPEx 同時發請求，外層多天並行（max_workers=4）
2. **DB 快取**：今日資料已在 DB → 完全跳過 API（重複執行只需幾秒）
3. **只存新資料**：已在 DB 的日期不重複寫入
4. **批次 INSERT**：`executemany` + 單次 commit

| 場景 | 預估時間 |
|------|---------|
| 首次執行（20天資料）| ~60s |
| 每日正常執行（API快取未命中）| ~20s |
| 同日第二次執行（快取命中）| < 5s |

---

## 資料庫（`db/market.db`）

存在 GitHub repo，每次 Actions push 更新：

| 表格 | 內容 |
|------|------|
| `inst` | 每日個股三大法人買賣超（上市+上櫃，張數） |
| `price` | 每日個股收盤價（上市+上櫃，OHLCV） |

- 主鍵 `(date, code)` 防重複
- WAL mode，支援讀寫並發
- 日期格式統一為 `YYYY-MM-DD`

---

## 檔案結構

```
├── pipeline.py          主程式（唯一入口）
├── requirements.txt
├── input/
│   ├── Group.csv        族群清單（Big5，58族群 809個股）
│   └── stock_list.csv   股票名稱對照（CP950，1937筆）
├── db/
│   └── market.db        SQLite（每日更新，存 GitHub）
├── docs/
│   ├── index.html       重定向到 sector.html
│   ├── sector.html      主頁面
│   └── assets/data/     每日 JSON（由 pipeline 生成）
└── .github/workflows/
    └── daily.yml        排程 + Pages 部署
```

---

## 安裝與首次執行

### 前置需求

- Python 3.11+（加入 PATH）
- Windows 自架 GitHub Actions Runner

### 首次執行（收盤後 15:30 以後）

```powershell
cd <repo根目錄>
pip install -r requirements.txt

# 清除舊資料（若有模擬資料）
Remove-Item db\market.db -ErrorAction SilentlyContinue
Remove-Item docs\assets\data\*.json -ErrorAction SilentlyContinue

# 執行（首次約 60 秒，下載 20 個交易日資料）
python pipeline.py

# push 到 GitHub
git add -A
git commit -m "data: init real data"
git push
```

### 執行選項

```powershell
python pipeline.py            # 正常執行（快取命中則跳 API）
python pipeline.py --force    # 強制重新抓取所有 API
python pipeline.py --dry-run  # 只從 DB 重算 JSON，不呼叫 API
```

---

## GitHub Pages 設定

1. repo → **Settings → Pages → Source → GitHub Actions**（必須選這個）
2. Settings → Actions → General → Workflow permissions → **Read and write** → Save
3. push 後 Pages 自動部署

**注意：Source 必須是「GitHub Actions」，不是「Deploy from a branch」，否則部署會 401 失敗。**

---

## Runner 設定

1. `python --version` 確認 3.11+（加入 PATH）
2. repo → Settings → Actions → Runners → New self-hosted runner → Windows x64
3. 依頁面指令下載並設定
4. 安裝成服務：`.\svc.cmd install && .\svc.cmd start`

---

## 障礙排除

| 症狀 | 原因 | 解法 |
|------|------|------|
| `pwsh: command not found` | Workflow 用了 PowerShell 7 | `daily.yml` 確認有 `shell: powershell` |
| push 403 | Token 無 push 權限 | Settings → Actions → General → Read and write |
| Pages 部署 401 | Pages Source 設定錯誤 | Settings → Pages → Source → **GitHub Actions** |
| TPEx 全部失敗 | 白天 API 不穩定 | 等 18:30 排程；或 `--dry-run` 用昨日資料 |
| T86 回傳 403 | 白天 IP 限制 | 等收盤後執行 |
| 漲跌幅全是 0 | 首次執行無歷史收盤 | 正常；第二天起有昨日資料即可計算 |
| `database is locked` | Pipeline 中斷殘留 | 刪除 `db\market.db-wal` 和 `db\market.db-shm` |
| 上櫃股資料為 0 | TPEx 法人 API 回傳空 | 收盤後重跑；收盤前用 `--dry-run` |

---

## 免責聲明

資料來自 TWSE/TPEx 公開 API，非即時，存在揭露延遲。本專案為個人研究用途，不構成投資建議。
