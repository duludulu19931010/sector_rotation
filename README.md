# TW$FLOW · 族群資金儀表板

TWSE/TPEx 官方 API 每日更新 → SQLite 累積 → GitHub Pages 呈現

---

## 資料來源（全部官方免費 API）

| 資料 | API |
|------|-----|
| 三大法人買賣超 | `https://www.twse.com.tw/rwd/zh/fund/T86` |
| 上市收盤價 + 股票名稱 | `https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL` |
| 上櫃收盤價 + 股票名稱 | `https://www.tpex.org.tw/openapi/v1/tpex/exchangeReport/daily_close_quotes` |

---

## 指標定義

| 指標 | 計算方式 | 用途 |
|------|----------|------|
| 資金出入量（億） | Σ 個股**五日**三大法人合計（張）× 收盤價 × 1000 ÷ 1e8 | X 軸 |
| 資金加速度（億） | Σ 個股**今日**三大法人合計（張）× 收盤價 × 1000 ÷ 1e8 | Y 軸 |
| 近20日累計（億） | 同上，視窗 = 20 個交易日 | 側欄詳情 |

**標籤：** 主力（五日流入>2億且漲幅>1%）、輪動（五日流入>0）、退潮（五日流出>2億）、觀望（其他）

**篩選一（流入低漲幅）：** 五日淨買超>0 且 五日漲幅<10%

**篩選二（偷偷布局）：** 今日淨買超>0 且 五日淨買超>0 且 五日漲幅<0%

---

## 檔案結構

```
├── pipeline.py          主程式（唯一入口）
├── requirements.txt
├── input/
│   ├── Group.csv        族群個股清單（Big5，58族群）
│   └── stock_list.csv   股票名稱對照（CP950，1937筆）
├── db/market.db         SQLite（每日累積，永不刪除歷史）
├── docs/
│   ├── index.html       重定向
│   ├── sector.html      主頁面
│   └── assets/data/     每日 JSON
└── .github/workflows/daily.yml
```

---

## 安裝與執行

### 前置需求

- Python 3.11+（加入 PATH）
- Windows 自架 GitHub Actions Runner

### 首次執行（收盤後 15:30 以後）

```powershell
# 在 repo 根目錄執行
pip install -r requirements.txt

# 清除任何舊資料
Remove-Item db\market.db -ErrorAction SilentlyContinue
Remove-Item docs\assets\data\*.json -ErrorAction SilentlyContinue

# 執行 pipeline（首次約 3-5 分鐘，下載 20 個交易日資料）
python pipeline.py

# push 到 GitHub
git add -A
git commit -m "data: first real data"
git push
```

### 每日自動執行

GitHub Actions 排程：每週一到五 **台灣時間 18:30** 自動執行，完成後 GitHub Pages 自動更新。

### 手動選項

```powershell
python pipeline.py              # 正常執行（有快取則跳過 API）
python pipeline.py --force      # 強制重新抓取所有 API
python pipeline.py --dry-run    # 只用 DB 資料重算 JSON，不呼叫 API
```

---

## GitHub Pages 設定

1. repo → Settings → Pages
2. Source：Deploy from a branch，Branch：`main`，資料夾：`/docs`
3. Save

網址：`https://duludulu19931010.github.io/Graphic_ETF_Sector_Rotation/`

> 私人 repo 需要 GitHub Pro。免費帳號請將 repo 改為 Public（Settings → Danger Zone）。

---

## Runner 設定

1. **確認 Python**：`python --version`（需 3.11+）
2. **下載 Runner**：repo → Settings → Actions → Runners → New self-hosted runner → Windows x64
3. **安裝成服務**：
   ```powershell
   .\svc.cmd install
   .\svc.cmd start
   ```
4. **授權 push**：Settings → Actions → General → Workflow permissions → **Read and write** → Save

---

## 障礙排除

| 錯誤 | 原因 | 解法 |
|------|------|------|
| `pwsh: command not found` | Workflow 用了 PowerShell 7 | 確認 `daily.yml` 有 `shell: powershell` |
| `push 403` | GITHUB_TOKEN 無 push 權限 | Settings → Actions → General → Read and write |
| TPEx SSL 錯誤 | TPEx 憑證問題 | 已內建 `verify=False` 處理 |
| T86 回傳 403 | 交易時段 IP 限制 | 等收盤後（15:30）再執行 |
| 漲跌幅異常 | DB 混入模擬資料 | `Remove-Item db\market.db`，重新執行 |
| `database is locked` | pipeline 中斷 | 刪除 `db\market.db-wal` 和 `db\market.db-shm` |

---

## 免責聲明

資料來自 TWSE/TPEx 公開 API，非即時，存在揭露延遲。本專案為個人研究用途，不構成投資建議。
