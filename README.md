# TW$FLOW · 族群資金儀表板

FinMind API 每日更新 → SQLite 累積 → GitHub Pages

---

## 資料來源

| Dataset | 內容 | 端點 |
|---------|------|------|
| `TaiwanStockPrice` | 股價日成交（開高低收、成交股數、成交金額） | `https://api.finmindtrade.com/api/v4/data` |
| `TaiwanStockInstitutionalInvestorsBuySell` | 個股三大法人買賣（外資/投信/自營商） | 同上 |

涵蓋上市（TWSE）與上櫃（TPEx），FinMind 已整合於同一 API，無需分別呼叫。

---

## FinMind Token 設定

1. 前往 [finmindtrade.com](https://finmindtrade.com) 免費註冊
2. 取得 API Token
3. 在 GitHub repo → Settings → Secrets → Actions，新增 Secret：
   - Name: `FINMIND_TOKEN`
   - Value: 你的 token

未設定 token 仍可執行，但受速率限制（每日 30 次請求）。

---

## 指標定義

| 指標 | 公式 |
|------|------|
| 今日淨買賣超（億） | `(成交股數 ÷ 成交金額) × 三大法人總買賣超張數 × 1000` |
| 五日淨買賣超（億） | 最新五筆今日淨買賣超之和 |
| 二十日淨買賣超（億） | 最新二十筆今日淨買賣超之和 |
| 今日漲跌幅（%） | `(今收 - 前一交易日收) ÷ 前一交易日收 × 100` |
| 五日漲跌幅（%） | `(今收 - 前六交易日收) ÷ 前六交易日收 × 100` |

X 軸：五日淨買賣超（資金出入量）  
Y 軸：今日淨買賣超（資金加速度）

三大法人淨買超（張）= `買進 - 賣出`，外資 + 投信 + 自營商合計。

---

## 檔案結構

```
├── pipeline.py
├── requirements.txt
├── input/
│   ├── Group.csv        族群個股清單（Big5，58 族群）
│   └── stock_list.csv   股票名稱對照（CP950，1937 筆）
├── db/market.db         SQLite
├── docs/
│   ├── index.html
│   ├── sector.html
│   └── assets/data/
└── .github/workflows/daily.yml
```

---

## 安裝與執行

```powershell
pip install -r requirements.txt

$env:FINMIND_TOKEN = "your_token_here"

Remove-Item db\market.db -ErrorAction SilentlyContinue
Remove-Item docs\assets\data\*.json -ErrorAction SilentlyContinue

python pipeline.py

git add -A
git commit -m "data: init"
git push
```

首次執行約 2～5 分鐘（FinMind 一次抓 21 天完整資料）。

---

## 每日自動執行

GitHub Actions 排程：週一到五台灣時間 18:30，workflow 讀取 `FINMIND_TOKEN` secret 自動執行。

```powershell
python pipeline.py              # 正常（快取則跳過 API）
python pipeline.py --force      # 強制重抓
python pipeline.py --dry-run    # 只用 DB 重算 JSON
```

---

## GitHub Pages 設定

1. repo → Settings → Pages
2. **Source 選 `GitHub Actions`**（不是 Deploy from a branch）
3. Save

---

## Runner 設定

```powershell
.\config.cmd --url https://github.com/... --token ...
.\svc.cmd install
.\svc.cmd start
```

repo → Settings → Actions → General → Workflow permissions → **Read and write** → Save

---

## 障礙排除

| 錯誤 | 原因 | 解法 |
|------|------|------|
| `Pages 401` | Source 未設為 GitHub Actions | Settings → Pages → Source → GitHub Actions |
| `pwsh not found` | 用了 PowerShell 7 | `daily.yml` 加 `shell: powershell` |
| `push 403` | Token 無 push 權限 | Settings → Actions → General → Read and write |
| FinMind `over limit` | 超過免費額度 | 設定 `FINMIND_TOKEN` secret；或等隔日重置 |
| 漲跌幅全為 0 | DB 只有一天資料 | 第二天起正常；首次執行會自動補歷史 |
| `database is locked` | 上次中斷 | 刪 `db\market.db-wal` 和 `db\market.db-shm` |

---

## 免責聲明

資料來自 FinMind（源自 TWSE/TPEx 公開資料），非即時，存在揭露延遲。本專案為個人研究用途，不構成投資建議。
