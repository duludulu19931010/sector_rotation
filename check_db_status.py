"""
快速診斷 DB 現狀：
1. 各日期的 trade_value/inst_net 是否正常
2. net_yi 分布
3. 今日 TWSE 欄位確認
請在 runner 上執行：python check_db_status.py
"""
import sys, io, sqlite3
from pathlib import Path
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

DB = Path(__file__).resolve().parent / "db" / "market.db"
con = sqlite3.connect(str(DB))
con.row_factory = sqlite3.Row

print("=== 各日期 TWSE 資料狀況 (最近10天) ===")
rows = con.execute("""
    SELECT date,
        COUNT(*) as total,
        SUM(CASE WHEN trade_value=0 THEN 1 ELSE 0 END) as val_zero,
        SUM(CASE WHEN inst_net=0 THEN 1 ELSE 0 END) as inst_zero,
        SUM(CASE WHEN net_yi=0 THEN 1 ELSE 0 END) as netyi_zero,
        ROUND(AVG(ABS(net_yi)),4) as avg_abs_netyi
    FROM daily WHERE market='TWSE'
    GROUP BY date ORDER BY date DESC LIMIT 10
""").fetchall()
for r in rows:
    print(f"{r['date']}: total={r['total']}, "
          f"val=0:{r['val_zero']}, inst=0:{r['inst_zero']}, "
          f"netyi=0:{r['netyi_zero']}, avg|netyi|={r['avg_abs_netyi']}")

print()
print("=== 2330 最近5筆完整資料 ===")
rows2 = con.execute("""
    SELECT date, close_price, trade_volume, trade_value, inst_net, net_yi
    FROM daily WHERE code='2330' ORDER BY date DESC LIMIT 5
""").fetchall()
for r in rows2:
    print(dict(r))

print()
print("=== TPEx 今日 (最新日期) 狀況 ===")
latest = con.execute("SELECT MAX(date) FROM daily").fetchone()[0]
tpex = con.execute("""
    SELECT COUNT(*) as total,
        SUM(CASE WHEN trade_value=0 THEN 1 ELSE 0 END) as val_zero,
        SUM(CASE WHEN inst_net=0 THEN 1 ELSE 0 END) as inst_zero
    FROM daily WHERE date=? AND market='TPEx'
""", (latest,)).fetchone()
print(f"latest={latest}, TPEx total={tpex['total']}, "
      f"trade_value=0:{tpex['val_zero']}, inst_net=0:{tpex['inst_zero']}")

print()
print("=== TPEx 3105 穩懋 最近3筆 ===")
rows3 = con.execute("""
    SELECT date, close_price, trade_volume, trade_value, inst_net, net_yi
    FROM daily WHERE code='3105' ORDER BY date DESC LIMIT 3
""").fetchall()
for r in rows3:
    print(dict(r))

con.close()
