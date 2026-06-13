import sqlite3
import sys
from pathlib import Path

DB = Path(__file__).resolve().parent / "db" / "market.db"


def main():
    if len(sys.argv) < 2:
        print("用法:")
        print("  python check_record.py 2330              查某股票所有日期")
        print("  python check_record.py 2330 2026-06-12   查某股票某日")
        print("  python check_record.py --date 2026-06-12 查某日全部資料筆數統計")
        return

    con = sqlite3.connect(str(DB))
    con.row_factory = sqlite3.Row

    if sys.argv[1] == "--date":
        date = sys.argv[2]
        rows = con.execute(
            "SELECT market, COUNT(*) as cnt, "
            "SUM(CASE WHEN inst_net!=0 THEN 1 ELSE 0 END) as inst_nonzero "
            "FROM daily WHERE date=? GROUP BY market", (date,)
        ).fetchall()
        print(f"日期 {date}:")
        for r in rows:
            print(f"  {r['market']}: {r['cnt']} 筆, inst_net!=0 有 {r['inst_nonzero']} 筆")
        return

    code = sys.argv[1].zfill(4)

    if len(sys.argv) >= 3:
        date = sys.argv[2]
        row = con.execute(
            "SELECT * FROM daily WHERE code=? AND date=?", (code, date)
        ).fetchone()
        if not row:
            print(f"找不到 {code} 在 {date} 的資料")
            return
        print_record(row)
    else:
        rows = con.execute(
            "SELECT * FROM daily WHERE code=? ORDER BY date", (code,)
        ).fetchall()
        if not rows:
            print(f"找不到 {code} 的任何資料")
            return
        for r in rows:
            print_record(r)
            print("-" * 40)


def print_record(r):
    avg_price = r["trade_value"] / r["trade_volume"] if r["trade_volume"] else 0
    print(f"日期       : {r['date']}")
    print(f"代號       : {r['code']}  ({r['name']})")
    print(f"市場       : {r['market']}")
    print(f"收盤價     : {r['close_price']}")
    print(f"成交股數   : {r['trade_volume']:,}")
    print(f"成交金額   : {r['trade_value']:,}")
    print(f"均價(金額/股數): {avg_price:.2f}")
    print(f"三大法人淨買超(張): {r['inst_net']:,}")
    print(f"今日淨買賣超(億)  : {r['net_yi']}")


if __name__ == "__main__":
    main()
