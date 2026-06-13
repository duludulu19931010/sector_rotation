import sqlite3
import sys
import io
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

DB = Path(__file__).resolve().parent / "db" / "market.db"


def main():
    if len(sys.argv) < 2:
        print("usage:")
        print("  python check_record.py CODE")
        print("  python check_record.py CODE DATE")
        print("  python check_record.py --date DATE")
        print("  python check_record.py --dup DATE   (find duplicate close_price/inst_net)")
        return

    con = sqlite3.connect(str(DB))
    con.row_factory = sqlite3.Row

    if sys.argv[1] == "--date":
        date = sys.argv[2]
        rows = con.execute(
            "SELECT market, COUNT(*) as cnt, "
            "SUM(CASE WHEN inst_net!=0 THEN 1 ELSE 0 END) as inst_nonzero, "
            "SUM(CASE WHEN trade_value=0 THEN 1 ELSE 0 END) as value_zero "
            "FROM daily WHERE date=? GROUP BY market", (date,)
        ).fetchall()
        print(f"date {date}:")
        for r in rows:
            print(f"  {r['market']}: total={r['cnt']}, inst_net!=0={r['inst_nonzero']}, trade_value=0 count={r['value_zero']}")
        return

    if sys.argv[1] == "--dup":
        date = sys.argv[2]
        rows = con.execute(
            "SELECT close_price, trade_volume, COUNT(*) as cnt "
            "FROM daily WHERE date=? AND market='TWSE' "
            "GROUP BY close_price, trade_volume HAVING cnt > 1 "
            "ORDER BY cnt DESC LIMIT 20", (date,)
        ).fetchall()
        print(f"duplicate (close_price, trade_volume) on {date}:")
        for r in rows:
            print(f"  close={r['close_price']}, volume={r['trade_volume']}, count={r['cnt']}")
        return

    code = sys.argv[1].zfill(4)

    if len(sys.argv) >= 3:
        date = sys.argv[2]
        row = con.execute(
            "SELECT * FROM daily WHERE code=? AND date=?", (code, date)
        ).fetchone()
        if not row:
            print(f"no record for {code} on {date}")
            return
        print_record(row)
    else:
        rows = con.execute(
            "SELECT * FROM daily WHERE code=? ORDER BY date", (code,)
        ).fetchall()
        if not rows:
            print(f"no record for {code}")
            return
        for r in rows:
            print_record(r)
            print("-" * 40)


def print_record(r):
    avg_price = r["trade_value"] / r["trade_volume"] if r["trade_volume"] else 0
    print(f"date         : {r['date']}")
    print(f"code         : {r['code']} ({r['name']})")
    print(f"market       : {r['market']}")
    print(f"close_price  : {r['close_price']}")
    print(f"trade_volume : {r['trade_volume']:,}")
    print(f"trade_value  : {r['trade_value']:,}")
    print(f"avg_price    : {avg_price:.2f}")
    print(f"inst_net     : {r['inst_net']:,}")
    print(f"net_yi       : {r['net_yi']}")


if __name__ == "__main__":
    main()
