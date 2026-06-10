# -*- coding: utf-8 -*-
import logging
import utils.db as db
from screeners.base import _fv, kline_stats, not_yet_risen, disp_name, pct

logger = logging.getLogger(__name__)


def _vol_ok(is_disp, vol, prev_vol):
    # Non-disp: vol > 1000 and vol > prev_vol
    # Disp:     vol > prev_vol
    if is_disp:
        return vol > prev_vol
    return vol > 1000 and vol > prev_vol


def _screen(today_df, disp, today_date,
            ma_key, prev_ma_key, vol_ma_key,
            breakout, vol_mult, tag):
    results = []
    for _, row in today_df.iterrows():
        code  = str(row["code"])
        close = _fv(row.get("close"))
        vol   = _fv(row.get("volume"))
        chg   = _fv(row.get("change_pct"))

        if not not_yet_risen(chg):
            continue

        stats      = kline_stats(code, close, today_date=today_date)
        ma_val     = stats.get(ma_key,      0.0)
        prev_ma    = stats.get(prev_ma_key,  0.0)
        vol_ma     = stats.get(vol_ma_key,   0.0)
        prev_close = stats.get("prev_close", 0.0)

        if ma_val <= 0 or vol_ma <= 0 or prev_close <= 0 or prev_ma <= 0:
            continue

        # MA must be rising (today MA > yesterday MA)
        if ma_val <= prev_ma:
            continue

        if breakout:
            # True breakout: today above MA, yesterday below MA
            if close <= ma_val or prev_close >= prev_ma:
                continue
            if vol < vol_ma * vol_mult:
                continue
        else:
            # False break: today below MA, yesterday above MA
            if close >= ma_val or prev_close <= prev_ma:
                continue
            if vol > vol_ma * vol_mult:
                continue

        hist     = db.load_chip(code, before_date=today_date)
        prev_vol = _fv(hist.iloc[-1].get("volume", 0)) if not hist.empty else 0.0
        if not _vol_ok(code in disp, vol, prev_vol):
            continue

        vol_ratio = round(vol / vol_ma, 2) if vol_ma > 0 else 0.0
        results.append([
            code, disp_name(str(row.get("name", "")), code, disp),
            close, vol_ratio,
            pct(chg), pct(stats["pct5"]), pct(stats["pct20"]),
            stats["ma5"], stats["ma20"],
        ])
    logger.info(f"[{tag}] {len(results)}")
    return results


def screen_ma5_breakout(today_df, disp, today_date):
    return _screen(today_df, disp, today_date,
                   "ma5", "prev_ma5", "vol_ma5",
                   True, 1.3, "MA5 breakout")


def screen_ma5_false(today_df, disp, today_date):
    return _screen(today_df, disp, today_date,
                   "ma5", "prev_ma5", "vol_ma5",
                   False, 0.7, "MA5 false break")


def screen_ma20_breakout(today_df, disp, today_date):
    return _screen(today_df, disp, today_date,
                   "ma20", "prev_ma20", "vol_ma20",
                   True, 1.3, "MA20 breakout")


def screen_ma20_false(today_df, disp, today_date):
    return _screen(today_df, disp, today_date,
                   "ma20", "prev_ma20", "vol_ma20",
                   False, 0.7, "MA20 false break")
