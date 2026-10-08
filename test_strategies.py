"""Synthetic end-to-end test of the strategy engine against local Postgres.

Uses symbol TESTNIFTY so real data is untouched; cleans up after itself.
Covers: execution gates (warm-up, persistence, market-hours), S1 + S3 + S4
firing and opening paper trades, risk sizing, cooldown suppression, MTM
update, TARGET exit on a debit spread, WALL REJECTED exit, after-hours
suppression, closed-market static payload, and dashboard rendering with
trades embedded in their owning strategy cards.
"""
import datetime
import math
import sys

import psycopg2
import strategies

SYMBOL = "TESTNIFTY"
DB = "postgresql://postgres:postgres@localhost:5432/postgres"
strategies.configure(dsn=DB, strat_dsn=DB)

SPOT = 24480.0
STRIKES = [24000 + 50 * i for i in range(21)]          # 24000..25000
EXPIRY = "30-Oct-2026"


def gamma(k):
    return 0.004 * math.exp(-((k - SPOT) / 180.0) ** 2)


def snap_args(spot, ltps, vols, ois, chg_oi, ivs, deltas):
    """Build the 25-field tuple list run_cycle expects."""
    args = []
    for k in STRIKES:
        ce_ltp, pe_ltp = ltps[k]
        ce_vol, pe_vol = vols[k]
        ce_oi, pe_oi = ois[k]
        ce_chg, pe_chg = chg_oi[k]
        ce_iv, pe_iv = ivs[k]
        ce_d, pe_d = deltas[k]
        g = gamma(k)
        args.append((SYMBOL, EXPIRY, float(k), spot, "01-Oct-26 14:30:00",
                     ce_ltp, 0.0, ce_oi, ce_chg, ce_vol, ce_iv,
                     ce_d, g, -8.0, 4.0,
                     pe_ltp, 0.0, pe_oi, pe_chg, pe_vol, pe_iv,
                     pe_d, g, -7.0, 4.0))
    return args


def base_snapshot():
    ltps, vols, ois, chg_oi, ivs, deltas = {}, {}, {}, {}, {}, {}
    for k in STRIKES:
        m = (k - SPOT) / 50.0
        # calls fall with moneyness, puts rise — floor at ₹5
        ltps[k] = (max(5.0, 120 - 30 * m), max(5.0, 120 + 30 * m))
        vols[k] = (25000, 22000)
        ois[k] = (40000, 45000)
        chg_oi[k] = (2000, 2000)
        ivs[k] = (12.0, 12.0)
        deltas[k] = (max(0.02, 0.5 - 0.08 * m), min(-0.02, -0.5 - 0.08 * m))
    # S1 setup: 24650 CE massive volume + OI build
    vols[24650] = (350000, 22000)
    ois[24650] = (60000, 45000)
    chg_oi[24650] = (25000, 2000)
    ivs[24650] = (12.5, 12.0)
    # S3 setup: call wall at 24600 with heavy gamma-weighted OI + volume
    ois[24600] = (200000, 45000)
    vols[24600] = (90000, 22000)
    # S4 setup: skewed OTM puts below spot (no OI build -> FADE)
    for k, piv in ((24350, 23.0), (24300, 22.5), (24250, 21.0)):
        ivs[k] = (12.0, piv)
    return ltps, vols, ois, chg_oi, ivs, deltas


def run(name, cond):
    print(("PASS " if cond else "FAIL ") + name)
    run.failed = getattr(run, "failed", False) or not cond


run.failed = False
NOW0 = datetime.datetime(2026, 10, 1, 14, 20)    # warm-up scan 1
NOW0B = datetime.datetime(2026, 10, 1, 14, 22)   # warm-up scan 2
NOW1 = datetime.datetime(2026, 10, 1, 14, 30)
NOW2 = datetime.datetime(2026, 10, 1, 14, 35)
NOW3 = datetime.datetime(2026, 10, 1, 14, 40)
NOWLATE = datetime.datetime(2026, 10, 1, 20, 0)  # after hours — no entries


def cleanup():
    conn = psycopg2.connect(DB)
    conn.autocommit = True
    cur = conn.cursor()
    for t in ("paper_trades", "strategy_signals"):
        cur.execute(f"DELETE FROM {t} WHERE symbol=%s", (SYMBOL,))
    conn.close()


def count(table):
    conn = psycopg2.connect(DB)
    cur = conn.cursor()
    cur.execute(f"SELECT count(*) FROM {table} WHERE symbol=%s", (SYMBOL,))
    v = cur.fetchone()[0]
    conn.close()
    return v


def main():
    strategies.ensure_schema()
    cleanup()

    # -------- warm-up + persistence gates: 2 hot scans, NO entries --------
    ltps, vols, ois, chg_oi, ivs, deltas = base_snapshot()
    args1 = snap_args(SPOT, ltps, vols, ois, chg_oi, ivs, deltas)
    p0 = strategies.run_cycle(SYMBOL, EXPIRY, SPOT, args1, NOW0, vix=None,
                              t_years=29 / 365)
    run("warm-up scan 1: hot data but no signals/trades",
        not p0["new_signals"] and not p0["open_trades"])
    p0b = strategies.run_cycle(SYMBOL, EXPIRY, SPOT, args1, NOW0B, vix=None,
                               t_years=29 / 365)
    run("warm-up scan 2: still no entries (WARMUP_CYCLES gate)",
        not p0b["new_signals"] and not p0b["open_trades"])

    # ------------- cycle 1 (3rd hot scan): signals fire, trades open ------
    p1 = strategies.run_cycle(SYMBOL, EXPIRY, SPOT, args1, NOW1, vix=None,
                              t_years=29 / 365)
    fired = {g["strategy"] for g in p1["new_signals"]}
    run("S1 fires on vol >3x median + OI build >25%", "S1" in fired)
    run("S3 fires near gamma wall (<1%, wall vol building)", "S3" in fired)
    run("S4 fires and picks FADE (skew without OI build)",
        "S4" in fired and any("FADE" in c["status"] for c in p1["cards"]))
    run("3 paper trades opened", len(p1["open_trades"]) == 3)
    run("trades risk-sized with lots >= 1",
        all(t["qty"] >= 1 for t in p1["open_trades"]))
    run("each debit trade risks <= 2% of account",
        all(t["qty"] * t["lot"] * max(t["cost"], 0.0) <= 20001
            for t in p1["open_trades"]))
    run("total open worst-case risk within heat cap",
        p1["summary"]["open_risk"] <= strategies.PAPER_ACCOUNT
        * strategies.MAX_TOTAL_HEAT + 1)
    s1t = next(t for t in p1["open_trades"] if t["strategy"] == "S1")
    run("S1 = bull call spread at the anomaly strike (24,650)",
        "24,650" in s1t["structure"] and "Bull Call" in s1t["structure"])
    s4t = next(t for t in p1["open_trades"] if t["strategy"] == "S4_FADE")
    run("S4 fade opened as a net CREDIT trade (negative net cost)",
        s4t["cost"] < 0)
    html = strategies.render_dashboard(p1)
    run("dashboard renders cards + open trades",
        "Quant Strategy Dashboard" in html and "Open paper trades" in html
        and "Bull Call Spread" in html)
    run("trades embedded in their owning strategy cards",
        "S1 trades:" in html and "S3 trades:" in html and "S4 trades:" in html)
    run("strategy identifiers named, not just codes",
        "S1 — Unusual OI + Volume" in html and "S4 — Put Skew (Fade)" in html)
    run("per-card 'How it works' reference + guard rules render",
        html.count("How it works") == 4 and "Rules in force" in html
        and "magnets" in html)

    # ---------------- cycle 2: MTM, TARGET exit, cooldown ----------------
    ltps[24650] = (130.0, 8.0)     # conviction leg repriced up
    ltps[24750] = (30.0, 6.0)
    args2 = snap_args(SPOT, ltps, vols, ois, chg_oi, ivs, deltas)
    p2 = strategies.run_cycle(SYMBOL, EXPIRY, SPOT, args2, NOW2, vix=None,
                              t_years=29 / 365)
    run("cooldown suppresses re-fire of same S1/S3/S4 keys",
        not p2["new_signals"])
    s1_closed = [t for t in p2["closed_trades"] if t["strategy"] == "S1"]
    run("S1 spread closes on TARGET +100% with positive P&L",
        s1_closed and s1_closed[0]["pnl"] > 0 and "TARGET" in s1_closed[0]["reason"])
    run("S3 trade still open (spot steady)",
        any(t["strategy"] == "S3" for t in p2["open_trades"]))
    run("S4 credit trade tracked with MTM",
        any(t["strategy"] == "S4_FADE" and t["value"] is not None
            for t in p2["open_trades"]))
    run("summary reports realized + unrealized",
        p2["summary"]["realized_today"] > 0 and "unrealized" in p2["summary"])

    # ------------- cycle 3: spot pullback -> WALL REJECTED -------------
    # prices unchanged (leg values near entry) so the thesis-reversal rule,
    # not the stop, must close the S3 trade
    l3, v3, o3, c3, i3, d3 = base_snapshot()
    args3 = snap_args(24300.0, ltps, v3, o3, c3, i3, d3)
    p3 = strategies.run_cycle(SYMBOL, EXPIRY, 24300.0, args3, NOW3, vix=None,
                              t_years=29 / 365)
    s3_closed = [t for t in p3["closed_trades"] if t["strategy"] == "S3"]
    run("S3 closes on WALL REJECTED when spot pulls back 0.5% from entry",
        s3_closed and s3_closed[0]["reason"] == "WALL REJECTED")

    # ------------- after hours: hot data at 20:00 must NOT enter ----------
    # (EOD rule closes any remaining open trades; no new signals recorded)
    pl = strategies.run_cycle(SYMBOL, EXPIRY, SPOT, args1, NOWLATE, vix=None,
                              t_years=29 / 365)
    run("no new signals outside 9:15-15:30 session", not pl["new_signals"])
    run("after-hours cycle leaves no open trades", not pl["open_trades"])

    # ---------------- closed-market payload ----------------
    st = strategies.static_payload(SYMBOL, spot=24300.0)
    h2 = strategies.render_dashboard(st)
    run("static (closed) dashboard renders", "scanner paused" in h2)
    run("closed-market payload shows trade history",
        len(st["closed_trades"]) >= 2)

    n_sig, n_tr = count("strategy_signals"), count("paper_trades")
    run(f"DB rows written ({n_sig} signals, {n_tr} trades)",
        n_sig >= 3 and n_tr >= 3)

    # ---- reset engine per-process state so these tests run on a clean slate ----
    strategies._scan_count.clear()
    strategies._hot_streak.clear()
    strategies._schema_done = False
    strategies.ensure_schema()
    cleanup()
    # loosen warm-up/persistence for these focused exit tests so a single hot
    # cycle opens a trade (we're validating exits, not entry gating here).
    _warmup_orig = strategies.WARMUP_CYCLES
    _persist_orig = strategies.PERSIST_SCANS
    strategies.WARMUP_CYCLES = 0
    strategies.PERSIST_SCANS = 1

    # ---- new: max-hold timer closes a trade that never hit target/stop ----
    # Cycle 1 (open): a HOT S1 snapshot so the trade opens.
    # Cycle 2 (61 min later): flat prices (value==cost, no target/stop) and a
    # COOLED flow (no fresh signal, no soft-book trigger) -> the only remaining
    # exit is MAX HOLD 60m.  Use a pre-15:28 clock so EOD doesn't fire first.
    NOW_A = datetime.datetime(2026, 10, 1, 14, 50)
    NOW_B = NOW_A + datetime.timedelta(minutes=61)   # 15:51 -> EOD would fire,
                                                       # but MAX HOLD is checked first
    ltps_open, vols_open, ois_open, chg_open, ivs_open, dels_open = base_snapshot()
    # keep the S1 hot setup intact for the open cycle (350000 CE vol @ 24650).
    ltps_open[24650] = (100.0, 8.0)     # long CE entry 100
    ltps_open[24750] = (40.0, 6.0)      # short CE entry 40  -> cost 60
    args_open = snap_args(SPOT, ltps_open, vols_open, ois_open, chg_open, ivs_open, dels_open)
    pA = strategies.run_cycle(SYMBOL, EXPIRY, SPOT, args_open, NOW_A, vix=None,
                              t_years=29 / 365)
    run("max-hold setup: S1 trade opened with flat prices (no target/stop possible)",
        any(t["strategy"] == "S1" for t in pA["open_trades"]))
    # hold cycle: flat prices, cooled flow.
    ltps_hold, vols_h, ois_h, chg_h, ivs_h, dels_h = base_snapshot()
    vols_h[24650] = (26000, 22000)      # cooled: below 3x median
    ltps_hold[24650] = (100.0, 8.0)     # same prices -> value == cost == 60
    ltps_hold[24750] = (40.0, 6.0)
    args_hold = snap_args(SPOT, ltps_hold, vols_h, ois_h, chg_h, ivs_h, dels_h)
    pB = strategies.run_cycle(SYMBOL, EXPIRY, SPOT, args_hold, NOW_B, vix=None,
                              t_years=29 / 365)
    s1_held = [t for t in pB["closed_trades"] if t["strategy"] == "S1"]
    run("max-hold 60m closes an open S1 trade that never hit target/stop",
        s1_held and s1_held[0]["reason"] == "MAX HOLD 60m")

    # ---- reset again for the soft-book test ----
    strategies._scan_count.clear()
    strategies._hot_streak.clear()
    strategies._schema_done = False
    strategies.ensure_schema()
    cleanup()

    # ---- new: soft profit-book at +70% debit when the signal has cooled ----
    # Cycle 1 (open): HOT S1 snapshot -> trade opens, cost 60 (long 100 / short 40).
    # Cycle 2 (2 min later): reprice long to 125, short to 18 -> value 107 = +78%
    # of cost, AND cool the flow -> SOFT BOOK +70% fires before +100% hard target.
    NOW_C = datetime.datetime(2026, 10, 1, 14, 55)
    NOW_D = NOW_C + datetime.timedelta(minutes=2)
    ltps_so, vols_so, ois_so, chg_so, ivs_so, dels_so = base_snapshot()
    ltps_so[24650] = (100.0, 8.0)    # entry
    ltps_so[24750] = (40.0, 6.0)
    args_so_open = snap_args(SPOT, ltps_so, vols_so, ois_so, chg_so, ivs_so, dels_so)
    pC = strategies.run_cycle(SYMBOL, EXPIRY, SPOT, args_so_open, NOW_C, vix=None,
                              t_years=29 / 365)
    run("soft-book setup: fresh S1 trade opened for the +70% test",
        any(t["strategy"] == "S1" for t in pC["open_trades"]))
    # soft cycle: repriced up + cooled flow.
    ltps_soft, vols_s2, ois_s2, chg_s2, ivs_s2, dels_s2 = base_snapshot()
    vols_s2[24650] = (26000, 22000)  # cooled: below 3x median
    ltps_soft[24650] = (125.0, 8.0)  # value 125 - 18 = 107 vs cost 60 = +78%
    ltps_soft[24750] = (18.0, 6.0)
    args_so_soft = snap_args(SPOT, ltps_soft, vols_s2, ois_s2, chg_s2, ivs_s2, dels_s2)
    pD = strategies.run_cycle(SYMBOL, EXPIRY, SPOT, args_so_soft, NOW_D, vix=None,
                              t_years=29 / 365)
    s1_soft = [t for t in pD["closed_trades"] if t["strategy"] == "S1"]
    run("S1 soft-books at +70% when flow cools, before the +100% hard target",
        s1_soft and s1_soft[0]["reason"] == "SOFT BOOK +70%")

    cleanup()
    strategies.WARMUP_CYCLES = _warmup_orig
    strategies.PERSIST_SCANS = _persist_orig

    cleanup()
    print("TESTNIFTY rows cleaned up.")
    print("ALL PASS" if not run.failed else "FAILURES PRESENT")
    return 1 if run.failed else 0


if __name__ == "__main__":
    sys.exit(main())
