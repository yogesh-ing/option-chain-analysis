"""Synthetic end-to-end test of the strategy engine against local Postgres.

Uses symbol TESTNIFTY so real data is untouched; cleans up after itself.
Covers: S1 + S3 + S4 firing and opening paper trades, cooldown suppression,
MTM update, TARGET exit on a debit spread, WALL REJECTED exit, closed-market
static payload, and dashboard rendering.
"""
import datetime
import math
import sys

import psycopg2
import strategies

SYMBOL = "TESTNIFTY"
DB = "postgresql://postgres:postgres@localhost:5432/postgres"
strategies.configure(dsn=DB)

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
NOW1 = datetime.datetime(2026, 10, 1, 14, 30)
NOW2 = datetime.datetime(2026, 10, 1, 14, 35)
NOW3 = datetime.datetime(2026, 10, 1, 14, 40)


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

    # ---------------- cycle 1: signals fire, trades open ----------------
    ltps, vols, ois, chg_oi, ivs, deltas = base_snapshot()
    args1 = snap_args(SPOT, ltps, vols, ois, chg_oi, ivs, deltas)
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

    # ---------------- closed-market payload ----------------
    st = strategies.static_payload(SYMBOL, spot=24300.0)
    h2 = strategies.render_dashboard(st)
    run("static (closed) dashboard renders", "scanner paused" in h2)
    run("closed-market payload shows trade history",
        len(st["closed_trades"]) >= 2)

    n_sig, n_tr = count("strategy_signals"), count("paper_trades")
    run(f"DB rows written ({n_sig} signals, {n_tr} trades)",
        n_sig >= 3 and n_tr >= 3)

    cleanup()
    print("TESTNIFTY rows cleaned up.")
    print("ALL PASS" if not run.failed else "FAILURES PRESENT")
    return 1 if run.failed else 0


if __name__ == "__main__":
    sys.exit(main())
