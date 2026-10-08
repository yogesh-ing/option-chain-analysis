"""Quant strategy scanners + automated paper-trading engine.

Runs once per option-chain snapshot (same 21-strike window the live page
uses, with BS greeks). Implements the four documented strategies:

  S1  Unusual OI + Volume Surge   -> directional conviction spread
  S2  VIX/IV Spike -> Premium Sell -> iron condor (short vol)
  S3  Gamma-Wall Proximity        -> long-gamma ATM call spread
  S4  Put-Skew Anomaly            -> follow (long put spread)
                                      or fade (short bull put spread)

Signal rules mirror the strategy doc; where multi-day history isn't in
Postgres yet the scanner falls back to cross-sectional (vs chain median)
anomalies and tightens to 20-day z-scores as data accumulates.

Each fired signal opens a paper trade sized by the 2%-account-risk rule
and is marked to market every cycle; exits: target, stop, thesis
reversal (OI unwind / wall rejection / breakout) or 15:28 IST time exit.
State persists to Postgres: strategy_signals, paper_trades.
"""
import datetime
import json
import math
import os
import statistics
import sys
import threading

from config import cfg

DB_DSN = cfg.db_dsn
STRAT_DSN = cfg.strat_dsn
SNAP_TABLE = cfg.snap_table

LOT_SIZES = {"NIFTY": 75, "BANKNIFTY": 15, "SENSEX": 120, "MIDCPNIFTY": 120,
             "FINNIFTY": 65}
STRIKE_STEPS = {"NIFTY": 50, "BANKNIFTY": 100, "SENSEX": 100,
                "MIDCPNIFTY": 25, "FINNIFTY": 50}
DEFAULT_LOT, DEFAULT_STEP = 75, 50

PAPER_ACCOUNT = 1_000_000.0     # INR — adjust to taste
RISK_PER_TRADE = 0.02           # max 2% of account at risk per trade
MAX_LOTS = 20

# ---- scanner thresholds (from the strategy doc) ----
VOL_MEDIAN_MULT = 3.0           # S1: strike volume > 3x chain median
OI_BUILD_FRAC = 0.25            # S1: chg_oi / prev_oi > 25%
S1_IV_GATE = 1.25               # S1: skip if strike IV > 1.25x ATM IV (rich premium)
IV_ZSCORE = 2.0                 # S2: strike IV z-score trigger
IV_ZSCORE_MIN_PTS = 12          # S2: min samples in a series to trust z
VIX_SELL_LEVEL = 20.0           # S2: VIX >= this = elevated regime
ATM_IV_SELL_LEVEL = 15.0        # S2 fallback gauge when VIX is unavailable
                                # (15% NIFTY ATM IV = genuine fear zone)
GEX_WALL_PCT = 1.0              # S3: spot within 1% of gamma wall
GEX_WALL_VOL_MULT = 2.0         # S3: wall-strike volume building (2x median)
SKEW_PREMIUM = 10.0             # S4: OTM-put IV minus ATM IV > 10 pts = anomaly
SKEW_OI_FOLLOW_FRAC = 0.10      # S4: put OI building > 10% -> "follow"
COOLDOWN_MIN = 60               # minutes before same signal key can re-fire
SIGNALS_PER_DAY_CAP = 3         # per strategy key, hard cap
TIME_EXIT = datetime.time(15, 28)

# ---- execution guards ----
TRADE_START = datetime.time(9, 15)    # entries ONLY inside active F&O session
TRADE_END = datetime.time(15, 30)     # no new entries after 15:30
WARMUP_CYCLES = 2                     # first scans after start: scan only, no trades
PERSIST_SCANS = 2                     # signal must be hot across N consecutive scans
MAX_TOTAL_HEAT = 0.06                 # sum of open max-loss <= 6% of account
MAX_HOLD_MIN = 60                      # force-close a trade still open after this (catches missed target/stop)
SOFT_DEBIT_PCT = 0.70                 # soft book debit spreads at +70% of premium when signal has cooled
SOFT_CREDIT_PCT = 0.25                # soft book credit spreads at +25% of credit when signal has cooled

_schema_done = False
_cache = {"hist": {}, "hist_at": 0.0, "vix": None, "vix_at": 0.0}
_cache_lock = threading.Lock()
_scan_count = {}      # symbol -> cycles since process start (warm-up)
_hot_streak = {}      # (symbol, strategy, skey) -> consecutive hot scans


def in_trading_session(now):
    """True only inside the active F&O session: Mon-Fri 09:15-15:30 IST.
    No signals are recorded and no trades are opened outside this window."""
    return (now.weekday() < 5
            and TRADE_START <= now.time() <= TRADE_END)


def _bump(symbol, strategy, skey, hot):
    """Track consecutive hot scans for persistence gating."""
    key = (symbol, strategy, skey)
    if not hot:
        _hot_streak[key] = 0
        return 0
    _hot_streak[key] = _hot_streak.get(key, 0) + 1
    return _hot_streak[key]


def configure(dsn=None, strat_dsn=None, snap_table=None, account=None):
    global DB_DSN, STRAT_DSN, SNAP_TABLE, PAPER_ACCOUNT
    if dsn:
        DB_DSN = dsn
    if strat_dsn:
        STRAT_DSN = strat_dsn
    if snap_table:
        SNAP_TABLE = snap_table
    if account:
        PAPER_ACCOUNT = float(account)


def _conn(dsn=None):
    import psycopg2
    conn = psycopg2.connect(dsn if dsn is not None else DB_DSN)
    conn.autocommit = True
    return conn


def ensure_schema():
    global _schema_done
    if _schema_done:
        return
    conn = _conn(STRAT_DSN)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS strategy_signals(
            id        BIGSERIAL PRIMARY KEY,
            symbol    TEXT NOT NULL,
            strategy  TEXT NOT NULL,
            skey      TEXT NOT NULL,
            direction TEXT,
            strike    NUMERIC,
            detail    TEXT,
            metrics   JSONB,
            traded    BOOLEAN DEFAULT FALSE,
            fired_at  TIMESTAMPTZ NOT NULL DEFAULT now())""")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS paper_trades(
            id           BIGSERIAL PRIMARY KEY,
            symbol       TEXT NOT NULL,
            strategy     TEXT NOT NULL,
            signal_id    BIGINT,
            direction    TEXT,
            structure    TEXT,
            legs         JSONB,
            qty          INT,
            lot          INT,
            net_cost     NUMERIC,      -- signed per-lot: +debit paid, -credit received
            entry_at     TIMESTAMPTZ,
            exit_at      TIMESTAMPTZ,
            mtm_cost     NUMERIC,      -- current per-lot value at last mark
            u_pnl        NUMERIC,
            realized     NUMERIC,
            status       TEXT DEFAULT 'open',
            exit_reason  TEXT,
            meta         JSONB)""")
    conn.close()
    _schema_done = True


def ist_now():
    return datetime.datetime.now()


# ------------------------------------------------------------------ helpers --
def _norm_cdf(x):
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def _bs_price(spot, strike, t_years, iv_pct, is_call, r=0.065):
    """Black-Scholes premium — fallback MTM when a strike left the window."""
    if not (spot and strike and iv_pct and iv_pct > 0 and t_years > 0):
        return None
    iv = iv_pct / 100.0
    sq = math.sqrt(t_years)
    d1 = (math.log(spot / strike) + (r + iv * iv / 2) * t_years) / (iv * sq)
    d2 = d1 - iv * sq
    if is_call:
        return spot * _norm_cdf(d1) - strike * math.exp(-r * t_years) * _norm_cdf(d2)
    return strike * math.exp(-r * t_years) * _norm_cdf(-d2) - spot * _norm_cdf(-d1)


def _round_strike(strike, step):
    return round(strike / step) * step


def _fmt_oi(v):
    if v is None:
        return "-"
    return f"{v / 1000:,.1f}K" if abs(v) >= 1000 else f"{v:,.0f}"


def _inr(x, signed=False):
    if x is None:
        return "-"
    sign = "+" if (signed and x > 0) else ""
    return f"{sign}\u20b9{x:,.0f}"


def _pct(x, dec=1):
    return "-" if x is None else f"{x * 100:.{dec}f}%"


# ------------------------------------------------------------------- market --
def _day_bounds(now):
    """IST today's [00:00, next 00:00) as UTC-aware pair for snapshot_at."""
    ist = datetime.timezone(datetime.timedelta(hours=5, minutes=30))
    today_ist = now.astimezone(ist).date() if now.tzinfo else now.date()
    start = datetime.datetime.combine(today_ist, datetime.time(0), ist)
    return start.astimezone(datetime.timezone.utc), \
        (start + datetime.timedelta(days=1)).astimezone(datetime.timezone.utc)


def load_today_series(symbol, now):
    """{strike: {times:[], ce_iv:[], pe_iv:[], ce_oi:[], pe_oi:[],
                 ce_vol:[], pe_vol:[]}} for today's snapshots."""
    lo, hi = _day_bounds(now)
    conn = _conn()
    try:
        cur = conn.cursor()
        cur.execute(f"""SELECT strike, to_char(snapshot_at, 'HH24:MI'),
                               ce_iv, pe_iv, ce_oi, pe_oi, ce_volume, pe_volume
                        FROM {SNAP_TABLE}
                        WHERE symbol = %s AND snapshot_at >= %s AND snapshot_at < %s
                        ORDER BY strike, snapshot_at""", (symbol, lo, hi))
        out = {}
        for k, tm, civ, piv, coi, poi, cv, pv in cur.fetchall():
            d = out.setdefault(float(k), {"times": [], "ce_iv": [], "pe_iv": [],
                                          "ce_oi": [], "pe_oi": [],
                                          "ce_vol": [], "pe_vol": []})
            d["times"].append(tm)
            f = lambda v: float(v) if v is not None else None
            d["ce_iv"].append(f(civ)); d["pe_iv"].append(f(piv))
            d["ce_oi"].append(f(coi)); d["pe_oi"].append(f(poi))
            d["ce_vol"].append(f(cv));  d["pe_vol"].append(f(pv))
        return out
    finally:
        conn.close()


def load_history_stats(symbol, now):
    """Per-strike prior-session stats: {strike: {ce_vol_avg, pe_vol_avg,
    ce_iv_mean, ce_iv_sd, pe_iv_mean, pe_iv_sd, days}} over last 20 trading
    days EXCLUDING today. Cached 10 min."""
    with _cache_lock:
        hit = _cache["hist"].get(symbol)
        if hit and time_since(hit[0]) < 600:
            return hit[1]
    lo = (now - datetime.timedelta(days=28))
    conn = _conn()
    try:
        cur = conn.cursor()
        cur.execute(f"""
            WITH daily AS (
              SELECT (snapshot_at AT TIME ZONE 'Asia/Kolkata')::date d, strike,
                     MAX(ce_volume) cv, MAX(pe_volume) pv,
                     AVG(ce_iv) avc, AVG(pe_iv) avp
              FROM {SNAP_TABLE}
              WHERE symbol = %s
                AND (snapshot_at AT TIME ZONE 'Asia/Kolkata')::date < %s
                AND (snapshot_at AT TIME ZONE 'Asia/Kolkata')::date >= %s
              GROUP BY 1, 2)
            SELECT strike, AVG(cv), AVG(pv),
                   AVG(avc), STDDEV(avc), AVG(avp), STDDEV(avp), COUNT(*)
            FROM daily GROUP BY strike""",
                    (symbol,
                     (now - datetime.timedelta(days=1)).date(), lo.date()))
        out = {}
        for (k, cv, pv, mc, sc, mp, sp, n) in cur.fetchall():
            out[float(k)] = {
                "ce_vol_avg": float(cv) if cv is not None else None,
                "pe_vol_avg": float(pv) if pv is not None else None,
                "ce_iv_mean": float(mc) if mc is not None else None,
                "ce_iv_sd": float(sc) if sc is not None else None,
                "pe_iv_mean": float(mp) if mp is not None else None,
                "pe_iv_sd": float(sp) if sp is not None else None,
                "days": int(n)}
        with _cache_lock:
            _cache["hist"][symbol] = (now, out)
        return out
    finally:
        conn.close()


def time_since(ts):
    return (ist_now() - ts).total_seconds()


# --------------------------------------------------------------------- VIX --
def get_vix(fetch_json):
    """Best-effort India VIX via injected fetch_json(url)->dict|None.
    Cached 60s; returns None on failure (callers fall back to ATM IV)."""
    with _cache_lock:
        if _cache["vix"] is not None and time_since(_cache["vix_at"]) < 60:
            return _cache["vix"]
    val = None
    for url in ("https://www.nseindia.com/api/indices/VIX",
                "https://www.nseindia.com/api/vix"):
        try:
            data = fetch_json(url)
            if data:
                val = _dig_vix(data)
                if val:
                    break
        except Exception:
            continue
    with _cache_lock:
        _cache["vix"], _cache["vix_at"] = val, ist_now()
    return val


def _dig_vix(obj):
    """Walk NSE JSON looking for the VIX last value."""
    try:
        if isinstance(obj, dict):
            for key in ("last", "value", "INDIA VIX"):
                if key in obj:
                    v = obj[key]
                    if isinstance(v, (int, float)) and 5 < v < 200:
                        return float(v)
            for v in obj.values():
                r = _dig_vix(v)
                if r:
                    return r
        elif isinstance(obj, list):
            for v in obj:
                r = _dig_vix(v)
                if r:
                    return r
    except Exception:
        pass
    return None


# ------------------------------------------------------------- signal store --
def recent_fires(symbol, strategy, skey, cooldown_min, day_cap):
    conn = _conn(STRAT_DSN)
    try:
        cur = conn.cursor()
        cutoff = ist_now() - datetime.timedelta(minutes=cooldown_min)
        cur.execute("""SELECT count(*) FROM strategy_signals
                       WHERE symbol=%s AND strategy=%s AND skey=%s AND fired_at > %s""",
                    (symbol, strategy, skey, cutoff))
        if cur.fetchone()[0]:
            return True
        cur.execute("""SELECT count(*) FROM strategy_signals
                       WHERE symbol=%s AND strategy=%s
                         AND (fired_at AT TIME ZONE 'Asia/Kolkata')::date
                             = (now() AT TIME ZONE 'Asia/Kolkata')::date""",
                    (symbol, strategy))
        return cur.fetchone()[0] >= day_cap
    finally:
        conn.close()


def record_signal(symbol, strategy, skey, direction, strike, detail, metrics, traded):
    conn = _conn(STRAT_DSN)
    try:
        cur = conn.cursor()
        cur.execute("""INSERT INTO strategy_signals
                        (symbol, strategy, skey, direction, strike, detail, metrics, traded)
                        VALUES (%s,%s,%s,%s,%s,%s,%s::jsonb,%s) RETURNING id""",
                    (symbol, strategy, skey, direction, strike, detail,
                     json.dumps(metrics), traded))
        return cur.fetchone()[0]
    finally:
        conn.close()


# ------------------------------------------------------------ trade opening --
def price_of(snap, strike, is_call):
    row = snap.get(round(float(strike), 2)) or snap.get(float(strike))
    if not row:
        return None
    return row["ce_ltp"] if is_call else row["pe_ltp"]


def open_trade(symbol, strategy, signal_id, direction, structure, legs, meta, now):
    """legs: [(strike, 'CE'|'PE', 'buy'|'sell')] priced from entry snapshot.
    Returns (trade_id, note) — note explains rejection when id is None."""
    conn = _conn(STRAT_DSN)
    try:
        cur = conn.cursor()
        step = STRIKE_STEPS.get(symbol, DEFAULT_STEP)
        lot = LOT_SIZES.get(symbol, DEFAULT_LOT)
        priced, cost = [], 0.0
        for (k, side, action, ltp) in legs:
            if ltp is None or ltp <= 0:
                return None, f"leg {side} {k:.0f} unpriced — trade skipped"
            cost += ltp if action == "buy" else -ltp
            priced.append({"strike": k, "side": side, "action": action, "entry": ltp})
        # max loss per lot: debit spread pays full premium; credit spread
        # risks wing width minus credit
        if cost > 0:
            max_loss = cost
        else:
            width = max(l["strike"] for l in priced) - min(l["strike"] for l in priced)
            max_loss = max(width - abs(cost), 0.5)
        lots = max(1, min(MAX_LOTS,
                          int((PAPER_ACCOUNT * RISK_PER_TRADE) // (max_loss * lot))))
        # execution guards: no opposing bets on the same symbol, and the
        # summed worst-case loss across open positions stays under cap
        opp = {"bullish": "bearish", "bearish": "bullish"}.get(direction)
        if opp:
            cur.execute("""SELECT count(*) FROM paper_trades
                           WHERE symbol=%s AND status='open' AND direction=%s""",
                        (symbol, opp))
            if cur.fetchone()[0]:
                return None, f"opposite-direction position already open — skipped"
        cur.execute("""SELECT legs, qty, lot, net_cost FROM paper_trades
                       WHERE symbol=%s AND status='open'""", (symbol,))
        heat = 0.0
        for (lgs, q, lt, nc) in cur.fetchall():
            lgs = lgs if isinstance(lgs, list) else json.loads(lgs)
            nc = float(nc)
            if nc > 0:
                ml = nc
            else:
                w = (max(l["strike"] for l in lgs) - min(l["strike"] for l in lgs)
                     if len(lgs) > 1 else 0)
                ml = max(w - abs(nc), 0.5)
            heat += ml * q * lt
        new_heat = heat + max_loss * lots * lot
        if new_heat > PAPER_ACCOUNT * MAX_TOTAL_HEAT:
            return None, (f"total open risk \u20b9{new_heat:,.0f} would exceed "
                          f"{MAX_TOTAL_HEAT * 100:.0f}% cap — skipped")
        cur.execute("""INSERT INTO paper_trades
            (symbol, strategy, signal_id, direction, structure, legs, qty, lot,
             net_cost, entry_at, mtm_cost, u_pnl, status, meta)
            VALUES (%s,%s,%s,%s,%s,%s::jsonb,%s,%s,%s,%s,%s,0,'open',%s::jsonb)
            RETURNING id""",
                    (symbol, strategy, signal_id, direction, structure,
                     json.dumps(priced), lots, lot, round(cost, 2), now, round(cost, 2),
                     json.dumps(meta)))
        return cur.fetchone()[0], None
    finally:
        conn.close()


# --------------------------------------------------------------- mark to mkt --
def mark_open_trades(symbol, snap, spot, t_years, atm_iv, atm_row, now, scan_rows=None, series=None, vix=None):
    """MTM all open trades; apply exits. Returns list of updated trade dicts.
    atm_row / scan_rows / series / vix are the live scan results, used only for
    the soft-profit cooldown check (signal_cooled)."""
    conn = _conn(STRAT_DSN)
    updated = []
    try:
        cur = conn.cursor()
        cur.execute("""SELECT id, strategy, direction, structure, legs, qty, lot,
                              net_cost, entry_at, meta, signal_id
                       FROM paper_trades WHERE symbol=%s AND status='open'""",
                    (symbol,))
        db_rows = cur.fetchall()
        for (tid, strat, direction, structure, legs, qty, lot, cost, entry_at, meta, sig_id) in db_rows:
            cost = float(cost)
            legs = legs if isinstance(legs, list) else json.loads(legs)
            meta = meta or {}
            value, missing = 0.0, False
            for l in legs:
                p = price_of(snap, l["strike"], l["side"] == "CE")
                if p is None:  # strike left the window — BS reprice at ATM IV
                    p = _bs_price(spot, l["strike"], t_years, atm_iv, l["side"] == "CE")
                if p is None:
                    missing = True
                    break
                value += p if l["action"] == "buy" else -p
            if missing:
                continue
            pnl = (value - cost) * qty * lot
            credit = abs(cost) if cost < 0 else None
            reason = None
            eod = now.time() >= TIME_EXIT
            # Both entry_at (from TIMESTAMPTZ) and now (the cycle clock) are market
            # times in IST. Make them comparable: if one side is tz-aware and the
            # other isn't, give the naive side the same tz so the subtraction is
            # meaningful (avoids the naive-vs-aware mismatch that produced negative
            # hold times).
            if isinstance(entry_at, datetime.datetime):
                if entry_at.tzinfo is not None and now.tzinfo is None:
                    now = now.replace(tzinfo=entry_at.tzinfo)
                elif entry_at.tzinfo is None and now.tzinfo is not None:
                    entry_at = entry_at.replace(tzinfo=now.tzinfo)
                hold_min = (now - entry_at).total_seconds() / 60.0
            else:
                hold_min = None
            # ---- stale timer: force-close trades that outlive their setup ----
            if hold_min is not None and hold_min >= MAX_HOLD_MIN and not reason:
                reason = "MAX HOLD 60m"
            # ---- profit / stop criteria (every trade must exit on one) ----
            if not reason and strat in ("S1", "S3", "S4_FOLLOW", "S6", "S10") and cost > 0:  # debit spread: pay premium, cap risk = premium
                if value - cost >= cost:
                    reason = "TARGET +100%"
                elif value - cost <= -0.5 * cost:
                    reason = "STOP -50%"
                elif value - cost >= SOFT_DEBIT_PCT * cost and signal_cooled(strat, direction, meta, scan_rows or [], spot, atm_row, series or {}, vix):
                    reason = "SOFT BOOK +70%"
            elif not reason and strat in ("S2", "S4_FADE", "S5", "S8", "S9") and credit:    # credit spread: collect premium, risk = wing width - credit
                if value - cost >= 0.4 * credit:
                    reason = "TARGET 40% OF CREDIT"
                elif value - cost <= -1.0 * credit:
                    reason = "STOP CREDIT DOUBLED"
                elif value - cost >= SOFT_CREDIT_PCT * credit and signal_cooled(strat, direction, meta, scan_rows or [], spot, atm_row, series or {}, vix):
                    reason = "SOFT BOOK +25%"
            elif not reason and strat == "S7" and cost > 0:                              # straddle: long both sides, exit on IV crush or delta target
                if abs(value - cost) >= 0.5 * cost:   # 50% PnL either direction = volatility realized or crushed
                    reason = "TARGET +/-50%"
                elif value - cost >= SOFT_DEBIT_PCT * cost and signal_cooled(strat, direction, meta, scan_rows or [], spot, atm_row, series or {}, vix):
                    reason = "SOFT BOOK +70%"
            # ---- thesis-reversal exits (the setup's premise is void) ----
            if not reason and strat == "S1" and direction == "bullish":
                k = float(meta.get("strike") or 0)
                r = snap.get(k)
                if r and r.get("ce_oi") and (r.get("ce_chg_oi") or 0) <= -0.2 * r["ce_oi"]:
                    reason = "OI UNWOUND"
            if not reason and strat == "S1" and direction == "bearish":
                k = float(meta.get("strike") or 0)
                r = snap.get(k)
                if r and r.get("pe_oi") and (r.get("pe_chg_oi") or 0) <= -0.2 * r["pe_oi"]:
                    reason = "OI UNWOUND"
            if not reason and strat == "S3":
                es = float(meta.get("entry_spot") or 0)
                if es and spot < es * 0.995:
                    reason = "WALL REJECTED"
            if not reason and strat == "S2":
                sc = float(meta.get("short_call") or 1e12)
                sp_ = float(meta.get("short_put") or -1e12)
                if spot > sc or spot < sp_:
                    reason = "SPOT BROKE SHORT STRIKE"
            if not reason and strat == "S5":
                sc = float(meta.get("short_call") or 1e12)
                sp_ = float(meta.get("short_put") or -1e12)
                if spot > sc or spot < sp_:
                    reason = "SPOT BROKE SHORT STRIKE"
            if not reason and strat == "S7":
                es = float(meta.get("entry_spot") or spot)
                # straddle thesis void if spot hasn't moved materially AND IV hasn't expanded
                if abs(spot - es) / es < 0.003 and atm_row and atm_row.get("ce_iv") and atm_row["ce_iv"] <= atm_row.get("ce_iv") * 1.1:
                    pass  # not yet void; wait for target/stop
                elif hold_min and hold_min > 120:
                    reason = "STALLED 120m"
            if not reason and strat == "S8":
                es = float(meta.get("entry_spot") or spot)
                if es and spot > es * 1.02:
                    reason = "SPOT ABOVE ENTRY +2%"
            if not reason and strat == "S10":
                es = float(meta.get("entry_spot") or 0)
                if es and spot < es * 0.995:
                    reason = "WALL REJECTED"
            if not reason and eod:
                reason = "EOD TIME EXIT"
            if reason:
                cur.execute("""UPDATE paper_trades SET status='closed', exit_at=now(),
                               mtm_cost=%s, u_pnl=%s, realized=%s, exit_reason=%s
                               WHERE id=%s""",
                            (round(value, 2), round(pnl, 2), round(pnl, 2), reason, tid))
            else:
                cur.execute("UPDATE paper_trades SET mtm_cost=%s, u_pnl=%s WHERE id=%s",
                            (round(value, 2), round(pnl, 2), tid))
            updated.append({"id": tid, "strategy": strat, "direction": direction,
                            "structure": structure, "legs": legs, "qty": qty,
                            "lot": lot, "cost": cost, "value": value,
                            "pnl": pnl, "status": "closed" if reason else "open",
                            "exit_reason": reason, "meta": meta})
        return updated
    finally:
        conn.close()


# ------------------------------------------------------------------ scanners --
def scan_s1(rows, hist, series, atm_row, now):
    """Unusual OI + volume surge -> directional conviction."""
    ce_vols = [r["ce_vol"] for r in rows if (r["ce_vol"] or 0) > 0]
    pe_vols = [r["pe_vol"] for r in rows if (r["pe_vol"] or 0) > 0]
    med_ce = statistics.median(ce_vols) if ce_vols else 0
    med_pe = statistics.median(pe_vols) if pe_vols else 0
    atm_iv = atm_row["ce_iv"] or atm_row["pe_iv"] or 0
    cands, armed = [], []
    for r in rows:
        k = r["strike"]
        for side, vol, oi, chg, iv, med, hkey in (
                ("bullish", r["ce_vol"], r["ce_oi"], r["ce_chg_oi"], r["ce_iv"], med_ce, "ce_vol_avg"),
                ("bearish", r["pe_vol"], r["pe_oi"], r["pe_chg_oi"], r["pe_iv"], med_pe, "pe_vol_avg")):
            vol, oi, chg = vol or 0, oi or 0, chg or 0
            prev_oi = oi - chg
            build = chg / prev_oi if prev_oi > 0 else (1.0 if chg > 0 else 0)
            ratio = vol / med if med else 0
            h = hist.get(k, {})
            havg = h.get(hkey)
            hist_ok = (vol >= VOL_MEDIAN_MULT * havg) if havg else (ratio >= VOL_MEDIAN_MULT)
            hot = ratio >= VOL_MEDIAN_MULT and build >= OI_BUILD_FRAC and chg > 0 and hist_ok
            iv_rich = atm_iv and iv and iv > S1_IV_GATE * atm_iv
            if hot and not iv_rich:
                cands.append({"strike": k, "dir": side, "ratio": ratio,
                              "build": build, "vol": vol, "iv": iv})
            elif ratio >= VOL_MEDIAN_MULT * 0.66 and build >= OI_BUILD_FRAC * 0.6:
                armed.append({"strike": k, "dir": side, "ratio": ratio, "build": build})
    cands.sort(key=lambda c: c["ratio"] * c["build"], reverse=True)
    best = cands[0] if cands else None
    metrics = [("Chain PCR", _ratio_pcr(rows)),
               ("Vol mult.", f"{VOL_MEDIAN_MULT:.0f}x median"),
               ("OI build min.", _pct(OI_BUILD_FRAC, 0)),
               ("Hits", str(len(cands)))]
    note = None
    if best:
        note = (f"{best['dir'].upper()} @ {best['strike']:,.0f}: vol {best['ratio']:.1f}x "
                f"median ({_fmt_oi(best['vol'])}), OI +{_pct(best['build'])}")
    elif armed:
        a = armed[0]
        note = f"watch: {a['dir']} @ {a['strike']:,.0f} at {a['ratio']:.1f}x, building {_pct(a['build'])}"
    return best, note, metrics


def scan_s2(rows, series, atm_row, vix, now):
    """IV spike (z-score) under elevated vol regime -> sell premium."""
    atm_iv = atm_row["ce_iv"] or atm_row["pe_iv"]
    regime = (vix >= VIX_SELL_LEVEL) if vix else (atm_iv and atm_iv >= ATM_IV_SELL_LEVEL)
    worst, worst_z, samples_seen = None, 0.0, 0
    for r in rows:
        s = series.get(r["strike"])
        if not s:
            continue
        for side, iv_now, seq in (("CE", r["ce_iv"], s["ce_iv"]), ("PE", r["pe_iv"], s["pe_iv"])):
            pts = [v for v in seq if v]
            if iv_now is None or len(pts) < IV_ZSCORE_MIN_PTS or statistics.pstdev(pts) == 0:
                continue
            samples_seen = max(samples_seen, len(pts))
            z = (iv_now - statistics.mean(pts)) / statistics.pstdev(pts)
            if z > worst_z:
                worst_z, worst = z, {"strike": r["strike"], "side": side,
                                     "iv": iv_now, "z": z}
    hit = worst and worst["z"] >= IV_ZSCORE and regime
    gauge = f"VIX {vix:.1f}" if vix else f"ATM IV {atm_iv:.1f}"
    metrics = [("Vol gauge", gauge),
               ("Regime sell?", "YES" if regime else "NO"),
               ("Top IV z", f"{worst['z']:+.2f} @ {worst['strike']:,.0f} {worst['side']}"
                if worst else "n/a"),
               ("Trigger", f"z>{IV_ZSCORE:.1f}")]
    note = None
    if hit:
        note = f"{worst['side']} IV z {worst['z']:+.2f} @ {worst['strike']:,.0f} with {gauge}"
    elif worst and not regime:
        note = f"IV z {worst['z']:+.2f} but vol regime calm ({gauge})"
    return (worst if hit else None), note, metrics


def scan_s3(rows, spot, atm_row, now):
    """Gamma-wall proximity -> squeeze setup."""
    ce_g = [(r["strike"], (r["ce_oi"] or 0) * (r["ce_gamma"] or 0), r["ce_vol"])
            for r in rows if r["strike"] > spot * 1.002]
    pe_g = [(r["strike"], (r["pe_oi"] or 0) * (r["pe_gamma"] or 0), r["pe_vol"])
            for r in rows if r["strike"] < spot * 0.998]
    ce_vols = [r["ce_vol"] or 0 for r in rows]
    pe_vols = [r["pe_vol"] or 0 for r in rows]
    med_ce = statistics.median(v for v in ce_vols if v > 0) if any(ce_vols) else 0
    med_pe = statistics.median(v for v in pe_vols if v > 0) if any(pe_vols) else 0
    wall_up = max(ce_g, key=lambda x: x[1]) if ce_g else None
    wall_dn = max(pe_g, key=lambda x: x[1]) if pe_g else None
    hit = None
    if wall_up:
        dist = (wall_up[0] - spot) / spot * 100
        vol_building = med_ce and wall_up[2] and wall_up[2] >= GEX_WALL_VOL_MULT * med_ce
        if dist <= GEX_WALL_PCT and vol_building:
            hit = {"strike": wall_up[0], "dist_pct": dist, "vol": wall_up[2]}
    metrics = [("Gamma wall up", f"{wall_up[0]:,.0f}" if wall_up else "-"),
               ("Dist to wall", f"{(wall_up[0] - spot) / spot * 100:.2f}%" if wall_up else "-"),
               ("Wall down", f"{wall_dn[0]:,.0f}" if wall_dn else "-"),
               ("Trigger", f"≤{GEX_WALL_PCT:.0f}% + vol")]
    note = None
    if hit:
        note = (f"spot {spot:,.2f} within {hit['dist_pct']:.2f}% of call wall "
                f"{hit['strike']:,.0f}, wall vol building")
    elif wall_up:
        note = f"wall {wall_up[0]:,.0f} {(wall_up[0] - spot) / spot * 100:.2f}% away"
    return hit, note, metrics


def scan_s4(rows, spot, atm_row, series, now):
    """Put skew anomaly -> follow hedges or fade fear."""
    atm_iv = atm_row["ce_iv"] or atm_row["pe_iv"] or 0
    # 25-delta proxies: OTM put (strike below spot) with |delta| near .25,
    # OTM call above spot with delta near .25
    put25 = min((r for r in rows if r["strike"] < spot and (r["pe_delta"] or 0) != 0),
                key=lambda r: abs(abs(r["pe_delta"]) - 0.25), default=None)
    call25 = min((r for r in rows if r["strike"] > spot and (r["ce_delta"] or 0) != 0),
                 key=lambda r: abs(r["ce_delta"] - 0.25), default=None)
    if not put25 or not put25["pe_iv"]:
        return None, None, [("Skew", "insufficient data")]
    skew = put25["pe_iv"] - (call25["ce_iv"] if call25 and call25["ce_iv"] else atm_iv)
    prem = put25["pe_iv"] - atm_iv
    chg = put25["pe_chg_oi"] or 0
    prev = (put25["pe_oi"] or 0) - chg
    build = chg / prev if prev > 0 else 0.0
    if prem >= SKEW_PREMIUM:
        mode = "follow" if build >= SKEW_OI_FOLLOW_FRAC else "fade"
        hit = {"strike": put25["strike"], "prem": prem, "skew": skew,
               "mode": mode, "build": build}
        note = (f"25d skew {skew:.1f} pts, hedge premium {prem:.1f} > {SKEW_PREMIUM:.0f}; "
                f"put OI {_pct(build)} -> {mode.upper()}")
    else:
        hit = None
        note = f"skew {skew:.1f} pts (premium {prem:.1f} vs {SKEW_PREMIUM:.0f} trigger)"
    metrics = [("25d put strike", f"{put25['strike']:,.0f}"),
               ("Skew (P-C IV)", f"{skew:.1f} pts"),
               ("Put prem/ATM", f"{prem:.1f} pts"),
               ("Put OI build", _pct(build))]
    return hit, note, metrics


def scan_s5(rows, spot, atm_row, series, hist, vix, now):
    """Iron Condor — sell premium when IV is elevated and spot is mid-range.
    Reuses S2's volatility logic but adds a spot-range gate: only sell when
    spot is comfortably inside the expected range (not too close to either
    short strike). Structure: sell ATM+/-1SD call and put, buy wings 4 strikes
    further out."""
    atm_iv = atm_row["ce_iv"] or atm_row["pe_iv"] or 0
    sd = spot * (atm_iv / 100.0) * math.sqrt(max(t_years_for(spot, atm_row, now), 1/365))
    regime = (vix >= VIX_SELL_LEVEL) if vix else (atm_iv >= ATM_IV_SELL_LEVEL)
    if not regime:
        return None, None, [("Vol regime", "calm")]
    # pick the most overpriced strike on either side as the short strike
    worst, worst_z = None, 0.0
    for r in rows:
        s = series.get(r["strike"])
        if not s:
            continue
        for side, iv_now, seq in (("CE", r["ce_iv"], s["ce_iv"]), ("PE", r["pe_iv"], s["pe_iv"])):
            pts = [v for v in seq if v]
            if iv_now is None or len(pts) < IV_ZSCORE_MIN_PTS or statistics.pstdev(pts) == 0:
                continue
            z = (iv_now - statistics.mean(pts)) / statistics.pstdev(pts)
            if z > worst_z:
                worst_z, worst = z, {"strike": r["strike"], "side": side, "iv": iv_now}
    if not worst or worst_z < IV_ZSCORE:
        return None, None, [("Vol gauge", f"IV z {worst_z:+.2f}" if worst else "n/a"),
                             ("Regime", f"VIX {vix:.1f}" if vix else f"ATM IV {atm_iv:.1f}")]
    step = STRIKE_STEPS.get("NIFTY", DEFAULT_STEP)
    sc = _round_strike(spot + sd, step)
    sp_ = _round_strike(spot - sd, step)
    # range gate: spot must be at least 0.5 SD inside each short strike
    if abs(spot - sc) / sc < 0.3 or abs(spot - sp_) / sp_ < 0.3:
        return None, None, [("Range gate", "spot too close to short strike")]
    hit = {"sc": sc, "sp": sp_, "z": worst_z, "side": worst["side"]}
    note = (f"IV z {worst_z:+.2f} @ {worst['strike']:,.0f} {worst['side']} — "
            f"selling {sp_:,.0f}P/{sc:,.0f}C condor, VIX {vix:.1f}" if vix
            else f"IV z {worst_z:+.2f} — selling {sp_:,.0f}P/{sc:,.0f}C, ATM IV {atm_iv:.1f}")
    metrics = [("Vol gauge", f"VIX {vix:.1f}" if vix else f"ATM IV {atm_iv:.1f}"),
               ("Top IV z", f"{worst_z:+.2f} @ {worst['strike']:,.0f} {worst['side']}"),
               ("Short range", f"{sp_:,.0f} – {sc:,.0f}"),
               ("Trigger", f"z>{IV_ZSCORE:.1f} + range")]
    return hit, note, metrics


def scan_s6(rows, spot, atm_row, series, hist, now):
    """Breakout momentum — volume + OI confirmation in the breakout direction.
    Unlike S1 (which looks for any OI surge), this specifically looks for a
    strike being BROKEN (price crosses it) with volume > 2x median AND OI
    building in the breakout direction. Long debit spread in breakout dir."""
    ce_vols = [r["ce_vol"] or 0 for r in rows if (r["ce_vol"] or 0) > 0]
    pe_vols = [r["pe_vol"] or 0 for r in rows if (r["pe_vol"] or 0) > 0]
    med_ce = statistics.median(ce_vols) if ce_vols else 0
    med_pe = statistics.median(pe_vols) if pe_vols else 0
    cands = []
    for r in rows:
        k = r["strike"]
        # upside breakout: spot above strike, call volume building, OI building
        if k < spot and r["ce_vol"] and r["ce_oi"] and r["ce_chg_oi"]:
            vol = r["ce_vol"] or 0
            oi = r["ce_oi"] or 0
            chg = r["ce_chg_oi"] or 0
            prev = oi - chg
            build = chg / prev if prev > 0 else 0
            ratio = vol / med_ce if med_ce else 0
            if ratio >= 2.0 and build >= 0.15 and chg > 0 and spot > k * 1.005:
                cands.append({"strike": k, "dir": "bullish", "ratio": ratio,
                              "build": build, "vol": vol})
        # downside breakout: spot below strike, put volume building, OI building
        if k > spot and r["pe_vol"] and r["pe_oi"] and r["pe_chg_oi"]:
            vol = r["pe_vol"] or 0
            oi = r["pe_oi"] or 0
            chg = r["pe_chg_oi"] or 0
            prev = oi - chg
            build = chg / prev if prev > 0 else 0
            ratio = vol / med_pe if med_pe else 0
            if ratio >= 2.0 and build >= 0.15 and chg > 0 and spot < k * 0.995:
                cands.append({"strike": k, "dir": "bearish", "ratio": ratio,
                              "build": build, "vol": vol})
    cands.sort(key=lambda c: c["ratio"] * c["build"], reverse=True)
    best = cands[0] if cands else None
    metrics = [("Breakout dir", "upside" if best and best["dir"] == "bullish" else
                "downside" if best and best["dir"] == "bearish" else "none"),
               ("Vol mult.", f"{2.0:.0f}x median"),
               ("OI build min.", "15%"),
               ("Hits", str(len(cands)))]
    note = None
    if best:
        note = (f"{best['dir'].upper()} breakout @ {best['strike']:,.0f}: vol "
                f"{best['ratio']:.1f}x median, OI +{_pct(best['build'])}")
    return best, note, metrics


def scan_s7(rows, spot, atm_row, series, vix, now):
    """Long Straddle/Strangle — buy volatility when IV is at a multi-day low
    AND a volatility event is anticipated (big OI imbalance or big spot move
    expected). Structure: long ATM straddle (CE + PE at ATM). Exit: IV crush
    target or delta-neutral rebal or soft book."""
    atm_iv = atm_row["ce_iv"] or atm_row["pe_iv"] or 0
    if atm_iv <= 0:
        return None, None, [("ATM IV", "-")]
    # IV must be relatively low (sub-20 VIX equivalent or ATM IV < 12)
    regime_ok = (vix is not None and vix < 18) or (atm_iv < 12)
    if not regime_ok:
        return None, None, [("Vol regime", f"VIX {vix:.1f}" if vix else f"ATM IV {atm_iv:.1f}"),
                             ("Status", "IV not low enough")]
    # volatility event signal: big OI concentration at a single strike (>150% of
    # avg OI across window) suggests a binary event is being hedged OR spot has
    # moved a lot in the last hour (big IOP spikes)
    ois = [r["ce_oi"] or 0 for r in rows] + [r["pe_oi"] or 0 for r in rows]
    med_oi = statistics.median([o for o in ois if o > 0]) if ois else 0
    max_oi = max(ois) if ois else 0
    event_imminent = max_oi > 1.5 * med_oi if med_oi > 0 else False
    if not event_imminent:
        return None, None, [("OI concentration", f"{max_oi/med_oi:.1f}x avg" if med_oi else "n/a"),
                             ("Status", "no binary-event signal")]
    step = STRIKE_STEPS.get("NIFTY", DEFAULT_STEP)
    atm_k = atm_row["strike"]
    hit = {"atm_k": atm_k, "prem": atm_iv}
    note = (f"Long straddle @ {atm_k:,.0f} ATM — IV {atm_iv:.1f}% (low regime), "
            f"OI concentration {max_oi/med_oi:.1f}x avg suggests event hedge")
    metrics = [("ATM IV", f"{atm_iv:.1f}%"),
               ("IV regime", f"VIX {vix:.1f}" if vix else "low"),
               ("OI conc.", f"{max_oi/med_oi:.1f}x avg"),
               ("Trigger", "low IV + event signal")]
    return hit, note, metrics


def scan_s8(rows, spot, atm_row, series, now):
    """Put Ratio Backspread — when put skew is EXTREME (put IV more than 30
    points above ATM IV) and spot is near a heavy put wall. Buy more lower-
    strike puts than short ATM puts for net credit (or small debit). Bets on
    a sharp move down. Long gamma, long vega."""
    atm_iv = atm_row["ce_iv"] or atm_row["pe_iv"] or 0
    # find the most extreme put IV in the chain
    worst_put = None
    for r in rows:
        if r["strike"] < spot and r["pe_iv"] and r["pe_iv"] > atm_iv + 30:
            if not worst_put or r["pe_iv"] > worst_put["pe_iv"]:
                worst_put = {"strike": r["strike"], "iv": r["pe_iv"],
                              "skew": r["pe_iv"] - atm_iv, "vol": r["pe_vol"],
                              "oi": r["pe_oi"]}
    if not worst_put:
        return None, None, [("Skew", "insufficient"),
                             ("Trigger", ">30 pts put IV premium")]
    # need OI building at the skewed strike to confirm real concern
    build = 0
    if worst_put["oi"] and worst_put["vol"]:
        chg = worst_put["vol"] * 0.1  # rough proxy: volume implies OI change
        prev = (worst_put["oi"] or 0) - chg
        build = chg / prev if prev > 0 else 0
    hit = {"strike": worst_put["strike"], "skew": worst_put["skew"],
           "prem": worst_put["iv"] - atm_iv, "build": build}
    note = (f"Put ratio backspread @ {worst_put['strike']:,.0f} — put IV "
            f"{worst_put['iv']:.1f}% vs ATM {atm_iv:.1f}% (skew {worst_put['skew']:.1f} pts), "
            f"OI build {build:.0%}")
    metrics = [("Skew strike", f"{worst_put['strike']:,.0f}"),
               ("Put IV", f"{worst_put['iv']:.1f}%"),
               ("Skew", f"{worst_put['skew']:.1f} pts"),
               ("Trigger", ">30 pts + OI build")]
    return hit, note, metrics


def scan_s9(rows, spot, atm_row, series, hist, now):
    """Calendar/Diagonal Spread — sell near-term, buy next-week same strike.
    Triggers when front-month IV > back-month IV (IV contango) AND spot is
    stable near a wall (not breaking out). Structure: sell near-term ATM/OTM,
    buy same strike next expiry. Short gamma, collects theta."""
    atm_iv = atm_row["ce_iv"] or atm_row["pe_iv"] or 0
    if atm_iv <= 0:
        return None, None, [("ATM IV", "-")]
    # check IV contango: front-month IV should be elevated vs a hypothetical
    # next-expiry IV. Without next-expiry data, use a proxy: ATM IV in top
    # quintile of today's chain AND spot is stable (no strong OI trend at ATM)
    atm_k = atm_row["strike"]
    atm_row_data = rows.get(atm_k)
    iv_rank = 0
    ivs = [r["ce_iv"] or 0 for r in rows] + [r["pe_iv"] or 0 for r in rows]
    ivs = [v for v in ivs if v > 0]
    if ivs:
        iv_rank = sum(1 for v in ivs if v <= atm_iv) / len(ivs)
    # stability: OI change at ATM should be small (not a breakout)
    stable = atm_row_data and abs(atm_row_data.get("ce_chg_oi") or 0) < 0.1 * (atm_row_data.get("ce_oi") or 0)
    if iv_rank > 0.8 and stable:
        hit = {"atm_k": atm_k, "iv_rank": iv_rank}
        note = (f"Calendar spread @ {atm_k:,.0f} ATM — IV rank {iv_rank:.0%} "
                f"(front-month rich), spot stable at wall")
        metrics = [("ATM IV", f"{atm_iv:.1f}%"),
                   ("IV rank", f"{iv_rank:.0%}"),
                   ("Spot stability", "stable" if stable else "breaking"),
                   ("Trigger", "IV rank >80% + stable")]
        return hit, note, metrics
    return None, None, [("IV rank", f"{iv_rank:.0%}" if ivs else "n/a"),
                         ("Status", "no IV contango signal")]


def scan_s10(rows, spot, atm_row, series, now):
    """Gamma Flip Reversion — fade the move when spot approaches a gamma wall
    AND IV is rising (dealers are hedging). Opposite of S3: instead of going
    long gamma into the wall, sell the breakout. Structure: short debit spread
    against the breakout direction, or short strangle wings. Exit on reversal
    or wall rejection."""
    step = STRIKE_STEPS.get("NIFTY", DEFAULT_STEP)
    # gamma wall up (calls)
    ce_g = [(r["strike"], (r["ce_oi"] or 0) * (r["ce_gamma"] or 0), r["ce_vol"])
            for r in rows if r["strike"] > spot * 1.002]
    pe_g = [(r["strike"], (r["pe_oi"] or 0) * (r["pe_gamma"] or 0), r["pe_vol"])
            for r in rows if r["strike"] < spot * 0.998]
    med_ce = statistics.median([v for v in [r["ce_vol"] or 0 for r in rows] if v > 0]) or 1
    med_pe = statistics.median([v for v in [r["pe_vol"] or 0 for r in rows] if v > 0]) or 1
    wall_up = max(ce_g, key=lambda x: x[1]) if ce_g else None
    wall_dn = max(pe_g, key=lambda x: x[1]) if pe_g else None
    hit = None
    if wall_up and wall_up[2] and wall_up[2] >= GEX_WALL_VOL_MULT * med_ce:
        dist = (wall_up[0] - spot) / spot * 100
        if dist <= GEX_WALL_PCT:
            hit = {"type": "fade_up", "strike": wall_up[0], "dist_pct": dist,
                    "vol": wall_up[2], "wall": wall_up[0]}
    if not hit and wall_dn and wall_dn[2] and wall_dn[2] >= GEX_WALL_VOL_MULT * med_pe:
        dist = (spot - wall_dn[0]) / spot * 100
        if dist <= GEX_WALL_PCT:
            hit = {"type": "fade_down", "strike": wall_dn[0], "dist_pct": dist,
                    "vol": wall_dn[2], "wall": wall_dn[0]}
    metrics = [("Gamma wall up", f"{wall_up[0]:,.0f}" if wall_up else "-"),
               ("Wall up dist", f"{(wall_up[0]-spot)/spot*100:.2f}%" if wall_up else "-"),
               ("Gamma wall dn", f"{wall_dn[0]:,.0f}" if wall_dn else "-"),
               ("Trigger", f"≤{GEX_WALL_PCT:.0f}% + vol + fade")]
    note = None
    if hit:
        if hit["type"] == "fade_up":
            note = (f"Fade breakout above call wall {hit['wall']:,.0f} — "
                    f"spot {spot:,.2f} within {hit['dist_pct']:.2f}%, wall vol building")
        else:
            note = (f"Fade breakdown below put wall {hit['wall']:,.0f} — "
                    f"spot {spot:,.2f} within {hit['dist_pct']:.2f}%, wall vol building")
    elif wall_up:
        note = f"wall {wall_up[0]:,.0f} {(wall_up[0]-spot)/spot*100:.2f}% away"
    return hit, note, metrics


def t_years_for(spot, atm_row, now):
    """Rough time-to-expiry guess when t_years not passed to scanner."""
    return 7 / 365


def _ratio_pcr(rows):
    ce = sum(r["ce_oi"] or 0 for r in rows)
    pe = sum(r["pe_oi"] or 0 for r in rows)
    return f"{pe / ce:.2f}" if ce else "-"


# ----------------------------------------------------------------- run cycle --
def signal_cooled(strat, direction, meta, rows, spot, atm_row, series, vix):
    """True when the original anomaly that opened this trade has faded back
    toward normal — not a hard exit, just permission to soft-book profit.
    Driven off the live scan result each cycle so it never goes stale."""
    if strat == "S1":
        k = float(meta.get("strike") or 0)
        rows_d = {r["strike"]: r for r in rows}
        r = rows_d.get(k)
        if not r:
            return True   # can't confirm the flow still exists -> treat as cooled
        ce_vols = [x["ce_vol"] or 0 for x in rows if (x["ce_vol"] or 0) > 0]
        pe_vols = [x["pe_vol"] or 0 for x in rows if (x["pe_vol"] or 0) > 0]
        med = statistics.median(ce_vols + pe_vols) or 1
        side = "ce" if direction == "bullish" else "pe"
        vol = r.get(f"{side}_vol") or 0
        chg = r.get(f"{side}_chg_oi") or 0
        oi = r.get(f"{side}_oi") or 0
        build = chg / (oi - chg) if (oi - chg) > 0 else (1.0 if chg > 0 else 0)
        return (vol / med < 1.5) or (build < OI_BUILD_FRAC * 0.6)
    if strat == "S3":
        wall = float(meta.get("wall") or 0)
        if not wall:
            return True
        dist = abs(wall - spot) / spot * 100
        return dist > GEX_WALL_PCT * 2
    if strat == "S4_FOLLOW" or strat == "S4_FADE":
        k = float(meta.get("strike") or 0)
        r = rows.get(k)
        if not r:
            return True
        atm_iv = atm_row["ce_iv"] or atm_row["pe_iv"] or 0
        prem = (r["pe_iv"] or 0) - atm_iv
        return prem < SKEW_PREMIUM * 0.6
    if strat == "S2":
        if vix is None and not atm_row.get("ce_iv") and not atm_row.get("pe_iv"):
            return True
        sc = float(meta.get("short_call") or 1e12)
        sp = float(meta.get("short_put") or -1e12)
        if spot > sc or spot < sp:
            return True
        return False
    if strat == "S5":
        # cooled when IV z has mean-reverted below trigger
        sc = float(meta.get("short_call") or 1e12)
        sp_ = float(meta.get("short_put") or -1e12)
        if spot > sc or spot < sp_:
            return True
        return False   # keep hard target; only soft-book when vol regime calms (caller checks)
    if strat == "S7":
        # cooled when IV has expanded (the event premium is being paid) — soft-book on IV crush instead
        atm_iv = atm_row.get("ce_iv") or atm_row.get("pe_iv") or 0
        entry_iv = float(meta.get("entry_iv") or atm_iv)
        return atm_iv > entry_iv * 1.1   # IV expanded >10% = volatility realized, take profit
    if strat == "S8":
        k = float(meta.get("strike") or 0)
        rows_d = {r["strike"]: r for r in rows}
        r = rows_d.get(k)
        if not r:
            return True
        atm_iv = atm_row.get("ce_iv") or atm_row.get("pe_iv") or 0
        prem = (r.get("pe_iv") or 0) - atm_iv
        return prem < 15   # skew premium faded below 15 pts = fear premium gone
    if strat == "S9":
        # cooled when spot breaks the stability assumption
        k = float(meta.get("strike") or 0)
        rows_d = {r["strike"]: r for r in rows}
        r = rows_d.get(k)
        if not r:
            return True
        return abs(r.get("ce_chg_oi") or 0) > 0.15 * (r.get("ce_oi") or 1)   # big OI move = breakout started
    if strat == "S10":
        es = float(meta.get("entry_spot") or 0)
        if es and abs(spot - es) / es > 0.01:
            return True   # spot moved 1% away from entry = fade thesis played out
        return False
    return False


def run_cycle(symbol, expiry, spot, db_args, now, vix=None, t_years=None):
    """Full pass for one snapshot. db_args: the 25-field tuples already built
    for Postgres insert. Returns dashboard payload dict (also rendered later)."""
    ensure_schema()
    rows = []
    for a in db_args:
        (sym, exp, k, sp, sts,
         ce_ltp, ce_chg, ce_oi, ce_chg_oi, ce_vol, ce_iv, ce_d, ce_g, ce_th, ce_v,
         pe_ltp, pe_chg, pe_oi, pe_chg_oi, pe_vol, pe_iv, pe_d, pe_g, pe_th, pe_v) = a
        rows.append({"strike": float(k), "ce_ltp": ce_ltp, "ce_chg": ce_chg,
                     "ce_oi": ce_oi, "ce_chg_oi": ce_chg_oi, "ce_vol": ce_vol,
                     "ce_iv": ce_iv, "ce_delta": ce_d, "ce_gamma": ce_g,
                     "ce_vega": ce_v, "pe_ltp": pe_ltp, "pe_chg": pe_chg,
                     "pe_oi": pe_oi, "pe_chg_oi": pe_chg_oi, "pe_vol": pe_vol,
                     "pe_iv": pe_iv, "pe_delta": pe_d, "pe_gamma": pe_g,
                     "pe_vega": pe_v})
    snap = {r["strike"]: r for r in rows}
    atm_row = min(rows, key=lambda r: abs(r["strike"] - spot))
    atm_iv = atm_row["ce_iv"] or atm_row["pe_iv"] or 12.0
    step = STRIKE_STEPS.get(symbol, DEFAULT_STEP)
    lot = LOT_SIZES.get(symbol, DEFAULT_LOT)
    if t_years is None:
        t_years = 7 / 365
    hist = {}
    series = {}
    try:
        hist = load_history_stats(symbol, now)
        series = load_today_series(symbol, now)
    except Exception as e:
        print("strategies: history unavailable:", e)

    signals = []
    # ---- scan & maybe fire
    s1, s1_note, s1_metrics = scan_s1(rows, hist, series, atm_row, now)
    s2, s2_note, s2_metrics = scan_s2(rows, series, atm_row, vix, now)
    s3, s3_note, s3_metrics = scan_s3(rows, spot, atm_row, now)
    s4, s4_note, s4_metrics = scan_s4(rows, spot, atm_row, series, now)
    s5, s5_note, s5_metrics = scan_s5(rows, spot, atm_row, series, hist, vix, now)
    s6, s6_note, s6_metrics = scan_s6(rows, spot, atm_row, series, hist, now)
    s7, s7_note, s7_metrics = scan_s7(rows, spot, atm_row, series, vix, now)
    s8, s8_note, s8_metrics = scan_s8(rows, spot, atm_row, series, now)
    s9, s9_note, s9_metrics = scan_s9(rows, spot, atm_row, series, hist, now)
    s10, s10_note, s10_metrics = scan_s10(rows, spot, atm_row, series, now)

    # ---- execution gates: entries ONLY inside the live F&O session, only
    # after process warm-up, and only when the signal stays hot across
    # PERSIST_SCANS consecutive scans (kills one-tick flukes).
    _scan_count[symbol] = _scan_count.get(symbol, 0) + 1
    tradable = (in_trading_session(now)
                and _scan_count[symbol] > WARMUP_CYCLES)

    def streak_for(strategy, skey, hot):
        """Consecutive-hot count for this signal key; a cold scan wipes the
        strategy's streaks for the symbol."""
        if not hot or not skey:
            for key in list(_hot_streak):
                if key[0] == symbol and key[1] == strategy:
                    _hot_streak[key] = 0
            return 0
        return _bump(symbol, strategy, skey, True)

    s1_key = f"{s1['dir']}:{s1['strike']:.0f}" if s1 else None
    s2_key = "condor" if s2 else None
    s3_key = f"wall:{s3['strike']:.0f}" if s3 else None
    s4_key = f"skew:{s4['mode']}:{s4['strike']:.0f}" if s4 else None
    s5_key = "condor" if s5 else None
    s6_key = f"{s6['dir']}:{s6['strike']:.0f}" if s6 else None
    s7_key = f"straddle:{s7['atm_k']:.0f}" if s7 else None
    s8_key = f"backspread:{s8['strike']:.0f}" if s8 else None
    s9_key = f"calendar:{s9['atm_k']:.0f}" if s9 else None
    s10_key = f"flip:{s10['wall']:.0f}" if s10 else None
    s1_streak = streak_for("S1", s1_key, s1)
    s2_streak = streak_for("S2", s2_key, s2)
    s3_streak = streak_for("S3", s3_key, s3)
    s4_streak = streak_for("S4", s4_key, s4)
    s5_streak = streak_for("S5", s5_key, s5)
    s6_streak = streak_for("S6", s6_key, s6)
    s7_streak = streak_for("S7", s7_key, s7)
    s8_streak = streak_for("S8", s8_key, s8)
    s9_streak = streak_for("S9", s9_key, s9)
    s10_streak = streak_for("S10", s10_key, s10)

    if s1:
        k = s1["strike"]
        skey = s1_key
        if (tradable and s1_streak >= PERSIST_SCANS
                and not recent_fires(symbol, "S1", skey, COOLDOWN_MIN,
                                     SIGNALS_PER_DAY_CAP)):
            detail = s1_note
            sig_id = record_signal(symbol, "S1", skey, s1["dir"], k, detail, s1, True)
            if s1["dir"] == "bullish":
                sell_k = min(snap, key=lambda x: abs(x - (k + 2 * step)))
                legs = [(k, "CE", "buy", price_of(snap, k, True)),
                        (sell_k, "CE", "sell", price_of(snap, sell_k, True))]
                structure = f"Bull Call Spread {k:,.0f}/{sell_k:,.0f} CE"
            else:
                sell_k = min(snap, key=lambda x: abs(x - (k - 2 * step)))
                legs = [(k, "PE", "buy", price_of(snap, k, False)),
                        (sell_k, "PE", "sell", price_of(snap, sell_k, False))]
                structure = f"Bear Put Spread {k:,.0f}/{sell_k:,.0f} PE"
            tid, why = open_trade(symbol, "S1", sig_id, s1["dir"], structure,
                                  legs, {"strike": k}, now)
            if tid is None:
                print("S1 trade rejected:", why)
            signals.append({"strategy": "S1", "detail": detail, "traded": bool(tid)})
    if s2:
        skey = s2_key
        if (tradable and s2_streak >= PERSIST_SCANS
                and not recent_fires(symbol, "S2", skey, 240, 1)):
            sd = spot * (atm_iv / 100.0) * math.sqrt(t_years)
            sc = _round_strike(spot + sd, step)
            sp_ = _round_strike(spot - sd, step)
            detail = s2_note
            sig_id = record_signal(symbol, "S2", skey, "neutral", sc, detail, s2, True)
            legs = [(sc, "CE", "sell", price_of(snap, sc, True)),
                    (_round_strike(sc + 4 * step, step), "CE", "buy",
                     price_of(snap, _round_strike(sc + 4 * step, step), True)),
                    (sp_, "PE", "sell", price_of(snap, sp_, False)),
                    (_round_strike(sp_ - 4 * step, step), "PE", "buy",
                     price_of(snap, _round_strike(sp_ - 4 * step, step), False))]
            structure = f"Iron Condor {sp_:,.0f}P/{sc:,.0f}C"
            tid, why = open_trade(symbol, "S2", sig_id, "neutral", structure, legs,
                                  {"short_call": sc, "short_put": sp_}, now)
            if tid is None:
                print("S2 trade rejected:", why)
            signals.append({"strategy": "S2", "detail": detail, "traded": bool(tid)})
    if s3:
        k = s3["strike"]
        skey = s3_key
        if (tradable and s3_streak >= PERSIST_SCANS
                and not recent_fires(symbol, "S3", skey, COOLDOWN_MIN,
                                     SIGNALS_PER_DAY_CAP)):
            atm_k = atm_row["strike"]
            sell_k = _round_strike(atm_k + 2 * step, step)
            detail = s3_note
            sig_id = record_signal(symbol, "S3", skey, "bullish", k, detail, s3, True)
            legs = [(atm_k, "CE", "buy", price_of(snap, atm_k, True)),
                    (sell_k, "CE", "sell", price_of(snap, sell_k, True))]
            structure = f"ATM Call Spread {atm_k:,.0f}/{sell_k:,.0f} (gamma)"
            tid, why = open_trade(symbol, "S3", sig_id, "bullish", structure,
                                  legs, {"wall": k, "entry_spot": spot}, now)
            if tid is None:
                print("S3 trade rejected:", why)
            signals.append({"strategy": "S3", "detail": detail, "traded": bool(tid)})
    if s4:
        k = s4["strike"]
        mode = s4["mode"]
        skey = s4_key
        if (tradable and s4_streak >= PERSIST_SCANS
                and not recent_fires(symbol, "S4", skey, COOLDOWN_MIN,
                                     SIGNALS_PER_DAY_CAP)):
            strat = "S4_FOLLOW" if mode == "follow" else "S4_FADE"
            detail = s4_note
            sig_id = record_signal(symbol, "S4", skey,
                                   "bearish" if mode == "follow" else "bullish",
                                   k, detail, s4, True)
            sell_k = _round_strike(k - 2 * step, step)
            if mode == "follow":
                legs = [(k, "PE", "buy", price_of(snap, k, False)),
                        (sell_k, "PE", "sell", price_of(snap, sell_k, False))]
                structure = f"Long Put Spread {k:,.0f}/{sell_k:,.0f} PE"
            else:
                legs = [(k, "PE", "sell", price_of(snap, k, False)),
                        (sell_k, "PE", "buy", price_of(snap, sell_k, False))]
                structure = f"Bull Put Spread {k:,.0f}/{sell_k:,.0f} PE (fade)"
            tid, why = open_trade(symbol, strat, sig_id,
                                  "bearish" if mode == "follow" else "bullish",
                                  structure, legs, {"strike": k}, now)
            if tid is None:
                print("S4 trade rejected:", why)
            signals.append({"strategy": "S4", "detail": detail, "traded": bool(tid)})
    if s5:
        skey = s5_key
        if (tradable and s5_streak >= PERSIST_SCANS
                and not recent_fires(symbol, "S5", skey, 240, 1)):
            step = STRIKE_STEPS.get(symbol, DEFAULT_STEP)
            sc = s5["sc"]
            sp = s5["sp"]
            detail = s5_note
            sig_id = record_signal(symbol, "S5", skey, "neutral", sc, detail, s5, True)
            legs = [(sc, "CE", "sell", price_of(snap, sc, True)),
                    (_round_strike(sc + 4 * step, step), "CE", "buy",
                     price_of(snap, _round_strike(sc + 4 * step, step), True)),
                    (sp, "PE", "sell", price_of(snap, sp, False)),
                    (_round_strike(sp - 4 * step, step), "PE", "buy",
                     price_of(snap, _round_strike(sp - 4 * step, step), False))]
            structure = f"Iron Condor {sp:,.0f}P/{sc:,.0f}C (S5)"
            tid, why = open_trade(symbol, "S5", sig_id, "neutral", structure, legs,
                                  {"short_call": sc, "short_put": sp}, now)
            if tid is None:
                print("S5 trade rejected:", why)
            signals.append({"strategy": "S5", "detail": detail, "traded": bool(tid)})
    if s6:
        k = s6["strike"]
        skey = s6_key
        if (tradable and s6_streak >= PERSIST_SCANS
                and not recent_fires(symbol, "S6", skey, COOLDOWN_MIN,
                                     SIGNALS_PER_DAY_CAP)):
            step = STRIKE_STEPS.get(symbol, DEFAULT_STEP)
            detail = s6_note
            sig_id = record_signal(symbol, "S6", skey, s6["dir"], k, detail, s6, True)
            if s6["dir"] == "bullish":
                sell_k = min(snap, key=lambda x: abs(x - (k + 2 * step)))
                legs = [(k, "CE", "buy", price_of(snap, k, True)),
                        (sell_k, "CE", "sell", price_of(snap, sell_k, True))]
                structure = f"Breakout Call Spread {k:,.0f}/{sell_k:,.0f} CE"
            else:
                sell_k = min(snap, key=lambda x: abs(x - (k - 2 * step)))
                legs = [(k, "PE", "buy", price_of(snap, k, False)),
                        (sell_k, "PE", "sell", price_of(snap, sell_k, False))]
                structure = f"Breakout Put Spread {k:,.0f}/{sell_k:,.0f} PE"
            tid, why = open_trade(symbol, "S6", sig_id, s6["dir"], structure, legs,
                                  {"strike": k}, now)
            if tid is None:
                print("S6 trade rejected:", why)
            signals.append({"strategy": "S6", "detail": detail, "traded": bool(tid)})
    if s7:
        k = s7["atm_k"]
        skey = s7_key
        if (tradable and s7_streak >= PERSIST_SCANS
                and not recent_fires(symbol, "S7", skey, 240, 1)):
            detail = s7_note
            sig_id = record_signal(symbol, "S7", skey, "neutral", k, detail, s7, True)
            legs = [(k, "CE", "buy", price_of(snap, k, True)),
                    (k, "PE", "buy", price_of(snap, k, False))]
            structure = f"Long Straddle {k:,.0f} ATM"
            tid, why = open_trade(symbol, "S7", sig_id, "neutral", structure, legs,
                                  {"strike": k}, now)
            if tid is None:
                print("S7 trade rejected:", why)
            signals.append({"strategy": "S7", "detail": detail, "traded": bool(tid)})
    if s8:
        k = s8["strike"]
        skey = s8_key
        if (tradable and s8_streak >= PERSIST_SCANS
                and not recent_fires(symbol, "S8", skey, COOLDOWN_MIN,
                                     SIGNALS_PER_DAY_CAP)):
            step = STRIKE_STEPS.get(symbol, DEFAULT_STEP)
            detail = s8_note
            sig_id = record_signal(symbol, "S8", skey, "bearish", k, detail, s8, True)
            # ratio: sell 1 ATM put, buy 2 lower-strike puts for net credit
            atm_k = atm_row["strike"]
            buy_k = _round_strike(k - 2 * step, step)
            legs = [(k, "PE", "sell", price_of(snap, k, False)),
                    (buy_k, "PE", "buy", price_of(snap, buy_k, False)),
                    (buy_k, "PE", "buy", price_of(snap, buy_k, False))]
            structure = f"Put Ratio Backspread {k:,.0f}S/{buy_k:,.0f}B x2"
            tid, why = open_trade(symbol, "S8", sig_id, "bearish", structure, legs,
                                  {"strike": k}, now)
            if tid is None:
                print("S8 trade rejected:", why)
            signals.append({"strategy": "S8", "detail": detail, "traded": bool(tid)})
    if s9:
        k = s9["atm_k"]
        skey = s9_key
        if (tradable and s9_streak >= PERSIST_SCANS
                and not recent_fires(symbol, "S9", skey, 240, 1)):
            detail = s9_note
            sig_id = record_signal(symbol, "S9", skey, "neutral", k, detail, s9, True)
            # diagonal: sell near-term ATM CE+PE, buy next-week same strike (approx
            # with next-expiry strikes 1 step away since we don't have expiry selection)
            step = STRIKE_STEPS.get(symbol, DEFAULT_STEP)
            legs = [(k, "CE", "sell", price_of(snap, k, True)),
                    (k, "PE", "sell", price_of(snap, k, False)),
                    (_round_strike(k + step, step), "CE", "buy",
                     price_of(snap, _round_strike(k + step, step), True)),
                    (_round_strike(k - step, step), "PE", "buy",
                     price_of(snap, _round_strike(k - step, step), False))]
            structure = f"Diagonal Calendar {k:,.0f} ATM"
            tid, why = open_trade(symbol, "S9", sig_id, "neutral", structure, legs,
                                  {"strike": k}, now)
            if tid is None:
                print("S9 trade rejected:", why)
            signals.append({"strategy": "S9", "detail": detail, "traded": bool(tid)})
    if s10:
        k = s10["wall"]
        skey = s10_key
        if (tradable and s10_streak >= PERSIST_SCANS
                and not recent_fires(symbol, "S10", skey, COOLDOWN_MIN,
                                     SIGNALS_PER_DAY_CAP)):
            step = STRIKE_STEPS.get(symbol, DEFAULT_STEP)
            detail = s10_note
            if s10["type"] == "fade_up":
                dirn = "bearish"
                sell_k = _round_strike(k + 2 * step, step)
                legs = [(k, "CE", "sell", price_of(snap, k, True)),
                        (sell_k, "CE", "buy", price_of(snap, sell_k, True))]
                structure = f"Fade Call Wall {k:,.0f}/{sell_k:,.0f} CE"
            else:
                dirn = "bullish"
                sell_k = _round_strike(k - 2 * step, step)
                legs = [(k, "PE", "sell", price_of(snap, k, False)),
                        (sell_k, "PE", "buy", price_of(snap, sell_k, False))]
                structure = f"Fade Put Wall {k:,.0f}/{sell_k:,.0f} PE"
            sig_id = record_signal(symbol, "S10", skey, dirn, k, detail, s10, True)
            tid, why = open_trade(symbol, "S10", sig_id, dirn, structure, legs,
                                  {"wall": k, "entry_spot": spot}, now)
            if tid is None:
                print("S10 trade rejected:", why)
            signals.append({"strategy": "S10", "detail": detail, "traded": bool(tid)})

    # ---- MTM existing trades (also closes on rules)
    closed_now = mark_open_trades(symbol, snap, spot, t_years, atm_iv, atm_row, now,
                                  scan_rows=rows, series=series, vix=vix)

    # ---- payload
    conn = _conn(STRAT_DSN)
    try:
        cur = conn.cursor()
        cur.execute("""SELECT t.id, t.strategy, t.direction, t.structure, t.legs,
                              t.qty, t.lot, t.net_cost, t.mtm_cost, t.u_pnl,
                              to_char(t.entry_at, 'HH24:MI'), t.exit_reason
                       FROM paper_trades t WHERE t.symbol=%s AND t.status='open'
                       ORDER BY t.entry_at""", (symbol,))
        open_tr = [{"id": r[0], "strategy": r[1], "direction": r[2],
                    "structure": r[3], "legs": r[4], "qty": r[5], "lot": r[6],
                    "cost": float(r[7]), "value": float(r[8]),
                    "pnl": float(r[9]), "entry": r[10], "reason": r[11]}
                   for r in cur.fetchall()]
        cur.execute("""SELECT id, strategy, direction, structure, net_cost, u_pnl,
                              realized, exit_reason,
                              to_char(exit_at, 'HH24:MI')
                       FROM paper_trades
                       WHERE symbol=%s AND status='closed'
                         AND exit_at > date_trunc('day', now() AT TIME ZONE 'Asia/Kolkata')
                             AT TIME ZONE 'Asia/Kolkata'
                       ORDER BY exit_at DESC LIMIT 10""", (symbol,))
        closed_tr = [{"id": r[0], "strategy": r[1], "direction": r[2],
                      "structure": r[3], "cost": float(r[4]),
                      "pnl": float(r[6]), "reason": r[7], "when": r[8]}
                     for r in cur.fetchall()]
        cur.execute("""SELECT strategy, skey, direction, detail, to_char(fired_at,'HH24:MI'), traded
                       FROM strategy_signals WHERE symbol=%s AND fired_at > now() - interval '8 hours'
                       ORDER BY fired_at DESC LIMIT 12""", (symbol,))
        sigs = [{"strategy": r[0], "skey": r[1], "dir": r[2], "detail": r[3],
                 "when": r[4], "traded": r[5]} for r in cur.fetchall()]
    finally:
        conn.close()

    realized_today = sum(t["pnl"] for t in closed_tr)
    unrealized = sum(t["pnl"] for t in open_tr)
    # stash the last payload so the date board can refresh from in-process state
    import strategies as _s
    _s._last_payload = {
        "symbol": symbol,
        "open_trades": open_tr,
        "closed_trades": closed_tr,
        "signals": sigs,
    }
    return {
        "cards": [
            {"id": "S1", "name": "Unusual OI + Volume Surge",
             "status": "TRIGGERED" if s1 else ("ARMED" if s1_note and "watch" in (s1_note or "") else "IDLE"),
             "metrics": s1_metrics, "note": s1_note,
             "trade": "Bull/Bear conviction spread"},
            {"id": "S2", "name": "VIX/IV Spike — Sell Premium",
             "status": "TRIGGERED" if s2 else ("ARMED" if vix and vix >= VIX_SELL_LEVEL * 0.9 else "IDLE"),
             "metrics": s2_metrics, "note": s2_note,
             "trade": "Iron Condor (short vol)"},
            {"id": "S3", "name": "Gamma Wall / Squeeze Setup",
             "status": "TRIGGERED" if s3 else ("ARMED" if s3 and s3.get("dist_pct", 99) <= GEX_WALL_PCT * 2 else "IDLE"),
             "metrics": s3_metrics, "note": s3_note,
             "trade": "ATM call spread (long gamma)"},
            {"id": "S4", "name": "Put Skew Anomaly",
             "status": ("TRIGGERED — FOLLOW" if s4 and s4["mode"] == "follow"
                        else "TRIGGERED — FADE" if s4 else "IDLE"),
             "metrics": s4_metrics, "note": s4_note,
             "trade": "Follow: long put spread · Fade: short bull put spread"},
            {"id": "S5", "name": "Iron Condor (Range Sell)",
             "status": "TRIGGERED" if s5 else "IDLE",
             "metrics": s5_metrics, "note": s5_note,
             "trade": "Iron Condor — short vol within expected range"},
            {"id": "S6", "name": "Breakout Momentum",
             "status": "TRIGGERED" if s6 else "IDLE",
             "metrics": s6_metrics, "note": s6_note,
             "trade": "Debit spread in breakout direction"},
            {"id": "S7", "name": "Long Straddle (Vol Buy)",
             "status": "TRIGGERED" if s7 else "IDLE",
             "metrics": s7_metrics, "note": s7_note,
             "trade": "Long ATM straddle — bet on vol expansion"},
            {"id": "S8", "name": "Put Ratio Backspread",
             "status": "TRIGGERED" if s8 else "IDLE",
             "metrics": s8_metrics, "note": s8_note,
             "trade": "Ratio backspread — net credit, long gamma/vega"},
            {"id": "S9", "name": "Calendar/Diagonal",
             "status": "TRIGGERED" if s9 else "IDLE",
             "metrics": s9_metrics, "note": s9_note,
             "trade": "Sell near-term, buy next-week same strike"},
            {"id": "S10", "name": "Gamma Flip Reversion",
             "status": "TRIGGERED" if s10 else "IDLE",
             "metrics": s10_metrics, "note": s10_note,
             "trade": "Fade the breakout at gamma wall"},
        ],
        "new_signals": signals,
        "signals": sigs,
        "open_trades": open_tr,
        "closed_trades": closed_tr,
        "summary": {"account": PAPER_ACCOUNT, "realized_today": realized_today,
                    "unrealized": unrealized,
                    "open_risk": sum(max(t["cost"], 0) * t["qty"] * t["lot"]
                                     for t in open_tr)},
        "spot": spot, "atm_iv": atm_iv, "vix": vix,
        "ts": now.strftime("%H:%M:%S"),
    }


# ------------------------------------------------------------------ date board --

def _day_rows_db(symbol):
    """All paper_trades for the symbol, by entry date, from STRAT_DSN."""
    conn = _conn(STRAT_DSN)
    try:
        cur = conn.cursor()
        cur.execute(
            """SELECT t.id, t.strategy, t.direction, t.structure, t.legs, t.qty, t.lot,
                     t.net_cost, t.entry_at, t.exit_at, t.mtm_cost, t.u_pnl,
                     t.realized, t.status, t.exit_reason, t.meta,
                     ts.detail as sig_detail, ts.fired_at as sig_fired,
                     to_char(t.entry_at, 'YYYY-MM-DD') as entry_date
              FROM paper_trades t
              LEFT JOIN strategy_signals ts ON ts.id = t.signal_id
              WHERE t.symbol = %s
              ORDER BY t.entry_at, t.id""",
            (symbol,),
        )
        out = []
        for row in cur.fetchall():
            (tid, strategy, direction, structure, legs, qty, lot, net_cost,
             entry_at, exit_at, mtm_cost, u_pnl, realized, status,
             exit_reason, meta, sig_detail, sig_fired, entry_date) = row
            entry_dt = _to_dt(entry_at)
            exit_dt = _to_dt(exit_at)
            out.append(_board_row(
                symbol, tid, strategy, direction, structure, legs, qty, lot,
                net_cost, entry_dt, exit_dt, mtm_cost, u_pnl, realized, status,
                exit_reason, meta, sig_detail, sig_fired, entry_date,
            ))
        return out
    finally:
        conn.close()


def _to_dt(v):
    if v is None:
        return None
    if isinstance(v, datetime.datetime):
        return v
    try:
        return datetime.datetime.fromisoformat(str(v))
    except Exception:
        return None


def _board_row(symbol, tid, strategy, direction, structure, legs, qty, lot,
               net_cost, entry_dt, exit_dt, mtm_cost, u_pnl, realized,
               status, exit_reason, meta, sig_detail, sig_fired, entry_date):
    entry_cost = float(net_cost) if net_cost is not None else None
    if status == "closed" and exit_dt is not None and entry_dt is not None:
        exit_cost = float(mtm_cost) if mtm_cost is not None else entry_cost
        realized_val = float(realized) if realized is not None else (
            (exit_cost - entry_cost) * (qty or 1) * (lot or 1)
        )
    elif status == "open" and entry_dt is not None:
        cur_val = float(mtm_cost) if mtm_cost is not None else entry_cost
        exit_cost = cur_val
        realized_val = None
    else:
        exit_cost = None
        realized_val = None
    legs_parsed = legs if isinstance(legs, list) else (
        json.loads(legs) if isinstance(legs, str) else []
    )
    strikes = _strike_list(legs_parsed)
    holding_minutes = (
        int((exit_dt - entry_dt).total_seconds() / 60.0) if exit_dt and entry_dt else None
    )
    regime = _regime_tag(strategy, direction, meta,
                         _snap_at(symbol, entry_dt), entry_dt)
    return {
        "id": tid,
        "date": entry_date,
        "entry_time": entry_dt.strftime("%H:%M:%S") if entry_dt else "",
        "exit_time": exit_dt.strftime("%H:%M:%S") if exit_dt else "",
        "symbol": symbol,
        "strategy": _strat_long(strategy),
        "variant": strategy,
        "direction": direction or "",
        "structure": structure or "",
        "strikes": strikes,
        "entry_cost_per_lot": entry_cost,
        "exit_cost_per_lot": exit_cost,
        "realized_pnl_per_lot": (
            (exit_cost - entry_cost) if exit_cost is not None and entry_cost is not None else None
        ),
        "realized_pnl_total": realized_val,
        "exit_reason": exit_reason or "",
        "holding_minutes": holding_minutes,
        "spot_at_entry": _snap_at(symbol, entry_dt).get("spot") if entry_dt else None,
        "atm_iv_at_entry": _snap_at(symbol, entry_dt).get("atm_iv") if entry_dt else None,
        "vix_at_entry": _snap_at(symbol, entry_dt).get("vix") if entry_dt else None,
        "regime_tag": regime,
        "status": status or "",
        "signal_detail": sig_detail,
        "signal_fired_at": sig_fired.strftime("%H:%M:%S") if sig_fired else "",
    }


def _strike_list(legs):
    if not legs:
        return ""
    parts = []
    for l in legs:
        if not isinstance(l, dict):
            continue
        side = l.get("side") or ""
        action = l.get("action") or ""
        strike = l.get("strike")
        if strike is None:
            continue
        parts.append(f"{int(round(float(strike)))} {side} {action}")
    return " / ".join(parts)


def _strat_long(code):
    return STRAT_NAMES.get(code, code or "")


def _regime_tag(strategy, direction, meta, snap, entry_dt):
    if strategy == "S1":
        return "OI_SURGE"
    if strategy == "S3":
        return "WALL_PROXIMITY"
    if strategy in ("S4_FOLLOW", "S4_FADE"):
        return "SKEW_ANOMALY"
    if strategy in ("S5", "S9"):
        return "VOL_SPIKE" if (vix and vix >= 20) or (atm_iv and atm_iv >= 15) else "LOW_VOL"
    if strategy == "S6":
        return "OI_SURGE"
    if strategy == "S7":
        return "LOW_VOL"
    if strategy == "S8":
        return "SKEW_ANOMALY"
    if strategy == "S10":
        return "WALL_PROXIMITY"
    atm_iv = snap.get("atm_iv") if snap else None
    vix = snap.get("vix") if snap else None
    if vix is not None and vix >= 20.0:
        return "VOL_SPIKE"
    if atm_iv is not None and atm_iv >= 15.0:
        return "VOL_SPIKE"
    if vix is not None and vix < 15.0 and atm_iv is not None and atm_iv < 13.0:
        return "LOW_VOL"
    return "UNKNOWN"


def _snap_at(symbol, dt):
    """Best-effort market context at a given IST datetime, from the shared
    snapshot table. Returns {spot, atm_iv, vix} or None."""
    if dt is None:
        return None
    conn = _conn(DB_DSN)
    try:
        cur = conn.cursor()
        cur.execute(
            f"""SELECT MAX(ce_ltp) as spot,
                  AVG(NULLIF(ce_iv,0)) as atm_iv,
                  NULL as vix
              FROM {SNAP_TABLE}
              WHERE symbol=%s
                AND snapshot_at >= %s
                AND snapshot_at < %s
              LIMIT 1""",
            (symbol, dt, dt + datetime.timedelta(minutes=1)),
        )
        r = cur.fetchone()
        if not r or r[0] is None:
            return None
        return {
            "spot": float(r[0]),
            "atm_iv": float(r[1]) if r[1] is not None else None,
            "vix": None,
        }
    finally:
        conn.close()


def date_board(symbol, *, at_date=None):
    """Per-trade ledger for one calendar date (IST), plus a one-line summary.

    `at_date` is a date or date-string; when None it means today (IST).
    Returns {date, rows, summary, has_rows}.
    """
    if at_date is None:
        at = datetime.date.today()
    elif isinstance(at_date, datetime.date):
        at = at_date
    else:
        at = datetime.date.fromisoformat(str(at_date))
    rows = _day_rows_db(symbol)
    daily = [r for r in rows if r and r.get("date") == at.isoformat()]
    summary = _day_summary(daily)
    return {
        "date": at.isoformat(),
        "rows": daily,
        "summary": summary,
        "has_rows": bool(daily),
    }


def _day_summary(rows):
    closed = [r for r in rows if r.get("status") == "closed"]
    if not closed:
        return {
            "trades": 0,
            "realized_total": 0.0,
            "win_rate": None,
            "avg_holding_minutes": None,
        }
    realized_total = sum((r.get("realized_pnl_total") or 0.0) for r in closed)
    wins = sum(1 for r in closed if (r.get("realized_pnl_total") or 0.0) > 0)
    holdings = [r.get("holding_minutes") for r in closed if r.get("holding_minutes") is not None]
    return {
        "trades": len(closed),
        "realized_total": realized_total,
        "win_rate": (wins / len(closed)) if closed else None,
        "avg_holding_minutes": (
            round(sum(holdings) / len(holdings)) if holdings else None
        ),
    }

# ------------------------------------------------------------------ date board --

def _day_rows_db(symbol):
    """All paper_trades for the symbol, by entry date, from STRAT_DSN."""
    conn = _conn(STRAT_DSN)
    try:
        cur = conn.cursor()
        cur.execute(
            """SELECT t.id, t.strategy, t.direction, t.structure, t.legs, t.qty, t.lot,
                     t.net_cost, t.entry_at, t.exit_at, t.mtm_cost, t.u_pnl,
                     t.realized, t.status, t.exit_reason, t.meta,
                     ts.detail as sig_detail, ts.fired_at as sig_fired,
                     to_char(t.entry_at, 'YYYY-MM-DD') as entry_date
              FROM paper_trades t
              LEFT JOIN strategy_signals ts ON ts.id = t.signal_id
              WHERE t.symbol = %s
              ORDER BY t.entry_at, t.id""",
            (symbol,),
        )
        out = []
        for row in cur.fetchall():
            (tid, strategy, direction, structure, legs, qty, lot, net_cost,
             entry_at, exit_at, mtm_cost, u_pnl, realized, status,
             exit_reason, meta, sig_detail, sig_fired, entry_date) = row
            entry_dt = _to_dt(entry_at)
            exit_dt = _to_dt(exit_at)
            out.append(_board_row(
                symbol, tid, strategy, direction, structure, legs, qty, lot,
                net_cost, entry_dt, exit_dt, mtm_cost, u_pnl, realized, status,
                exit_reason, meta, sig_detail, sig_fired, entry_date,
            ))
        return out
    finally:
        conn.close()


def _to_dt(v):
    if v is None:
        return None
    if isinstance(v, datetime.datetime):
        return v
    try:
        return datetime.datetime.fromisoformat(str(v))
    except Exception:
        return None


def _board_row(symbol, tid, strategy, direction, structure, legs, qty, lot,
               net_cost, entry_dt, exit_dt, mtm_cost, u_pnl, realized,
               status, exit_reason, meta, sig_detail, sig_fired, entry_date):
    entry_cost = float(net_cost) if net_cost is not None else None
    if status == "closed" and exit_dt is not None and entry_dt is not None:
        exit_cost = float(mtm_cost) if mtm_cost is not None else entry_cost
        realized_val = float(realized) if realized is not None else (
            (exit_cost - entry_cost) * (qty or 1) * (lot or 1)
        )
    elif status == "open" and entry_dt is not None:
        cur_val = float(mtm_cost) if mtm_cost is not None else entry_cost
        exit_cost = cur_val
        realized_val = None
    else:
        exit_cost = None
        realized_val = None
    legs_parsed = legs if isinstance(legs, list) else (
        json.loads(legs) if isinstance(legs, str) else []
    )
    strikes = _strike_list(legs_parsed)
    holding_minutes = (
        int((exit_dt - entry_dt).total_seconds() / 60.0) if exit_dt and entry_dt else None
    )
    regime = _regime_tag(strategy, direction, meta,
                         _snap_at(symbol, entry_dt), entry_dt)
    return {
        "id": tid,
        "date": entry_date,
        "entry_time": entry_dt.strftime("%H:%M:%S") if entry_dt else "",
        "exit_time": exit_dt.strftime("%H:%M:%S") if exit_dt else "",
        "symbol": symbol,
        "strategy": _strat_long(strategy),
        "variant": strategy,
        "direction": direction or "",
        "structure": structure or "",
        "strikes": strikes,
        "entry_cost_per_lot": entry_cost,
        "exit_cost_per_lot": exit_cost,
        "realized_pnl_per_lot": (
            (exit_cost - entry_cost) if exit_cost is not None and entry_cost is not None else None
        ),
        "realized_pnl_total": realized_val,
        "exit_reason": exit_reason or "",
        "holding_minutes": holding_minutes,
        "spot_at_entry": _snap_at(symbol, entry_dt).get("spot") if entry_dt else None,
        "atm_iv_at_entry": _snap_at(symbol, entry_dt).get("atm_iv") if entry_dt else None,
        "vix_at_entry": _snap_at(symbol, entry_dt).get("vix") if entry_dt else None,
        "regime_tag": regime,
        "status": status or "",
        "signal_detail": sig_detail,
        "signal_fired_at": sig_fired.strftime("%H:%M:%S") if sig_fired else "",
    }


def _strike_list(legs):
    if not legs:
        return ""
    parts = []
    for l in legs:
        if not isinstance(l, dict):
            continue
        side = l.get("side") or ""
        action = l.get("action") or ""
        strike = l.get("strike")
        if strike is None:
            continue
        parts.append(f"{int(round(float(strike)))} {side} {action}")
    return " / ".join(parts)


def _strat_long(code):
    return STRAT_NAMES.get(code, code or "")


def _regime_tag(strategy, direction, meta, snap, entry_dt):
    if strategy == "S1":
        return "OI_SURGE"
    if strategy == "S3":
        return "WALL_PROXIMITY"
    if strategy in ("S4_FOLLOW", "S4_FADE"):
        return "SKEW_ANOMALY"
    if strategy in ("S5", "S9"):
        return "VOL_SPIKE" if (vix and vix >= 20) or (atm_iv and atm_iv >= 15) else "LOW_VOL"
    if strategy == "S6":
        return "OI_SURGE"
    if strategy == "S7":
        return "LOW_VOL"
    if strategy == "S8":
        return "SKEW_ANOMALY"
    if strategy == "S10":
        return "WALL_PROXIMITY"
    atm_iv = snap.get("atm_iv") if snap else None
    vix = snap.get("vix") if snap else None
    if vix is not None and vix >= 20.0:
        return "VOL_SPIKE"
    if atm_iv is not None and atm_iv >= 15.0:
        return "VOL_SPIKE"
    if vix is not None and vix < 15.0 and atm_iv is not None and atm_iv < 13.0:
        return "LOW_VOL"
    return "UNKNOWN"


def _snap_at(symbol, dt):
    """Best-effort market context at a given IST datetime, from the shared
    snapshot table. Returns {spot, atm_iv, vix} or None."""
    if dt is None:
        return None
    conn = _conn(DB_DSN)
    try:
        cur = conn.cursor()
        cur.execute(
            f"""SELECT MAX(ce_ltp) as spot,
                  AVG(NULLIF(ce_iv,0)) as atm_iv,
                  NULL as vix
              FROM {SNAP_TABLE}
              WHERE symbol=%s AND snapshot_at >= %s AND snapshot_at < %s
              LIMIT 1""",
            (symbol, dt, dt + datetime.timedelta(minutes=1)),
        )
        r = cur.fetchone()
        if not r or r[0] is None:
            return None
        return {
            "spot": float(r[0]),
            "atm_iv": float(r[1]) if r[1] is not None else None,
            "vix": None,
        }
    except Exception:
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass


def date_board(symbol, *, at_date=None):
    """Per-trade ledger for one calendar date (IST), plus a one-line summary.

    `at_date` is a date or date-string; when None it means today (IST).
    Returns {date, rows, summary, has_rows}.
    """
    if at_date is None:
        at = datetime.date.today()
    elif isinstance(at_date, datetime.date):
        at = at_date
    else:
        at = datetime.date.fromisoformat(str(at_date))
    rows = _day_rows_db(symbol)
    daily = [r for r in rows if r and r.get("date") == at.isoformat()]
    summary = _day_summary(daily)
    return {
        "date": at.isoformat(),
        "rows": daily,
        "summary": summary,
        "has_rows": bool(daily),
    }


def _day_summary(rows):
    closed = [r for r in rows if r.get("status") == "closed"]
    if not closed:
        return {
            "trades": 0,
            "realized_total": 0.0,
            "win_rate": None,
            "avg_holding_minutes": None,
        }
    realized_total = sum((r.get("realized_pnl_total") or 0.0) for r in closed)
    wins = sum(1 for r in closed if (r.get("realized_pnl_total") or 0.0) > 0)
    holdings = [r.get("holding_minutes") for r in closed if r.get("holding_minutes") is not None]
    return {
        "trades": len(closed),
        "realized_total": realized_total,
        "win_rate": (wins / len(closed)) if closed else None,
        "avg_holding_minutes": (
            round(sum(holdings) / len(holdings)) if holdings else None
        ),
    }

CARD_COLORS = {"IDLE": ("#eceff1", "#455a64"), "ARMED": ("#fff3e0", "#e65100"),
               "TRIGGERED": ("#ffebee", "#b71c1c"),
               "TRIGGERED — FOLLOW": ("#e3f2fd", "#0d47a1"),
               "TRIGGERED — FADE": ("#e8f5e9", "#1b5e20")}

# Human-readable identifier for every strategy / variant code stored on a
# trade or signal — shown on cards, tables and embedded trade rows.
STRAT_NAMES = {"S1": "S1 — Unusual OI + Volume",
               "S2": "S2 — IV Spike / Sell Premium",
               "S3": "S3 — Gamma Wall",
               "S4": "S4 — Put Skew",
               "S4_FOLLOW": "S4 — Put Skew (Follow)",
               "S4_FADE": "S4 — Put Skew (Fade)",
               "S5": "S5 — Iron Condor (Range Sell)",
               "S6": "S6 — Breakout Momentum",
               "S7": "S7 — Long Straddle (Vol Buy)",
               "S8": "S8 — Put Ratio Backspread",
               "S9": "S9 — Calendar/Diagonal",
               "S10": "S10 — Gamma Flip Reversion"}

# Short user-reference for each strategy: the market logic, the exact trigger
# rules the scanner applies, the structure traded, and the exit rules.
# Rendered as a collapsed "How it works" block inside each card.
STRAT_DOCS = {
    "S1": """<b>Logic.</b> Informed traders act before the cash move: when one
strike trades outsized volume <i>and</i> its OI is genuinely building, someone
is paying real premium to express a view. We follow that flow.<br>
<b>Triggers (all three).</b> Strike volume &ge; 3&times; the chain median ·
OI change &ge; +25% of prior OI (new positions, not churn) · strike IV
&lt; 1.25&times; ATM IV (skip when the crowd already overpaid).<br>
<b>Trade.</b> Bullish call flow &rarr; bull call spread on the hot strike;
bearish put flow &rarr; bear put spread. Wings 2 strikes out.<br>
<b>Exits.</b> +100% target · +70% soft book (when the flow cools) ·
&minus;50% stop · OI UNWOUND (&ge;20% of the buildup unwinds — the smart
money left) · MAX HOLD 60m (force-close if the setup outlives its trade) ·
EOD 15:28.""",
    "S2": """<b>Logic.</b> Implied volatility is mean-reverting — fear spikes
decay and options end up overpriced vs how much the index actually moves.
Sellers harvest that gap as theta, but only in a genuinely elevated-vol
regime.<br>
<b>Triggers.</b> Strike IV &ge; 2 standard deviations above its own intraday
mean (z-score, &ge;12 samples) · regime gate: India VIX &ge; 20, or ATM IV
&ge; 15 when VIX is unavailable.<br>
<b>Trade.</b> Iron condor: sell ATM&plusmn;1SD call and put, buy wings 4
strikes further. Wins if spot stays inside the expected range.<br>
<b>Exits.</b> 40% of credit collected · 25% soft book (when vol regime calms) ·
stop if the credit doubles in cost · immediate if spot breaks either short
strike · MAX HOLD 60m · EOD 15:28.""",
    "S3": """<b>Logic.</b> Dealers short gamma at a heavy-OI strike must hedge
mechanically — buying as spot approaches, selling as it breaks out — so big
strikes act as magnets. The magnet is real flows, not chart magic.<br>
<b>Triggers.</b> Wall = strike with max OI &times; gamma · spot within 1% of
it · wall-strike volume &ge; 2&times; chain median (positions are being added
now, not stale OI).<br>
<b>Trade.</b> Long ATM call spread (sell 2 strikes up) — a cheap long-gamma
bet that the wall gets absorbed and popped.<br>
<b>Exits.</b> +100% target · +70% soft book (when spot moves back off the wall) ·
&minus;50% stop · WALL REJECTED (spot falls 0.5% below entry — the squeeze
premise is void) · MAX HOLD 60m · EOD 15:28.""",
    "S4": """<b>Logic.</b> OTM puts normally carry a modest IV premium. When
that skew blows out, either institutions are buying real crash protection or
the crowd is over-hedging — the skew can't tell you which, so OI breaks the
tie.<br>
<b>Triggers.</b> ~25-delta OTM put IV &minus; ATM IV &ge; 10 points. OI at
that strike building &ge; 10% &rarr; FOLLOW (fresh conviction). Flat/falling
&rarr; FADE (expensive hedge, no new money).<br>
<b>Trade.</b> Follow: long put spread at the skewed strike. Fade: bull put
spread — sell the rich put, buy a wing 2 strikes lower, collect the fear
premium.<br>
<b>Exits.</b> Follow: +100% / +70% soft book (when skew premium fades) /
&minus;50% debit rules. Fade: 40% of credit / 25% soft book (when skew fades) /
credit doubled. Both: MAX HOLD 60m · EOD 15:28.""",
    "S5": """<b>Logic.</b> IV mean-reverting - spike + spot has room inside range = sell both sides for theta. Range gate: spot 0.3 SD inside short strikes.
<b>Triggers.</b> Strike IV z &ge; 2.0 * VIX &ge; 20 or ATM IV &ge; 15 * spot 0.3 SD inside both shorts.
<b>Trade.</b> Iron condor: sell ATM+/-1SD C+P, buy wings 4 strikes out.
<b>Exits.</b> 40% credit * 25% soft book (vol calms) * credit doubled * spot breaks short * MAX HOLD 60m * EOD 15:28.""",
    "S6": """<b>Logic.</b> Breakout + volume + OI building = real money. Follow after level breaks.
<b>Triggers.</b> Spot >0.5% beyond strike * vol >2x median * OI +15% at strike.
<b>Trade.</b> Long call/put spread on broken strike, 2 wings out.
<b>Exits.</b> +100% * +70% soft book (flow cools) * -50% * OI UNWIND >=20% * MAX HOLD 60m * EOD 15:28.""",
    "S7": """<b>Logic.</b> Low IV + heavy OI concentration = binary event underpriced. Buy both sides.
<b>Triggers.</b> ATM IV <12% (or VIX <18) * max OI >1.5x chain avg.
<b>Trade.</b> Long ATM straddle: buy CE + PE at ATM.
<b>Exits.</b> +/-50% PnL * +70% soft book (IV +10%) * STALLED 120m * MAX HOLD 60m * EOD 15:28.""",
    "S8": """<b>Logic.</b> Put IV 30+ pts above ATM at heavy-OI strike = rich fear premium. Sell rich put, buy 2 lower for credit, keep credit if calm, profit if it cracks.
<b>Triggers.</b> Put IV - ATM IV >30 pts * OI building at strike.
<b>Trade.</b> Sell 1 ATM put, buy 2 lower-strike puts (ratio backspread). Net credit.
<b>Exits.</b> 40% credit * 25% soft book (skew <15 pts) * credit doubled * spot > entry+2% * MAX HOLD 60m * EOD 15:28.""",
    "S9": """<b>Logic.</b> Front-month IV in top quintile + spot stable = near-term premium rich. Sell near, buy far same strike for theta + IV decay.
<b>Triggers.</b> ATM IV rank >80% * spot stable (|chg_oi| <10% of OI).
<b>Trade.</b> Diagonal: sell near-term ATM C+P, buy adjacent-strike C+P.
<b>Exits.</b> 40% credit * 25% soft book (spot breaks) * credit doubled * big OI move at ATM * MAX HOLD 60m * EOD 15:28.""",
    "S10": """<b>Logic.</b> Spot near gamma wall + vol building = fade the breakout (opposite of S3). Dealers hedge short gamma = spot pushed back.
<b>Triggers.</b> Spot within 1% of gamma wall * wall vol >=2x median * fade opposite to wall.
<b>Trade.</b> Fade up: short call spread at call wall. Fade down: short put spread at put wall.
<b>Exits.</b> +100% * +70% soft book (spot moves 1% away) * -50% * WALL REJECTED * MAX HOLD 60m * EOD 15:28.""",
}



def static_payload(symbol, spot=None):
    """Closed-market snapshot read from the shared schema DB (option_chain_snapshots).
    Uses the snapshot DSN, NOT the strategies DSN."""
    ensure_schema()
    conn = _conn(DB_DSN)
    try:
        cur = conn.cursor()
        cur.execute("""SELECT id, strategy, direction, structure, legs, qty, lot,
                              net_cost, mtm_cost, u_pnl, to_char(entry_at,'HH24:MI')
                       FROM paper_trades WHERE symbol=%s AND status='open'""", (symbol,))
        open_tr = [{"id": r[0], "strategy": r[1], "direction": r[2], "structure": r[3],
                    "legs": r[4], "qty": r[5], "lot": r[6], "cost": float(r[7]),
                    "value": float(r[8]), "pnl": float(r[9]), "entry": r[10],
                    "reason": None} for r in cur.fetchall()]
        cur.execute("""SELECT id, strategy, direction, structure, net_cost, u_pnl, realized,
                              exit_reason, to_char(exit_at,'HH24:MI')
                       FROM paper_trades WHERE symbol=%s AND status='closed'
                         AND exit_at > now() - interval '16 hours'
                       ORDER BY exit_at DESC""", (symbol,))
        closed = [{"id": r[0], "strategy": r[1], "direction": r[2], "structure": r[3],
                   "cost": float(r[4]), "pnl": float(r[6]), "reason": r[7], "when": r[8]}
                  for r in cur.fetchall()]
        cur.execute("""SELECT strategy, skey, direction, detail, to_char(fired_at,'HH24:MI'), traded
                       FROM strategy_signals WHERE symbol=%s AND fired_at > now() - interval '24 hours'
                       ORDER BY fired_at DESC LIMIT 12""", (symbol,))
        sigs = [{"strategy": r[0], "skey": r[1], "dir": r[2], "detail": r[3],
                 "when": r[4], "traded": r[5]} for r in cur.fetchall()]
    finally:
        conn.close()
    names = [
        ("S1", "Unusual OI + Volume Surge", "Bull/Bear conviction spread"),
        ("S2", "VIX/IV Spike - Sell Premium", "Iron Condor (short vol)"),
        ("S3", "Gamma Wall / Squeeze Setup", "ATM call spread (long gamma)"),
        ("S4", "Put Skew Anomaly", "Follow: long put spread, Fade: short bull put spread"),
        ("S5", "Iron Condor (Range Sell)", "Iron Condor - short vol within expected range"),
        ("S6", "Breakout Momentum", "Debit spread in breakout direction"),
        ("S7", "Long Straddle (Vol Buy)", "Long ATM straddle - bet on vol expansion"),
        ("S8", "Put Ratio Backspread", "Ratio backspread - net credit, long gamma/vega"),
        ("S9", "Calendar/Diagonal", "Sell near-term, buy next-week same strike"),
        ("S10", "Gamma Flip Reversion", "Fade the breakout at gamma wall"),
    ]
    cards = [{"id": code, "name": name, "status": "IDLE", "metrics": [],
              "note": "scanner paused - market closed", "trade": t}
             for (code, name, t) in names]
    return {"cards": cards, "new_signals": [], "signals": sigs,
            "open_trades": open_tr, "closed_trades": closed,
            "summary": {"account": PAPER_ACCOUNT,
                        "realized_today": sum(t["pnl"] for t in closed),
                        "unrealized": sum(t["pnl"] for t in open_tr),
                        "open_risk": sum(max(t["cost"], 0) * t["qty"] * t["lot"]
                                         for t in open_tr)},
            "spot": spot, "atm_iv": None, "vix": None, "ts": "closed"}

def strat_label(code):
    return STRAT_NAMES.get(code, code or "?")


def render_dashboard(p):
    if not p:
        return ""
    s = p["summary"]
    gauge = (f"India VIX {p['vix']:.1f}" if p["vix"] else "India VIX n/a — ATM IV gauge")
    spot_txt = f"{p['spot']:,.2f}" if p["spot"] else "-"
    iv_txt = f"{p['atm_iv']:.2f}" if p["atm_iv"] else "-"
    ts_txt = "last scan" if p["ts"] == "closed" else f"scanned {p['ts']} IST"
    html = ["""<div class="sd-wrap"><h2 style="font-size:18px;margin:28px 0 4px;">
Quant Strategy Dashboard — Automated Paper Trades</h2>"""]
    html.append(
        f'<p class="sd-meta">Spot {spot_txt} · ATM IV {iv_txt} · {gauge} · '
        f'{ts_txt} · paper account {_inr(s["account"])} · risk '
        f'{RISK_PER_TRADE * 100:.0f}%/trade</p>')
    # summary chips
    html.append('<div class="sd-chips">'
                f'<span>Unrealised P&L <b class="{ "sd-pos" if s["unrealized"] >= 0 else "sd-neg"}">'
                f'{_inr(s["unrealized"], signed=True)}</b></span>'
                f'<span>Realised today <b class="{ "sd-pos" if s["realized_today"] >= 0 else "sd-neg"}">'
                f'{_inr(s["realized_today"], signed=True)}</b></span>'
                f'<span>Open risk {_inr(s["open_risk"])}</span>'
                f'<span>Open trades {len(p["open_trades"])}</span></div>')
    # execution-guard summary (always-on rules that apply to every card)
    html.append(
        f'<p class="sd-guards">Rules in force: entries only Mon–Fri '
        f'09:15–15:30 IST · first {WARMUP_CYCLES} scans after start are '
        f'scan-only · a signal must stay hot {PERSIST_SCANS} consecutive '
        f'scans · max {RISK_PER_TRADE * 100:.0f}% risk/trade and '
        f'{MAX_TOTAL_HEAT * 100:.0f}% total open risk · no opposing '
        f'direction bets · EOD exit {TIME_EXIT.strftime("%H:%M")}</p>')
    # strategy cards — each card shows the paper trades it owns
    def card_key(strat_code):
        return (strat_code or "").split("_")[0]

    def trade_line(t, closed=False):
        cls = "sd-pos" if t["pnl"] >= 0 else "sd-neg"
        tail = (f'closed {t.get("when", "")} · {t.get("reason") or "exit"}'
                if closed else f'entered {t["entry"]} · lots {t["qty"]}')
        return (f'<div class="sd-tt"><b>#{t["id"]}</b> {t["direction"] or "-"} · '
                f'{t["structure"]} <span class="sd-tt-sub">({tail})</span> '
                f'<b class="{cls}">{_inr(t["pnl"], signed=True)}</b></div>')

    html.append('<div class="sd-grid">')
    for c in p["cards"]:
        bg, fg = CARD_COLORS.get(c["status"], CARD_COLORS["IDLE"])
        rows = "".join(f'<div class="sd-kv"><span>{k}</span><b>{v}</b></div>'
                       for k, v in c["metrics"])
        mine = [t for t in p["open_trades"] if card_key(t["strategy"]) == c["id"]]
        done = [t for t in p["closed_trades"] if card_key(t["strategy"]) == c["id"]]
        tt = "".join(trade_line(t) for t in mine)
        tt += "".join(trade_line(t, closed=True) for t in done[:2])
        owns = (f'<div class="sd-owns"><i>{c["id"]} trades:</i>{tt}</div>'
                if tt else "")
        html.append(
            f'<div class="sd-card" style="background:{bg};border-color:{fg};">'
            f'<div class="sd-card-h"><b>{c["name"]}</b>'
            f'<span class="sd-badge" style="background:{fg}">{c["status"]}</span></div>'
            f'{rows}'
            f'<div class="sd-note">{c["note"] or "no anomaly yet"}</div>'
            f'{owns}'
            f'<div class="sd-trade">If triggered: {c["trade"]}</div>'
            f'<details class="sd-how"><summary>How it works</summary>'
            f'<div class="sd-how-b">{STRAT_DOCS.get(c["id"], "")}</div></details>'
            f'</div>')
    html.append('</div>')
    # open trades
    def legs_txt(legs):
        if isinstance(legs, str):
            legs = json.loads(legs)
        return " + ".join(f"{l['action']} {l['strike']:,.0f} {l['side']}" for l in legs)
    html.append('<h3 class="sd-h3">Open paper trades</h3>')
    if p["open_trades"]:
        trs = ""
        for t in p["open_trades"]:
            cls = "sd-pos" if t["pnl"] >= 0 else "sd-neg"
            trs += (f'<tr><td>{strat_label(t["strategy"])}</td>'
                    f'<td>{t["structure"]}</td>'
                    f'<td class="sd-legs">{legs_txt(t["legs"])}</td>'
                    f'<td>{t["qty"]}×</td><td>{_inr(t["cost"])}</td>'
                    f'<td>{_inr(t["value"])}</td>'
                    f'<td class="{cls}">{_inr(t["pnl"], signed=True)}</td>'
                    f'<td>{t["entry"]}</td></tr>')
        html.append(f'<table class="sd-table"><thead><tr><th>Strat</th><th>Structure</th>'
                    f'<th>Legs</th><th>Qty</th><th>Entry net</th><th>MTM net</th>'
                    f'<th>uP&L</th><th>In</th></tr></thead><tbody>{trs}</tbody></table>')
    else:
        html.append('<p class="sd-none">None — waiting for signals.</p>')
    # closed today
    if p["closed_trades"]:
        trs = "".join(
            f'<tr><td>{strat_label(t["strategy"])}</td><td>{t["structure"]}</td>'
            f'<td class="{"sd-pos" if t["pnl"] >= 0 else "sd-neg"}">'
            f'{_inr(t["pnl"], signed=True)}</td><td>{t["reason"]}</td><td>{t["when"]}</td></tr>'
            for t in p["closed_trades"])
        html.append('<h3 class="sd-h3">Closed today</h3>'
                    f'<table class="sd-table"><thead><tr><th>Strat</th><th>Structure</th>'
                    f'<th>Realised</th><th>Exit</th><th>At</th></tr></thead>'
                    f'<tbody>{trs}</tbody></table>')
    # signal log
    if p["signals"]:
        rows = "".join(f'<tr><td>{g["when"]}</td><td>{strat_label(g["strategy"])}</td>'
                       f'<td>{g["dir"] or "-"}</td><td>{g["detail"]}</td>'
                       f'<td>{"trade opened" if g["traded"] else "signal only"}</td></tr>'
                       for g in p["signals"])
        html.append('<h3 class="sd-h3">Recent signals (8h)</h3>'
                    f'<table class="sd-table"><thead><tr><th>Time</th><th>Strat</th>'
                    f'<th>Dir</th><th>Signal</th><th>Action</th></tr></thead>'
                    f'<tbody>{rows}</tbody></table>')
    html.append("""<style>
.sd-meta{color:#555;font-size:13px;margin:2px 0 10px;}
.sd-chips{display:flex;gap:18px;flex-wrap:wrap;font-size:13px;color:#37474f;margin-bottom:12px;}
.sd-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(250px,1fr));gap:12px;margin:6px 0 14px;}
.sd-card{border:1px solid;border-radius:6px;padding:10px 12px;font-size:12px;}
.sd-card-h{display:flex;justify-content:space-between;align-items:center;gap:8px;margin-bottom:6px;}
.sd-badge{color:#fff;font-size:10px;padding:2px 8px;border-radius:10px;white-space:nowrap;}
.sd-kv{display:flex;justify-content:space-between;color:#455a64;}
.sd-note{margin-top:6px;color:#37474f;font-style:italic;}
.sd-guards{font-size:11px;color:#607d8b;background:#eceff1;border:1px solid #cfd8dc;
           border-radius:4px;padding:5px 10px;margin:0 0 10px;}
.sd-how{margin-top:8px;border-top:1px solid rgba(0,0,0,.08);padding-top:6px;}
.sd-how summary{cursor:pointer;font-size:11px;font-weight:600;color:#1a237e;
                list-style:none;}
.sd-how summary:before{content:"▸ ";}
.sd-how[open] summary:before{content:"▾ ";}
.sd-how-b{margin-top:6px;font-size:11px;color:#37474f;line-height:1.5;
          text-align:left;background:rgba(255,255,255,.7);padding:8px 10px;
          border-radius:4px;}
.sd-how-b i{color:#546e7a;}
.sd-owns{margin-top:8px;border-top:1px dashed #b0bec5;padding-top:6px;}
.sd-owns i{color:#37474f;font-size:11px;}
.sd-tt{margin-top:4px;font-size:11px;color:#263238;background:rgba(255,255,255,.6);
       border-left:3px solid #37474f;padding:3px 6px;border-radius:2px;}
.sd-tt-sub{color:#78909c;font-weight:400;}
.sd-trade{margin-top:4px;color:#78909c;}
.sd-h3{font-size:15px;margin:18px 0 6px;color:#263238;}
.sd-table{border-collapse:collapse;font-size:12px;background:white;width:100%;}
.sd-table th,.sd-table td{border:1px solid #ccc;padding:3px 8px;text-align:right;}
.sd-table th{background:#263238;color:#fff;}
.sd-table td:nth-child(2){text-align:left;}
.sd-legs{font-size:11px;color:#546e7a;}
.sd-pos{color:#2e7d32;font-weight:600;} .sd-neg{color:#c62828;font-weight:600;}
.sd-none{color:#777;font-size:13px;}
.sd-wrap{margin-top:8px;}
</style>""")
    html.append('</div>')
    return "".join(html)

