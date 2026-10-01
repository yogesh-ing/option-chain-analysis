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
import threading

DB_DSN = os.environ.get("OCA_DB_DSN",
                        "postgresql://postgres:postgres@localhost:5432/postgres")
SNAP_TABLE = "option_chain_snapshots"

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
ATM_IV_SELL_LEVEL = 13.5        # S2 fallback gauge when VIX is unavailable
GEX_WALL_PCT = 1.0              # S3: spot within 1% of gamma wall
GEX_WALL_VOL_MULT = 2.0         # S3: wall-strike volume building (2x median)
SKEW_PREMIUM = 10.0             # S4: OTM-put IV minus ATM IV > 10 pts = anomaly
SKEW_OI_FOLLOW_FRAC = 0.10      # S4: put OI building > 10% -> "follow"
COOLDOWN_MIN = 60               # minutes before same signal key can re-fire
SIGNALS_PER_DAY_CAP = 3         # per strategy key, hard cap
TIME_EXIT = datetime.time(15, 28)

_schema_done = False
_cache = {"hist": {}, "hist_at": 0.0, "vix": None, "vix_at": 0.0}
_cache_lock = threading.Lock()


def configure(dsn=None, snap_table=None, account=None):
    global DB_DSN, SNAP_TABLE, PAPER_ACCOUNT
    if dsn:
        DB_DSN = dsn
    if snap_table:
        SNAP_TABLE = snap_table
    if account:
        PAPER_ACCOUNT = float(account)


def _conn():
    import psycopg2
    conn = psycopg2.connect(DB_DSN)
    conn.autocommit = True
    return conn


def ensure_schema():
    global _schema_done
    if _schema_done:
        return
    conn = _conn()
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
    conn = _conn()
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
    conn = _conn()
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
    conn = _conn()
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
        # max loss per lot (premium/width already × lot multiplier below)
        if cost > 0:
            max_loss = cost
        else:
            width = max(l["strike"] for l in priced) - min(l["strike"] for l in priced)
            max_loss = max(width - abs(cost), 0.5)
        lots = max(1, min(MAX_LOTS, int((PAPER_ACCOUNT * RISK_PER_TRADE)
                                        // (max_loss * lot))))
        cur.execute("""INSERT INTO paper_trades
            (symbol, strategy, signal_id, direction, structure, legs, qty, lot,
             net_cost, entry_at, mtm_cost, u_pnl, status, meta)
            VALUES (%s,%s,%s,%s,%s,%s::jsonb,%s,%s,%s,now(),%s,0,'open',%s::jsonb)
            RETURNING id""",
                    (symbol, strategy, signal_id, direction, structure,
                     json.dumps(priced), lots, lot, round(cost, 2), round(cost, 2),
                     json.dumps(meta)))
        return cur.fetchone()[0], None
    finally:
        conn.close()


# --------------------------------------------------------------- mark to mkt --
def mark_open_trades(symbol, snap, spot, t_years, atm_iv, now):
    """MTM all open trades; apply exits. Returns list of updated trade dicts."""
    conn = _conn()
    updated = []
    try:
        cur = conn.cursor()
        cur.execute("""SELECT id, strategy, direction, structure, legs, qty, lot,
                              net_cost, meta, signal_id
                       FROM paper_trades WHERE symbol=%s AND status='open'""",
                    (symbol,))
        rows = cur.fetchall()
        for (tid, strat, direction, structure, legs, qty, lot, cost, meta, sig_id) in rows:
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
            if strat in ("S1", "S3", "S4_FOLLOW") and cost > 0:      # debit
                if value - cost >= cost:
                    reason = "TARGET +100%"
                elif value - cost <= -0.5 * cost:
                    reason = "STOP -50%"
            elif strat in ("S2", "S4_FADE") and credit:              # credit
                if value - cost >= 0.4 * credit:
                    reason = "TARGET 40% OF CREDIT"
                elif value - cost <= -1.0 * credit:
                    reason = "STOP CREDIT DOUBLED"
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


def _ratio_pcr(rows):
    ce = sum(r["ce_oi"] or 0 for r in rows)
    pe = sum(r["pe_oi"] or 0 for r in rows)
    return f"{pe / ce:.2f}" if ce else "-"


# ----------------------------------------------------------------- run cycle --
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

    if s1:
        k = s1["strike"]
        skey = f"{s1['dir']}:{k:.0f}"
        if not recent_fires(symbol, "S1", skey, COOLDOWN_MIN, SIGNALS_PER_DAY_CAP):
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
        skey = "condor"
        if not recent_fires(symbol, "S2", skey, 240, 1):
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
        skey = f"wall:{k:.0f}"
        if not recent_fires(symbol, "S3", skey, COOLDOWN_MIN, SIGNALS_PER_DAY_CAP):
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
        skey = f"skew:{mode}:{k:.0f}"
        if not recent_fires(symbol, "S4", skey, COOLDOWN_MIN, SIGNALS_PER_DAY_CAP):
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

    # ---- MTM existing trades (also closes on rules)
    closed_now = mark_open_trades(symbol, snap, spot, t_years, atm_iv, now)

    # ---- payload
    conn = _conn()
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


# ------------------------------------------------------------------ rendering --
CARD_COLORS = {"IDLE": ("#eceff1", "#455a64"), "ARMED": ("#fff3e0", "#e65100"),
               "TRIGGERED": ("#ffebee", "#b71c1c"),
               "TRIGGERED — FOLLOW": ("#e3f2fd", "#0d47a1"),
               "TRIGGERED — FADE": ("#e8f5e9", "#1b5e20")}


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
    # strategy cards
    html.append('<div class="sd-grid">')
    for c in p["cards"]:
        bg, fg = CARD_COLORS.get(c["status"], CARD_COLORS["IDLE"])
        rows = "".join(f'<div class="sd-kv"><span>{k}</span><b>{v}</b></div>'
                       for k, v in c["metrics"])
        html.append(
            f'<div class="sd-card" style="background:{bg};border-color:{fg};">'
            f'<div class="sd-card-h"><b>{c["name"]}</b>'
            f'<span class="sd-badge" style="background:{fg}">{c["status"]}</span></div>'
            f'{rows}'
            f'<div class="sd-note">{c["note"] or "no anomaly yet"}</div>'
            f'<div class="sd-trade">If triggered: {c["trade"]}</div></div>')
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
            trs += (f'<tr><td>{t["strategy"]}</td><td>{t["structure"]}</td>'
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
            f'<tr><td>{t["strategy"]}</td><td>{t["structure"]}</td>'
            f'<td class="{"sd-pos" if t["pnl"] >= 0 else "sd-neg"}">'
            f'{_inr(t["pnl"], signed=True)}</td><td>{t["reason"]}</td><td>{t["when"]}</td></tr>'
            for t in p["closed_trades"])
        html.append('<h3 class="sd-h3">Closed today</h3>'
                    f'<table class="sd-table"><thead><tr><th>Strat</th><th>Structure</th>'
                    f'<th>Realised</th><th>Exit</th><th>At</th></tr></thead>'
                    f'<tbody>{trs}</tbody></table>')
    # signal log
    if p["signals"]:
        rows = "".join(f'<tr><td>{g["when"]}</td><td>{g["strategy"]}</td>'
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


def static_payload(symbol, spot=None):
    """For the frozen closed-market page: DB-only view, no new scanning."""
    ensure_schema()
    conn = _conn()
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
    cards = [{"id": f"S{i}", "name": n, "status": "IDLE", "metrics": [],
              "note": "scanner paused — market closed", "trade": t}
             for i, (n, t) in enumerate([
                 ("Unusual OI + Volume Surge", "Bull/Bear conviction spread"),
                 ("VIX/IV Spike — Sell Premium", "Iron Condor (short vol)"),
                 ("Gamma Wall / Squeeze Setup", "ATM call spread (long gamma)"),
                 ("Put Skew Anomaly", "Follow: long put spread · Fade: short bull put spread")], 1)]
    return {"cards": cards, "new_signals": [], "signals": sigs,
            "open_trades": open_tr, "closed_trades": closed,
            "summary": {"account": PAPER_ACCOUNT,
                        "realized_today": sum(t["pnl"] for t in closed),
                        "unrealized": sum(t["pnl"] for t in open_tr),
                        "open_risk": sum(max(t["cost"], 0) * t["qty"] * t["lot"]
                                         for t in open_tr)},
            "spot": spot, "atm_iv": None, "vix": None, "ts": "closed"}
