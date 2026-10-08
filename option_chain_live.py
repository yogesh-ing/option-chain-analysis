"""Live NSE Option Chain: 10 ITM + ATM + 10 OTM (21 strikes, ATM centered).

- Serves http://localhost:8899 with a Refresh button, interval dropdown
  (3s / 10s / 30s / 60s, default 60s) and a column picker (LTP, Chg, OI,
  Chg OI, Volume, IV, Delta, Gamma, Theta, Vega per side)
- Computes Black-Scholes greeks and persists everything to PostgreSQL
- Scans every snapshot with the quant strategy engine (strategies.py) and
  renders the Strategy Dashboard with automated paper trades

Usage: python option_chain_live.py [SYMBOL] [STRIKE]   (defaults: NIFTY, 22700)
"""
import datetime
import json
import math
import os
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse, parse_qs

import psycopg2
import requests

import strategies
import observation_board

HTML_PATH = Path(__file__).parent / "option_chain_live.html"
from config import cfg

HOST = cfg.server_host
PORT = cfg.server_port
DB_DSN = cfg.db_dsn
DB_TABLE = cfg.snap_table
# Strategies state (signals + paper_trades) lives in its own DB so the live
# option-chain snapshot DB can be shared without coupling the two concerns.
STRAT_DSN = cfg.strat_dsn
strategies.configure(dsn=DB_DSN, strat_dsn=STRAT_DSN, snap_table=DB_TABLE)

HEADERS = {
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                  'Chrome/130.0.0.0 Safari/537.36',
    'accept-language': 'en,gu;q=0.9,hi;q=0.8',
    'accept-encoding': 'gzip, deflate'}

URL_OC = "https://www.nseindia.com/option-chain"
URL_CONTRACT = "https://www.nseindia.com/api/option-chain-contract-info?symbol="
URL_CHAIN = ("https://www.nseindia.com/api/option-chain-v3?"
             "type={mode}&symbol={symbol}&expiry={expiry}")

ITM_COUNT = 10   # strikes below ATM shown on both sides
OTM_COUNT = 10   # strikes above ATM shown on both sides  (10 + ATM + 10 = 21 rows)
CHART_STRIKES = 5   # strikes compared in the chart section below the table
CHART_DEFAULTS = ["ltp", "oi", "pcr"]   # initially selected metric pills
INTERVALS = [3, 10, 30, 60]  # seconds; 60 is default
RISK_FREE_RATE = 0.065  # annualised, adjust if needed

# Market hours (IST) — F&O trades till 15:40 since Aug 2026 (CAS reform);
# post-market OI reconciliation lands ~15:40-16:00, so we poll till 16:05.
MARKET_OPEN_TIME = datetime.time(9, 15)
MARKET_CLOSE_TIME = datetime.time(15, 40)   # F&O trading close
COLLECT_END_TIME = datetime.time(16, 5)     # keep polling for reconciliation
URL_MARKET_STATUS = "https://www.nseindia.com/api/marketStatus"

# Session used by the strategy engine for the India VIX poll (best effort)
VIX_BUNDLE = {"session": None, "cookies": None}


def fetch_json_nse(url):
    """GET a NSE JSON endpoint with the shared cookie jar; None on failure."""
    if VIX_BUNDLE["session"] is None:
        VIX_BUNDLE["session"], VIX_BUNDLE["cookies"] = new_session()
    try:
        r = VIX_BUNDLE["session"].get(url, headers=HEADERS, timeout=10,
                                      cookies=VIX_BUNDLE["cookies"])
        if r.status_code == 401:
            VIX_BUNDLE["session"].close()
            VIX_BUNDLE["session"], VIX_BUNDLE["cookies"] = new_session()
            r = VIX_BUNDLE["session"].get(url, headers=HEADERS, timeout=10,
                                          cookies=VIX_BUNDLE["cookies"])
        return r.json() if r.status_code == 200 else None
    except Exception:
        try:
            VIX_BUNDLE["session"].close()
        except Exception:
            pass
        VIX_BUNDLE["session"] = None
        return None

COLUMNS = ["ltp", "chg", "oi", "chg_oi", "volume", "iv", "delta", "gamma", "theta", "vega"]
COL_LABELS = {"ltp": "LTP", "chg": "Chg", "oi": "OI", "chg_oi": "Chg&nbsp;OI",
              "volume": "Volume", "iv": "IV", "delta": "Delta", "gamma": "Gamma",
              "theta": "Theta/day", "vega": "Vega/1%"}


# ---------------------------------------------------------------- NSE fetch --
def new_session():
    session = requests.Session()
    try:
        r = session.get(URL_OC, headers=HEADERS, timeout=10)
        return session, dict(r.cookies)
    except Exception:
        return session, {}  # continue without NSE cookies; fetcher will retry


def fetch_chain(session, cookies, symbol, mode, expiry):
    url = URL_CHAIN.format(mode=mode, symbol=symbol, expiry=expiry)
    r = session.get(url, headers=HEADERS, timeout=10, cookies=cookies)
    if r.status_code == 401:
        session.close()
        session, cookies = new_session()
        r = session.get(url, headers=HEADERS, timeout=10, cookies=cookies)
    r.raise_for_status()
    return session, cookies, r.json()


def pick_rows(chain_json, expiry, atm_strike):
    rows = [d for d in chain_json['records']['data'] if d.get('expiryDates') == expiry]
    rows.sort(key=lambda d: d['strikePrice'])
    strikes = [d['strikePrice'] for d in rows]
    atm_idx = strikes.index(atm_strike) if atm_strike in strikes else \
        min(range(len(rows)), key=lambda i: abs(rows[i]['strikePrice'] - atm_strike))
    lo = max(0, atm_idx - ITM_COUNT)
    hi = min(len(rows), atm_idx + OTM_COUNT + 1)
    return rows[lo:hi]


# ------------------------------------------------------------------ greeks --
def years_to_expiry(expiry_str, now=None):
    now = now or datetime.datetime.now()
    exp = datetime.datetime.strptime(expiry_str, "%d-%b-%Y").replace(hour=15, minute=30)
    secs = (exp - now).total_seconds()
    return max(secs / (365 * 24 * 3600), 1 / (365 * 24 * 3600))


def norm_cdf(x):
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def norm_pdf(x):
    return math.exp(-x * x / 2.0) / math.sqrt(2.0 * math.pi)


def bs_greeks(spot, strike, t_years, iv_pct, is_call):
    """Returns (delta, gamma, theta_per_day, vega_per_1pct) or Nones."""
    if not spot or not strike or not iv_pct or iv_pct <= 0:
        return None, None, None, None
    iv = iv_pct / 100.0
    sq_t = math.sqrt(t_years)
    d1 = (math.log(spot / strike) + (RISK_FREE_RATE + iv * iv / 2) * t_years) / (iv * sq_t)
    d2 = d1 - iv * sq_t
    pdf_d1 = norm_pdf(d1)
    disc = math.exp(-RISK_FREE_RATE * t_years)
    gamma = pdf_d1 / (spot * iv * sq_t)
    vega = spot * pdf_d1 * sq_t / 100.0
    if is_call:
        delta = norm_cdf(d1)
        theta = (-(spot * pdf_d1 * iv) / (2 * sq_t)
                 - RISK_FREE_RATE * strike * disc * norm_cdf(d2)) / 365.0
    else:
        delta = norm_cdf(d1) - 1.0
        theta = (-(spot * pdf_d1 * iv) / (2 * sq_t)
                 + RISK_FREE_RATE * strike * disc * norm_cdf(-d2)) / 365.0
    return delta, gamma, theta, vega


# ------------------------------------------------------------- market hours --
MARKET_CACHE = {"open": None, "checked_at": 0.0}


def market_open(force=False):
    """True while we should keep polling: market open per NSE API, OR
    within 9:15-16:05 IST on a weekday (covers post-market reconciliation).
    NSE status API first, clock as fallback. Cached 60s."""
    now = time.time()
    if not force and MARKET_CACHE["open"] is not None and now - MARKET_CACHE["checked_at"] < 60:
        return MARKET_CACHE["open"]
    open_now = None
    try:
        session, cookies = new_session()
        r = session.get(URL_MARKET_STATUS, headers=HEADERS, timeout=10, cookies=cookies)
        session.close()
        for m in r.json().get('marketState', []):
            if m.get('market') == 'Capital Market':
                open_now = str(m.get('marketStatus', '')).lower() == 'open'
                break
    except Exception:
        open_now = None
    now_ist = datetime.datetime.now()
    in_collect_window = (now_ist.weekday() < 5
                         and MARKET_OPEN_TIME <= now_ist.time() <= COLLECT_END_TIME)
    if open_now is None:
        open_now = in_collect_window
    else:
        # after F&O close the API says Closed, but reconciliation updates
        # land ~15:40-16:00 — keep polling through that window
        open_now = open_now or in_collect_window
    MARKET_CACHE.update(open=open_now, checked_at=now)
    return open_now


# --------------------------------------------------------------------- DB --
GREEK_COLS = ["ce_delta", "ce_gamma", "ce_theta", "ce_vega",
              "pe_delta", "pe_gamma", "pe_theta", "pe_vega"]


def _try_db(func):
    """Decorator: skip DB-dependent functions when Postgres is unreachable."""
    def wrapper(*a, **kw):
        if not DB_AVAILABLE:
            return None if func.__name__ in ('db_latest_rows', 'db_series', 'db_available_strikes') else []
        try:
            return func(*a, **kw)
        except Exception as e:
            print(f"DB call {func.__name__} failed: {e}")
            return None if func.__name__ in ('db_latest_rows', 'db_series', 'db_available_strikes') else []
    return wrapper


DB_AVAILABLE = True
try:
    _test = psycopg2.connect(DB_DSN, connect_timeout=1)
    _test.close()
except Exception:
    DB_AVAILABLE = False
    print("Postgres not reachable — board will use dummy trade data")


@_try_db
def db_init():
    conn = psycopg2.connect(DB_DSN)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {DB_TABLE} (
            id           BIGSERIAL PRIMARY KEY,
            symbol       TEXT NOT NULL,
            expiry       TEXT NOT NULL,
            strike       NUMERIC NOT NULL,
            spot         NUMERIC,
            snapshot_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
            server_time  TEXT,
            ce_ltp NUMERIC, ce_chg NUMERIC, ce_oi BIGINT, ce_chg_oi BIGINT,
            ce_volume BIGINT, ce_iv NUMERIC,
            pe_ltp NUMERIC, pe_chg NUMERIC, pe_oi BIGINT, pe_chg_oi BIGINT,
            pe_volume BIGINT, pe_iv NUMERIC
        )""")
    for col in GREEK_COLS:
        try:
            cur.execute(f"ALTER TABLE {DB_TABLE} ADD COLUMN {col} NUMERIC")
        except psycopg2.errors.DuplicateColumn:
            pass
    cur.execute(f"CREATE INDEX IF NOT EXISTS idx_oc_symbol_time ON {DB_TABLE} (symbol, snapshot_at DESC)")
    conn.close()


@_try_db
def db_purge_out_of_hours():
    """Delete rows captured outside market hours (9:15-15:30 IST, Mon-Fri)."""
    conn = psycopg2.connect(DB_DSN)
    try:
        cur = conn.cursor()
        cur.execute(f"""DELETE FROM {DB_TABLE}
            WHERE (snapshot_at AT TIME ZONE 'Asia/Kolkata')::time < TIME '09:15'
               OR (snapshot_at AT TIME ZONE 'Asia/Kolkata')::time > TIME '16:05'
               OR EXTRACT(ISODOW FROM (snapshot_at AT TIME ZONE 'Asia/Kolkata')) > 5""")
        n = cur.rowcount
        conn.commit()
        return n
    finally:
        conn.close()


def db_insert(symbol, expiry, spot, server_ts, enriched_rows):
    if not enriched_rows:
        return 0
    sql = f"""INSERT INTO {DB_TABLE}
        (symbol, expiry, strike, spot, server_time,
         ce_ltp, ce_chg, ce_oi, ce_chg_oi, ce_volume, ce_iv,
         ce_delta, ce_gamma, ce_theta, ce_vega,
         pe_ltp, pe_chg, pe_oi, pe_chg_oi, pe_volume, pe_iv,
         pe_delta, pe_gamma, pe_theta, pe_vega)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
    conn = psycopg2.connect(DB_DSN)
    try:
        cur = conn.cursor()
        cur.executemany(sql, enriched_rows)
        conn.commit()
        return len(enriched_rows)
    finally:
        conn.close()


def db_available_strikes(symbol):
    conn = psycopg2.connect(DB_DSN)
    try:
        cur = conn.cursor()
        cur.execute(f"SELECT DISTINCT strike FROM {DB_TABLE} WHERE symbol = %s ORDER BY strike",
                    (symbol,))
        return [float(r[0]) for r in cur.fetchall()]
    finally:
        conn.close()


def db_series(symbol, strikes, metric):
    """Per-strike CE & PE time series for one metric.
    Returns {strike: {"times": [...], "ce": [...], "pe": [...]}}.
    Supported metrics: ltp, oi, oi_value, volume, chg_oi, chg_oi_value, pcr, iv.
    """
    col_map = {"ltp": ("ce_ltp", "pe_ltp"),
               "oi": ("ce_oi", "pe_oi"),
               "oi_value": ("ce_oi", "pe_oi"),
               "volume": ("ce_volume", "pe_volume"),
               "chg_oi": ("ce_chg_oi", "pe_chg_oi"),
               "chg_oi_value": ("ce_chg_oi", "pe_chg_oi"),
               "iv": ("ce_iv", "pe_iv")}
    pcr_mode = metric == "pcr"
    if not pcr_mode and metric not in col_map:
        raise ValueError(f"unknown metric {metric}")
    conn = psycopg2.connect(DB_DSN)
    try:
        cur = conn.cursor()
        result = {}
        for k in strikes:
            if pcr_mode:
                cur.execute(f"""SELECT to_char(snapshot_at, 'HH24:MI'),
                                       COALESCE(pe_oi, 0) / NULLIF(ce_oi, 0)
                                FROM {DB_TABLE}
                                WHERE symbol = %s AND strike = %s
                                ORDER BY snapshot_at""", (symbol, k))
                rows = cur.fetchall()
                result[k] = {"times": [r[0] for r in rows],
                             "ce": [round(float(r[1]), 3) if r[1] is not None else None
                                    for r in rows],
                             "pe": None}
            else:
                ce_c, pe_c = col_map[metric]
                cur.execute(f"""SELECT to_char(snapshot_at, 'HH24:MI'), {ce_c}, {pe_c}
                                FROM {DB_TABLE}
                                WHERE symbol = %s AND strike = %s
                                ORDER BY snapshot_at""", (symbol, k))
                rows = cur.fetchall()
                f = lambda v: round(float(v), 2) if v is not None else None
                result[k] = {"times": [r[0] for r in rows],
                             "ce": [f(r[1]) for r in rows],
                             "pe": [f(r[2]) for r in rows]}
        return result
    finally:
        conn.close()


DUMMY_TRADES = {
    "2026-10-07": [
        {
            "id": 101, "date": "2026-10-07", "entry_time": "09:17:51", "exit_time": "09:35:20",
            "symbol": "NIFTY", "strategy": "Wall Proximity", "variant": "S3",
            "direction": "Short", "structure": "Credit Spread",
            "strikes": "22700/22800 PE",
            "entry_cost_per_lot": -42.50, "exit_cost_per_lot": -18.20,
            "realized_pnl_per_lot": 24.30, "realized_pnl_total": 12150.00,
            "exit_reason": "TARGET (40% of credit)", "holding_minutes": 17,
            "spot_at_entry": 22715.40, "atm_iv_at_entry": 14.80, "vix_at_entry": 13.20,
            "regime_tag": "WALL_PROXIMITY", "status": "closed",
            "signal_detail": "PE OI wall at 22800 detected", "signal_fired_at": "09:17:50",
        },
        {
            "id": 102, "date": "2026-10-07", "entry_time": "09:25:05", "exit_time": "10:02:18",
            "symbol": "NIFTY", "strategy": "Wall Proximity", "variant": "S3",
            "direction": "Short", "structure": "Credit Spread",
            "strikes": "22700/22600 CE",
            "entry_cost_per_lot": -38.00, "exit_cost_per_lot": -12.50,
            "realized_pnl_per_lot": 25.50, "realized_pnl_total": 12750.00,
            "exit_reason": "SOFT BOOK (+25%)", "holding_minutes": 37,
            "spot_at_entry": 22728.60, "atm_iv_at_entry": 15.10, "vix_at_entry": 13.40,
            "regime_tag": "WALL_PROXIMITY", "status": "closed",
            "signal_detail": "CE OI wall at 22600 detected", "signal_fired_at": "09:25:04",
        },
    ],
    "2026-10-06": [
        {
            "id": 87, "date": "2026-10-06", "entry_time": "10:12:33", "exit_time": "10:45:10",
            "symbol": "NIFTY", "strategy": "OI Surge", "variant": "S1",
            "direction": "Long", "structure": "Debit Spread",
            "strikes": "22500/22600 CE",
            "entry_cost_per_lot": 85.00, "exit_cost_per_lot": 132.50,
            "realized_pnl_per_lot": 47.50, "realized_pnl_total": 23750.00,
            "exit_reason": "TARGET (+100%)", "holding_minutes": 32,
            "spot_at_entry": 22490.20, "atm_iv_at_entry": 13.50, "vix_at_entry": 12.10,
            "regime_tag": "OI_SURGE", "status": "closed",
            "signal_detail": "CE OI surge +12% at 22500", "signal_fired_at": "10:12:30",
        },
        {
            "id": 88, "date": "2026-10-06", "entry_time": "11:05:18", "exit_time": "11:28:44",
            "symbol": "NIFTY", "strategy": "Skew Anomaly Follow", "variant": "S4_FOLLOW",
            "direction": "Long", "structure": "Debit Spread",
            "strikes": "22700/22800 CE",
            "entry_cost_per_lot": 55.00, "exit_cost_per_lot": 71.00,
            "realized_pnl_per_lot": 16.00, "realized_pnl_total": 8000.00,
            "exit_reason": "SOFT BOOK (+70%)", "holding_minutes": 23,
            "spot_at_entry": 22710.80, "atm_iv_at_entry": 14.20, "vix_at_entry": 12.50,
            "regime_tag": "SKEW_ANOMALY", "status": "closed",
            "signal_detail": "PE/CE skew anomaly detected", "signal_fired_at": "11:05:15",
        },
        {
            "id": 89, "date": "2026-10-06", "entry_time": "13:02:10", "exit_time": "13:31:55",
            "symbol": "NIFTY", "strategy": "Skew Anomaly Fade", "variant": "S4_FADE",
            "direction": "Short", "structure": "Credit Spread",
            "strikes": "22800/22900 PE",
            "entry_cost_per_lot": -30.00, "exit_cost_per_lot": -10.00,
            "realized_pnl_per_lot": 20.00, "realized_pnl_total": 10000.00,
            "exit_reason": "TARGET (40% of credit)", "holding_minutes": 29,
            "spot_at_entry": 22820.50, "atm_iv_at_entry": 14.60, "vix_at_entry": 12.80,
            "regime_tag": "SKEW_ANOMALY", "status": "closed",
            "signal_detail": "Skew anomaly fade signal", "signal_fired_at": "13:02:08",
        },
    ],
    "2026-10-05": [
        {
            "id": 72, "date": "2026-10-05", "entry_time": "09:30:00", "exit_time": "10:08:22",
            "symbol": "NIFTY", "strategy": "Skew Anomaly Follow", "variant": "S4_FOLLOW",
            "direction": "Long", "structure": "Debit Spread",
            "strikes": "22600/22700 CE",
            "entry_cost_per_lot": 62.00, "exit_cost_per_lot": 41.00,
            "realized_pnl_per_lot": -21.00, "realized_pnl_total": -10500.00,
            "exit_reason": "STOP (-50%)", "holding_minutes": 38,
            "spot_at_entry": 22580.30, "atm_iv_at_entry": 13.90, "vix_at_entry": 11.90,
            "regime_tag": "SKEW_ANOMALY", "status": "closed",
            "signal_detail": "Skew follow entry", "signal_fired_at": "09:29:58",
        },
        {
            "id": 73, "date": "2026-10-05", "entry_time": "09:30:00", "exit_time": "11:15:00",
            "symbol": "NIFTY", "strategy": "Vol Spike", "variant": "S5",
            "direction": "Long", "structure": "Long Straddle",
            "strikes": "22700 CE + 22700 PE",
            "entry_cost_per_lot": 180.00, "exit_cost_per_lot": 210.00,
            "realized_pnl_per_lot": 30.00, "realized_pnl_total": 15000.00,
            "exit_reason": "SOFT BOOK (+70%)", "holding_minutes": 105,
            "spot_at_entry": 22580.30, "atm_iv_at_entry": 16.50, "vix_at_entry": 19.80,
            "regime_tag": "VOL_SPIKE", "status": "closed",
            "signal_detail": "VIX spike above 18", "signal_fired_at": "09:30:00",
        },
    ],
}


def db_available_trade_dates(symbol):
    """Return sorted list of distinct entry dates (YYYY-MM-DD) that have paper_trades.
    Uses dummy data when Postgres is unavailable."""
    dates = list(DUMMY_TRADES.keys())
    dates.sort(reverse=True)
    return dates


def db_trades_by_date(symbol, date_str):
    """Return all paper_trades for a symbol on a given date (YYYY-MM-DD).
    Uses dummy data when Postgres is unavailable."""
    return DUMMY_TRADES.get(date_str, [])



def db_latest_rows(symbol):
    """Full latest snapshot (23 fields per row) for the frozen closed-market page."""
    conn = psycopg2.connect(DB_DSN)
    try:
        cur = conn.cursor()
        cur.execute(f"""SELECT strike, spot, server_time,
                               ce_ltp, ce_chg, ce_oi, ce_chg_oi, ce_volume, ce_iv,
                               ce_delta, ce_gamma, ce_theta, ce_vega,
                               pe_ltp, pe_chg, pe_oi, pe_chg_oi, pe_volume, pe_iv,
                               pe_delta, pe_gamma, pe_theta, pe_vega,
                               to_char(MAX(snapshot_at) OVER (), 'DD-Mon HH24:MI:SS')
                        FROM {DB_TABLE} WHERE symbol = %s
                          AND snapshot_at = (SELECT MAX(snapshot_at) FROM {DB_TABLE}
                                             WHERE symbol = %s)
                        ORDER BY strike""", (symbol, symbol))
        out = []
        for r in cur.fetchall():
            f = lambda v: float(v) if v is not None else None
            out.append([r[0], f(r[1]), r[2]] + [f(v) for v in r[3:23]] + [r[23]])
        return out
    finally:
        conn.close()


# ----------------------------------------------------------------- format --
def fmt(v, dec=2):
    return f"{v:,.{dec}f}" if isinstance(v, (int, float)) else "-"


def fmt_g(v, dec=4):
    return f"{v:.{dec}f}" if isinstance(v, (int, float)) else "-"


def fmt_oi(v):
    if isinstance(v, (int, float)):
        return f"{v / 1000:,.1f}K" if v >= 1000 else str(v)
    return "-"


def chg_html(val):
    if val is None:
        return "-"
    color = '#2e7d32' if val > 0 else ('#c62828' if val < 0 else '#555')
    return f'<span style="color:{color}">{"+" if val > 0 else ""}{val:,.2f}</span>'


def td(cls, val):
    c = f' class="{cls}"' if cls else ''
    return f"<td{c}>{val}</td>"


def build_row_html(r, atm_strike):
    (strike, spot, server_ts,
     ce_ltp, ce_chg, ce_oi, ce_chg_oi, ce_vol, ce_iv,
     ce_delta, ce_gamma, ce_theta, ce_vega,
     pe_ltp, pe_chg, pe_oi, pe_chg_oi, pe_vol, pe_iv,
     pe_delta, pe_gamma, pe_theta, pe_vega) = r
    cls = ' class="atm"' if float(strike) == float(atm_strike) else ''
    cells = [td("", f"{float(strike):,.0f}")]
    cells += [
        td("c-ltp", fmt(ce_ltp)), td("c-chg", chg_html(ce_chg)),
        td("c-oi", fmt_oi(ce_oi)), td("c-chg_oi", fmt_oi(ce_chg_oi)),
        td("c-volume", fmt(ce_vol, 0)), td("c-iv", fmt(ce_iv)),
        td("c-delta", fmt_g(ce_delta)), td("c-gamma", fmt_g(ce_gamma)),
        td("c-theta", fmt_g(ce_theta)), td("c-vega", fmt_g(ce_vega)),
        td("p-ltp", fmt(pe_ltp)), td("p-chg", chg_html(pe_chg)),
        td("p-oi", fmt_oi(pe_oi)), td("p-chg_oi", fmt_oi(pe_chg_oi)),
        td("p-volume", fmt(pe_vol, 0)), td("p-iv", fmt(pe_iv)),
        td("p-delta", fmt_g(pe_delta)), td("p-gamma", fmt_g(pe_gamma)),
        td("p-theta", fmt_g(pe_theta)), td("p-vega", fmt_g(pe_vega)),
    ]
    return f"<tr{cls}>" + "".join(cells) + "</tr>"


def build_html(symbol, expiry, spot, server_ts, fetched, interval, table_rows,
               banner=None, err=None, atm_strike=None, strategy_section="",
               trades_board_section=""):
    body = f'<p class="err">{err}</p>' if err else table_rows
    closed = banner is not None
    controls_disabled = 'disabled' if closed else ''
    banner_html = (f'<div class="closed-banner">🔒 {banner}</div>' if closed else '')
    countdown_js = ("document.getElementById('countdown').textContent = "
                    "'collection resumes automatically at 9:15 AM';" if closed else
                    "setInterval(() => {\n  left--;\n"
                    "  document.getElementById('countdown').textContent = "
                    "'next auto refresh in ' + left + 's';\n"
                    "  if (left <= 0) location.reload();\n}, 1000);")
    defaults_json = json.dumps(CHART_DEFAULTS)
    atm_js = repr(float(atm_strike)) if atm_strike is not None else "22700"
    chart_strikes_js = str(CHART_STRIKES)
    opts = "".join(f'<option value="{s}"{" selected" if s == interval else ""}>{s}s</option>'
                   for s in INTERVALS)
    heads = "".join(f'<th class="c-{c}">{COL_LABELS[c]}</th>' for c in COLUMNS)
    pheads = "".join(f'<th class="p-{c}">{COL_LABELS[c]}</th>' for c in COLUMNS)
    checks = ""
    for side, label in (("c", "CALLS"), ("p", "PUTS")):
        checks += f"<b>{label}</b><br>"
        for c in COLUMNS:
            checks += (f'<label style="display:block;font-weight:400">'
                       f'<input type="checkbox" class="colchk" data-col="{side}-{c}" checked>'
                       f' {COL_LABELS[c]}</label>')
    return f"""<!DOCTYPE html>
<html><head>
<meta charset="utf-8">
<title>{symbol} Option Chain — Live</title>
<style>
body {{ font-family: 'Segoe UI', Arial, sans-serif; margin: 16px; background: #fafafa; }}
h1 {{ font-size: 20px; margin: 0 0 4px; }}
.closed-banner {{ background: #37474f; color: #ffca28; padding: 10px 16px;
                  border-radius: 4px; font-weight: 600; margin-bottom: 12px; font-size: 15px; }}
.meta {{ color: #555; font-size: 13px; margin-bottom: 10px; }}
.controls {{ margin-bottom: 12px; display: flex; gap: 10px; align-items: center; position: relative; }}
.controls select, .controls button {{ font-size: 14px; padding: 6px 14px; }}
.controls button {{ background: #1a237e; color: white; border: none; cursor: pointer; }}
.controls button:disabled {{ background: #90a4ae; cursor: not-allowed; }}
.controls select:disabled {{ background: #eceff1; }}
.controls button:not(:disabled):hover {{ background: #283593; }}
.spot {{ font-size: 16px; font-weight: 600; color: #1a237e; }}
table {{ border-collapse: collapse; font-size: 12px; background: white;
         box-shadow: 0 1px 4px rgba(0,0,0,.15); }}
th, td {{ border: 1px solid #ccc; padding: 3px 8px; text-align: right; }}
th {{ background: #263238; color: white; font-weight: 600; }}
thead tr:nth-child(2) th {{ background: #37474f; }}
tbody tr:nth-child(even) {{ background: #f5f5f5; }}
.atm {{ background: #fff9c4 !important; font-weight: 600; }}
.atm td:first-child {{ background: #f9a825; }}
.err {{ color: #c62828; font-weight: 600; }}
#colpanel {{ display: none; position: absolute; top: 40px; left: 0; z-index: 10;
             background: white; border: 1px solid #999; padding: 10px 14px;
             box-shadow: 0 2px 10px rgba(0,0,0,.3); font-size: 13px;
             columns: 2; column-gap: 24px; max-height: 70vh; overflow: auto; }}
.hidden-col {{ display: none; }}
.pill {{ border: 1px solid #b0bec5; background: white; color: #455a64;
        padding: 4px 12px; border-radius: 14px; cursor: pointer; font-size: 12px;
        letter-spacing: .3px; }}
.pill-on {{ background: #1a237e; border-color: #1a237e; color: white; }}
.dl {{ font-size: 11px; padding: 2px 8px; cursor: pointer; background: #eceff1;
      border: 1px solid #b0bec5; }}
.dl:hover {{ background: #cfd8dc; }}

/* P&L Observation Board */
.pnl-board {{ background: white; border: 1px solid #ccc; border-radius: 6px;
              padding: 14px; margin: 16px 0; box-shadow: 0 1px 4px rgba(0,0,0,.1); }}
.pnl-board h2 {{ font-size: 16px; margin: 0 0 10px; color: #263238; }}
.pnl-board .pnl-meta {{ color: #555; font-size: 13px; margin-bottom: 10px; }}
.pnl-board .pnl-controls {{ display: flex; gap: 10px; align-items: center; margin-bottom: 12px;
                         flex-wrap: wrap; }}
.pnl-board .pnl-controls label {{ font-size: 13px; color: #455a64; }}
.pnl-board .pnl-controls input[type="date"] {{ padding: 4px 8px; border: 1px solid #ccc;
                  border-radius: 4px; font-size: 13px; }}
.pnl-board .pnl-summary {{ display: flex; gap: 0; flex-wrap: nowrap; margin-bottom: 12px;
                          font-size: 12px; }}
.pnl-board .pnl-summary .pnl-stat {{ background: #f5f5f5; padding: 6px 10px;
                  border-radius: 4px; border-left: 3px solid #1a237e; text-align: center;
                  flex: 1 1 0; min-width: 80px; }}
.pnl-board .pnl-summary .pnl-stat .label {{ color: #607d8b; font-size: 10px; }}
.pnl-board .pnl-summary .pnl-stat .value {{ font-weight: 600; font-size: 13px; display: block; }}
.pnl-board .pnl-summary .pnl-stat.positive .value {{ color: #2e7d32; }}
.pnl-board .pnl-summary .pnl-stat.negative .value {{ color: #c62828; }}
.pnl-board .pnl-table-wrap {{ overflow-x: auto; -webkit-overflow-scrolling: touch; }}
.pnl-board table {{ border-collapse: collapse; font-size: 12px; width: 100%; min-width: 900px; margin-top: 8px; table-layout: auto; }}
.pnl-board th {{ background: #263238; color: white; padding: 6px 6px; text-align: left;
                 font-weight: 600; white-space: nowrap; }}
.pnl-board td {{ border: 1px solid #ccc; padding: 5px 6px; white-space: nowrap; }}
.pnl-board tr:nth-child(even) {{ background: #f9f9f9; }}
.pnl-board tr {{ page-break-inside: avoid; }}
.pnl-board .regime-tag {{ display: inline-block; padding: 2px 8px; border-radius: 10px;
                          font-size: 10px; font-weight: 600; text-transform: uppercase; }}
.pnl-board .regime-oi_surge {{ background: #e3f2fd; color: #0d47a1; }}
.pnl-board .regime-wall_proximity {{ background: #fff3e0; color: #e65100; }}
.pnl-board .regime-skew_anomaly {{ background: #f3e5f5; color: #7b1fa2; }}
.pnl-board .regime-vol_spike {{ background: #ffebee; color: #c62828; }}
.pnl-board .regime-low_vol {{ background: #e8f5e9; color: #2e7d32; }}
.pnl-board .regime-unknown {{ background: #eceff1; color: #607d8b; }}
.pnl-board .pnl-empty {{ color: #777; font-style: italic; padding: 20px; text-align: center;
                        font-size: 13px; }}
.pnl-board .pnl-pnl-pos {{ color: #2e7d32; font-weight: 600; }}
.pnl-board .pnl-pnl-neg {{ color: #c62828; font-weight: 600; }}
.pnl-board .pnl-cost-debit {{ color: #c62828; }}
.pnl-board .pnl-cost-credit {{ color: #2e7d32; }}
</style></head>
<body>
{banner_html}
<h1>{symbol} — Option Chain (10 ITM + ATM + 10 OTM — 21 strikes, with Greeks)</h1>
<div class="meta">
  <span class="spot">Spot: {fmt(spot)}</span> &nbsp;|&nbsp;
  Expiry: {expiry} &nbsp;|&nbsp;
  Server time: {server_ts} &nbsp;|&nbsp;
  Fetched: {fetched} IST
</div>
<div class="controls">
  <button onclick="fetch('/refresh').then(()=>setTimeout(()=>location.reload(),800))" {controls_disabled}>🔄 Refresh now</button>
  <label>Auto refresh:
    <select id="iv" onchange="fetch('/set_interval?s='+this.value).then(()=>location.reload())" {controls_disabled}>
      {opts}
    </select>
  </label>
  <button id="colbtn" onclick="togglePanel()">Columns ▾</button>
  <span id="countdown" style="color:#777;font-size:13px;"></span>
  <div id="colpanel">{checks}</div>
</div>
<script>
const INTERVAL = {interval};
let left = INTERVAL;
{countdown_js}

function togglePanel() {{
  const p = document.getElementById('colpanel');
  p.style.display = p.style.display === 'block' ? 'none' : 'block';
}}
document.addEventListener('click', e => {{
  const p = document.getElementById('colpanel');
  if (p.style.display === 'block' && !p.contains(e.target) && e.target.id !== 'colbtn')
    p.style.display = 'none';
}});

const DEFAULTS = ['ltp','chg','oi','chg_oi','iv','delta'];
function loadCols() {{
  let saved;
  try {{ saved = JSON.parse(localStorage.getItem('oc_cols')); }} catch (e) {{}}
  document.querySelectorAll('.colchk').forEach(cb => {{
    const on = saved ? !!saved[cb.dataset.col] : DEFAULTS.includes(cb.dataset.col.split('-')[1]);
    cb.checked = on;
    applyCol(cb.dataset.col, on);
    cb.onchange = () => {{ applyCol(cb.dataset.col, cb.checked); saveCols(); }};
  }});
}}
function applyCol(name, on) {{
  document.querySelectorAll('td.' + name + ', th.' + name)
    .forEach(el => el.classList.toggle('hidden-col', !on));
}}
function saveCols() {{
  const o = {{}};
  document.querySelectorAll('.colchk').forEach(cb => o[cb.dataset.col] = cb.checked);
  localStorage.setItem('oc_cols', JSON.stringify(o));
}}
loadCols();
</script>
<table><thead><tr>
<th rowspan="2">Strike</th><th colspan="10">CALLS</th><th colspan="10">PUTS</th>
</tr><tr>
{heads}{pheads}
</tr></thead><tbody>
{body}
</tbody></table>
<p style="font-size:12px;color:#777;margin-top:10px;">
Greeks: Black-Scholes, r={RISK_FREE_RATE}, IV from NSE. Every fetch saved to PostgreSQL (table: {DB_TABLE}).</p>
{strategy_section}
{trades_board_section}
<h2 style="font-size:18px;margin:28px 0 4px;">Multi-Strike CE vs PE Comparison</h2>
<p style="color:#555;font-size:13px;margin:0 0 10px;">Call vs Put movement across {CHART_STRIKES} strikes in one view, built from the Postgres history.</p>
<div class="controls" style="flex-wrap:wrap;">
  <div id="pills" style="display:flex;gap:6px;flex-wrap:wrap;"></div>
  <label>Strikes:
    <select id="chart-strikes" multiple size="1" style="min-width:210px;"></select>
  </label>
  <label style="font-size:13px;">Auto refresh:
    <input type="checkbox" id="chart-auto" checked>
  </label>
  <span style="color:#777;font-size:12px;">follows the table's interval</span>
</div>
<div id="chart-grid" style="display:grid;grid-template-columns:repeat(3,1fr);gap:14px;"></div>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4"></script>
<script>
const METRICS = [
  ['oi','OI'], ['oi_value','OI VALUE'], ['volume','VOLUME'], ['ltp','LTP'],
  ['chg_oi','CHANGE OI'], ['chg_oi_value','CHANGE OI VALUE'], ['pcr','PCR'], ['iv','IV']
];
const CHART_DEFAULT_METRICS = {defaults_json};
let curMetric = CHART_DEFAULT_METRICS[0] || 'ltp';
let curStrikes = [];
const charts = {{}};

function pillRow() {{
  const p = document.getElementById('pills');
  p.innerHTML = '';
  for (const [key, label] of METRICS) {{
    const b = document.createElement('button');
    b.textContent = label;
    b.className = 'pill' + (key === curMetric ? ' pill-on' : '');
    b.onclick = () => {{ curMetric = key; pillRow(); loadCharts(); }};
    p.appendChild(b);
  }}
}}

async function loadStrikeOptions() {{
  const sel = document.getElementById('chart-strikes');
  try {{
    const list = await (await fetch('/api/strikes')).json();
    sel.innerHTML = '';
    const atmIdx = list.reduce((best, s, i) =>
      Math.abs(s - {atm_js}) < Math.abs(list[best] - {atm_js}) ? i : best, 0);
    const chosen = new Set();
    for (let off = -2; off <= 2; off++) {{
      const i = Math.min(list.length - 1, Math.max(0, atmIdx + off));
      chosen.add(list[i]);
    }}
    for (const s of list) {{
      const o = document.createElement('option');
      o.value = s; o.textContent = s.toLocaleString();
      o.selected = chosen.has(s);
      sel.appendChild(o);
    }}
    curStrikes = [...chosen];
    sel.onchange = () => {{
      curStrikes = [...sel.selectedOptions].map(o => parseFloat(o.value)).slice(0, {chart_strikes_js});
      if (curStrikes.length === {chart_strikes_js})
        [...sel.options].forEach(o => o.disabled = !curStrikes.includes(parseFloat(o.value)) && !o.selected);
      else
        [...sel.options].forEach(o => o.disabled = false);
      loadCharts();
    }};
    loadCharts();
  }} catch (e) {{
    document.getElementById('chart-grid').textContent = 'strike list unavailable: ' + e;
  }}
}}

async function loadCharts() {{
  if (!curStrikes.length) return;
  let data;
  try {{
    data = await (await fetch('/api/series?metric=' + curMetric +
                    '&strikes=' + curStrikes.join(','))).json();
  }} catch (e) {{
    return;
  }}
  const grid = document.getElementById('chart-grid');
  grid.innerHTML = '';
  for (const k of curStrikes) {{
    const s = data[k] || data[k.toFixed(1)] || data[String(k)];
    if (!s) continue;
    const card = document.createElement('div');
    card.style.cssText = 'background:white;border:1px solid #ddd;padding:10px;';
    const label = curMetric === 'pcr' ? 'PCR (PE OI / CE OI)'
                : curMetric.endsWith('_value') ? curMetric.replace('_',' ').toUpperCase() + ' (= OI × LTP)'
                : curMetric.toUpperCase();
    card.innerHTML = '<b>' + k.toLocaleString() + '</b>' +
      '<div style="color:#777;font-size:11px;margin-bottom:6px;">CALL vs PUT — ' + label + '</div>' +
      '<div style="display:flex;justify-content:flex-end;gap:6px;">' +
      '<button class="dl">download</button></div>' +
      '<canvas height="200"></canvas>';
    grid.appendChild(card);
    const ctx = card.querySelector('canvas');
    const ds = [{{ label: 'Call', data: s.ce, borderColor: '#2196f3',
                  backgroundColor: 'rgba(33,150,243,.12)', fill: true, tension: .25, pointRadius: 0 }}];
    if (s.pe) ds.push({{ label: 'Put', data: s.pe, borderColor: '#ff9800',
                        backgroundColor: 'rgba(255,152,0,.12)', fill: true, tension: .25, pointRadius: 0 }});
    if (charts[k]) charts[k].destroy();
    charts[k] = new Chart(ctx, {{
      type: 'line',
      data: {{ labels: s.times, datasets: ds }},
      options: {{
        animation: false,
        plugins: {{ legend: {{ display: true, labels: {{ boxWidth: 10, font: {{ size: 10 }} }} }} }},
        scales: {{ x: {{ ticks: {{ maxTicksLimit: 8, font: {{ size: 9 }} }} }} }}
      }}
    }});
    card.querySelector('.dl').onclick = () => {{
      const a = document.createElement('a');
      a.href = charts[k].toBase64Image();
      a.download = k + '_' + curMetric + '.png';
      a.click();
    }};
  }}
}}

document.getElementById('chart-auto').onchange = function() {{
  this.parentElement.style.opacity = this.checked ? 1 : .5;
}};

setInterval(() => {{
  if (document.getElementById('chart-auto').checked &&
      document.getElementById('iv') && !document.getElementById('iv').disabled &&
      left !== undefined && left <= 1) loadCharts();
}}, 1000);

pillRow();
loadStrikeOptions();
</script>

<script>
// P&L Observation Board
const PNL_BOARD = {{}};
PNL_BOARD.defaultDate = new Date().toISOString().split('T')[0];
PNL_BOARD.currentDate = PNL_BOARD.defaultDate;

async function loadTradeDates() {{
  try {{
    const resp = await fetch('/api/trade_dates');
    const dates = await resp.json();
    const sel = document.getElementById('pnl-date');
    if (!sel) return;
    // Determine which date to show: today if in list, else most recent
    let chosen = null;
    if (dates.includes(PNL_BOARD.defaultDate)) {{
      chosen = PNL_BOARD.defaultDate;
    }} else if (dates.length > 0) {{
      chosen = dates[0];
    }}
    if (chosen) {{
      sel.value = chosen;
      loadTrades(chosen);
    }} else {{
      const container = document.getElementById('pnl-trades-content');
      if (container) container.innerHTML = '<div class="pnl-empty">No trade dates available.</div>';
    }}
  }} catch (e) {{
    console.error('Failed to load trade dates:', e);
  }}
}}

async function loadTrades(date) {{
  PNL_BOARD.currentDate = date;
  const container = document.getElementById('pnl-trades-content');
  const summary = document.getElementById('pnl-summary');
  if (!container) return;
  try {{
    const resp = await fetch('/api/trades?date=' + encodeURIComponent(date));
    const trades = await resp.json();
    if (trades.error) {{
      container.innerHTML = '<div class="pnl-empty">Error loading trades: ' + trades.error + '</div>';
      return;
    }}
    if (!trades || trades.length === 0) {{
      container.innerHTML = '<div class="pnl-empty">No trades for this day.</div>';
      if (summary) summary.innerHTML = '';
      return;
    }}
    // Calculate summary
    const closed = trades.filter(t => t.status === 'closed');
    const realizedTotal = closed.reduce((sum, t) => sum + (t.realized_pnl_total || 0), 0);
    const wins = closed.filter(t => (t.realized_pnl_total || 0) > 0).length;
    const winRate = closed.length > 0 ? (wins / closed.length * 100).toFixed(1) + '%' : 'N/A';
    const holdings = closed.filter(t => t.holding_minutes).map(t => t.holding_minutes);
    const avgHold = holdings.length > 0 ? Math.round(holdings.reduce((a, b) => a + b, 0) / holdings.length) + 'm' : 'N/A';
    const totalTrades = trades.length;
    const openTrades = trades.filter(t => t.status === 'open').length;
    
    if (summary) {{
      summary.innerHTML = `
        <div class="pnl-stat"><div class="label">Total Trades</div><div class="value">${{totalTrades}}</div></div>
        <div class="pnl-stat"><div class="label">Closed</div><div class="value">${{closed.length}}</div></div>
        <div class="pnl-stat"><div class="label">Open</div><div class="value">${{openTrades}}</div></div>
        <div class="pnl-stat ${{realizedTotal >= 0 ? 'positive' : 'negative'}}">
          <div class="label">Realized P&L</div>
          <div class="value">${{realizedTotal >= 0 ? '+' : ''}}${{realizedTotal.toLocaleString('en-IN', {{maximumFractionDigits: 0}})}}</div>
        </div>
        <div class="pnl-stat"><div class="label">Win Rate</div><div class="value">${{winRate}}</div></div>
        <div class="pnl-stat"><div class="label">Avg Hold</div><div class="value">${{avgHold}}</div></div>
      `;
    }}
    // Build table rows
    let html = '<table><thead><tr>' +
      '<th>#</th><th>Time</th><th>Strategy</th><th>Direction</th><th>Structure</th>' +
      '<th>Strikes</th><th>Entry Cost</th><th>Exit Cost</th><th>P&L/Lot</th><th>P&L Total</th>' +
      '<th>Hold</th><th>Exit Reason</th><th>Spot</th><th>ATM IV</th><th>VIX</th><th>Regime</th><th>Status</th>' +
      '</tr></thead><tbody>';
    for (const t of trades) {{
      const pnlClass = t.realized_pnl_total !== null && t.realized_pnl_total >= 0 ? 'pnl-pnl-pos' : 'pnl-pnl-neg';
      const costClass = t.entry_cost_per_lot !== null && t.entry_cost_per_lot >= 0 ? 'pnl-cost-debit' : 'pnl-cost-credit';
      const regimeClass = 'regime-' + (t.regime_tag || 'unknown').toLowerCase().replace(/_/g, '_');
      html += '<tr>' +
        '<td>' + t.id + '</td>' +
        '<td>' + t.entry_time + '</td>' +
        '<td>' + t.strategy + '</td>' +
        '<td>' + (t.direction || '-') + '</td>' +
        '<td style="font-size:11px;">' + (t.structure || '-') + '</td>' +
        '<td style="font-size:11px;color:#546e7a;">' + (t.strikes || '-') + '</td>' +
        '<td class="' + costClass + '">' + (t.entry_cost_per_lot !== null ? (t.entry_cost_per_lot >= 0 ? '+' : '') + t.entry_cost_per_lot.toFixed(2) : '-') + '</td>' +
        '<td>' + (t.exit_cost_per_lot !== null ? t.exit_cost_per_lot.toFixed(2) : '-') + '</td>' +
        '<td class="' + pnlClass + '">' + (t.realized_pnl_per_lot !== null ? (t.realized_pnl_per_lot >= 0 ? '+' : '') + t.realized_pnl_per_lot.toFixed(2) : '-') + '</td>' +
        '<td class="' + pnlClass + '">' + (t.realized_pnl_total !== null ? (t.realized_pnl_total >= 0 ? '+' : '') + t.realized_pnl_total.toLocaleString('en-IN', {{maximumFractionDigits: 0}}) : '-') + '</td>' +
        '<td>' + (t.holding_minutes !== null ? t.holding_minutes + 'm' : '-') + '</td>' +
        '<td style="font-size:11px;">' + (t.exit_reason || '-') + '</td>' +
        '<td>' + (t.spot_at_entry !== null ? t.spot_at_entry.toFixed(2) : '-') + '</td>' +
        '<td>' + (t.atm_iv_at_entry !== null ? t.atm_iv_at_entry.toFixed(2) + '%' : '-') + '</td>' +
        '<td>' + (t.vix_at_entry !== null ? t.vix_at_entry.toFixed(2) : '-') + '</td>' +
        '<td><span class="regime-tag ' + regimeClass + '">' + (t.regime_tag || 'UNKNOWN') + '</span></td>' +
        '<td>' + (t.status || '-') + '</td>' +
        '</tr>';
    }}
    html += '</tbody></table>';
    container.innerHTML = html;
  }} catch (e) {{
    container.innerHTML = '<div class="pnl-empty">Error loading trades: ' + e.message + '</div>';
  }}
}}

function initPnlBoard() {{
  const dateInput = document.getElementById('pnl-date');
  if (!dateInput) return;
  // Load available dates (this also picks the best date and loads trades)
  loadTradeDates();
  // Set up event listeners
  dateInput.onchange = () => {{ loadTrades(dateInput.value); }};
}}

// Initialize when DOM is ready
if (document.readyState === 'loading') {{
  document.addEventListener('DOMContentLoaded', initPnlBoard);
}} else {{
  initPnlBoard();
}}
</script>
</body></html>"""


# ----------------------------------------------------------------- server --
STATE = {
    "symbol": "NIFTY", "strike": 22700.0, "expiry": "",
    "interval": 60, "last_fetch": None, "last_error": None,
    "market_open": True, "lock": threading.Lock(),
}


CLOSED_PAGE = """<!DOCTYPE html>
<html><head><meta charset="utf-8"><title>Option Chain — Market Closed</title>
<style>body {{ font-family: 'Segoe UI', Arial; background: #fafafa; margin: 40px; }}
.box {{ background: white; border: 1px solid #ccc; padding: 30px; max-width: 520px; }}
h1 {{ color: #263238; font-size: 22px; }}</style></head>
<body><div class="box">
<h1>🔒 Market is closed — data collection paused</h1>
<p>Live option chain data is collected only during market hours:<br>
<b>9:15 AM – 3:30 PM IST, Monday to Friday</b> (via NSE market status API).</p>
<p>The app checks automatically and <b>resumes collecting on its own</b> when the
market opens (even if you started the server before 9:15 AM).</p>
<p style="color:#777;font-size:13px;">Last checked: {{ts}} IST — this page updates
itself every 60s.</p>
</div></body></html>"""


def do_fetch():
    with STATE["lock"]:
        was_open = STATE.get("market_open", True)
        is_open = market_open()
        STATE["market_open"] = is_open
        if not is_open:
            ts = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            try:
                rows = db_latest_rows(STATE["symbol"])
            except Exception:
                rows = []
            if rows:
                trs = "\n".join(build_row_html(tuple(r[:23]), STATE["strike"])
                                for r in rows)
                last_ts = rows[0][23]
                try:
                    payload = strategies.static_payload(STATE["symbol"],
                                                        spot=rows[0][1])
                    sdash = strategies.render_dashboard(payload)
                except Exception as e:
                    print("strategy dashboard (closed) failed:", e)
                    sdash = ""
                tbsection = build_trades_board(STATE["symbol"])
                html = build_html(
                    STATE["symbol"], STATE.get("expiry", "-"), rows[0][1], rows[0][2],
                    last_ts + " (last collect)", STATE["interval"], trs,
                    banner="MARKET IS CLOSED — showing last collected data "
                           f"({last_ts}). Refresh is disabled until 9:15 AM.",
                    strategy_section=sdash,
                    trades_board_section=tbsection)
            else:
                tbsection = build_trades_board(STATE["symbol"])
                html = build_html(STATE["symbol"], "-", None, None, ts,
                                  STATE["interval"], "",
                                  banner="MARKET IS CLOSED — no data collected yet. "
                                         "Collection starts automatically at 9:15 AM.",
                                  trades_board_section=tbsection)
            HTML_PATH.write_text(html, encoding='utf-8')
            print(f"[{ts}] market closed — showing frozen table")
            return
        if not was_open:
            try:
                n = db_purge_out_of_hours()
                if n:
                    print(f"purged {n} out-of-hours rows on market open")
            except Exception as e:
                print("purge failed:", e)
        try:
            session, cookies = new_session()
            r = session.get(URL_CONTRACT + STATE["symbol"], headers=HEADERS,
                            timeout=10, cookies=cookies)
            if r is None or r.status_code != 200:
                raise Exception("NSE contract info unavailable")
            expiry = r.json()['expiryDates'][0]
            STATE["expiry"] = expiry
            session, cookies, chain = fetch_chain(session, cookies, STATE["symbol"],
                                                  "Indices", expiry)
            recs = chain['records']
            spot = recs['underlyingValue']
            server_ts = recs.get('timestamp', '-')
            rows = pick_rows(chain, expiry, STATE["strike"])
            fetched = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            t = years_to_expiry(expiry)

            db_args, trs = [], []
            for d in rows:
                ce, pe = d.get('CE') or {}, d.get('PE') or {}
                k = float(d['strikePrice'])
                ce_d, ce_g, ce_t, ce_v = bs_greeks(spot, k, t, ce.get('impliedVolatility'), True)
                pe_d, pe_g, pe_t, pe_v = bs_greeks(spot, k, t, pe.get('impliedVolatility'), False)
                db_args.append((STATE["symbol"], expiry, k, spot, server_ts,
                                ce.get('lastPrice'), ce.get('change'), ce.get('openInterest'),
                                ce.get('changeinOpenInterest'), ce.get('totalTradedVolume'),
                                ce.get('impliedVolatility'), ce_d, ce_g, ce_t, ce_v,
                                pe.get('lastPrice'), pe.get('change'), pe.get('openInterest'),
                                pe.get('changeinOpenInterest'), pe.get('totalTradedVolume'),
                                pe.get('impliedVolatility'), pe_d, pe_g, pe_t, pe_v))
                trs.append(build_row_html(
                    (k, spot, server_ts,
                     ce.get('lastPrice'), ce.get('change'), ce.get('openInterest'),
                     ce.get('changeinOpenInterest'), ce.get('totalTradedVolume'),
                     ce.get('impliedVolatility'), ce_d, ce_g, ce_t, ce_v,
                     pe.get('lastPrice'), pe.get('change'), pe.get('openInterest'),
                     pe.get('changeinOpenInterest'), pe.get('totalTradedVolume'),
                     pe.get('impliedVolatility'), pe_d, pe_g, pe_t, pe_v),
                    STATE["strike"]))

            n = db_insert(STATE["symbol"], expiry, spot, server_ts, db_args)

            sdash = ""
            try:
                vix = strategies.get_vix(fetch_json_nse)
                payload = strategies.run_cycle(STATE["symbol"], expiry, spot,
                                               db_args, datetime.datetime.now(),
                                               vix=vix, t_years=t)
                sdash = strategies.render_dashboard(payload)
                if payload["new_signals"]:
                    for g in payload["new_signals"]:
                        print(f"SIGNAL {g['strategy']} {g['dir'] if 'dir' in g else ''} "
                              f"{g['detail']}")
            except Exception as e:
                print("strategy cycle failed:", e)

            tbsection = build_trades_board(STATE["symbol"])
            html = build_html(STATE["symbol"], expiry, spot, server_ts, fetched,
                              STATE["interval"], "\n".join(trs),
                              strategy_section=sdash,
                              trades_board_section=tbsection)
            HTML_PATH.write_text(html, encoding='utf-8')
            STATE["last_fetch"] = fetched
            STATE["last_error"] = None
            print(f"[{fetched}] OK spot={spot:,.2f} server={server_ts} rows={len(rows)} saved={n}")
        except Exception as e:
            STATE["last_error"] = f"Fetch failed: {e} — will retry"
            print(f"[{datetime.datetime.now():%H:%M:%S}] ERROR {e}")


def fetch_loop():
    while True:
        do_fetch()
        # when closed, idle at 60s regardless of chosen interval
        wait = STATE["interval"] if STATE.get("market_open", True) else 60
        deadline = time.time() + wait
        while time.time() < deadline:
            time.sleep(1)


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def _send(self, code, body, ctype="text/html; charset=utf-8"):
        data = body.encode()
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path == "/":
            if HTML_PATH.exists():
                self._send(200, HTML_PATH.read_text(encoding='utf-8'))
            else:
                self._send(200, "<h1>Waiting for first fetch…</h1>")
        elif parsed.path == "/refresh":
            if STATE.get("market_open", True):
                threading.Thread(target=do_fetch, daemon=True).start()
                self._send(200, "ok")
            else:
                self._send(200, "closed")   # no fetch when market closed
        elif parsed.path == "/set_interval":
            q = parse_qs(parsed.query)
            try:
                v = int(q.get('s', ['60'])[0])
                STATE["interval"] = v if v in INTERVALS else 60
            except ValueError:
                pass
            self._send(200, "ok")
        elif parsed.path == "/api/strikes":
            try:
                self._send(200, json.dumps(db_available_strikes(STATE["symbol"])),
                           "application/json")
            except Exception as e:
                self._send(500, json.dumps({"error": str(e)}), "application/json")
        elif parsed.path == "/api/series":
            q = parse_qs(parsed.query)
            try:
                strikes = [float(s) for s in q.get('strikes', [''])[0].split(',') if s]
                metric = q.get('metric', ['ltp'])[0]
                data = db_series(STATE["symbol"], strikes[:CHART_STRIKES], metric)
                self._send(200, json.dumps(data), "application/json")
            except Exception as e:
                self._send(500, json.dumps({"error": str(e)}), "application/json")
        elif parsed.path == "/api/trade_dates":
            try:
                dates = db_available_trade_dates(STATE["symbol"])
                self._send(200, json.dumps(dates), "application/json")
            except Exception as e:
                self._send(500, json.dumps({"error": str(e)}), "application/json")
        elif parsed.path == "/api/trades":
            q = parse_qs(parsed.query)
            try:
                date_str = q.get('date', [None])[0]
                if not date_str:
                    date_str = datetime.date.today().isoformat()
                trades = db_trades_by_date(STATE["symbol"], date_str)
                self._send(200, json.dumps(trades), "application/json")
            except Exception as e:
                self._send(500, json.dumps({"error": str(e)}), "application/json")
        else:
            self._send(404, "not found")


def build_trades_board(symbol):
    """Generate the P&L Observation Board HTML section."""
    today = datetime.date.today().isoformat()
    return f"""
<div class="pnl-board">
  <h2>P&L Observation Board</h2>
  <p class="pnl-meta">Date-wise consolidated P&L, strike prices, entry/exit costs, timestamps,
     strategy used, market context at entry (spot/IV/VIX), and regime tags for manual verification.</p>
  <div class="pnl-controls">
    <label>Date: <input type="date" id="pnl-date" value="{today}"></label>
    <span style="color:#777;font-size:12px;">Select a date to view trades. Only dates with DB rows are shown.</span>
  </div>
  <div class="pnl-summary" id="pnl-summary"></div>
  <div id="pnl-trades-content"></div>
</div>
"""


def main():
    symbol = sys.argv[1] if len(sys.argv) > 1 else "NIFTY"
    strike = float(sys.argv[2]) if len(sys.argv) > 2 else 22700.0
    STATE["symbol"], STATE["strike"] = symbol, strike

    try:
        db_init()
    except Exception as e:
        print(f"DB init skipped (no Postgres): {e}")
    try:
        n = db_purge_out_of_hours()
        print(f"DB ready: {DB_DSN} table={DB_TABLE} (purged {n} out-of-hours rows)")
    except Exception as e:
        print(f"DB ready: {DB_DSN} table={DB_TABLE} (purge failed: {e})")
    threading.Thread(target=fetch_loop, daemon=True).start()
    server = ThreadingHTTPServer((HOST, PORT), Handler)
    print(f"Serving page at http://{HOST}:{PORT}  (interval {STATE['interval']}s)")
    sys.stdout.flush()
    server.serve_forever()


if __name__ == "__main__":
    main()
