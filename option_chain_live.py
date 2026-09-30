"""Live NSE Option Chain: 10 ITM + ATM + 10 OTM (21 strikes, ATM centered).

- Serves http://localhost:8899 with a Refresh button, interval dropdown
  (3s / 10s / 30s / 60s, default 60s) and a column picker (LTP, Chg, OI,
  Chg OI, Volume, IV, Delta, Gamma, Theta, Vega per side)
- Computes Black-Scholes greeks and persists everything to PostgreSQL

Usage: python option_chain_live.py [SYMBOL] [STRIKE]   (defaults: NIFTY, 22700)
"""
import datetime
import math
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse, parse_qs

import psycopg2
import requests

HTML_PATH = Path(__file__).parent / "option_chain_live.html"
HOST, PORT = "127.0.0.1", 8899
DB_DSN = "postgresql://postgres:postgres@localhost:5432/postgres"
DB_TABLE = "option_chain_snapshots"

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
INTERVALS = [3, 10, 30, 60]  # seconds; 60 is default
RISK_FREE_RATE = 0.065  # annualised, adjust if needed

# Market hours (IST) — F&O trades till 15:40 since Aug 2026 (CAS reform);
# post-market OI reconciliation lands ~15:40-16:00, so we poll till 16:05.
MARKET_OPEN_TIME = datetime.time(9, 15)
MARKET_CLOSE_TIME = datetime.time(15, 40)   # F&O trading close
COLLECT_END_TIME = datetime.time(16, 5)     # keep polling for reconciliation
URL_MARKET_STATUS = "https://www.nseindia.com/api/marketStatus"

COLUMNS = ["ltp", "chg", "oi", "chg_oi", "volume", "iv", "delta", "gamma", "theta", "vega"]
COL_LABELS = {"ltp": "LTP", "chg": "Chg", "oi": "OI", "chg_oi": "Chg&nbsp;OI",
              "volume": "Volume", "iv": "IV", "delta": "Delta", "gamma": "Gamma",
              "theta": "Theta/day", "vega": "Vega/1%"}


# ---------------------------------------------------------------- NSE fetch --
def new_session():
    session = requests.Session()
    r = session.get(URL_OC, headers=HEADERS, timeout=10)
    return session, dict(r.cookies)


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
               banner=None, err=None):
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
            # show the LAST collected table, frozen, with a closed banner
            try:
                rows = db_latest_rows(STATE["symbol"])
            except Exception:
                rows = []
            if rows:
                trs = "\n".join(build_row_html(tuple(r[:23]), STATE["strike"])
                                for r in rows)
                last_ts = rows[0][23]
                html = build_html(
                    STATE["symbol"], STATE.get("expiry", "-"), rows[0][1], rows[0][2],
                    last_ts + " (last collect)", STATE["interval"], trs,
                    banner="MARKET IS CLOSED — showing last collected data "
                           f"({last_ts}). Refresh is disabled until 9:15 AM.")
            else:
                html = build_html(STATE["symbol"], "-", None, None, ts,
                                  STATE["interval"], "",
                                  banner="MARKET IS CLOSED — no data collected yet. "
                                         "Collection starts automatically at 9:15 AM.")
            HTML_PATH.write_text(html, encoding='utf-8')
            print(f"[{ts}] market closed — showing frozen table")
            return
        if not was_open:   # transition closed -> open
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
            html = build_html(STATE["symbol"], expiry, spot, server_ts, fetched,
                              STATE["interval"], "\n".join(trs))
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
        else:
            self._send(404, "not found")


def main():
    symbol = sys.argv[1] if len(sys.argv) > 1 else "NIFTY"
    strike = float(sys.argv[2]) if len(sys.argv) > 2 else 22700.0
    STATE["symbol"], STATE["strike"] = symbol, strike

    db_init()
    try:
        n = db_purge_out_of_hours()
        print(f"DB ready: {DB_DSN} table={DB_TABLE} (purged {n} out-of-hours rows)")
    except Exception as e:
        print(f"DB ready: {DB_DSN} table={DB_TABLE} (purge failed: {e})")
    threading.Thread(target=fetch_loop, daemon=True).start()
    server = ThreadingHTTPServer((HOST, PORT), Handler)
    print(f"Serving page at http://{HOST}:{PORT}  (interval {STATE['interval']}s)")
    server.serve_forever()


if __name__ == "__main__":
    main()
