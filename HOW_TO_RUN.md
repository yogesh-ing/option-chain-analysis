# How to Run — NSE Option Chain Live Viewer

## Quick Start (Double-click)

**Double-click `run_option_chain.bat`** — that's it.

A console opens showing fetch logs, and your browser opens
**http://localhost:8899** — the live table.

- Close the console window to stop fetching.
- Keep it running during market hours (9:15 AM – 3:30 PM IST) for live data.

## Market Hours Handling

- Data is collected while the **NSE Capital Market is Open** (detected via NSE's
  official market status API) — plus a built-in clock fallback
  (Mon–Fri, **9:15 AM – 4:05 PM IST**).
- Why 4:05 PM: F&O trades till **3:40 PM** (since Aug 2026) and NSE posts the
  final/reconciled open-interest data shortly after close (~3:40–4:00 PM),
  so the collector keeps polling through that post-market window.
- Start the server anytime — before market open, during the day, or after
  close. While closed it shows the **last collected table frozen**, with a
  "Market is closed" header and Refresh/auto-refresh disabled. It
  **resumes collecting automatically** when the market opens.
- Rows captured outside the 9:15–16:05 window are deleted from the DB
  automatically (on startup and at every market open). History stays clean.

## Refresh Controls (on the page)

- **🔄 Refresh now** button — fetches fresh data from NSE immediately
- **Auto refresh dropdown** — 3s / 10s / 30s / 60s; **60s is default**
- **Columns ▾ button** — choose which columns show (per side: LTP, Chg, OI,
  Chg OI, Volume, IV, Delta, Gamma, Theta/day, Vega/1%). Your selection is
  remembered in the browser. Defaults: LTP, Chg, OI, Chg OI, IV, Delta.
- A countdown shows when the next auto refresh happens
- The chosen interval persists until you close the app

## Greeks

Black-Scholes greeks (Delta, Gamma, Theta/day, Vega per 1% IV move) are
computed each fetch from NSE's IV, spot and time to expiry (risk-free rate
6.5% — edit `RISK_FREE_RATE` in `option_chain_live.py`). They are stored in
the DB and shown on the page.

## Database (PostgreSQL)

Every fetch is saved automatically.

- DSN: `postgresql://postgres:postgres@localhost:5432/postgres`
- Table: `option_chain_snapshots` (created automatically)
- One row per strike per fetch: symbol, expiry, strike, spot, snapshot_at,
  server_time, and CE/PE ltp, chg, oi, chg_oi, volume, iv, delta, gamma,
  theta, vega
- Note: at 3s refresh this is ~42k rows/hour — use 60s for long sessions
- Sample query — latest snapshot for NIFTY:

```sql
SELECT * FROM option_chain_snapshots
WHERE symbol = 'NIFTY'
  AND snapshot_at = (SELECT MAX(snapshot_at) FROM option_chain_snapshots
                     WHERE symbol = 'NIFTY')
ORDER BY strike;
```

## Default Settings

| Setting | Default | How to Change |
|---------|---------|---------------|
| Symbol  | NIFTY   | Edit the `.bat` file, line `set SYMBOL=NIFTY` |
| Strike  | 22700   | Edit the `.bat` file, line `set STRIKE=22700` |
| Refresh | 60s     | Pick from the dropdown on the page |
| DB DSN  | postgres:postgres@localhost:5432 | Edit `DB_DSN` in `option_chain_live.py` |

Other symbols that work: `BANKNIFTY`, `FINNIFTY`, `MIDCPNIFTY`, `NIFTYNXT50`
(any NSE index or stock symbol).

## Manual Run (Terminal)

```bat
python option_chain_live.py NIFTY 22700
```

- Argument 1 = symbol (default NIFTY)
- Argument 2 = strike price (default 22700)

Press `Ctrl+C` to stop.

## Output

- **http://localhost:8899** — the live table (10 ITM + ATM + 10 OTM = 21 strikes,
  ATM always the middle row; Calls and Puts with LTP / Chg / OI / Chg OI / Volume / IV)
- ATM row is highlighted yellow
- `option_chain_live.html` — offline static copy of the latest snapshot (fallback)

## First-Time Setup (Already Done)

If you move this folder to another PC:

```bat
pip install -r requirements.txt
```

Needs Python 3.6+ with tkinter (standard installer from python.org works).

## Troubleshooting

| Problem | Fix |
|---------|-----|
| "Fetch failed" in page | NSE blocked the request — wait, it retries automatically |
| Data frozen after 3:30 PM | Market closed; values are last traded ones |
| `python` not found | Install Python from python.org, tick "Add to PATH" |
| Page won't open (8899 busy) | Another instance is running — close it first |
| DB errors in console | Check PostgreSQL is running and password is postgres:postgres |
| Page shows "Market is closed" | Normal outside 9:15–16:05 IST Mon–Fri; frozen table shown, resumes by itself |
