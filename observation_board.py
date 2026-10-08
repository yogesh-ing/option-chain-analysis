"""
Postgres-backed per-trade observation board for manual audit and export.

Single responsibility: read `paper_trades` + `strategy_signals` and return a
flat, dated, per-trade ledger in the exact column order the user asked for, so
it can be reviewed manually and fed into a backtest platform.

This module is read-only. It does not create tables, write trades, or change
engine state. It is intentionally small: one query path, one row builder, one
date-board function, and a thin CSV/JSON exporter.

Sign convention (documented here so a human reading the CSV is never confused):
- entry_cost_per_lot is signed exactly as paper_trades.net_cost is stored:
  positive = debit paid, negative = credit received.
- exit_cost_per_lot is the per-lot MTM value at exit, signed the same way.
- realized_pnl_per_lot = exit_cost_per_lot - entry_cost_per_lot.
- realized_pnl_total = realized_pnl_per_lot * qty * lot.

Regime tags are simple and derived only from what we can confirm at entry time.
If market context is missing, the tag is UNKNOWN and the P&L fields are still
complete. We never fabricate spot/IV/VIX.
"""

from __future__ import annotations

import datetime
import json
import typing as t

from strategies import (
    STRAT_DSN,
    _conn,
    _to_dt,
    _strat_long,
    _regime_tag,
    _snap_at,
)


SELECTED_COLUMNS = [
    "t.id",
    "t.strategy",
    "t.direction",
    "t.structure",
    "t.legs",
    "t.qty",
    "t.lot",
    "t.net_cost",
    "t.entry_at",
    "t.exit_at",
    "t.mtm_cost",
    "t.u_pnl",
    "t.realized",
    "t.status",
    "t.exit_reason",
    "t.meta",
    "ts.detail AS sig_detail",
    "ts.fired_at AS sig_fired",
    "to_char(t.entry_at, 'YYYY-MM-DD') AS entry_date",
    "t.symbol",
]


def _board_rows(symbol: str) -> list[dict]:
    """All paper_trades for `symbol`, by entry date, from STRAT_DSN.

    This is the only query in the board. It is deliberately one SELECT so the
    shape is easy to audit and so it survives restarts: it does not depend on
    any in-process payload.
    """
    conn = _conn(STRAT_DSN)
    try:
        cur = conn.cursor()
        columns = ",\r\n                ".join(SELECTED_COLUMNS)
        cur.execute(
            f"""
            SELECT
                {columns}
            FROM paper_trades t
            LEFT JOIN strategy_signals ts ON ts.id = t.signal_id
            WHERE t.symbol = %s
            ORDER BY t.entry_at, t.id
            """,
            (symbol,),
        )
        rows = cur.fetchall()
        return [_build_row(row) for row in rows]
    finally:
        conn.close()


UNPACK_COUNT = len(SELECTED_COLUMNS)

_BOARD_FIELDS = [
    "id",
    "strategy",
    "direction",
    "structure",
    "legs",
    "qty",
    "lot",
    "net_cost",
    "entry_at",
    "exit_at",
    "mtm_cost",
    "u_pnl",
    "realized",
    "status",
    "exit_reason",
    "meta",
    "sig_detail",
    "sig_fired",
    "entry_date",
    "symbol",
]


def _build_row(row: tuple) -> dict:
    if len(row) != UNPACK_COUNT:
        raise ValueError(
            f"expected {UNPACK_COUNT} columns from board query, got {len(row)}"
        )

    row_map = dict(zip(_BOARD_FIELDS, row))
    symbol = row_map["symbol"]
    strategy = row_map["strategy"]
    direction = row_map["direction"]
    structure = row_map["structure"]
    legs = row_map["legs"]
    qty = row_map["qty"]
    lot = row_map["lot"]
    net_cost = row_map["net_cost"]
    entry_at = row_map["entry_at"]
    exit_at = row_map["exit_at"]
    mtm_cost = row_map["mtm_cost"]
    u_pnl = row_map["u_pnl"]
    realized = row_map["realized"]
    status = row_map["status"]
    exit_reason = row_map["exit_reason"]
    meta = row_map["meta"]
    sig_detail = row_map["sig_detail"]
    sig_fired = row_map["sig_fired"]
    entry_date = row_map["entry_date"]
    tid = row_map["id"]

    entry_dt = _to_dt(entry_at)
    exit_dt = _to_dt(exit_at)

    entry_cost: t.Optional[float] = float(net_cost) if net_cost is not None else None

    if status == "closed" and exit_dt is not None and entry_dt is not None:
        exit_cost = float(mtm_cost) if mtm_cost is not None else entry_cost
        realized_val: t.Optional[float] = (
            float(realized)
            if realized is not None
            else (exit_cost - entry_cost) * (qty or 1) * (lot or 1)
        )
    elif status == "open" and entry_dt is not None:
        exit_cost = float(mtm_cost) if mtm_cost is not None else entry_cost
        realized_val: t.Optional[float] = None
    else:
        exit_cost: t.Optional[float] = None
        realized_val = None

    per_lot: t.Optional[float] = (
        (exit_cost - entry_cost)
        if exit_cost is not None and entry_cost is not None
        else None
    )

    legs_parsed = legs if isinstance(legs, list) else (
        json.loads(legs) if isinstance(legs, str) else []
    )
    strikes = _strike_list(legs_parsed)

    holding_minutes: t.Optional[int] = None
    if exit_dt and entry_dt:
        holding_minutes = int((exit_dt - entry_dt).total_seconds() / 60.0)

    snap = _snap_at(symbol, entry_dt) if entry_dt and symbol else None
    regime = _regime_tag(strategy, direction, meta, snap, entry_dt)

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
        "realized_pnl_per_lot": per_lot,
        "realized_pnl_total": realized_val,
        "exit_reason": exit_reason or "",
        "holding_minutes": holding_minutes,
        "spot_at_entry": snap.get("spot") if snap else None,
        "atm_iv_at_entry": snap.get("atm_iv") if snap else None,
        "vix_at_entry": snap.get("vix") if snap else None,
        "regime_tag": regime,
        "status": status or "",
        "signal_detail": sig_detail,
        "signal_fired_at": sig_fired.strftime("%H:%M:%S") if sig_fired else "",
    }


def _strike_list(legs: list[dict]) -> str:
    if not legs:
        return ""
    parts: list[str] = []
    for leg in legs:
        if not isinstance(leg, dict):
            continue
        side = leg.get("side") or ""
        action = leg.get("action") or ""
        strike = leg.get("strike")
        if strike is None:
            continue
        parts.append(f"{int(round(float(strike)))} {side} {action}")
    return " / ".join(parts)


def date_board(
    symbol: str,
    *,
    at_date: t.Optional[t.Union[datetime.date, str]] = None,
) -> dict:
    """Per-trade ledger for one calendar date (IST), plus a one-line summary.

    `at_date`:
    - None means today (IST).
    - a datetime.date or ISO date string means that calendar date.

    Returns:
    {
        "date": "YYYY-MM-DD",
        "rows": [ board row, ... ],
        "summary": { trades, realized_total, win_rate, avg_holding_minutes },
        "has_rows": bool,
    }

    When there are no trades for the chosen date, `rows` is empty and
    `has_rows` is False. That is the normal "no trades for this day" state.
    """
    if at_date is None:
        at = datetime.date.today()
    elif isinstance(at_date, datetime.date):
        at = at_date
    else:
        at = datetime.date.fromisoformat(str(at_date))

    rows = _board_rows(symbol)
    daily = [r for r in rows if r and r.get("date") == at.isoformat()]
    return {
        "date": at.isoformat(),
        "rows": daily,
        "summary": _day_summary(daily),
        "has_rows": bool(daily),
    }


def _day_summary(rows: list[dict]) -> dict:
    closed = [r for r in rows if r.get("status") == "closed"]

    all_trades = len(rows)
    closed_trades = len(closed)

    if closed:
        realized_total = sum((r.get("realized_pnl_total") or 0.0) for r in closed)
        wins = sum(1 for r in closed if (r.get("realized_pnl_total") or 0.0) > 0)
        holdings = [r.get("holding_minutes") for r in closed if r.get("holding_minutes") is not None]
    else:
        realized_total = 0.0
        wins = 0
        holdings = []

    return {
        "trades": closed_trades,
        "total_trades": all_trades,
        "realized_total": realized_total,
        "win_rate": (wins / closed_trades) if closed_trades else None,
        "avg_holding_minutes": (
            round(sum(holdings) / len(holdings)) if holdings else None
        ),
    }


def board_to_csv(rows: list[dict]) -> str:
    """Flat CSV for one date's rows, in the documented column order.

    Empty fields use an empty string, never the word "None".
    """
    if not rows:
        header = ",".join(CSV_COLUMNS)
        return header + "\n"

    lines: list[str] = [",".join(CSV_COLUMNS)]
    for row in rows:
        lines.append(",".join(_csv_field(row.get(c, "")) for c in CSV_COLUMNS))
    return "\n".join(lines) + "\n"


def board_to_json(rows: list[dict]) -> str:
    return json.dumps(rows, indent=2, default=str) + "\n"


CSV_COLUMNS = [
    "date",
    "entry_time",
    "exit_time",
    "symbol",
    "strategy",
    "variant",
    "direction",
    "structure",
    "strikes",
    "entry_cost_per_lot",
    "exit_cost_per_lot",
    "realized_pnl_per_lot",
    "realized_pnl_total",
    "exit_reason",
    "holding_minutes",
    "spot_at_entry",
    "atm_iv_at_entry",
    "vix_at_entry",
    "regime_tag",
    "status",
    "signal_detail",
    "signal_fired_at",
]


def _csv_field(value: t.Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        need_quote = "," in value or '"' in value or "\n" in value
        if need_quote:
            return '"' + value.replace('"', '""') + '"'
        return value
    return str(value)
