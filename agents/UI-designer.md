---
name: trading-ui
description: Design system + engineering rules for the trading platform. Use when building or reviewing any screen: dashboards, order books, charts, order tickets, positions/P&L tables, watchlists, alerts. Triggers on trading, orderbook, candlestick, ticker, buy/sell, market data, UI polish, redesign, "feels off", density, tabular numbers, price flash, hover/focus states, animations, icons, dark mode.
---

# Trading UI

A trading interface is judged on how fast a user can read a number, trust it, and act. Every decision below optimizes for scan speed, precision, and calm under high update frequency. Nothing should draw attention that isn't information.

Core tenets: maximum data legibility, low latency, optical precision, zero visual friction.

## 1. Design tokens

### Color (dark-first; light is a derived theme)

| Token | Dark | Light | Purpose |
|---|---|---|---|
| bg | #0B0E14 | #FFFFFF | App background |
| surface | #12161F | #F6F7F9 | Panels, cards |
| surface-raised | #1A1F2B | #FFFFFF | Popovers, modals, sticky headers, row hover |
| border | rgba(255,255,255,0.08) | rgba(0,0,0,0.08) | Structural dividers only |
| text | #E6E8EC | #111318 | Primary text |
| text-muted | #8B919E | #5F6673 | Labels, secondary |
| accent | #6D5EF0 | #5B4BDB | Primary actions, selected state, focus ring |
| accent-fg | #FFFFFF | #FFFFFF | Text/icons on accent fills |
| accent-text | #A79BFF | #5B4BDB | Accent-colored text/links on bg or surface |
| up | #16C784 | #057A55 | Gains, bids, buy (text, glyphs, chart, flashes) |
| down | #EA3943 | #D92D20 | Losses, asks, sell (text, glyphs, chart, flashes) |
| up-solid | #0A7F56 | #0A7F56 | Buy button fill (white text) |
| down-solid | #D92D20 | #D92D20 | Sell button fill (white text) |
| warning | #F5A524 | #B45309 | Margin calls, expiring, caution, form validation |
| neutral-change | text-muted | text-muted | 0.00% / unchanged |

Colorblind palette (user toggle) swaps tokens:

| Token | Dark | Light |
|---|---|---|
| up | #3B9EFF | #0B63CE |
| down | #FF8A1F | #C2410C |
| up-solid | #0B63CE | #0B63CE |
| down-solid | #C2410C | #C2410C |

Reference CSS variables (map to Tailwind theme):

```css
:root[data-theme="dark"] {
  --bg:#0B0E14; --surface:#12161F; --surface-raised:#1A1F2B;
  --border:rgba(255,255,255,.08);
  --text:#E6E8EC; --text-muted:#8B919E;
  --accent:#6D5EF0; --accent-fg:#fff; --accent-text:#A79BFF;
  --up:#16C784; --down:#EA3943; --up-solid:#0A7F56; --down-solid:#D92D20;
  --warning:#F5A524;
}
:root[data-theme="light"] {
  --bg:#fff; --surface:#F6F7F9; --surface-raised:#fff;
  --border:rgba(0,0,0,.08);
  --text:#111318; --text-muted:#5F6673;
  --accent:#5B4BDB; --accent-fg:#fff; --accent-text:#5B4BDB;
  --up:#057A55; --down:#D92D20; --up-solid:#0A7F56; --down-solid:#D92D20;
  --warning:#B45309;
}
:root[data-palette="colorblind"][data-theme="dark"]  { --up:#3B9EFF; --down:#FF8A1F; --up-solid:#0B63CE; --down-solid:#C2410C; }
:root[data-palette="colorblind"][data-theme="light"] { --up:#0B63CE; --down:#C2410C; --up-solid:#0B63CE; --down-solid:#C2410C; }
```

Rules:
- `up`/`down` are reserved for market/financial direction, buy/sell, and order fills/rejections. Never use them for generic success/error UI (use accent/warning), so red always means "down or sell."
- Never rely on color alone for direction: always pair with a sign (+/−) or arrow glyph.
- Borders are for structure and selection. Elevation (popovers, dropdowns) uses layered shadows: `0 0 0 1px rgba(0,0,0,.4), 0 8px 24px rgba(0,0,0,.35)` in dark.
- Buy/Sell button fills use `up-solid`/`down-solid` with `accent-fg` white text. Never white text on `up`/`down` directly.
- If a brand tweak breaks contrast, adjust the token, never the rule.

### Typography
- **UI font: Inter** (variable, self-hosted, preloaded). Never rounded/friendly geometric fonts.
- **Numbers:** Inter with `font-variant-numeric: tabular-nums slashed-zero` (Tailwind: `tabular-nums slashed-zero`). **JetBrains Mono** for order book and ledger columns.
- **Every price, quantity, percentage, and timestamp uses tabular-nums. No exceptions.** Right-align numeric columns; align decimal points.
- **Ticks must never cause layout shift or jitter.** Use fixed decimal precision per instrument, reserve column width for the max digits, and never let the sign or arrow change a column's width.
- Scale (px / line-height): 11/16 micro labels · 12/16 dense table · 13/18 default body · 14/20 comfortable body · 16/24 section title · 20/28 page title · 28/32 hero number (portfolio value).
- Weight: 400 body, 500 labels/column headers, 600 key numbers. Never 700+ except the single hero figure.
- Truncate with ellipsis, never wrap, in tables and tickers.

### Spacing & density
- Scale: 2 / 4 / 6 / 8 / 12 / 16 / 24 / 32.
- Default density is compact:
  - Tables: rows 28-32px.
  - **Order book rows: 24px** (20-22px only if rows are not interactive).
  - Inputs and buttons: 32px. Primary Buy/Sell buttons: 36px.
- Offer a "comfortable" density setting (+4px per step).
- Panels are separated by 1px borders or 8px gaps, not 24-48px whitespace. Whitespace is spent inside groups, not between panels. Generous whitespace is forbidden in data grids.
- Radius: 4px controls, 6px panels/cards, 8px modals.
- Concentric rule: for nested elements inset by padding, outer radius = inner radius + padding. Elements flush to a panel edge need no nested radius.

## 2. Craft principles

1. **Concentric border radius**, as above.
2. **Optical over geometric alignment:** icon buttons, arrows in change chips, ticker badges, and play/pause on chart replay.
3. **Shadows for elevation, borders for structure:** panels use borders, floating layers use layered shadows.
4. **Interruptible animations:** CSS transitions for all state changes; keyframes only for one-shot sequences (e.g., order-filled confirmation).
5. **Motion restraint is the rule:** no entrance animations on price updates, order book rows, ticker changes, or hover. Price change feedback is a **≤150ms background flash** (`up`/`down` at 15% alpha) that fades on ease-out. Nothing moves; only color changes.
6. **Never animate layout in live data:** no height/width transitions on rows that reflow with ticks. Fixed row heights everywhere.
7. **Split & stagger only for infrequent entrances** (first dashboard load, onboarding) at ~100ms. Never on tab switches, panel refreshes, or core trading screens.
8. **Subtle exits:** small fixed `translateY(4px)` + opacity, ease-out, softer than enters.
9. **Icon swaps** (e.g., star/unstar) cross-fade with opacity 0→1, scale 0.25→1, blur 4px→0. With framer-motion: `{ type: "spring", duration: 0.3, bounce: 0 }`. Without: two icons in DOM, CSS `cubic-bezier(0.2,0,0,1)`.
10. **Press feedback:**
    - General and secondary buttons: `active:scale-[0.98] transition-transform duration-75`.
    - **No scale on Buy / Sell / Cancel or on any button inside a live-updating row.** Moving targets cause mis-clicks. Use an instant `:active` brightness/darken change instead.
11. **Skip animation on page load:** `initial={false}` on `AnimatePresence`, dropdowns, order modals, and popovers. State icons must not animate in on mount.
12. **Never `transition: all`.** Name exact properties (`transition-colors duration-150`, `transition-transform duration-100`). `will-change` only for `transform`/`opacity`, only after observed stutter (e.g., live charts, dragged depth charts).
13. **Icons:** one library, one stroke weight (1.5px beside 400-500 text, 2px beside 600). `currentColor` only; state via CSS. Outline default, fill for active. Flip directional icons in RTL.
14. **Image/logo outlines:** asset logos and avatars get a 1px `oklch(1 0 0 / 0.1)` (dark) / `oklch(0 0 0 / 0.1)` (light) outline, never a tinted neutral.
15. **Motion is never the only feedback:** every animated change also has a static cue (color, glyph, label).

## 3. Component rules

Each component defines: default · hover · focus-visible · active · disabled · loading · error · empty · stale-data.

- **Ticker / price cell:** tabular-nums, right-aligned, sign + color + flash-on-change. Show "stale" (muted + clock icon) after **5s** without an update for quotes and order books, **15s** for positions/P&L.
- **Order book:** fixed 24px rows, JetBrains Mono, right-aligned numbers, depth bars as background fill (`up`/`down` at **12% alpha**) behind text, no row animations, spread row visually distinct.
- **Change chip:** `+1.24%` / `−0.87%` with arrow glyph, color, and 4px radius. Neutral is muted with no arrow.
- **Order ticket:**
  - Buy = `up-solid`, Sell = `down-solid`, 36px, full width.
  - A confirm step is always required for market orders.
  - Disabled state explains why (insufficient balance, market closed).
  - Toggle states (Buy/Sell, Long/Short) are high-contrast.
  - Inputs show persistent unit labels (USDT, BTC) pinned inside the right edge as non-interactive `text-muted`.
  - Validation errors appear inline directly below the input in `warning` with an icon and message text (never color or motion alone). Use `down` only for order rejections.
- **Positions / P&L table:** sticky header, sticky first column, zebra off, row hover = `surface-raised`, selected row = accent 8% bg + 2px accent left border.
- **Charts:** colors use `up`/`down`/`accent`; gridlines = `border`; crosshair labels use tabular-nums; no draw-in animation after first load.
- **Alerts / toasts:** fills and rejections use `up`/`down`; system messages use accent/warning. Auto-dismiss ≥6s, never during active order entry.
- **Skeletons** match final row heights exactly to prevent layout shift.

## 4. Accessibility (WCAG 2.2 AA)
- Verified contrast: text on bg ≥ 12:1, text-muted on surface ≥ 4.5:1, `up`/`down`/`warning`/`accent-text` on bg and surface ≥ 4.5:1, button text on fills ≥ 4.5:1.
- Focus ring: 2px accent, 2px offset, visible on every interactive element including table rows.
- Live regions (prices, P&L) use `aria-live="off"` by default. A single polite region announces order fills/rejections. Don't read out every tick.
- Respect `prefers-reduced-motion`: flashes become instant color changes, all transitions ≤0ms.
- Hit targets ≥ 24×24px even in compact density. Icon buttons get padding, not smaller icons.
- Offer the colorblind palette toggle in settings and in the first-run flow.

## 5. Output modes

**Build mode** (generating code):
1. Provide complete, production-ready components in React + TypeScript + Tailwind CSS, using the tokens above as CSS variables.
2. Include all component states from section 3.
3. Confirm `tabular-nums` is applied to every numeric cell.
4. Confirm contrast meets section 4.

**Review mode** (auditing a screen):
Slow animations to 10% in the Animations panel and walk every state, including stale-data and empty. Output findings grouped by principle in a table: Severity · Location · Before · After · Why.
- HIGH = misleading, unresponsive, or repeatedly disruptive (e.g., animated price rows, non-tabular numbers, color-only direction, layout shift on tick).
- Verdict: Block / Needs changes / Approve.