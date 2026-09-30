@echo off
title NSE Option Chain - Live Viewer
cd /d "%~dp0"

set SYMBOL=NIFTY
set STRIKE=22700

echo ============================================
echo   NSE Option Chain Live Viewer
echo   Symbol : %SYMBOL%
echo   Strike : %STRIKE%
echo   Page   : http://localhost:8899
echo   Refresh: 3s / 10s / 30s / 60s (choose on page; default 60s)
echo   Saves  : every fetch to PostgreSQL
echo   Close this window to stop.
echo ============================================
echo.

start "" http://localhost:8899

python option_chain_live.py %SYMBOL% %STRIKE%

echo.
echo Fetcher stopped.
pause
