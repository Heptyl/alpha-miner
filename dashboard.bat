@echo off
rem Alpha Miner dashboard launcher - double click to start (English-only: GBK console safety)
cd /d %~dp0
set PYTHONUTF8=1
.venv\Scripts\python.exe scripts\dashboard.py --open %*
pause
