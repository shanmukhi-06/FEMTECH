@echo off
title The Case File — Backend Server
echo.
echo  ==========================================
echo   THE CASE FILE - AI Investigator Backend
echo  ==========================================
echo.
echo  Starting FastAPI server on http://127.0.0.1:8000
echo  Keep this window OPEN while using the dashboard.
echo  Press Ctrl+C to stop the server.
echo.
cd /d "%~dp0backend"
python server.py
pause
