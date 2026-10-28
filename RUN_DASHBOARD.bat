@echo off
setlocal
title Urban Civic Complaint Analytics Dashboard

echo ====================================================================
echo   URBAN CIVIC COMPLAINT & SERVICE RESOLUTION ANALYTICS
echo   Hadoop MapReduce Big Data Web Dashboard
echo ====================================================================
echo.

cd /d "%~dp0"

if not exist ".venv\Scripts\streamlit.exe" (
    echo [ERROR] Virtual environment not found or Streamlit not installed.
    echo Please ensure .venv is created and dependencies are installed.
    pause
    exit /b 1
)

echo Starting Streamlit Local Dashboard on http://localhost:8501 ...
echo Press Ctrl+C in this terminal window to stop the dashboard.
echo.

start "" "http://localhost:8501"
".venv\Scripts\streamlit.exe" run dashboard/app.py --server.port 8501 --server.headless false --browser.gatherUsageStats false

pause
