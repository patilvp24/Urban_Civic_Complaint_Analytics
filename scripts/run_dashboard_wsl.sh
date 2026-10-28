#!/usr/bin/env bash
set -e

PROJECT_DIR="/mnt/c/Users/veetrag/Desktop/B.Tech_IT/BDAL/Urban_Civic_Complaint_Analytics"
cd "$PROJECT_DIR"

echo "===================================================================="
echo "  STARTING URBAN CIVIC DASHBOARD (WSL COMPATIBLE LAUNCHER)"
echo "===================================================================="

# Check if windows .venv exists or linux python3-streamlit
if [ -f "$PROJECT_DIR/.venv/Scripts/streamlit.exe" ]; then
    echo "Launching via project .venv Streamlit on Windows host..."
    /mnt/c/Windows/System32/cmd.exe /c RUN_DASHBOARD.bat
else
    echo "Launching via WSL python3 streamlit..."
    python3 -m streamlit run dashboard/app.py --server.port 8501 --server.headless true
fi
