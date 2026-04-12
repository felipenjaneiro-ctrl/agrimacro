@echo off
title AgriMacro Intelligence
color 0A
echo.
echo  ======================================
echo   AgriMacro Intelligence - Starting...
echo   %date% %time:~0,5%
echo  ======================================
echo.

cd /d "C:\Users\felip\OneDrive\Area de Trabalho\agrimacro"

echo [1/4] Waiting 2 minutes for system initialization...
timeout /t 120 /nobreak >nul

echo [2/4] Running pipeline (25 steps including Grok email)...
start "AgriMacro Pipeline" cmd /c "cd /d \"C:\Users\felip\OneDrive\Area de Trabalho\agrimacro\" && python pipeline\run_pipeline.py && echo. && echo Pipeline finished. Press any key... && pause >nul"

echo [3/4] Starting dashboard...
cd /d "C:\Users\felip\OneDrive\Area de Trabalho\agrimacro\agrimacro-dash"
start "AgriMacro Dashboard" cmd /c "cd /d \"C:\Users\felip\OneDrive\Area de Trabalho\agrimacro\agrimacro-dash\" && npm run dev"

echo [4/4] Waiting 15s for server startup...
timeout /t 15 /nobreak >nul

echo Opening browser...
start http://localhost:3000

echo.
echo  AgriMacro is running!
echo  Dashboard: http://localhost:3000
echo  Pipeline: running in separate window
echo  Close this window anytime.
echo.
