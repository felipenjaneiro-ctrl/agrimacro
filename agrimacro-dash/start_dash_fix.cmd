@echo off
title AgriMacro Dashboard
echo ============================================
echo   AgriMacro Intelligence - Dashboard Start
echo ============================================
echo.

:: Check if port 3000 is already in use
netstat -aon | findstr ":3000 " | findstr "LISTENING" >nul 2>&1
if %errorlevel%==0 (
    echo [!] Port 3000 is already in use. Killing existing process...
    for /f "tokens=5" %%p in ('netstat -aon ^| findstr ":3000 " ^| findstr "LISTENING"') do (
        echo     Killing PID %%p ...
        taskkill /F /PID %%p >nul 2>&1
    )
    timeout /t 2 /nobreak >nul
    echo [OK] Port 3000 freed.
    echo.
)

:: Navigate to project directory
cd /d "C:\Users\felip\OneDrive\Área de Trabalho\agrimacro\agrimacro-dash"
if %errorlevel% neq 0 (
    echo [ERROR] Project directory not found!
    pause
    exit /b 1
)

echo [*] Starting Next.js dev server on port 3000...
echo [*] Browser will open in 8 seconds...
echo.

:: Start the dev server in background
start "" /min cmd /c "npx next dev --port 3000"

:: Wait for server to be ready
timeout /t 8 /nobreak >nul

:: Open browser
start http://localhost:3000

echo [OK] Dashboard launched at http://localhost:3000
echo [*] Close this window to keep the server running in background.
echo [*] To stop: taskkill /F /IM node.exe (kills ALL node processes)
echo.
pause
