@echo off
title AgriMacro Full (Pipeline + Dashboard)
color 0A
echo.
echo  ======================================
echo   AgriMacro Intelligence - Full Start
echo   %date% %time:~0,5%
echo  ======================================
echo.

cd /d "C:\Users\felip\OneDrive\Área de Trabalho\agrimacro"

:: Mata processo anterior na porta 3000 se existir
for /f "tokens=5" %%a in ('netstat -aon ^| findstr ":3000.*LISTENING" 2^>nul') do (
    taskkill /PID %%a /F >nul 2>&1
)

echo [1/3] Rodando pipeline em background...
start /min "AgriMacro Pipeline" cmd /c "cd /d \"C:\Users\felip\OneDrive\Área de Trabalho\agrimacro\" && python pipeline\run_pipeline.py && echo Pipeline OK. && timeout /t 5 >nul"

echo [2/3] Iniciando dashboard...
cd /d "C:\Users\felip\OneDrive\Área de Trabalho\agrimacro\agrimacro-dash"
start /min "AgriMacro Dashboard" cmd /c "npx next dev --port 3000"

echo [3/3] Aguardando servidor (max 30s)...
set /a count=0
:wait_loop
timeout /t 2 /nobreak >nul
set /a count+=2
powershell -Command "Test-NetConnection -ComputerName localhost -Port 3000 -WarningAction SilentlyContinue | Select-Object -ExpandProperty TcpTestSucceeded" 2>nul | findstr "True" >nul && goto :server_ready
if %count% lss 30 goto :wait_loop

echo [AVISO] Timeout - abrindo browser de qualquer forma...
:server_ready
start http://localhost:3000

echo.
echo  AgriMacro rodando!
echo  Dashboard: http://localhost:3000
echo  Pipeline: rodando em janela separada
echo  Feche esta janela quando quiser.
echo.
