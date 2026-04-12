@echo off
title AgriMacro Dashboard Only
color 0A
echo.
echo  ======================================
echo   AgriMacro Dashboard (sem pipeline)
echo   %date% %time:~0,5%
echo  ======================================
echo.

set "DASHDIR=C:\Users\felip\OneDrive\Área de Trabalho\agrimacro\agrimacro-dash"
cd /d "%DASHDIR%"

:: Mata processo anterior na porta 3000 se existir
for /f "tokens=5" %%a in ('netstat -aon ^| findstr ":3000.*LISTENING" 2^>nul') do (
    taskkill /PID %%a /F >nul 2>&1
)

echo [1/2] Iniciando Next.js...
start /min "NextDev" cmd /c "cd /d "%DASHDIR%" && npx next dev --port 3000"

echo [2/2] Aguardando servidor (max 45s)...
set /a count=0
:wait_loop
timeout /t 3 /nobreak >nul
set /a count+=3
netstat -aon | findstr ":3000.*LISTENING" >nul && goto :server_ready
if %count% lss 45 goto :wait_loop

echo [AVISO] Timeout - abrindo browser de qualquer forma...
:server_ready
start http://localhost:3000

echo.
echo  Dashboard aberto em http://localhost:3000
echo  Feche esta janela para encerrar.
echo.
