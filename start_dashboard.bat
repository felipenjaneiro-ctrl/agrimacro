@echo off
title AgriMacro Dashboard
cd /d "%USERPROFILE%\OneDrive\?rea de Trabalho\agrimacro\agrimacro-dash"
echo Limpando cache...
rmdir /s /q .next 2>nul
echo Iniciando servidor...
start /b cmd /c "timeout /t 10 /nobreak >nul && start http://localhost:3000"
npm run dev
