@echo off
title AgriMacro - Backup GitHub
color 0B
echo.
echo  ======================================
echo   AgriMacro - Backup para GitHub
echo   %date% %time:~0,5%
echo  ======================================
echo.

cd /d "C:\Users\felip\OneDrive\Área de Trabalho\agrimacro"

echo [1/3] Adicionando arquivos...
git add .

echo [2/3] Commitando...
git commit -m "Backup automatico %date% %time:~0,5%"

echo [3/3] Enviando para GitHub...
git push origin main

echo.
echo  ======================================
echo   Backup concluido!
echo  ======================================
echo.
pause
