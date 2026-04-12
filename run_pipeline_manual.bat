@echo off
title AgriMacro Pipeline Manual
color 0E
echo.
echo  ======================================
echo   AgriMacro Pipeline (manual)
echo   %date% %time:~0,5%
echo  ======================================
echo.

cd /d "C:\Users\felip\OneDrive\Área de Trabalho\agrimacro"

echo Rodando pipeline AgriMacro...
echo.
python pipeline\run_pipeline.py

echo.
echo  ======================================
echo   Pipeline concluido!
echo  ======================================
echo.
pause
