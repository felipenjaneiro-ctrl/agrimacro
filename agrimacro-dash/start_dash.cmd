@echo off
subst A: "C:\Users\felip\OneDrive\READET~1\AGRIMA~2\agrimacro-dash" 2>nul
cd /d A:\
"C:\Program Files\nodejs\node.exe" node_modules\next\dist\bin\next dev --port 3000
