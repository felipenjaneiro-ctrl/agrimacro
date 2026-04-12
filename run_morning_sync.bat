@echo off
cd /d C:\Users\felip\OneDrive\READET~1\AGRIMA~2
if not exist pipeline\logs mkdir pipeline\logs
python pipeline\sync_morning_intel.py >> pipeline\logs\morning_sync.log 2>&1
