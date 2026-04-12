Set oShell = CreateObject("WScript.Shell")
oShell.Run "cmd /c pm2 resurrect", 0, False
Set oShell = Nothing
