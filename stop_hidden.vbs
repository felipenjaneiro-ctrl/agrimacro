Set oShell = CreateObject("WScript.Shell")
oShell.Run "pm2 stop agrimacro-dash", 0, True
Set oShell = Nothing
