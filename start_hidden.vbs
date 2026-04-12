Set oShell = CreateObject("WScript.Shell")
Set oFSO = CreateObject("Scripting.FileSystemObject")

sEco = oFSO.GetParentFolderName(WScript.ScriptFullName) & "\agrimacro-dash\ecosystem.config.js"

' Try restart first, if fails do fresh start
iRet = oShell.Run("pm2 restart agrimacro-dash", 0, True)
If iRet <> 0 Then
    oShell.Run "pm2 start """ & sEco & """", 0, True
End If

oShell.Run "pm2 save", 0, True

Set oFSO = Nothing
Set oShell = Nothing
