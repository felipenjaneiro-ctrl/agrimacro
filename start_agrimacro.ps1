# AgriMacro Dashboard - Start Script
# Mata qualquer Next.js anterior
Get-Process -Name "node" -ErrorAction SilentlyContinue | Stop-Process -Force
Start-Sleep 2

# Inicia Next.js
$job = Start-Process powershell -ArgumentList "-Command cd 'C:\Users\felip\OneDrive\Área de Trabalho\agrimacro\agrimacro-dash'; npx next dev" -WindowStyle Minimized -PassThru

# Aguarda servidor responder (testa a cada 2s, ate 60s)
$ok = $false
for ($i = 0; $i -lt 30; $i++) {
    Start-Sleep 2
    try {
        $r = Invoke-WebRequest -Uri "http://localhost:3000" -TimeoutSec 2 -ErrorAction Stop
        $ok = $true
        break
    } catch {}
}

if ($ok) {
    Start-Process "http://localhost:3000"
} else {
    Add-Type -AssemblyName PresentationFramework
    [System.Windows.MessageBox]::Show("Dashboard nao respondeu em 60s. Verifique o PowerShell.")
}
