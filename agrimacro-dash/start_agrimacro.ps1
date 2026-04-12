# AgriMacro Dashboard - Auto-Start Script
# Registered via Task Scheduler (AtLogOn)

# Aguarda 15 segundos para o Windows terminar de carregar
Start-Sleep -Seconds 15

# Mata qualquer processo node na porta 3000
$port = 3000
$process = Get-NetTCPConnection -LocalPort $port -ErrorAction SilentlyContinue
if ($process) {
    $pid_found = $process.OwningProcess
    Stop-Process -Id $pid_found -Force -ErrorAction SilentlyContinue
    Start-Sleep -Seconds 2
}

# Inicia o Next.js
$projectDir = "C:\Users\felip\OneDrive\Área de Trabalho\agrimacro\agrimacro-dash"
Set-Location $projectDir
$env:NODE_OPTIONS = "--max-old-space-size=4096"
Start-Process -FilePath "cmd.exe" -ArgumentList "/c npx next dev --port 3000" -WorkingDirectory $projectDir -WindowStyle Minimized

# Aguarda o servidor estar pronto (testa porta)
$maxWait = 60
$waited = 0
do {
    Start-Sleep -Seconds 2
    $waited += 2
    $ready = Test-NetConnection -ComputerName localhost -Port 3000 -WarningAction SilentlyContinue
} while (-not $ready.TcpTestSucceeded -and $waited -lt $maxWait)

# Abre o browser somente quando o servidor responder
if ($ready.TcpTestSucceeded) {
    Start-Process "http://localhost:3000"
}
