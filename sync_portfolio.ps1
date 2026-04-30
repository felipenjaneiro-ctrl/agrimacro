$ErrorActionPreference = "Stop"
$projectRoot = "$env:USERPROFILE\OneDrive\Área de Trabalho\agrimacro"
Set-Location $projectRoot

Write-Host "[1/3] Atualizando portfolio via TWS..." -ForegroundColor Cyan
python pipeline\collect_ibkr.py
if ($LASTEXITCODE -ne 0) {
    Write-Host "ERRO: Falha ao coletar IBKR. TWS aberto e logado?" -ForegroundColor Red
    exit 1
}

Write-Host "[2/3] Enviando JSONs para VPS via SCP (best-effort)..." -ForegroundColor Cyan
$sshKey = "$env:USERPROFILE\.ssh\agrimacro_vps"
$vpsTarget = "root@134.209.78.35:/var/www/agrimacro/agrimacro-dash/public/data/processed/"
$localDir = "agrimacro-dash/public/data/processed"
$jsonFiles = @(
    "ibkr_portfolio.json",
    "ibkr_greeks.json",
    "price_history.json",
    "contract_history.json"
)
$scpFailures = 0
foreach ($f in $jsonFiles) {
    $localPath = "$localDir/$f"
    if (-not (Test-Path $localPath)) {
        Write-Host "  [ERRO] $f NAO EXISTE em $localDir (collect_ibkr.py falhou silenciosamente?)" -ForegroundColor Red
        $scpFailures++
        continue
    }
    try {
        scp -i $sshKey -o StrictHostKeyChecking=accept-new $localPath $vpsTarget
        if ($LASTEXITCODE -eq 0) {
            Write-Host "  [OK]   $f enviado" -ForegroundColor Green
        } else {
            Write-Host "  [ERRO] $f FALHOU (scp exit $LASTEXITCODE)" -ForegroundColor Red
            $scpFailures++
        }
    } catch {
        Write-Host "  [ERRO] $f EXCECAO PS: $_" -ForegroundColor Red
        $scpFailures++
    }
}
if ($scpFailures -gt 0) {
    Write-Host "ERRO: $scpFailures/$($jsonFiles.Count) SCPs falharam. Servidor parcialmente atualizado." -ForegroundColor Red
    Write-Host "Arquivos com falha mostrarao dados antigos no dashboard. Retentar manualmente." -ForegroundColor Yellow
    exit 1
}
Write-Host "  Todos os $($jsonFiles.Count) arquivos enviados com sucesso." -ForegroundColor Green

Write-Host "[3/3] Notificando servidor (POST /api/sync-portfolio)..." -ForegroundColor Cyan
$pair = "felipe:F&lipeJ@neiro11312"
$basic = [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes($pair))
$response = Invoke-RestMethod -Uri "https://agri-macro.com/api/sync-portfolio" -Method POST -Headers @{Authorization = "Basic $basic"}
Write-Host "Servidor: $($response.status)" -ForegroundColor Green

Write-Host "Sync completo!" -ForegroundColor Green
