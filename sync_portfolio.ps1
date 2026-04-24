$ErrorActionPreference = "Stop"
$projectRoot = "$env:USERPROFILE\OneDrive\Área de Trabalho\agrimacro"
Set-Location $projectRoot

Write-Host "[1/5] Atualizando portfolio via TWS..." -ForegroundColor Cyan
python pipeline\collect_ibkr.py
if ($LASTEXITCODE -ne 0) {
    Write-Host "ERRO: Falha ao coletar IBKR. TWS aberto e logado?" -ForegroundColor Red
    exit 1
}

Write-Host "[2/5] Commitando no git..." -ForegroundColor Cyan
git add "agrimacro-dash/public/data/processed/ibkr_portfolio.json"
git add "agrimacro-dash/public/data/processed/ibkr_greeks.json"
git add "agrimacro-dash/public/data/processed/price_history.json"
git add "agrimacro-dash/public/data/processed/contract_history.json"

$changes = git status --porcelain
if ([string]::IsNullOrWhiteSpace($changes)) {
    Write-Host "Nenhuma mudanca. Pulando commit." -ForegroundColor Yellow
} else {
    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm"
    git commit -m "portfolio: sync $timestamp"
    Write-Host "[3/5] Enviando para GitHub..." -ForegroundColor Cyan
    git push origin main
}

Write-Host "[4/5] Sincronizando servidor..." -ForegroundColor Cyan
$pair = "felipe:F&lipeJ@neiro11312"
$basic = [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes($pair))
$response = Invoke-RestMethod -Uri "https://agri-macro.com/api/sync-portfolio" -Method POST -Headers @{Authorization = "Basic $basic"}
Write-Host "Servidor: $($response.status)" -ForegroundColor Green

Write-Host "[5/5] SCP options_chain.json + iv_analytics.json (fora do git)..." -ForegroundColor Cyan
$sshKey = "$env:USERPROFILE\.ssh\agrimacro_vps"
$vpsTarget = "root@134.209.78.35:/var/www/agrimacro/agrimacro-dash/public/data/processed/"
$localDir = "agrimacro-dash/public/data/processed"
foreach ($f in @("options_chain.json", "iv_analytics.json")) {
    $localPath = "$localDir/$f"
    if (Test-Path $localPath) {
        scp -i $sshKey -o StrictHostKeyChecking=accept-new $localPath $vpsTarget
        if ($LASTEXITCODE -eq 0) {
            Write-Host "  $f enviado" -ForegroundColor Green
        } else {
            Write-Host "  $f FALHOU (exit $LASTEXITCODE)" -ForegroundColor Red
        }
    } else {
        Write-Host "  $f nao existe localmente (pular)" -ForegroundColor Yellow
    }
}

Write-Host "Sync completo!" -ForegroundColor Green
