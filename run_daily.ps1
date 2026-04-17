# ============================================================
# AgriMacro — Pipeline diario completo
# Uso manual:  powershell -ExecutionPolicy Bypass -File run_daily.ps1
# Uso agendado: registrado via setup_scheduled_tasks.ps1
# ============================================================

$ErrorActionPreference = "Continue"
$BASE = Split-Path -Parent $MyInvocation.MyCommand.Definition
$LOG  = Join-Path $BASE "logs"
if (!(Test-Path $LOG)) { New-Item -ItemType Directory -Path $LOG | Out-Null }
$ts   = Get-Date -Format "yyyy-MM-dd_HHmmss"
$logFile = Join-Path $LOG "daily_$ts.log"

function Log($msg) {
    $line = "$(Get-Date -Format 'HH:mm:ss') | $msg"
    Write-Host $line
    Add-Content -Path $logFile -Value $line
}

Log "========== AgriMacro Daily Pipeline =========="
Log "Inicio: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')"

# --- STEP 1: Pipeline principal (18 steps) ---
Log "STEP 1/3: run_pipeline.py"
$t1 = Measure-Command {
    python "$BASE\pipeline\run_pipeline.py" 2>&1 | ForEach-Object { Log "  $_" }
}
if ($LASTEXITCODE -eq 0) { Log "STEP 1 OK ($([math]::Round($t1.TotalSeconds))s)" }
else { Log "STEP 1 ERRO (exit=$LASTEXITCODE, $([math]::Round($t1.TotalSeconds))s)" }

# --- STEP 2: Opportunity Ranker ---
Log "STEP 2/3: opportunity_ranker.py"
$t2 = Measure-Command {
    python "$BASE\pipeline\opportunity_ranker.py" 2>&1 | ForEach-Object { Log "  $_" }
}
if ($LASTEXITCODE -eq 0) { Log "STEP 2 OK ($([math]::Round($t2.TotalSeconds))s)" }
else { Log "STEP 2 ERRO (exit=$LASTEXITCODE, $([math]::Round($t2.TotalSeconds))s)" }

# --- STEP 3: Council Full (via API local) ---
Log "STEP 3/3: Council Full (POST /api/council)"
try {
    $body = '{"mode":"full"}'
    $resp = Invoke-RestMethod -Uri "http://localhost:3000/api/council" -Method POST -Body $body -ContentType "application/json" -TimeoutSec 10
    if ($resp.jobId) {
        Log "Council job iniciado: $($resp.jobId)"
        Log "  (Council roda em background — poll GET /api/council?jobId=$($resp.jobId) para status)"
    } else {
        Log "Council resposta inesperada: $($resp | ConvertTo-Json -Compress)"
    }
} catch {
    Log "Council ERRO (dashboard offline?): $($_.Exception.Message)"
    Log "  Para rodar Council, o dashboard precisa estar ativo em localhost:3000"
}

$total = [math]::Round(($t1.TotalSeconds + $t2.TotalSeconds), 0)
Log "========== Concluido em ${total}s =========="
Log "Log salvo em: $logFile"
