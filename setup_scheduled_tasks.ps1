# ============================================================
# AgriMacro — Registrar tarefas no Task Scheduler do Windows
#
# EXECUTAR COMO ADMINISTRADOR:
#   powershell -ExecutionPolicy Bypass -File setup_scheduled_tasks.ps1
#
# Cria 3 tarefas agendadas:
#   1. AgriMacro_Pipeline_5h30  — run_pipeline.py          (05:30 diario)
#   2. AgriMacro_Ranker_5h45   — opportunity_ranker.py     (05:45 diario)
#   3. AgriMacro_Council_6h00  — Council Full via curl      (06:00 diario)
#
# RunLevel Highest = roda mesmo sem usuario logado (requer senha na criacao)
# ============================================================

$ErrorActionPreference = "Stop"
$BASE = Split-Path -Parent $MyInvocation.MyCommand.Definition

$pythonExe = (Get-Command python -ErrorAction SilentlyContinue).Source
if (-not $pythonExe) {
    Write-Host "ERRO: python nao encontrado no PATH. Instale Python e tente novamente." -ForegroundColor Red
    exit 1
}
Write-Host "Python: $pythonExe" -ForegroundColor Cyan

# ── Helper: registrar tarefa ──
function Register-AgriTask {
    param(
        [string]$Name,
        [string]$Time,
        [string]$Execute,
        [string]$Arguments,
        [string]$WorkDir
    )

    # Remove tarefa anterior se existir
    $existing = Get-ScheduledTask -TaskName $Name -ErrorAction SilentlyContinue
    if ($existing) {
        Unregister-ScheduledTask -TaskName $Name -Confirm:$false
        Write-Host "  Removida tarefa anterior: $Name" -ForegroundColor Yellow
    }

    $action  = New-ScheduledTaskAction -Execute $Execute -Argument $Arguments -WorkingDirectory $WorkDir
    $trigger = New-ScheduledTaskTrigger -Daily -At $Time
    $settings = New-ScheduledTaskSettingsSet `
        -AllowStartIfOnBatteries `
        -DontStopIfGoingOnBatteries `
        -StartWhenAvailable `
        -ExecutionTimeLimit (New-TimeSpan -Hours 1)
    $principal = New-ScheduledTaskPrincipal -UserId "$env:USERDOMAIN\$env:USERNAME" -RunLevel Highest -LogonType S4U

    Register-ScheduledTask -TaskName $Name -Action $action -Trigger $trigger `
        -Settings $settings -Principal $principal -Description "AgriMacro automated task" | Out-Null

    Write-Host "  OK: $Name -> $Time diario" -ForegroundColor Green
}

Write-Host ""
Write-Host "========== AgriMacro — Task Scheduler Setup ==========" -ForegroundColor Cyan
Write-Host "Base: $BASE" -ForegroundColor Gray
Write-Host ""

# ── Tarefa 1: Pipeline principal (05:30) ──
Write-Host "[1/3] Pipeline principal (05:30 AM)" -ForegroundColor White
Register-AgriTask `
    -Name "AgriMacro_Pipeline_5h30" `
    -Time "05:30" `
    -Execute $pythonExe `
    -Arguments "`"$BASE\pipeline\run_pipeline.py`"" `
    -WorkDir "$BASE\pipeline"

# ── Tarefa 2: Opportunity Ranker (05:45) ──
Write-Host "[2/3] Opportunity Ranker (05:45 AM)" -ForegroundColor White
Register-AgriTask `
    -Name "AgriMacro_Ranker_5h45" `
    -Time "05:45" `
    -Execute $pythonExe `
    -Arguments "`"$BASE\pipeline\opportunity_ranker.py`"" `
    -WorkDir "$BASE\pipeline"

# ── Tarefa 3: Council Full (06:00) ──
Write-Host "[3/3] Council Full via curl (06:00 AM)" -ForegroundColor White

# Usa powershell para fazer o POST (curl pode nao estar no PATH)
$councilCmd = "-NoProfile -ExecutionPolicy Bypass -Command `"try { Invoke-RestMethod -Uri 'http://localhost:3000/api/council' -Method POST -Body '{`\`"mode`\`":`\`"full`\`"}' -ContentType 'application/json' -TimeoutSec 15 } catch { Write-Host 'Council ERRO (dashboard offline?)' }`""

Register-AgriTask `
    -Name "AgriMacro_Council_6h00" `
    -Time "06:00" `
    -Execute "powershell.exe" `
    -Arguments $councilCmd `
    -WorkDir $BASE

Write-Host ""
Write-Host "========== Todas as tarefas registradas ==========" -ForegroundColor Green
Write-Host ""

# ── Confirmar tarefas ──
Write-Host "Verificando tarefas criadas:" -ForegroundColor Cyan
@("AgriMacro_Pipeline_5h30", "AgriMacro_Ranker_5h45", "AgriMacro_Council_6h00") | ForEach-Object {
    $task = Get-ScheduledTask -TaskName $_ -ErrorAction SilentlyContinue
    if ($task) {
        $info = $task | Get-ScheduledTaskInfo
        $trigger = ($task.Triggers | Select-Object -First 1)
        Write-Host "  OK  $_" -ForegroundColor Green -NoNewline
        Write-Host "  -> Estado: $($task.State), Proximo: $($info.NextRunTime)" -ForegroundColor Gray
    } else {
        Write-Host "  ERRO  $_ nao encontrada!" -ForegroundColor Red
    }
}

Write-Host ""
Write-Host "Horarios (Eastern Time):" -ForegroundColor Yellow
Write-Host "  05:30  Pipeline (coleta + processamento + PDF)" -ForegroundColor Gray
Write-Host "  05:45  Opportunity Ranker (ranking multi-fator)" -ForegroundColor Gray
Write-Host "  06:00  Council Full (analise IA — requer dashboard ativo)" -ForegroundColor Gray
Write-Host ""

# ============================================================
# PARA REMOVER TODAS AS TAREFAS (descomente e execute):
#
#   Unregister-ScheduledTask -TaskName "AgriMacro_Pipeline_5h30" -Confirm:$false
#   Unregister-ScheduledTask -TaskName "AgriMacro_Ranker_5h45" -Confirm:$false
#   Unregister-ScheduledTask -TaskName "AgriMacro_Council_6h00" -Confirm:$false
#   Write-Host "Todas as tarefas AgriMacro removidas." -ForegroundColor Yellow
#
# ============================================================
