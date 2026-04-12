# Patch dashboard.tsx -- adiciona aba "Grain Ratios"
# Execute do diretorio raiz: cd "$env:USERPROFILE\OneDrive\Área de Trabalho\agrimacro"

$file = "agrimacro-dash\src\app\dashboard.tsx"
$c = [System.IO.File]::ReadAllText((Resolve-Path $file), [System.Text.Encoding]::UTF8)

# 1. Type Tab
$c = $c.Replace('"Bilateral";', '"Bilateral"|"Grain Ratios";')

# 2. Array TABS
$c = $c.Replace('"Bilateral"]', '"Bilateral","Grain Ratios"]')

# 3. Import (depois de "use client")
$c = $c.Replace('"use client"', '"use client"
// @ts-ignore
import GrainRatiosTab from "./GrainRatiosTab"')

# 4. Bloco da aba (antes de "Tab: Bilateral")
$c = $c.Replace('// -- Tab: Bilateral', '// -- Tab: Grain Ratios -------------------------------------------------
  if(tab==="Grain Ratios") return <GrainRatiosTab />

  // -- Tab: Bilateral')

[System.IO.File]::WriteAllText((Resolve-Path $file), $c, [System.Text.Encoding]::UTF8)
Write-Host "OK - Grain Ratios adicionado ao dashboard"
