"""
_patch_dashboard_pipeline_btn.py — Adiciona botao "Atualizar Pipeline" no dashboard
Uso: cd agrimacro-dash && python ../pipeline/_patch_dashboard_pipeline_btn.py
"""
import shutil, datetime, sys, re
from pathlib import Path

TARGET = Path(__file__).resolve().parent.parent / "agrimacro-dash" / "src" / "app" / "dashboard.tsx"
if not TARGET.exists():
    print(f"[ERRO] {TARGET} nao encontrado!")
    sys.exit(1)

bak = TARGET.parent / f"dashboard.bak_{datetime.datetime.now():%Y%m%d_%H%M%S}.tsx"
shutil.copy2(TARGET, bak)
print(f"[BACKUP] {bak}")

src = TARGET.read_text(encoding="utf-8")
changes = 0

# ── 1. Add state variables after ibkrRefreshing state ──
old1 = "const [ibkrRefreshing,setIbkrRefreshing] = useState(false);"
new1 = """const [ibkrRefreshing,setIbkrRefreshing] = useState(false);
  const [pipelineRefreshing,setPipelineRefreshing] = useState(false);
  const [pipelineResult,setPipelineResult] = useState<string>("");"""

if "pipelineRefreshing" not in src:
    src = src.replace(old1, new1, 1)
    changes += 1
    print("[FIX 1] State variables adicionados")
else:
    print("[FIX 1] Ja existe — skip")

# ── 2. Add refreshPipeline function after refreshIbkr function ──
marker = "  useEffect(()=>{\n    const interval = setInterval(()=>{ refreshIbkr(); }, 5*60*1000);"
if marker not in src:
    # Try alternate spacing
    marker = "  useEffect(()=>{"
    # Find the interval line near refreshIbkr
    idx = src.find("const interval = setInterval(()=>{ refreshIbkr(); }")
    if idx > 0:
        # Find the useEffect before it
        ue_idx = src.rfind("useEffect(", 0, idx)
        if ue_idx > 0:
            marker_start = src.rfind("\n", 0, ue_idx)
            marker = src[marker_start+1:idx+len("const interval = setInterval(()=>{ refreshIbkr(); }")]

REFRESH_FN = """  const refreshPipeline = async () => {
    if(pipelineRefreshing) return;
    setPipelineRefreshing(true);
    setPipelineResult("Rodando pipeline...");
    try {
      const res = await fetch("/api/refresh-pipeline", {method:"POST"});
      const data = await res.json();
      if(data.status === "ok") {
        setPipelineResult(data.summary + " em " + Math.round(data.totalTime/1000) + "s");
        setTimeout(()=>window.location.reload(), 2000);
      } else if(data.status === "skipped") {
        setPipelineResult("Aguarde 2min entre execucoes");
      } else {
        setPipelineResult("Erro: " + (data.summary||"falha"));
      }
    } catch(e){ setPipelineResult("Erro de conexao"); }
    setPipelineRefreshing(false);
  };
"""

if "refreshPipeline" not in src:
    # Insert before the useEffect with interval
    old2 = "  useEffect(()=>{\n    const interval = setInterval(()=>{ refreshIbkr(); }, 5*60*1000);"
    if old2 in src:
        src = src.replace(old2, REFRESH_FN + old2, 1)
        changes += 1
        print("[FIX 2] refreshPipeline function adicionada")
    else:
        # Try to find it more loosely
        pattern = r'(  useEffect\(\(\)=>\{\s*\n\s*const interval = setInterval\(\(\)=>\{ refreshIbkr\(\); \})'
        match = re.search(pattern, src)
        if match:
            src = src[:match.start()] + REFRESH_FN + src[match.start():]
            changes += 1
            print("[FIX 2] refreshPipeline function adicionada (regex)")
        else:
            print("[FIX 2] FALHOU — nao encontrou useEffect/interval")
else:
    print("[FIX 2] Ja existe — skip")

# ── 3. Add button next to IBKR button ──
old3 = '<span style={{fontSize:10,color:C.textMuted}}>Ultima atualizacao: {ibkrTime}</span>'

new3 = """<span style={{fontSize:10,color:C.textMuted}}>Ultima atualizacao: {ibkrTime}</span>
          <button onClick={refreshPipeline} disabled={pipelineRefreshing} style={{padding:"6px 16px",fontSize:11,fontWeight:600,background:pipelineRefreshing?"#555":"#e94560",color:"#fff",border:"none",borderRadius:6,cursor:pipelineRefreshing?"wait":"pointer",marginLeft:8}}>
            {pipelineRefreshing?"Rodando Pipeline...":"Atualizar Pipeline"}
          </button>
          {pipelineResult && <span style={{fontSize:10,color:C.textMuted,marginLeft:8}}>{pipelineResult}</span>}"""

if "refreshPipeline" in src and "Atualizar Pipeline" not in src:
    if old3 in src:
        src = src.replace(old3, new3, 1)
        changes += 1
        print("[FIX 3] Botao Pipeline adicionado na UI")
    else:
        print("[FIX 3] FALHOU — nao encontrou span ibkrTime")
elif "Atualizar Pipeline" in src:
    print("[FIX 3] Ja existe — skip")
else:
    print("[FIX 3] SKIP — refreshPipeline nao encontrado")

# ── Save ──
TARGET.write_text(src, encoding="utf-8")
print(f"\n[OK] {changes} fixes aplicados.")
print("Reinicie o dashboard (feche terminal + clique atalho) para ver o botao.")
