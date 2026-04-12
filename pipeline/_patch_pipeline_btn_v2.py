"""
_patch_pipeline_btn_v2.py — Adiciona botao Pipeline no header do dashboard
Mais preciso que v1 — insere no header junto ao IBKR Refresh

Uso: cd pipeline && python _patch_pipeline_btn_v2.py
"""
import shutil, datetime, sys
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

if "Atualizar Pipeline" in src:
    print("[SKIP] Botao Pipeline ja existe!")
    sys.exit(0)

# ── 1. Add state vars after ibkrRefreshing ──
old1 = "const [ibkrRefreshing,setIbkrRefreshing] = useState(false);"
new1 = (
    "const [ibkrRefreshing,setIbkrRefreshing] = useState(false);\n"
    "  const [pipeRefresh,setPipeRefresh] = useState(false);\n"
    '  const [pipeMsg,setPipeMsg] = useState("");'
)
if old1 in src:
    src = src.replace(old1, new1, 1)
    changes += 1
    print("[1] State vars adicionados")
else:
    print("[1] FALHOU — ibkrRefreshing nao encontrado")
    sys.exit(1)

# ── 2. Add refreshPipeline function after setIbkrRefreshing(false) block ──
old2 = "    setIbkrRefreshing(false);\n  };"
new2 = '''    setIbkrRefreshing(false);
  };
  const refreshPipeline = async () => {
    if(pipeRefresh) return;
    setPipeRefresh(true); setPipeMsg("Rodando...");
    try {
      const res = await fetch("/api/refresh-pipeline", {method:"POST"});
      const d = await res.json();
      if(d.status==="ok"){ setPipeMsg(d.summary); setTimeout(()=>window.location.reload(), 2000); }
      else if(d.status==="skipped"){ setPipeMsg("Aguarde 2min"); }
      else { setPipeMsg("Erro"); }
    } catch(e){ setPipeMsg("Falha"); }
    setPipeRefresh(false);
  };'''

if "refreshPipeline" not in src:
    if old2 in src:
        src = src.replace(old2, new2, 1)
        changes += 1
        print("[2] refreshPipeline function adicionada")
    else:
        print("[2] FALHOU — nao encontrou setIbkrRefreshing(false)")
        sys.exit(1)

# ── 3. Add button in header after IBKR span ──
old3 = '<span style={{fontSize:10,color:C.textMuted}}>IBKR: {ibkrTime}</span>'
new3 = (
    '<span style={{fontSize:10,color:C.textMuted}}>IBKR: {ibkrTime}</span>\n'
    '              <button onClick={refreshPipeline} disabled={pipeRefresh} style={{padding:"3px 10px",fontSize:10,fontWeight:600,background:pipeRefresh?"#555":"#e94560",color:"#fff",border:"none",borderRadius:4,cursor:pipeRefresh?"wait":"pointer",letterSpacing:0.5,marginLeft:8}}>\n'
    '                {pipeRefresh?"↻ Rodando...":"Atualizar Pipeline"}\n'
    '              </button>\n'
    '              {pipeMsg && <span style={{fontSize:9,color:C.textMuted,marginLeft:4}}>{pipeMsg}</span>}'
)

if old3 in src:
    src = src.replace(old3, new3, 1)
    changes += 1
    print("[3] Botao adicionado no header")
else:
    print("[3] FALHOU — span IBKR nao encontrado")
    sys.exit(1)

TARGET.write_text(src, encoding="utf-8")
print(f"\n[OK] {changes} fixes. Reinicie o dashboard.")
