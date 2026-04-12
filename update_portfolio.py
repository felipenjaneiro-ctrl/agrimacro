import re, shutil, os
from datetime import datetime

FILE = r"C:\Users\felip\OneDrive\Área de Trabalho\agrimacro\agrimacro-dash\src\app\dashboard.tsx"

# Backup
shutil.copy2(FILE, FILE + f".bak_portfolio_{datetime.now().strftime('%Y%m%d_%H%M%S')}")
print("Backup criado")

with open(FILE, "r", encoding="utf-8") as f:
    code = f.read()

# 1. Update fetch line: ibkr_portfolio.json -> ibkr_export.json
old_fetch = 'fetch("/data/processed/ibkr_portfolio.json").then(r=>r.json()).then(d=>{setPortfolio(d);setLastIbkrRefresh(d.timestamp||new Date().toISOString());}).catch(()=>console.warn("No IBKR portfolio")),'
new_fetch = 'fetch("/data/processed/ibkr_export.json").then(r=>r.json()).then(d=>{setPortfolio(d);setLastIbkrRefresh(d.export_timestamp||new Date().toISOString());}).catch(()=>console.warn("No IBKR data")),'
code = code.replace(old_fetch, new_fetch)
print("Fetch atualizado: ibkr_export.json")

# 2. Update refresh function to use ibkr_export.json
code = code.replace(
    'const p = await fetch("/data/processed/ibkr_portfolio.json?t="+Date.now());',
    'const p = await fetch("/data/processed/ibkr_export.json?t="+Date.now());'
)
print("Refresh atualizado")

# 3. Replace renderPortfolio function
old_render_start = "  const renderPortfolio = () => {"
old_render_end = '    {positions.length===0 && <DataPlaceholder title="Sem posicoes" detail="Execute o pipeline com IBKR conectado" />}\n      </div>'

# Find the start and end positions
start_idx = code.find(old_render_start)
# Find the end of the left panel (before the AI chat section)
end_marker = '{/* RIGHT: AI Chat */}'
end_idx = code.find(end_marker, start_idx)

if start_idx == -1 or end_idx == -1:
    print("ERRO: Nao encontrou renderPortfolio ou AI Chat marker")
    exit(1)

new_render = '''  const renderPortfolio = () => {
    const acct = portfolio?.account || {};
    const byUnderlying = portfolio?.positions_by_underlying || {};
    const equities = portfolio?.equity_positions || {};
    const fixedInc = portfolio?.fixed_income || {};
    const summary = portfolio?.portfolio_summary || {};
    const netLiq = acct.net_liquidation || 0;
    const cash = acct.total_cash || 0;
    const unrealPnl = acct.unrealized_pnl || 0;
    const realPnl = acct.realized_pnl || 0;
    const buyPow = acct.buying_power || 0;
    const grossPos = acct.gross_position_value || 0;
    const marginUtil = acct.margin_utilization_pct || 0;
    const symbols = Object.keys(byUnderlying).sort();

    const suggestions = [
      "Analise meu portfolio e sugira hedges",
      "Quais posicoes tem mais risco agora?",
      "Sugira um trade em milho baseado nos spreads",
      "Como proteger minhas posicoes em wheat?",
      "Qual o cenario para soja nos proximos 30 dias?",
    ];

    return (
    <div style={{display:"flex",gap:0,height:"100%"}}>
      {/* LEFT: Portfolio Data */}
      <div style={{flex:1,overflow:"auto",padding:24}}>
        {/* Account Summary Cards */}
        <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(160px,1fr))",gap:12,marginBottom:24}}>
          {[
            {label:"Net Liquidation",value:netLiq,fmt:"$",color:C.text},
            {label:"Buying Power",value:buyPow,fmt:"$",color:C.blue},
            {label:"Cash",value:cash,fmt:"$",color:C.cyan},
            {label:"Gross Position",value:grossPos,fmt:"$",color:C.text},
            {label:"Unrealized P&L",value:unrealPnl,fmt:"$",color:unrealPnl>=0?C.green:C.red},
            {label:"Margin Util",value:marginUtil,fmt:"",color:marginUtil>50?C.red:C.green,suffix:"%"},
          ].map((card,i)=>(
            <div key={i} style={{padding:16,background:C.panelAlt,borderRadius:8,border:\`1px solid $\{C.border}\`}}>
              <div style={{fontSize:9,color:C.textMuted,textTransform:"uppercase",letterSpacing:.5,marginBottom:8}}>{card.label}</div>
              <div style={{fontSize:20,fontWeight:700,color:card.color,fontFamily:"monospace"}}>{card.fmt}{typeof card.value==="number"?card.value.toLocaleString("en-US",{minimumFractionDigits:0,maximumFractionDigits:0}):card.value}{(card as any).suffix||""}</div>
            </div>
          ))}
        </div>

        {/* Portfolio Bias Banner */}
        {summary.dominant_strategy && (
          <div style={{padding:"12px 16px",background:C.panelAlt,borderRadius:8,border:\`1px solid $\{C.border}\`,marginBottom:20,display:"flex",gap:24,alignItems:"center",flexWrap:"wrap"}}>
            <div><span style={{fontSize:9,color:C.textMuted,textTransform:"uppercase"}}>Estrategia Dominante</span><div style={{fontSize:12,fontWeight:600,color:C.amber,marginTop:2}}>{summary.dominant_strategy}</div></div>
            <div><span style={{fontSize:9,color:C.textMuted,textTransform:"uppercase"}}>Bias Direcional</span><div style={{fontSize:12,fontWeight:600,color:C.text,marginTop:2}}>{summary.directional_bias}</div></div>
            <div><span style={{fontSize:9,color:C.textMuted,textTransform:"uppercase"}}>Perfil de Risco</span><div style={{fontSize:12,fontWeight:600,color:C.text,marginTop:2}}>{summary.risk_profile}</div></div>
            <div><span style={{fontSize:9,color:C.textMuted,textTransform:"uppercase"}}>Vencimentos</span><div style={{fontSize:12,fontWeight:600,color:C.red,marginTop:2}}>{summary.nearest_expirations}</div></div>
          </div>
        )}

        {/* Refresh button */}
        <div style={{display:"flex",alignItems:"center",gap:12,marginBottom:20}}>
          <button onClick={refreshIbkr} disabled={ibkrRefreshing} style={{padding:"6px 16px",fontSize:11,fontWeight:600,background:ibkrRefreshing?"#555":C.blue,color:"#fff",border:"none",borderRadius:6,cursor:ibkrRefreshing?"wait":"pointer"}}>
            {ibkrRefreshing?"Atualizando...":"Atualizar IBKR"}
          </button>
          <span style={{fontSize:10,color:C.textMuted}}>Ultima atualizacao: {ibkrTime}</span>
          <span style={{fontSize:10,color:C.textMuted}}>|</span>
          <span style={{fontSize:10,color:C.textMuted}}>{summary.total_option_legs||0} legs, {summary.total_option_contracts_gross||0} contratos</span>
        </div>

        {/* Strategy Cards by Underlying */}
        <div style={{fontSize:14,fontWeight:700,color:C.text,marginBottom:14}}>Estrategias por Commodity ({symbols.length})</div>
        {symbols.map(sym=>{
          const data = byUnderlying[sym];
          const legs = data.legs || [];
          const structures = data.structures || [];
          return (
            <div key={sym} style={{marginBottom:20}}>
              {/* Header */}
              <div style={{display:"flex",alignItems:"center",gap:10,marginBottom:8}}>
                <span style={{fontSize:14,fontWeight:700,color:C.amber}}>{data.name || sym}</span>
                <span style={{fontSize:10,color:C.textMuted,background:C.panel,padding:"2px 8px",borderRadius:4}}>{sym}</span>
                <span style={{fontSize:10,color:C.cyan,fontWeight:600}}>{data.strategy_summary}</span>
              </div>

              {/* Structures */}
              {structures.length > 0 && (
                <div style={{display:"flex",gap:10,marginBottom:8,flexWrap:"wrap"}}>
                  {structures.map((st:any,si:number)=>(
                    <div key={si} style={{padding:"8px 14px",background:C.panelAlt,borderRadius:6,border:\`1px solid $\{C.border}\`,fontSize:10}}>
                      <div style={{color:C.gold,fontWeight:700,textTransform:"uppercase",marginBottom:4}}>{(st.type||"").replace(/_/g," ")}</div>
                      <div style={{color:C.text}}>
                        {st.long_strike && st.short_strike ? \`$\{st.long_strike}/$\{st.short_strike}\` : st.lower && st.upper ? \`$\{st.lower}/$\{st.middle}/$\{st.upper}\` : ""}
                        {st.expiry ? \` $\{st.expiry}\` : ""}
                        {st.qty ? \` x$\{st.qty}\` : ""}
                      </div>
                      {st.max_risk_per_lot && <div style={{color:C.red,marginTop:2}}>Max risk/lot: ${st.max_risk_per_lot}</div>}
                      {st.credit_received_per_lot && <div style={{color:C.green,marginTop:2}}>Credit/lot: ${st.credit_received_per_lot.toFixed(2)}</div>}
                      {st.note && <div style={{color:C.textMuted,marginTop:2,fontStyle:"italic"}}>{st.note}</div>}
                    </div>
                  ))}
                </div>
              )}

              {/* Legs Table */}
              <div style={{background:C.panelAlt,borderRadius:6,border:\`1px solid $\{C.border}\`,overflow:"hidden"}}>
                <table style={{width:"100%",borderCollapse:"collapse",fontSize:10}}>
                  <thead>
                    <tr style={{borderBottom:\`1px solid $\{C.border}\`}}>
                      {["Contrato","Tipo","Strike","Venc","Qtd","Side","Custo Med"].map(h=>(
                        <th key={h} style={{padding:"8px 10px",textAlign:"left",color:C.textMuted,fontWeight:600,fontSize:9,textTransform:"uppercase"}}>{h}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {legs.map((leg:any,j:number)=>(
                      <tr key={j} style={{borderBottom:\`1px solid $\{C.border}22\`}}>
                        <td style={{padding:"6px 10px",color:C.text,fontFamily:"monospace",fontWeight:500}}>{leg.local_symbol}</td>
                        <td style={{padding:"6px 10px",color:leg.type==="PUT"?C.red:C.green,fontWeight:600}}>{leg.type}</td>
                        <td style={{padding:"6px 10px",color:C.text,fontFamily:"monospace"}}>{leg.strike}</td>
                        <td style={{padding:"6px 10px",color:C.textMuted}}>{leg.expiry}</td>
                        <td style={{padding:"6px 10px",color:leg.position>0?C.green:C.red,fontWeight:600,fontFamily:"monospace"}}>{leg.position>0?"+":""}{leg.position}</td>
                        <td style={{padding:"6px 10px",color:leg.side==="LONG"?C.green:C.red,fontWeight:600}}>{leg.side}</td>
                        <td style={{padding:"6px 10px",color:C.textMuted,fontFamily:"monospace"}}>${leg.avg_cost.toFixed(2)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          );
        })}

        {/* Equity & Fixed Income */}
        {(Object.keys(equities).length > 0 || Object.keys(fixedInc).length > 0) && (
          <div style={{marginTop:20}}>
            <div style={{fontSize:14,fontWeight:700,color:C.text,marginBottom:14}}>Equity & Renda Fixa</div>
            <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(200px,1fr))",gap:12}}>
              {Object.entries(equities).map(([k,v]:any)=>(
                <div key={k} style={{padding:14,background:C.panelAlt,borderRadius:8,border:\`1px solid $\{C.border}\`}}>
                  <div style={{fontSize:12,fontWeight:700,color:C.amber}}>{v.name}</div>
                  <div style={{fontSize:10,color:C.textMuted,marginTop:2}}>{v.sec_type} | {v.position.toLocaleString()} shares</div>
                  <div style={{fontSize:14,fontWeight:700,color:C.text,fontFamily:"monospace",marginTop:6}}>${v.notional_at_cost.toLocaleString("en-US",{minimumFractionDigits:0})}</div>
                  <div style={{fontSize:9,color:C.textMuted}}>Custo medio: ${v.avg_cost.toFixed(2)}</div>
                </div>
              ))}
              {Object.entries(fixedInc).map(([k,v]:any)=>(
                <div key={k} style={{padding:14,background:C.panelAlt,borderRadius:8,border:\`1px solid $\{C.border}\`}}>
                  <div style={{fontSize:12,fontWeight:700,color:C.blue}}>{v.name}</div>
                  <div style={{fontSize:10,color:C.textMuted,marginTop:2}}>{v.sec_type} | Qty {v.position}</div>
                  <div style={{fontSize:14,fontWeight:700,color:C.text,fontFamily:"monospace",marginTop:6}}>${v.notional_at_cost.toLocaleString("en-US",{minimumFractionDigits:0})}</div>
                </div>
              ))}
            </div>
          </div>
        )}

        {symbols.length===0 && Object.keys(equities).length===0 && <DataPlaceholder title="Sem posicoes" detail="Faca export do IBKR via Claude chat" />}
      </div>

      '''

# Replace the section
code = code[:start_idx] + new_render + code[end_idx:]

with open(FILE, "w", encoding="utf-8") as f:
    f.write(code)

print("renderPortfolio atualizado com sucesso!")
print("Dashboard agora le de ibkr_export.json com estrategias identificadas")
