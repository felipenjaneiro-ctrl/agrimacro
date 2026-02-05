import os, json
from datetime import datetime

def gen():
    with open('data/raw/price_history.json') as f:
        prices = json.load(f)
    with open('data/processed/spreads.json') as f:
        spreads = json.load(f)
    with open('data/processed/seasonality.json') as f:
        seas = json.load(f)
    with open('data/processed/stocks_watch.json') as f:
        watch = json.load(f)

    comms = watch.get('commodities', {})
    below = [(s,d) for s,d in comms.items() if 'APERTO' in d.get('state','')]
    above = [(s,d) for s,d in comms.items() if 'EXCESSO' in d.get('state','')]
    neutral = [(s,d) for s,d in comms.items() if 'NEUTRO' in d.get('state','')]

    spread_rows = ''
    for sid, d in spreads.get('spreads',{}).items():
        if d.get('status') != 'ok': continue
        color = '#ef5350' if d['regime']=='EXTREMO' else '#f2c94c' if d['regime']=='DISSONANCIA' else '#26a69a'
        spread_rows += '<tr><td>%s</td><td>%.4f %s</td><td>%s</td><td>%s%%</td><td style="color:%s;font-weight:bold">%s</td></tr>' % (d['name'],d['current'],d['unit'],d.get('zscore_1y',''),d.get('percentile',''),color,d['regime'])

    seas_rows = ''
    items = []
    for sym, d in seas.items():
        s = d.get('stats')
        if not s: continue
        items.append((sym, s['current_price'], s['avg_price'], s['deviation_pct']))
    items.sort(key=lambda x: x[3])
    for sym, cur, avg, dev in items:
        color = '#26a69a' if dev > 0 else '#ef5350'
        seas_rows += '<tr><td>%s</td><td>%.2f</td><td>%.2f</td><td style="color:%s;font-weight:bold">%+.1f%%</td></tr>' % (sym,cur,avg,color,dev)

    below_html = ''.join(['<span class="badge badge-red">%s (%+.1f%%)</span>' % (s,d.get('price_vs_avg',0)) for s,d in below])
    above_html = ''.join(['<span class="badge badge-green">%s (%+.1f%%)</span>' % (s,d.get('price_vs_avg',0)) for s,d in above])
    neutral_html = ''.join(['<span class="badge badge-gray">%s</span>' % s for s,d in neutral])

    symbols_json = json.dumps(list(prices.keys()))
    prices_json = json.dumps(prices)
    seas_json = json.dumps(seas)
    now = datetime.now().strftime('%d/%m/%Y %H:%M')

    html = []
    html.append('<!DOCTYPE html><html lang="pt-BR"><head><meta charset="UTF-8">')
    html.append('<meta name="viewport" content="width=device-width, initial-scale=1.0">')
    html.append('<title>AgriMacro Dashboard</title>')
    html.append('<script src="https://cdnjs.cloudflare.com/ajax/libs/lightweight-charts/4.2.0/lightweight-charts.standalone.production.js"><\/script>')
    html.append('<style>')
    html.append('*{margin:0;padding:0;box-sizing:border-box}')
    html.append('body{background:#0b0e11;color:#d1d4dc;font-family:system-ui,-apple-system,sans-serif}')
    html.append('.header{background:#16213e;padding:16px 24px;display:flex;justify-content:space-between;align-items:center;border-bottom:1px solid #2a2e39}')
    html.append('.header h1{font-size:20px;color:#f2c94c}')
    html.append('.header span{font-size:12px;color:#9aa4b2}')
    html.append('.container{max-width:1400px;margin:0 auto;padding:16px}')
    html.append('.grid{display:grid;grid-template-columns:1fr 1fr;gap:16px;margin-bottom:16px}')
    html.append('.grid-3{display:grid;grid-template-columns:1fr 1fr 1fr;gap:12px;margin-bottom:16px}')
    html.append('.card{background:#1a1a2e;border:1px solid #2a2e39;border-radius:10px;padding:16px}')
    html.append('.card h2{font-size:14px;color:#f2c94c;margin-bottom:12px}')
    html.append('.card h3{font-size:12px;color:#9aa4b2;margin-bottom:8px}')
    html.append('.stat-box{text-align:center;padding:12px}')
    html.append('.stat-num{font-size:28px;font-weight:bold}')
    html.append('.stat-label{font-size:11px;color:#9aa4b2;margin-top:4px}')
    html.append('table{width:100%;border-collapse:collapse;font-size:12px}')
    html.append('th{background:#16213e;padding:8px;text-align:left;color:#9aa4b2;font-weight:normal}')
    html.append('td{padding:8px;border-bottom:1px solid #1e222d}')
    html.append('.badge{display:inline-block;padding:4px 10px;border-radius:12px;font-size:11px;margin:3px}')
    html.append('.badge-red{background:#3d1a1a;color:#ef5350}')
    html.append('.badge-green{background:#1a3d2a;color:#26a69a}')
    html.append('.badge-gray{background:#1e222d;color:#9aa4b2}')
    html.append('.chart-select{background:#16213e;color:#d1d4dc;border:1px solid #2a2e39;border-radius:6px;padding:6px 12px;font-size:12px;margin-right:8px}')
    html.append('.btn{background:#16213e;color:#d1d4dc;border:1px solid #2a2e39;border-radius:6px;padding:6px 12px;font-size:11px;cursor:pointer;margin:2px}')
    html.append('.btn:hover{background:#2a2e39}')
    html.append('.btn.active{background:#f2c94c;color:#0b0e11;border-color:#f2c94c}')
    html.append('#chart-container{width:100%;height:400px}')
    html.append('.disclaimer{text-align:center;padding:20px;font-size:10px;color:#555}')
    html.append('</style></head><body>')
    html.append('<div class="header"><h1>AgriMacro Dashboard</h1><span>%s | Dados reais | ZERO MOCK</span></div>' % now)
    html.append('<div class="container">')
    html.append('<div class="grid-3">')
    html.append('<div class="card stat-box"><div class="stat-num" style="color:#ef5350">%d</div><div class="stat-label">ABAIXO DA MEDIA (Aperto)</div></div>' % len(below))
    html.append('<div class="card stat-box"><div class="stat-num" style="color:#9aa4b2">%d</div><div class="stat-label">NEUTRO</div></div>' % len(neutral))
    html.append('<div class="card stat-box"><div class="stat-num" style="color:#26a69a">%d</div><div class="stat-label">ACIMA DA MEDIA (Excesso)</div></div>' % len(above))
    html.append('</div>')
    html.append('<div class="card" style="margin-bottom:16px"><h2>Estado do Mercado</h2>')
    html.append('<h3>Abaixo da media (potencial aperto)</h3><div>%s</div>' % (below_html or '<span class="badge badge-gray">Nenhum</span>'))
    html.append('<h3 style="margin-top:10px">Acima da media (potencial excesso)</h3><div>%s</div>' % (above_html or '<span class="badge badge-gray">Nenhum</span>'))
    html.append('<h3 style="margin-top:10px">Neutro</h3><div>%s</div></div>' % (neutral_html or '<span class="badge badge-gray">Nenhum</span>'))
    html.append('<div class="card" style="margin-bottom:16px">')
    html.append('<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:12px">')
    html.append('<h2 style="margin:0">Grafico de Precos</h2><div>')
    html.append('<select id="symbolSelect" class="chart-select" onchange="updateChart()"></select>')
    html.append('<button class="btn" onclick="setRange(30)">1M</button>')
    html.append('<button class="btn" onclick="setRange(90)">3M</button>')
    html.append('<button class="btn" onclick="setRange(180)">6M</button>')
    html.append('<button class="btn active" onclick="setRange(365)">1Y</button>')
    html.append('<button class="btn" onclick="setRange(730)">2Y</button>')
    html.append('<button class="btn" onclick="setRange(9999)">ALL</button>')
    html.append('</div></div>')
    html.append('<div id="chart-legend" style="font-size:11px;color:#9aa4b2;margin-bottom:6px"></div>')
    html.append('<div id="chart-container"></div></div>')
    html.append('<div class="grid"><div class="card"><h2>Spreads & Relative Value</h2><table>')
    html.append('<tr><th>Spread</th><th>Valor</th><th>Z-score</th><th>Percentil</th><th>Regime</th></tr>')
    html.append(spread_rows)
    html.append('</table></div><div class="card"><h2>Sazonalidade - vs Media 5 Anos</h2><table>')
    html.append('<tr><th>Commodity</th><th>Atual</th><th>Media 5Y</th><th>Desvio</th></tr>')
    html.append(seas_rows)
    html.append('</table></div></div>')
    html.append('<div class="card" style="margin-top:16px"><h2>Perguntas-Guia</h2>')
    html.append('<div style="display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-top:8px">')
    html.append('<div class="card" style="border-color:#f2c94c"><b style="color:#f2c94c">1.</b> O que mudou desde ontem?</div>')
    html.append('<div class="card" style="border-color:#f2c94c"><b style="color:#f2c94c">2.</b> O que NAO mudou, mas deveria?</div>')
    html.append('<div class="card" style="border-color:#f2c94c"><b style="color:#f2c94c">3.</b> O que esta em extremo historico?</div>')
    html.append('<div class="card" style="border-color:#f2c94c"><b style="color:#f2c94c">4.</b> O que o mercado parece ignorar?</div>')
    html.append('</div></div>')
    html.append('<div class="disclaimer">AgriMacro v2.0 | Diagnostico de mercado | Nao e recomendacao | Dados: Yahoo Finance</div>')
    html.append('</div>')

    js = """<script>
var ALL_PRICES = %s;
var SYMBOLS = %s;
var SEAS = %s;
var chart, candleSeries, volSeries, sma20S, sma50S, sma200S;
var currentRange = 365;
function initChart() {
  var sel = document.getElementById("symbolSelect");
  SYMBOLS.forEach(function(s){var o=document.createElement("option");o.value=s;o.text=s;sel.add(o);});
  chart = LightweightCharts.createChart(document.getElementById("chart-container"), {
    layout:{background:{type:"solid",color:"#1a1a2e"},textColor:"#d1d4dc"},
    grid:{vertLines:{color:"#1e222d"},horzLines:{color:"#1e222d"}},
    rightPriceScale:{borderColor:"#2a2e39"},
    timeScale:{borderColor:"#2a2e39",timeVisible:true},
    crosshair:{vertLine:{color:"#758696"},horzLine:{color:"#758696"}},
    width:document.getElementById("chart-container").clientWidth,height:400
  });
  candleSeries = chart.addCandlestickSeries({upColor:"#26a69a",downColor:"#ef5350",borderUpColor:"#26a69a",borderDownColor:"#ef5350",wickUpColor:"#26a69a",wickDownColor:"#ef5350"});
  volSeries = chart.addHistogramSeries({priceFormat:{type:"volume"},priceScaleId:"vol"});
  chart.priceScale("vol").applyOptions({scaleMargins:{top:0.85,bottom:0}});
  sma20S = chart.addLineSeries({lineWidth:1,color:"#f2c94c",priceLineVisible:false});
  sma50S = chart.addLineSeries({lineWidth:1,color:"#56ccf2",priceLineVisible:false});
  sma200S = chart.addLineSeries({lineWidth:1,color:"#bb6bd9",priceLineVisible:false});
  chart.subscribeCrosshairMove(function(param){
    if(!param.time)return;
    var d=param.seriesData.get(candleSeries);
    if(d)document.getElementById("chart-legend").innerHTML="O:"+d.open.toFixed(2)+" H:"+d.high.toFixed(2)+" L:"+d.low.toFixed(2)+" C:"+d.close.toFixed(2);
  });
  updateChart();
  window.addEventListener("resize",function(){chart.resize(document.getElementById("chart-container").clientWidth,400);});
}
function calcSMA(data,period){
  var r=[];
  for(var i=period-1;i<data.length;i++){var sum=0;for(var j=i-period+1;j<=i;j++)sum+=data[j].close;r.push({time:data[i].time,value:sum/period});}
  return r;
}
function updateChart(){
  var sym=document.getElementById("symbolSelect").value;
  var raw=ALL_PRICES[sym]||[];
  var data=raw.map(function(r){return{time:r.date,open:r.open,high:r.high,low:r.low,close:r.close,volume:r.volume||0};});
  var cutoff=currentRange<9999?data.slice(-currentRange):data;
  candleSeries.setData(cutoff.map(function(d){return{time:d.time,open:d.open,high:d.high,low:d.low,close:d.close};}));
  volSeries.setData(cutoff.map(function(d){return{time:d.time,value:d.volume,color:d.close>=d.open?"#26a69a44":"#ef535044"};}));
  sma20S.setData(calcSMA(cutoff,20));
  sma50S.setData(calcSMA(cutoff,50));
  sma200S.setData(calcSMA(cutoff,Math.min(200,Math.floor(cutoff.length*0.8))));
  chart.timeScale().fitContent();
  var s=SEAS[sym]?SEAS[sym].stats:null;
  var leg=document.getElementById("chart-legend");
  if(s)leg.innerHTML=sym+" | Atual: "+s.current_price+" | Media 5Y: "+s.avg_price+" | Desvio: "+(s.deviation_pct>0?"+":"")+s.deviation_pct+"%%";
  else leg.innerHTML=sym;
}
function setRange(days){
  currentRange=days;
  document.querySelectorAll(".btn").forEach(function(b){b.classList.remove("active");});
  event.target.classList.add("active");
  updateChart();
}
initChart();
</script>""" % (prices_json, symbols_json, seas_json)

    html.append(js)
    html.append('</body></html>')

    os.makedirs('dashboard', exist_ok=True)
    with open('dashboard/index.html', 'w', encoding='utf-8') as f:
        f.write('\\n'.join(html))
    print('Dashboard gerado: dashboard/index.html')

gen()
