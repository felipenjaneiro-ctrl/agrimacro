import json
from pathlib import Path
from datetime import datetime

DISCLOSURE = {
    'version': '1.0',
    'last_updated': '2026-02-12',
    'title': 'Aviso Legal',
    'subtitle': 'Leia antes de tomar qualquer decisao',
    'sections': [
        {'heading': 'Finalidade do Relatorio', 'content': 'Este relatorio tem carater exclusivamente informativo e educacional. Seu objetivo e fornecer ao produtor rural brasileiro uma visao organizada do mercado de commodities agricolas, com dados de fontes publicas e oficiais. Nenhuma informacao aqui apresentada constitui recomendacao de compra, venda, retencao ou qualquer outra decisao comercial ou financeira.'},
        {'heading': 'Nao e Consultoria', 'content': 'O AgriMacro nao e e nao substitui consultoria financeira, agronomica, juridica ou de investimentos. Decisoes de comercializacao, hedge, armazenagem ou contratacao devem ser tomadas com o apoio de profissionais habilitados que conhecam a realidade especifica da sua propriedade, regiao e situacao financeira. O autor nao possui registro na CVM, CREA ou qualquer orgao regulador para fins de consultoria.'},
        {'heading': 'Fontes e Precisao dos Dados', 'content': 'Os dados sao coletados de fontes consideradas confiaveis, incluindo USDA, EIA, CME Group, ICE, Banco Central do Brasil, IBGE e CEPEA/ESALQ. Apesar do esforco para garantir precisao, nao ha garantia de que os dados estejam livres de erros, atrasos ou omissoes. Dados de mercado podem ter defasagem de ate 24 horas. O relatorio passa por controle de qualidade automatizado (AA Engine), mas falhas podem ocorrer.'},
        {'heading': 'Riscos do Mercado', 'content': 'O mercado de commodities envolve riscos significativos. Precos podem variar de forma abrupta e imprevisivel por fatores climaticos, geopoliticos, cambiais, logisticos e regulatorios. Resultados passados nao garantem resultados futuros. Variacoes percentuais e tendencias apresentadas neste relatorio refletem movimentos historicos e nao devem ser interpretadas como previsao.'},
        {'heading': 'Conflitos de Interesse', 'content': 'O autor deste relatorio pode manter posicoes pessoais em commodities agricolas, contratos futuros ou ativos relacionados aos mercados cobertos nesta publicacao. Essas posicoes podem ser alteradas a qualquer momento, sem aviso previo e sem obrigacao de divulgacao.'},
        {'heading': 'Propriedade Intelectual', 'content': 'Todo o conteudo deste relatorio, incluindo textos, graficos, tabelas e analises, e de propriedade do AgriMacro. E permitido o compartilhamento para fins pessoais e educacionais, desde que citada a fonte. A reproducao comercial sem autorizacao previa por escrito e proibida.'},
        {'heading': 'Limitacao de Responsabilidade', 'content': 'O AgriMacro e seu autor nao se responsabilizam por perdas, danos ou prejuizos de qualquer natureza, diretos ou indiretos, decorrentes do uso das informacoes contidas neste relatorio. Ao utilizar este material, o leitor reconhece que compreende os riscos envolvidos e assume total responsabilidade por suas decisoes.'},
        {'heading': 'Contato', 'content': 'Duvidas, correcoes ou sugestoes podem ser enviadas para: [inserir e-mail]. Erros factuais reportados serao corrigidos na proxima edicao.'},
    ],
    'footer': 'AgriMacro v3.3 - Este aviso legal e parte integrante do relatorio. Ao acessar qualquer conteudo, o leitor declara ciencia e concordancia com os termos acima.',
}

COVER_DISCLAIMER = 'Este relatorio tem carater exclusivamente informativo e educacional. Nao constitui recomendacao de compra, venda ou qualquer decisao comercial ou financeira. O autor pode manter posicoes nos mercados cobertos. Dados de fontes publicas (USDA, EIA, CME, ICE, BCB, CEPEA) sujeitos a atrasos e erros. Resultados passados nao garantem resultados futuros. Veja Aviso Legal completo na ultima pagina.'

def get_cover_disclaimer():
    return COVER_DISCLAIMER

def get_disclosure_page():
    return {
        'page_type': 'disclosure',
        'title': DISCLOSURE['title'],
        'subtitle': DISCLOSURE['subtitle'],
        'version': DISCLOSURE['version'],
        'sections': DISCLOSURE['sections'],
        'footer': DISCLOSURE['footer'],
        'last_updated': DISCLOSURE['last_updated'],
    }

def get_disclosure_text():
    lines = ['# ' + DISCLOSURE['title'], DISCLOSURE['subtitle'], '']
    for s in DISCLOSURE['sections']:
        lines.append('## ' + s['heading'])
        lines.append(s['content'])
        lines.append('')
    lines.append('---')
    lines.append(DISCLOSURE['footer'])
    return chr(10).join(lines)

if __name__ == '__main__':
    print('OK - disclosure.py funcional')
    print(get_cover_disclaimer()[:80] + '...')