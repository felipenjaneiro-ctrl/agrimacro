src = open('bottleneck_backtest.py', encoding='utf-8').read()

# Encontra a linha exata com equity
for i, l in enumerate(src.splitlines(), 1):
    if 'equity' in l.lower() and ('*=' in l or '* (' in l or 'ret_fwd' in l.lower()):
        print(f'L{i}: {repr(l)}')
