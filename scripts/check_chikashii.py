"""
近しい住所の取得状況を確認するスクリプト

使い方:
    python check_chikashii.py [データディレクトリ] [パート数]

例:
    python check_chikashii.py ./output 58
"""
import pandas as pd
import sys
from pathlib import Path

sys.stdout.reconfigure(encoding='utf-8')

if len(sys.argv) < 2:
    print("使い方: python check_chikashii.py [データディレクトリ] [パート数]")
    print("例: python check_chikashii.py ./output 58")
    sys.exit(1)

output_dir = Path(sys.argv[1])
total_parts = int(sys.argv[2]) if len(sys.argv) > 2 else 58

print('| パート | 住所が見つかりません | 近しい住所取得済 | 残り未解決 |')
print('|--------|----------------------|------------------|------------|')

total_notfound = 0
total_fixed = 0
total_remain = 0

for i in range(1, total_parts + 1):
    files = list(output_dir.glob(f'result_*_part{i:03d}.csv'))
    if not files:
        continue
    file = files[0]

    df = pd.read_csv(file, encoding='utf-8-sig', dtype=str)

    notfound = (df['エラー'] == '住所が見つかりません').sum()

    if '近しい住所' in df.columns:
        fixed = ((df['エラー'] == '住所が見つかりません') & (df['近しい住所'].fillna('') != '')).sum()
    else:
        fixed = 0

    remain = notfound - fixed

    total_notfound += notfound
    total_fixed += fixed
    total_remain += remain

    if notfound > 0:
        print(f'| {i:03d} | {notfound:,} | {fixed:,} | {remain:,} |')

print('|--------|----------------------|------------------|------------|')
print(f'| **合計** | **{total_notfound:,}** | **{total_fixed:,}** | **{total_remain:,}** |')
