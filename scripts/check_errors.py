"""
ジオコーディング結果のエラー集計スクリプト

使い方:
    python check_errors.py [データディレクトリ] [パート数]

例:
    python check_errors.py ./output 58
"""
import pandas as pd
import sys
from pathlib import Path

sys.stdout.reconfigure(encoding='utf-8')

if len(sys.argv) < 2:
    print("使い方: python check_errors.py [データディレクトリ] [パート数]")
    print("例: python check_errors.py ./output 58")
    sys.exit(1)

output_dir = Path(sys.argv[1])
total_parts = int(sys.argv[2]) if len(sys.argv) > 2 else 58

print('| パート | 総件数 | 住所が空です | 住所が見つかりません | その他エラー |')
print('|--------|--------|--------------|----------------------|--------------|')

total_empty = 0
total_notfound = 0
total_other = 0

for i in range(1, total_parts + 1):
    files = list(output_dir.glob(f'result_*_part{i:03d}.csv'))
    if not files:
        continue
    file = files[0]

    df = pd.read_csv(file, encoding='utf-8-sig', dtype=str)
    err = df['エラー'].fillna('')

    empty = (err == '住所が空です').sum()
    notfound = (err == '住所が見つかりません').sum()
    other_mask = (err != '') & (err != '住所が空です') & (err != '住所が見つかりません')
    other = other_mask.sum()

    total_empty += empty
    total_notfound += notfound
    total_other += other

    if empty > 0 or notfound > 0 or other > 0:
        print(f'| {i:03d} | {len(df):,} | {empty:,} | {notfound:,} | {other:,} |')

print('|--------|--------|--------------|----------------------|--------------|')
print(f'| **合計** | - | **{total_empty:,}** | **{total_notfound:,}** | **{total_other:,}** |')
