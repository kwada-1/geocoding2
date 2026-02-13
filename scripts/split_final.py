"""
最終結果CSVを指定個数に分割するスクリプト

使い方:
    python split_final.py [入力ファイル] [分割数]

例:
    python split_final.py final_geocoded_result.csv 50
"""
import math
import os
import sys
from pathlib import Path

import pandas as pd

sys.stdout.reconfigure(encoding='utf-8')

if len(sys.argv) < 2:
    print('使い方: python split_final.py [入力ファイル] [分割数]')
    print('例: python split_final.py final_geocoded_result.csv 50')
    sys.exit(1)

input_file = Path(sys.argv[1])
num_splits = int(sys.argv[2]) if len(sys.argv) > 2 else 50

out_dir = input_file.parent / 'final_split'
os.makedirs(out_dir, exist_ok=True)

# 総行数カウント
total = sum(1 for _ in open(input_file, encoding='utf-8-sig')) - 1
chunk_size = math.ceil(total / num_splits)
print(f'総レコード数: {total:,}  1ファイルあたり: {chunk_size:,}件')

for i, chunk in enumerate(
    pd.read_csv(input_file, encoding='utf-8-sig', dtype=str, chunksize=chunk_size), 1
):
    out_path = out_dir / f'final_geocoded_part{i:02d}.csv'
    chunk.to_csv(out_path, index=False, encoding='utf-8-sig')
    print(f'  part{i:02d}: {len(chunk):,}件')

print(f'\n完了: {out_dir} に{num_splits}ファイル出力')
