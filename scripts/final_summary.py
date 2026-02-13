"""
ジオコーディング結果の最終サマリーを表示するスクリプト

使い方:
    python final_summary.py [データディレクトリ] [パート数]

例:
    python final_summary.py ./output 58
"""
import pandas as pd
import sys
from pathlib import Path

sys.stdout.reconfigure(encoding='utf-8')

if len(sys.argv) < 2:
    print("使い方: python final_summary.py [データディレクトリ] [パート数]")
    print("例: python final_summary.py ./output 58")
    sys.exit(1)

output_dir = Path(sys.argv[1])
total_parts = int(sys.argv[2]) if len(sys.argv) > 2 else 58

total_records = 0
total_success = 0
total_empty = 0
total_notfound = 0
total_notfound_fixed = 0
total_other = 0

for i in range(1, total_parts + 1):
    files = list(output_dir.glob(f'result_*_part{i:03d}.csv'))
    if not files:
        continue
    file = files[0]

    df = pd.read_csv(file, encoding='utf-8-sig', dtype=str)

    total_records += len(df)

    err = df['エラー'].fillna('')
    empty = (err == '住所が空です').sum()
    notfound = (err == '住所が見つかりません').sum()
    other = ((err != '') & (err != '住所が空です') & (err != '住所が見つかりません')).sum()
    success = len(df) - empty - notfound - other

    total_empty += empty
    total_notfound += notfound
    total_other += other
    total_success += success

    if '近しい住所' in df.columns:
        fixed = ((df['エラー'] == '住所が見つかりません') & (df['近しい住所'].fillna('') != '')).sum()
        total_notfound_fixed += fixed

print('=' * 60)
print('全国ジオコーディング結果サマリー')
print('=' * 60)
print(f'\n総レコード数: {total_records:,}件')
print()
print('【ジオコーディング結果】')
print(f'  成功（緯度経度取得）: {total_success:,}件 ({total_success/total_records*100:.2f}%)')
print(f'  住所が空です: {total_empty:,}件 ({total_empty/total_records*100:.2f}%)')
print(f'  住所が見つかりません: {total_notfound:,}件 ({total_notfound/total_records*100:.2f}%)')
print(f'  その他エラー: {total_other:,}件 ({total_other/total_records*100:.3f}%)')
print()
if total_notfound > 0:
    print('【「住所が見つかりません」の補完状況】')
    print(f'  近しい住所で補完成功: {total_notfound_fixed:,}件 ({total_notfound_fixed/total_notfound*100:.1f}%)')
    print(f'  補完できず: {total_notfound - total_notfound_fixed:,}件 ({(total_notfound - total_notfound_fixed)/total_notfound*100:.1f}%)')
    print()
print('【最終的な緯度経度取得率】')
final_success = total_success + total_notfound_fixed
print(f'  取得成功（本来+補完）: {final_success:,}件 ({final_success/total_records*100:.2f}%)')
print(f'  取得失敗: {total_records - final_success:,}件 ({(total_records - final_success)/total_records*100:.2f}%)')
print()
print('=' * 60)
