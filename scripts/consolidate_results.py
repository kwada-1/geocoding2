"""
ジオコーディング結果を統合し、最終的な緯度・経度を1列にまとめるスクリプト

出力カラム:
  - 法人番号
  - 住所（元の住所）
  - 該当住所（直接マッチした住所）
  - 緯度（直接マッチの緯度）
  - 経度（直接マッチの経度）
  - エラー
  - 近しい住所（補完で見つかった住所）
  - 近しい住所の緯度
  - 近しい住所の経度
  - 最終緯度（直接マッチ優先、なければ近しい住所の緯度）
  - 最終経度（直接マッチ優先、なければ近しい住所の経度）

使い方:
    python consolidate_results.py [データディレクトリ] [パート数]

例:
    python consolidate_results.py . 57
"""
import pandas as pd
import sys
from pathlib import Path

sys.stdout.reconfigure(encoding='utf-8')

if len(sys.argv) < 2:
    print("使い方: python consolidate_results.py [データディレクトリ] [パート数]")
    print("例: python consolidate_results.py . 57")
    sys.exit(1)

output_dir = Path(sys.argv[1])
total_parts = int(sys.argv[2]) if len(sys.argv) > 2 else 57

output_file = output_dir / 'final_geocoded_result.csv'

first = True
total_records = 0
total_with_coords = 0

for i in range(1, total_parts + 1):
    files = list(output_dir.glob(f'result_*_part{i:03d}.csv'))
    if not files:
        continue
    file = files[0]

    df = pd.read_csv(file, encoding='utf-8-sig', dtype=str)
    total_records += len(df)

    # 近しい住所の列がなければ空で作成
    if '近しい住所' not in df.columns:
        df['近しい住所'] = ''
    if '近しい住所の緯度' not in df.columns:
        df['近しい住所の緯度'] = ''
    if '近しい住所の経度' not in df.columns:
        df['近しい住所の経度'] = ''

    # 最終緯度・経度を決定
    # 直接マッチ（エラーが空）の場合は緯度・経度を使用
    # エラーありで近しい住所がある場合は近しい住所の緯度・経度を使用
    df['最終緯度'] = df.apply(
        lambda row: row['緯度'] if pd.notna(row['緯度']) and str(row['緯度']).strip() != ''
        else (row['近しい住所の緯度'] if pd.notna(row.get('近しい住所の緯度')) and str(row.get('近しい住所の緯度', '')).strip() != ''
              else ''),
        axis=1
    )
    df['最終経度'] = df.apply(
        lambda row: row['経度'] if pd.notna(row['経度']) and str(row['経度']).strip() != ''
        else (row['近しい住所の経度'] if pd.notna(row.get('近しい住所の経度')) and str(row.get('近しい住所の経度', '')).strip() != ''
              else ''),
        axis=1
    )

    total_with_coords += (df['最終緯度'] != '').sum()

    # 必要なカラムだけ抽出
    out_columns = [
        '法人番号', '住所',
        '該当住所', '緯度', '経度', 'エラー',
        '近しい住所', '近しい住所の緯度', '近しい住所の経度',
        '最終緯度', '最終経度'
    ]

    # 存在するカラムだけ選択
    available = [c for c in out_columns if c in df.columns]
    df_out = df[available]

    df_out.to_csv(output_file, mode='w' if first else 'a', header=first,
                  index=False, encoding='utf-8-sig')
    first = False

    print(f'パート{i:03d}: {len(df):,}件処理')

print(f'\n=== 統合完了 ===')
print(f'総レコード数: {total_records:,}')
print(f'最終緯度経度あり: {total_with_coords:,} ({total_with_coords/total_records*100:.2f}%)')
print(f'出力ファイル: {output_file}')
