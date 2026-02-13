"""
国税庁法人番号公表サイトのZIPファイルから、ジオコーディング用入力ファイルを作成

処理内容:
1. ZIPファイル内のCSV（Shift_JIS）を読み込み
2. 承継先法人番号（列25）がNULLのレコードのみ抽出
3. 都道府県＋市区町村＋丁目番地（列9+10+11）を結合して住所列を作成
4. 法人番号（列1）と住所の2列だけのCSVを出力

入力:
    00_zenkoku_all_YYYYMMDD.zip（国税庁の全件データ）

出力:
    input_light.csv（法人番号, 住所）

使い方:
    python prepare_input.py <ZIPファイルパス> [出力ディレクトリ]

例:
    python prepare_input.py 00_zenkoku_all_20260130.zip .
"""
import sys
import zipfile
from pathlib import Path

import pandas as pd

sys.stdout.reconfigure(encoding='utf-8')

# 国税庁CSVのカラム定義（ヘッダーなし、全31列）
COLUMNS = [
    '序番',                     # 0
    '法人番号',                 # 1
    '処理区分',                 # 2
    '訂正区分',                 # 3
    '更新年月日',               # 4
    '変更年月日',               # 5
    '法人名',                   # 6
    '法人名ふりがな',           # 7
    '都道府県',                 # 8  ← 住所に使用
    '市区町村',                 # 9  ← 住所に使用
    '丁目番地等',               # 10 ← 住所に使用
    '法人名英語',               # 11
    '都道府県英語',             # 12
    '市区町村英語',             # 13
    '丁目番地等英語',           # 14
    '郵便番号',                 # 15
    '国内所在地イメージID',     # 16
    '法人種別',                 # 17
    '最新届出年月日',           # 18
    '届出整理番号',             # 19
    '登記記録の閉鎖等年月日',   # 20
    '閉鎖の事由',               # 21
    '商号又は名称のフリガナ',   # 22
    '国内所在地（都道府県）',   # 23
    '国内所在地（市区町村）',   # 24
    '承継先法人番号',           # 25 ← NULLのみ抽出
    '変更事由の詳細',           # 26
    '法人番号指定年月日',       # 27
    '最終更新年月日',           # 28
    '廃止年月日',               # 29
    'EN法人名',                 # 30
]


def main(zip_path: str, output_dir: str = '.'):
    zip_path = Path(zip_path)
    output_dir = Path(output_dir)

    if not zip_path.exists():
        print(f'エラー: {zip_path} が見つかりません')
        sys.exit(1)

    print(f'入力ZIP: {zip_path}')
    print(f'出力先: {output_dir}')

    # ZIP内のCSVファイルを特定
    with zipfile.ZipFile(zip_path, 'r') as zf:
        csv_files = [f for f in zf.namelist() if f.endswith('.csv')]
        if not csv_files:
            print('エラー: ZIP内にCSVファイルが見つかりません')
            sys.exit(1)

        csv_name = csv_files[0]
        print(f'CSV: {csv_name}')

        # 読み込み（ヘッダーなし、Shift_JIS）
        print('読み込み中...')
        with zf.open(csv_name) as f:
            df = pd.read_csv(
                f,
                encoding='cp932',
                header=None,
                names=COLUMNS,
                dtype=str,
                low_memory=False,
            )

    print(f'全レコード数: {len(df):,}')

    # 承継先法人番号がNULLのレコードのみ抽出
    df_filtered = df[df['承継先法人番号'].isna()].copy()
    print(f'承継先法人番号がNULL: {len(df_filtered):,}件')

    # 住所列を作成（都道府県 + 市区町村 + 丁目番地等）
    df_filtered['住所'] = (
        df_filtered['都道府県'].fillna('') +
        df_filtered['市区町村'].fillna('') +
        df_filtered['丁目番地等'].fillna('')
    )

    # 法人番号と住所の2列だけ出力
    df_out = df_filtered[['法人番号', '住所']]

    output_file = output_dir / 'input_light.csv'
    df_out.to_csv(output_file, index=False, encoding='utf-8-sig')

    print(f'\n=== 完了 ===')
    print(f'出力レコード数: {len(df_out):,}')
    print(f'出力ファイル: {output_file}')


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print('使い方: python prepare_input.py <ZIPファイルパス> [出力ディレクトリ]')
        print('例: python prepare_input.py 00_zenkoku_all_20260130.zip .')
        sys.exit(1)

    zip_file = sys.argv[1]
    out_dir = sys.argv[2] if len(sys.argv) > 2 else '.'
    main(zip_file, out_dir)
