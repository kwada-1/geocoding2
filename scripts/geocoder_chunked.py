"""
国土地理院APIを使用して、大規模CSVの住所列から緯度・経度を一括取得するツール
チャンク処理版 - メモリ効率的に大規模ファイルを処理

使い方:
    python geocoder_chunked.py 入力ファイル.csv [住所列名] [同時接続数] [分割件数]

例:
    python geocoder_chunked.py addresses.csv 住所 100 100000
"""

import asyncio
import sys
import urllib.parse
from pathlib import Path

import aiohttp
import pandas as pd
from tqdm.asyncio import tqdm_asyncio


async def get_coordinates(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    address: str,
    retry_count: int = 3,
) -> dict:
    """住所から緯度・経度を取得する（非同期版）"""
    base_url = "https://msearch.gsi.go.jp/address-search/AddressSearch?q="

    if pd.isna(address) or str(address).strip() == "":
        return {"該当住所": "", "緯度": None, "経度": None, "エラー": "住所が空です"}

    address = str(address).strip()
    encoded_address = urllib.parse.quote(address)

    async with semaphore:
        for attempt in range(retry_count):
            try:
                async with session.get(base_url + encoded_address, timeout=aiohttp.ClientTimeout(total=30)) as response:
                    response.raise_for_status()
                    data = await response.json()

                    if not data:
                        return {"該当住所": "", "緯度": None, "経度": None, "エラー": "住所が見つかりません"}

                    coordinates = data[0]["geometry"]["coordinates"]
                    return {
                        "該当住所": data[0]["properties"]["title"],
                        "緯度": coordinates[1],
                        "経度": coordinates[0],
                        "エラー": "",
                    }

            except asyncio.TimeoutError:
                if attempt < retry_count - 1:
                    await asyncio.sleep(0.5)
                    continue
                return {"該当住所": "", "緯度": None, "経度": None, "エラー": "タイムアウト"}
            except aiohttp.ClientError as e:
                if attempt < retry_count - 1:
                    await asyncio.sleep(0.5)
                    continue
                return {"該当住所": "", "緯度": None, "経度": None, "エラー": f"通信エラー: {e}"}
            except (KeyError, IndexError) as e:
                return {"該当住所": "", "緯度": None, "経度": None, "エラー": f"解析エラー: {e}"}

    return {"該当住所": "", "緯度": None, "経度": None, "エラー": "リトライ上限"}


async def process_chunk(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    df_chunk: pd.DataFrame,
    address_column: str,
    chunk_num: int,
) -> pd.DataFrame:
    """チャンクを非同期で処理"""
    addresses = df_chunk[address_column].tolist()

    tasks = [get_coordinates(session, semaphore, addr) for addr in addresses]
    results = await tqdm_asyncio.gather(*tasks, desc=f"チャンク {chunk_num}")

    df_chunk = df_chunk.copy()
    df_chunk["該当住所"] = [r["該当住所"] for r in results]
    df_chunk["緯度"] = [r["緯度"] for r in results]
    df_chunk["経度"] = [r["経度"] for r in results]
    df_chunk["エラー"] = [r["エラー"] for r in results]

    return df_chunk


async def process_file_chunked(
    input_file: str,
    address_column: str = "住所",
    max_concurrent: int = 100,
    split_size: int = 100000,
):
    """チャンク単位でファイルを処理"""
    input_path = Path(input_file)
    output_dir = input_path.parent
    base_name = input_path.stem

    print(f"入力ファイル: {input_file}")
    print(f"同時接続数: {max_concurrent}")
    print(f"ファイル分割: {split_size:,}件ごと")

    # 進捗ファイル
    progress_file = output_dir / f"result_{base_name}_progress.txt"
    start_part = 1

    if progress_file.exists():
        with open(progress_file, "r") as f:
            start_part = int(f.read().strip()) + 1
        print(f"前回の続きから処理します: パート {start_part} から")

    # エンコーディング検出
    encodings = ["utf-8-sig", "utf-8", "shift_jis", "cp932"]
    encoding_to_use = None
    for enc in encodings:
        try:
            pd.read_csv(input_file, encoding=enc, nrows=1)
            encoding_to_use = enc
            break
        except:
            continue

    if not encoding_to_use:
        print("エラー: エンコーディングを特定できません")
        sys.exit(1)

    print(f"エンコーディング: {encoding_to_use}")

    # 総行数をカウント
    print("総行数をカウント中...")
    total_rows = sum(1 for _ in open(input_file, encoding=encoding_to_use)) - 1  # ヘッダー除く
    total_parts = (total_rows + split_size - 1) // split_size
    print(f"総件数: {total_rows:,} 件 ({total_parts} パート)")

    # セマフォとコネクタ
    semaphore = asyncio.Semaphore(max_concurrent)
    connector = aiohttp.TCPConnector(limit=max_concurrent, limit_per_host=max_concurrent)

    success_total = 0
    error_total = 0

    try:
        async with aiohttp.ClientSession(connector=connector) as session:
            # チャンクごとに読み込んで処理
            chunk_iter = pd.read_csv(
                input_file,
                encoding=encoding_to_use,
                dtype=str,
                chunksize=split_size,
            )

            for part_num, df_chunk in enumerate(chunk_iter, start=1):
                # スキップ済みパート
                if part_num < start_part:
                    print(f"パート {part_num}/{total_parts} スキップ")
                    continue

                print(f"\nパート {part_num}/{total_parts} 処理中 ({len(df_chunk):,}件)...")

                if address_column not in df_chunk.columns:
                    print(f"エラー: 列 '{address_column}' が見つかりません")
                    print(f"利用可能な列: {list(df_chunk.columns)}")
                    sys.exit(1)

                # 非同期処理
                df_result = await process_chunk(
                    session, semaphore, df_chunk, address_column, part_num
                )

                # 結果を保存
                output_file = output_dir / f"result_{base_name}_part{part_num:03d}.csv"
                df_result.to_csv(output_file, index=False, encoding="utf-8-sig")
                print(f"  保存: {output_file.name}")

                # 統計
                success = df_result["緯度"].notna().sum()
                error = (df_result["エラー"].fillna("") != "").sum()
                success_total += success
                error_total += error
                print(f"  成功: {success:,}, 失敗: {error:,}")

                # 進捗保存
                with open(progress_file, "w") as f:
                    f.write(str(part_num))

    except KeyboardInterrupt:
        print("\n\n中断されました。再度実行すると続きから処理できます。")
        sys.exit(0)

    # 完了
    if progress_file.exists():
        progress_file.unlink()

    print(f"\n=== 完了 ===")
    print(f"総パート数: {total_parts}")
    print(f"成功: {success_total:,} 件")
    print(f"失敗: {error_total:,} 件")
    print(f"出力先: {output_dir / f'result_{base_name}_part*.csv'}")


def main():
    if len(sys.argv) < 2:
        print("使い方: python geocoder_chunked.py 入力ファイル.csv [住所列名] [同時接続数] [分割件数]")
        print("例: python geocoder_chunked.py addresses.csv 住所 100 100000")
        sys.exit(1)

    input_file = sys.argv[1]
    address_column = sys.argv[2] if len(sys.argv) > 2 else "住所"
    max_concurrent = int(sys.argv[3]) if len(sys.argv) > 3 else 100
    split_size = int(sys.argv[4]) if len(sys.argv) > 4 else 100000

    asyncio.run(process_file_chunked(input_file, address_column, max_concurrent, split_size))


if __name__ == "__main__":
    main()
