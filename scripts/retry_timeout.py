"""
「タイムアウト」「通信エラー」のレコードを再処理するスクリプト

geocoder_chunked.pyで取得できなかったタイムアウト・通信エラーのレコードを
再度APIに問い合わせて、該当住所・緯度・経度・エラーを上書き更新する。

使い方:
    python retry_timeout.py [データディレクトリ] [パート数] [同時接続数]

例:
    python retry_timeout.py . 57 500
"""
import asyncio
import sys
import urllib.parse
from pathlib import Path

import aiohttp
import pandas as pd
from tqdm.asyncio import tqdm_asyncio

sys.stdout.reconfigure(encoding='utf-8')


async def get_coordinates(session, semaphore, address, retry_count=5):
    """住所から緯度経度を取得（リトライ間隔を長めに設定）"""
    base_url = 'https://msearch.gsi.go.jp/address-search/AddressSearch?q='

    if pd.isna(address) or str(address).strip() == '':
        return {'success': False, 'error': '住所が空です'}

    address = str(address).strip()
    encoded = urllib.parse.quote(address)

    async with semaphore:
        for attempt in range(retry_count):
            try:
                async with session.get(
                    base_url + encoded,
                    timeout=aiohttp.ClientTimeout(total=60)
                ) as resp:
                    if resp.status != 200:
                        if attempt < retry_count - 1:
                            await asyncio.sleep(2.0 * (attempt + 1))
                            continue
                        return {'success': False, 'error': f'通信エラー: HTTP {resp.status}'}

                    content_type = resp.headers.get('Content-Type', '')
                    if 'json' not in content_type and 'text/html' in content_type:
                        if attempt < retry_count - 1:
                            await asyncio.sleep(2.0 * (attempt + 1))
                            continue
                        return {'success': False, 'error': f'通信エラー: HTML返却'}

                    data = await resp.json()

                    if not data:
                        return {
                            'success': True,
                            '該当住所': '',
                            '緯度': '',
                            '経度': '',
                            'error': '住所が見つかりません',
                        }

                    coords = data[0]['geometry']['coordinates']
                    return {
                        'success': True,
                        '該当住所': data[0]['properties']['title'],
                        '緯度': str(coords[1]),
                        '経度': str(coords[0]),
                        'error': '',
                    }

            except asyncio.TimeoutError:
                if attempt < retry_count - 1:
                    await asyncio.sleep(2.0 * (attempt + 1))
                    continue
                return {'success': False, 'error': 'タイムアウト'}
            except aiohttp.ClientError as e:
                if attempt < retry_count - 1:
                    await asyncio.sleep(2.0 * (attempt + 1))
                    continue
                return {'success': False, 'error': f'通信エラー: {e}'}
            except (KeyError, IndexError, Exception) as e:
                if attempt < retry_count - 1:
                    await asyncio.sleep(1.0)
                    continue
                return {'success': False, 'error': f'解析エラー: {e}'}

    return {'success': False, 'error': 'リトライ上限'}


async def main(output_dir: Path, total_parts: int, max_concurrent: int):
    semaphore = asyncio.Semaphore(max_concurrent)
    connector = aiohttp.TCPConnector(limit=max_concurrent, limit_per_host=max_concurrent)

    total_targets = 0
    total_fixed = 0
    total_still_error = 0

    async with aiohttp.ClientSession(connector=connector) as session:
        for part in range(1, total_parts + 1):
            files = list(output_dir.glob(f'result_*_part{part:03d}.csv'))
            if not files:
                continue
            file = files[0]

            df = pd.read_csv(file, encoding='utf-8-sig', dtype=str)

            # 「タイムアウト」または「通信エラー」を含むレコードを対象
            mask = (
                df['エラー'].fillna('').str.contains('タイムアウト', na=False) |
                df['エラー'].fillna('').str.contains('通信エラー', na=False)
            )
            error_indices = df[mask].index.tolist()

            if not error_indices:
                continue

            total_targets += len(error_indices)
            print(f'パート{part}: {len(error_indices)}件をリトライ中...')

            addr_col = '住所'
            if addr_col not in df.columns:
                addr_col = df.columns[1]

            tasks = [
                get_coordinates(session, semaphore, df.loc[i, addr_col])
                for i in error_indices
            ]
            results = await tqdm_asyncio.gather(*tasks, desc=f'Part{part}')

            fixed = 0
            still_error = 0
            for idx, res in zip(error_indices, results):
                if res['success']:
                    df.loc[idx, '該当住所'] = res['該当住所']
                    df.loc[idx, '緯度'] = res['緯度']
                    df.loc[idx, '経度'] = res['経度']
                    df.loc[idx, 'エラー'] = res['error']
                    fixed += 1
                else:
                    df.loc[idx, 'エラー'] = res['error']
                    still_error += 1

            print(f'  → 解決: {fixed}件, 未解決: {still_error}件')
            total_fixed += fixed
            total_still_error += still_error

            df.to_csv(file, index=False, encoding='utf-8-sig')

    print(f'\n=== 完了 ===')
    print(f'対象総数: {total_targets}件')
    print(f'解決: {total_fixed}件')
    print(f'未解決: {total_still_error}件')
    if total_targets > 0:
        print(f'解決率: {total_fixed/total_targets*100:.1f}%')


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("使い方: python retry_timeout.py [データディレクトリ] [パート数] [同時接続数]")
        print("例: python retry_timeout.py . 57 500")
        sys.exit(1)

    output_dir = Path(sys.argv[1])
    total_parts = int(sys.argv[2]) if len(sys.argv) > 2 else 57
    max_concurrent = int(sys.argv[3]) if len(sys.argv) > 3 else 500

    asyncio.run(main(output_dir, total_parts, max_concurrent))
