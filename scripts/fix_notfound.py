"""
「住所が見つかりません」エラーに対して：
1. 浜松市は区名変換（2024年再編対応）
2. 京都の通り名正規化
3. 一般的な住所正規化

結果を「近しい住所」「近しい住所の緯度」「近しい住所の経度」列に追加

使い方:
    python fix_notfound.py [データディレクトリ] [パート数]

例:
    python fix_notfound.py ./output 58
"""
import asyncio
import pandas as pd
import aiohttp
import urllib.parse
import re
import sys
from pathlib import Path
from tqdm.asyncio import tqdm_asyncio

sys.stdout.reconfigure(encoding='utf-8')

# 三方原地区の町名（北区→中央区）
MIKATAHARA_TOWNS = ['初生町', '三方原町', '東三方町', '豊岡町', '三幸町', '大原町', '根洗町']


def convert_hamamatsu_address(address: str) -> str:
    """浜松市の旧区名を新区名に変換（2024年再編対応）"""
    if not address or '浜松市' not in address:
        return None

    addr = str(address)

    # 中区 → 中央区
    if '浜松市中区' in addr:
        return addr.replace('浜松市中区', '浜松市中央区')
    # 東区 → 中央区
    if '浜松市東区' in addr:
        return addr.replace('浜松市東区', '浜松市中央区')
    # 西区 → 中央区
    if '浜松市西区' in addr:
        return addr.replace('浜松市西区', '浜松市中央区')
    # 南区 → 中央区
    if '浜松市南区' in addr:
        return addr.replace('浜松市南区', '浜松市中央区')
    # 北区 → 三方原地区なら中央区、それ以外は浜名区
    if '浜松市北区' in addr:
        for town in MIKATAHARA_TOWNS:
            if town in addr:
                return addr.replace('浜松市北区', '浜松市中央区')
        return addr.replace('浜松市北区', '浜松市浜名区')
    # 浜北区 → 浜名区
    if '浜松市浜北区' in addr:
        return addr.replace('浜松市浜北区', '浜松市浜名区')

    return None


def normalize_kyoto_address(address: str) -> list:
    """京都の通り名表記を正規化"""
    if not address or '京都' not in address:
        return []

    variants = []
    addr = str(address)

    # パターン1: 通り名を省略して町名だけで検索
    match = re.search(r'(京都府京都市[^区]+区).*?([^\d通入上下東西]+町)\d*', addr)
    if match:
        simplified = f"{match.group(1)}{match.group(2)}"
        variants.append(simplified)

    # パターン2: 番地以降を削除
    v2 = re.sub(r'\d+番地.*$', '', addr)
    if v2 != addr and len(v2) > 15:
        variants.append(v2)

    return variants


def normalize_general_address(address: str) -> list:
    """一般的な住所正規化"""
    if not address:
        return []

    variants = []
    addr = str(address)

    # 「番地の○」→「-○」
    v1 = re.sub(r'(\d+)番地の(\d+)', r'\1-\2', addr)
    if v1 != addr:
        variants.append(v1)

    # 「番地」削除
    v2 = re.sub(r'番地', '', addr)
    if v2 != addr and v2 not in variants:
        variants.append(v2)

    # 番地以降を削除
    v3 = re.sub(r'\d+番地.*$', '', addr)
    if v3 != addr and len(v3) > 10 and v3 not in variants:
        variants.append(v3)

    # 丁目以降を削除
    v4 = re.sub(r'\d+丁目.*$', '', addr)
    if v4 != addr and len(v4) > 10 and v4 not in variants:
        variants.append(v4)

    return variants


async def get_coordinates(session, semaphore, address):
    """住所から緯度経度を取得"""
    base_url = 'https://msearch.gsi.go.jp/address-search/AddressSearch?q='
    encoded = urllib.parse.quote(address)

    async with semaphore:
        try:
            async with session.get(base_url + encoded, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                data = await resp.json()
                if data:
                    coords = data[0]['geometry']['coordinates']
                    return {
                        'found': True,
                        'matched_address': data[0]['properties']['title'],
                        'query_address': address,
                        'lat': str(coords[1]),
                        'lon': str(coords[0])
                    }
        except:
            pass
    return {'found': False}


async def try_address_variants(session, semaphore, original_address):
    """様々な変換を試してマッチする住所を探す"""

    # 1. 浜松市の区名変換
    if '浜松市' in str(original_address):
        converted = convert_hamamatsu_address(original_address)
        if converted:
            result = await get_coordinates(session, semaphore, converted)
            if result['found']:
                return result

    # 2. 京都の通り名正規化
    if '京都' in str(original_address):
        for variant in normalize_kyoto_address(original_address):
            result = await get_coordinates(session, semaphore, variant)
            if result['found']:
                return result

    # 3. 一般的な正規化
    for variant in normalize_general_address(original_address):
        result = await get_coordinates(session, semaphore, variant)
        if result['found']:
            return result

    return {'found': False}


async def main(output_dir: Path, total_parts: int):
    semaphore = asyncio.Semaphore(100)
    connector = aiohttp.TCPConnector(limit=100)

    total_fixed = 0
    total_errors = 0

    async with aiohttp.ClientSession(connector=connector) as session:
        for part in range(1, total_parts + 1):
            file = output_dir / f'result_*_part{part:03d}.csv'
            files = list(output_dir.glob(f'result_*_part{part:03d}.csv'))
            if not files:
                continue
            file = files[0]

            df = pd.read_csv(file, encoding='utf-8-sig', dtype=str)

            # 新しい列を追加（なければ）
            if '近しい住所' not in df.columns:
                df['近しい住所'] = ''
            if '近しい住所の緯度' not in df.columns:
                df['近しい住所の緯度'] = ''
            if '近しい住所の経度' not in df.columns:
                df['近しい住所の経度'] = ''

            # 「住所が見つかりません」で、まだ「近しい住所」が未取得のもの
            mask = (df['エラー'] == '住所が見つかりません') & (df['近しい住所'].fillna('') == '')
            error_indices = df[mask].index.tolist()
            total_errors += len(error_indices)

            if not error_indices:
                df.to_csv(file, index=False, encoding='utf-8-sig')
                continue

            print(f'パート{part}: {len(error_indices)}件を処理中...')

            # 非同期で処理
            tasks = [try_address_variants(session, semaphore, df.loc[i, '住所']) for i in error_indices]
            results = await tqdm_asyncio.gather(*tasks, desc=f'Part{part}')

            fixed = 0
            for idx, res in zip(error_indices, results):
                if res['found']:
                    df.loc[idx, '近しい住所'] = res['query_address']
                    df.loc[idx, '近しい住所の緯度'] = res['lat']
                    df.loc[idx, '近しい住所の経度'] = res['lon']
                    fixed += 1

            print(f'  → {fixed}/{len(error_indices)}件で近しい住所を取得')
            total_fixed += fixed

            df.to_csv(file, index=False, encoding='utf-8-sig')

    print(f'\n=== 完了 ===')
    print(f'対象エラー総数: {total_errors}件')
    print(f'近しい住所取得成功: {total_fixed}件')


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("使い方: python fix_notfound.py [データディレクトリ] [パート数]")
        print("例: python fix_notfound.py ./output 58")
        sys.exit(1)

    output_dir = Path(sys.argv[1])
    total_parts = int(sys.argv[2]) if len(sys.argv) > 2 else 58

    asyncio.run(main(output_dir, total_parts))
