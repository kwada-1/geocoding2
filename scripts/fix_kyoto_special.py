"""
京都の特殊な通り名表記を処理するスクリプト

例: 「五辻通千本東入西五辻東町」→「西五辻東町」だけで検索

使い方:
    python fix_kyoto_special.py [データディレクトリ] [パート数]

例:
    python fix_kyoto_special.py ./output 58
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


def extract_kyoto_town(address: str) -> list:
    """京都の住所から町名を抽出"""
    if not address or '京都' not in address:
        return []

    variants = []
    addr = str(address)

    # パターン1: 「○○町」を抽出（通り名の後）
    # 例: 「上京区五辻通千本東入西五辻東町47番地」→「上京区西五辻東町」
    match = re.search(r'(京都府京都市[^区]+区).*?([^通入上下東西ル]+町)', addr)
    if match:
        # 町名だけ
        town = match.group(2)
        # 「入」「ル」などの文字で始まる場合はスキップ
        if not town.startswith(('入', 'ル', '上', '下', '東', '西')):
            variants.append(f"{match.group(1)}{town}")

    # パターン2: 数字より前の町名を探す
    match2 = re.search(r'(京都府京都市[^区]+区).*?([\u4e00-\u9fff]+町)\d', addr)
    if match2:
        town = match2.group(2)
        # 通り名の一部（通、入、上、下、東、西、ル）を除去
        town_clean = re.sub(r'^.*?(入|ル)', '', town)
        if town_clean and len(town_clean) > 1:
            v = f"{match2.group(1)}{town_clean}"
            if v not in variants:
                variants.append(v)

    # パターン3: 町名の直前の漢字を含めて検索
    # 例: 「天使突抜3丁目」→「下京区天使突抜」
    match3 = re.search(r'(京都府京都市[^区]+区).*([\u4e00-\u9fff]{2,})\d+丁目', addr)
    if match3:
        v = f"{match3.group(1)}{match3.group(2)}"
        if v not in variants:
            variants.append(v)

    # パターン4: 区名だけで検索（最終手段）
    match4 = re.search(r'(京都府京都市[^区]+区)', addr)
    if match4:
        v = match4.group(1)
        if v not in variants:
            variants.append(v)

    # パターン5: 「町」で終わる部分を全て試す
    for m in re.finditer(r'([\u4e00-\u9fff]{2,}町)', addr):
        town = m.group(1)
        # 通り名の一部を除外
        if '通' not in town and len(town) >= 3:
            base = re.search(r'(京都府京都市[^区]+区)', addr)
            if base:
                v = f"{base.group(1)}{town}"
                if v not in variants:
                    variants.append(v)

    return variants


def normalize_other_addresses(address: str) -> list:
    """その他の住所正規化"""
    if not address:
        return []

    variants = []
    addr = str(address)

    # 「ノ」→「の」
    v1 = addr.replace('ノ', 'の')
    if v1 != addr:
        variants.append(v1)

    # 「の」を削除
    v2 = re.sub(r'番地の(\d+)', r'-\1', addr)
    if v2 != addr and v2 not in variants:
        variants.append(v2)

    # 全角数字を半角に
    v3 = addr.translate(str.maketrans('０１２３４５６７８９', '0123456789'))
    if v3 != addr and v3 not in variants:
        variants.append(v3)

    # 「番」「号」を「-」に
    v4 = re.sub(r'(\d+)番(\d+)号?', r'\1-\2', addr)
    if v4 != addr and v4 not in variants:
        variants.append(v4)

    # 区なしの浜松市（古い住所）→中央区を追加
    if '浜松市' in addr and '区' not in addr:
        v5 = addr.replace('浜松市', '浜松市中央区')
        if v5 not in variants:
            variants.append(v5)

    # 「字」の後ろの名前だけで検索
    match = re.search(r'(.*?[市区町村])字?([\u4e00-\u9fff]+)\d', addr)
    if match:
        v6 = f"{match.group(1)}{match.group(2)}"
        if v6 not in variants:
            variants.append(v6)

    # 番地以前だけ
    v7 = re.sub(r'\d+番地.*$', '', addr)
    if v7 != addr and len(v7) > 10 and v7 not in variants:
        variants.append(v7)

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
                        'query_address': address,
                        'lat': str(coords[1]),
                        'lon': str(coords[0])
                    }
        except:
            pass
    return {'found': False}


async def try_kyoto_variants(session, semaphore, original_address):
    """京都の住所変換を試す"""

    # 1. 京都の町名抽出
    for variant in extract_kyoto_town(original_address):
        result = await get_coordinates(session, semaphore, variant)
        if result['found']:
            return result

    # 2. その他の正規化
    for variant in normalize_other_addresses(original_address):
        result = await get_coordinates(session, semaphore, variant)
        if result['found']:
            return result

    # 3. 組み合わせ
    for v1 in extract_kyoto_town(original_address):
        for v2 in normalize_other_addresses(v1):
            result = await get_coordinates(session, semaphore, v2)
            if result['found']:
                return result

    return {'found': False}


async def main(output_dir: Path, total_parts: int):
    semaphore = asyncio.Semaphore(100)
    connector = aiohttp.TCPConnector(limit=100)

    total_fixed = 0
    total_remain = 0

    async with aiohttp.ClientSession(connector=connector) as session:
        for part in range(1, total_parts + 1):
            files = list(output_dir.glob(f'result_*_part{part:03d}.csv'))
            if not files:
                continue
            file = files[0]
            df = pd.read_csv(file, encoding='utf-8-sig', dtype=str)

            # 「住所が見つかりません」で、まだ「近しい住所」が未取得のもの
            mask = (df['エラー'] == '住所が見つかりません') & (df['近しい住所'].fillna('') == '')
            error_indices = df[mask].index.tolist()

            if not error_indices:
                continue

            print(f'パート{part}: {len(error_indices)}件を処理中...')

            tasks = [try_kyoto_variants(session, semaphore, df.loc[i, '住所']) for i in error_indices]
            results = await tqdm_asyncio.gather(*tasks, desc=f'Part{part}')

            fixed = 0
            for idx, res in zip(error_indices, results):
                if res['found']:
                    df.loc[idx, '近しい住所'] = res['query_address']
                    df.loc[idx, '近しい住所の緯度'] = res['lat']
                    df.loc[idx, '近しい住所の経度'] = res['lon']
                    fixed += 1

            remain = len(error_indices) - fixed
            print(f'  → {fixed}/{len(error_indices)}件で近しい住所を取得（残り{remain}件）')
            total_fixed += fixed
            total_remain += remain

            df.to_csv(file, index=False, encoding='utf-8-sig')

    print(f'\n=== 完了 ===')
    print(f'今回取得成功: {total_fixed}件')
    print(f'最終残り: {total_remain}件')


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("使い方: python fix_kyoto_special.py [データディレクトリ] [パート数]")
        print("例: python fix_kyoto_special.py ./output 58")
        sys.exit(1)

    output_dir = Path(sys.argv[1])
    total_parts = int(sys.argv[2]) if len(sys.argv) > 2 else 58
    asyncio.run(main(output_dir, total_parts))
