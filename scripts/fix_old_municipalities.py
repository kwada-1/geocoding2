"""
旧市町村名を新市町村名に変換してジオコーディングをリトライするスクリプト

対象:
- 浜松市旧区名（2024年再編: 中区→中央区 等）  ※fix_notfoundで漏れた分
- 奥州市の「区」表記（水沢区→水沢 等）
- 高知市の文字化け（高＿ね→高そね）
- 平成の大合併で消滅した旧町名
- その他旧市町村名

使い方:
    python fix_old_municipalities.py [データディレクトリ] [パート数] [同時接続数]

例:
    python fix_old_municipalities.py . 57 100
"""
import asyncio
import re
import sys
import urllib.parse
from pathlib import Path

import aiohttp
import pandas as pd
from tqdm.asyncio import tqdm_asyncio

sys.stdout.reconfigure(encoding='utf-8')

# 旧市町村名 → 新市町村名 マッピング
OLD_TO_NEW = {
    # 浜松市2024年再編（fix_notfoundで漏れた分の追加対策）
    '浜松市中区': '浜松市中央区',
    '浜松市東区': '浜松市中央区',
    '浜松市西区': '浜松市中央区',
    '浜松市南区': '浜松市中央区',
    '浜松市北区': '浜松市浜名区',
    '浜松市浜北区': '浜松市浜名区',
    # 奥州市
    '奥州市水沢区': '奥州市水沢',
    '奥州市江刺区': '奥州市江刺',
    '奥州市前沢区': '奥州市前沢',
    '奥州市胆沢区': '奥州市胆沢',
    '奥州市衣川区': '奥州市衣川',
    # 愛知県
    '愛知郡長久手町': '長久手市',
    '西春日井郡新川町': '清須市',
    '海部郡七宝町': 'あま市',
    '海部郡美和町': 'あま市',
    '海部郡甚目寺町': 'あま市',
    '西春日井郡春日町': '清須市',
    '西春日井郡西枇杷島町': '清須市',
    # 長崎県
    '北松浦郡江迎町': '佐世保市江迎町',
    '北松浦郡鹿町町': '佐世保市鹿町町',
    '西彼杵郡三和町': '長崎市三和町',
    '西彼杵郡野母崎町': '長崎市野母崎町',
    # 宮城県
    '黒川郡富谷町': '富谷市',
    # 埼玉県
    '北足立郡伊奈町': '北足立郡伊奈町',  # そのまま（確認用）
    # 北海道
    '釧路市阿寒町': '釧路市阿寒町',
    '釧路市音別町': '釧路市音別町',
    # 福島県
    '安達郡本宮町': '本宮市',
    # 茨城県
    '稲敷郡茎崎町': 'つくば市茎崎',
    # 栃木県
    '上都賀郡粟野町': '鹿沼市粟野',
    '河内郡河内町': '宇都宮市河内町',
    # 千葉県
    '山武郡成東町': '山武市成東',
    # 新潟県
    '北蒲原郡豊浦町': '新発田市豊浦町',
    '北蒲原郡紫雲寺町': '新発田市紫雲寺',
    # 山梨県
    '東八代郡石和町': '笛吹市石和町',
    # 長野県
    '更級郡大岡村': '長野市大岡',
    # 奈良県
    '北葛城郡新庄町': '葛城市新庄',
    # 岡山県
    '御津郡御津町': '岡山市北区御津',
    # 香川県
    '仲多度郡仲南町': 'まんのう町',
    # 高知県 文字化け対応
    # '高＿ね' は実際のデータで確認してから対応
    # 熊本県
    '下益城郡富合町': '熊本市南区富合町',
    '下益城郡城南町': '熊本市南区城南町',
    # 鹿児島県
    '揖宿郡頴娃町': '南九州市頴娃町',
}


def convert_address(address: str) -> list:
    """旧市町村名を新市町村名に変換した候補リストを返す"""
    if not address:
        return []

    addr = str(address)
    candidates = []

    # 1. OLD_TO_NEW マッピングで変換
    for old, new in OLD_TO_NEW.items():
        if old in addr:
            converted = addr.replace(old, new)
            if converted != addr:
                candidates.append(converted)

    # 2. 高知市の文字化け対応: ＿ → そ
    if '＿' in addr:
        candidates.append(addr.replace('＿', 'そ'))

    # 3. 番地以降を削除してリトライ
    v = re.sub(r'\d+番地.*$', '', addr)
    if v != addr and len(v) > 10 and v not in candidates:
        candidates.append(v)

    # 4. 丁目以降を削除してリトライ
    v2 = re.sub(r'\d+丁目.*$', '', addr)
    if v2 != addr and len(v2) > 10 and v2 not in candidates:
        candidates.append(v2)

    # 5. 「字」以降を削除してリトライ
    v3 = re.sub(r'字.+$', '', addr)
    if v3 != addr and len(v3) > 8 and v3 not in candidates:
        candidates.append(v3)

    # 6. 都道府県+市区町村だけにしてリトライ
    m = re.match(r'((?:北海道|東京都|(?:京都|大阪)府|.{2,3}県).+?[市区町村])', addr)
    if m:
        short = m.group(1)
        if short != addr and short not in candidates:
            candidates.append(short)

    return candidates


async def get_coordinates(session, semaphore, address):
    """住所から緯度経度を取得"""
    base_url = 'https://msearch.gsi.go.jp/address-search/AddressSearch?q='
    encoded = urllib.parse.quote(address)

    async with semaphore:
        try:
            async with session.get(
                base_url + encoded,
                timeout=aiohttp.ClientTimeout(total=30)
            ) as resp:
                content_type = resp.headers.get('Content-Type', '')
                if 'json' not in content_type:
                    return {'found': False}
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
    """変換候補を順に試す"""
    candidates = convert_address(original_address)
    for candidate in candidates:
        result = await get_coordinates(session, semaphore, candidate)
        if result['found']:
            return result
    return {'found': False}


async def main(output_dir: Path, total_parts: int, max_concurrent: int):
    semaphore = asyncio.Semaphore(max_concurrent)
    connector = aiohttp.TCPConnector(limit=max_concurrent, limit_per_host=max_concurrent)

    total_targets = 0
    total_fixed = 0

    async with aiohttp.ClientSession(connector=connector) as session:
        for part in range(1, total_parts + 1):
            files = list(output_dir.glob(f'result_*_part{part:03d}.csv'))
            if not files:
                continue
            file = files[0]

            df = pd.read_csv(file, encoding='utf-8-sig', dtype=str)

            # 近しい住所列がなければ追加
            for col in ['近しい住所', '近しい住所の緯度', '近しい住所の経度']:
                if col not in df.columns:
                    df[col] = ''

            # 「住所が見つかりません」で近しい住所が未取得のもの
            mask = (
                (df['エラー'].fillna('') == '住所が見つかりません') &
                (df['近しい住所'].fillna('') == '')
            )
            error_indices = df[mask].index.tolist()

            if not error_indices:
                continue

            total_targets += len(error_indices)
            print(f'パート{part}: {len(error_indices)}件を処理中...')

            addr_col = '住所'
            if addr_col not in df.columns:
                addr_col = df.columns[1]

            tasks = [
                try_address_variants(session, semaphore, df.loc[i, addr_col])
                for i in error_indices
            ]
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
    print(f'対象総数: {total_targets}件')
    print(f'取得成功: {total_fixed}件')
    if total_targets > 0:
        print(f'成功率: {total_fixed/total_targets*100:.1f}%')


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("使い方: python fix_old_municipalities.py [データディレクトリ] [パート数] [同時接続数]")
        print("例: python fix_old_municipalities.py . 57 100")
        sys.exit(1)

    output_dir = Path(sys.argv[1])
    total_parts = int(sys.argv[2]) if len(sys.argv) > 2 else 57
    max_concurrent = int(sys.argv[3]) if len(sys.argv) > 3 else 100

    asyncio.run(main(output_dir, total_parts, max_concurrent))
