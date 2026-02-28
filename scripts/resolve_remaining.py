"""
未解決住所の追加ジオコーディング解決スクリプト

Step 5（consolidate_results.py）の実行後に残った「最終緯度が空」のレコードに対し、
追加の旧自治体名マッピングと住所正規化を適用してジオコーディングを試みる。

主な対応:
  - 奥州市: 「水沢区字〇〇」→「水沢〇〇」（区・字の除去）
  - 富谷町: 2016年市制施行（黒川郡富谷町→富谷市）
  - 京都府: 通り名表記の拡張パターンで町名抽出
  - 長野県旧村: 中条村→長野市中条、四賀村→松本市四賀 等
  - 長久手町: 2012年市制施行（愛知郡長久手町→長久手市）
  - その他旧自治体: 各地の合併漏れ（栃木・福島・茨城・埼玉・千葉・山梨・岡山・香川・高知・長崎・熊本・鹿児島）

使い方:
    python resolve_remaining.py [データディレクトリ]

例:
    python resolve_remaining.py .

入力: final_geocoded_result.csv（Step 5の出力）
出力: resolved_remaining.csv（解決済みの結果）
"""
import asyncio
import re
import ssl
import sys
import urllib.parse
from pathlib import Path

import aiohttp
import pandas as pd

sys.stdout.reconfigure(encoding='utf-8')

# ============================================================
# 追加の旧自治体名マッピング
# ============================================================
ADDITIONAL_MUNICIPALITY_MAP = {
    # 宮城県
    '宮城県黒川郡富谷町': '宮城県富谷市',
    # 福島県
    '福島県伊達郡伊達町': '福島県伊達市',
    # 茨城県
    '茨城県那珂郡那珂町': '茨城県那珂市',
    # 栃木県
    '栃木県下都賀郡岩舟町': '栃木県栃木市岩舟町',
    '栃木県下都賀郡藤岡町': '栃木県栃木市藤岡町',
    # 埼玉県
    '埼玉県南埼玉郡菖蒲町': '埼玉県久喜市菖蒲町',
    # 千葉県
    '千葉県山武郡大網白里町': '千葉県大網白里市',
    # 山梨県
    '山梨県南巨摩郡増穂町': '山梨県南巨摩郡富士川町',
    # 長野県
    '長野県東筑摩郡四賀村': '長野県松本市四賀',
    '長野県上水内郡中条村': '長野県長野市中条',
    '長野県上水内郡豊野町': '長野県長野市豊野町',
    '長野県北佐久郡北御牧村': '長野県東御市',
    # 愛知県
    '愛知県愛知郡長久手町': '愛知県長久手市',
    # 岡山県
    '岡山県浅口郡金光町': '岡山県浅口市金光町',
    '岡山県浅口郡鴨方町': '岡山県浅口市鴨方町',
    # 香川県
    '香川県木田郡庵治町': '香川県高松市庵治町',
    # 高知県
    '高知県吾川郡春野町': '高知県高知市春野町',
    '高知県高岡郡東津野村': '高知県高岡郡津野町',
    # 長崎県
    '長崎県北松浦郡福島町': '長崎県松浦市福島町',
    # 熊本県
    '熊本県八代郡鏡町': '熊本県八代市鏡町',
    # 鹿児島県
    '鹿児島県姶良郡牧園町': '鹿児島県霧島市牧園町',
    # 北海道
    '北海道上川郡風連町': '北海道名寄市風連町',
    # 静岡県（浜松市2024年再編の追加）
    '静岡県浜松市中区': '静岡県浜松市中央区',
}

# 奥州市の区→区なし変換
OSHU_MAP = {
    '奥州市水沢区': '奥州市水沢',
    '奥州市江刺区': '奥州市江刺',
    '奥州市前沢区': '奥州市前沢',
    '奥州市胆沢区': '奥州市胆沢',
    '奥州市衣川区': '奥州市衣川',
}


# ============================================================
# GSI API呼び出し
# ============================================================
async def get_coordinates(session, semaphore, address):
    """住所から緯度経度を取得（GSI API）"""
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
                    return None
                data = await resp.json()
                if data:
                    coords = data[0]['geometry']['coordinates']
                    lat = float(coords[1])
                    lon = float(coords[0])
                    if 24 <= lat <= 46 and 122 <= lon <= 146:
                        return {
                            'matched_address': data[0]['properties']['title'],
                            'query_address': address,
                            'lat': lat,
                            'lon': lon,
                        }
        except Exception:
            pass
    return None


# ============================================================
# 住所正規化（段階的簡略化）
# ============================================================
def generate_general_variants(address: str) -> list:
    """変換後の住所を段階的に簡略化した候補リストを生成"""
    if not address:
        return []

    addr = str(address)
    variants = [addr]

    # 「番地の〇」→「-〇」、「番戸」→「番地」
    v = re.sub(r'(\d+)番地の(\d+)', r'\1-\2', addr)
    v = re.sub(r'番戸', '番地', v)
    if v != addr:
        variants.append(v)

    # 「字」を除去
    v = re.sub(r'字(?=[^\d])', '', addr)
    if v != addr and v not in variants:
        variants.append(v)

    # 「大字」を除去
    v = re.sub(r'大字', '', addr)
    if v != addr and v not in variants:
        variants.append(v)

    # 「大字」と「字」を両方除去
    v = re.sub(r'大字', '', addr)
    v = re.sub(r'字(?=[^\d])', '', v)
    if v != addr and v not in variants:
        variants.append(v)

    # 番地以降を除去
    v = re.sub(r'\d+番地.*$', '', addr)
    if v != addr and len(v) > 8 and v not in variants:
        variants.append(v)

    # 番地以降除去 + 字除去
    v2 = re.sub(r'字(?=[^\d])', '', v)
    if v2 != v and v2 not in variants:
        variants.append(v2)

    # 丁目以降を除去
    v = re.sub(r'\d+丁目.*$', '', addr)
    if v != addr and len(v) > 8 and v not in variants:
        variants.append(v)

    # 都道府県＋市区町村のみ
    m = re.match(
        r'((?:北海道|東京都|(?:京都|大阪)府|.{2,3}県).+?[市区町村郡](?:[^市区町村]*?[市区町村])?)',
        addr
    )
    if m:
        short = m.group(1)
        if short != addr and short not in variants:
            variants.append(short)

    return variants


# ============================================================
# 奥州市特殊処理
# ============================================================
def generate_oshu_variants(address: str) -> list:
    """奥州市の特殊住所変換候補を生成"""
    if '奥州市' not in address:
        return []

    addr = str(address)
    variants = []

    for old, new in OSHU_MAP.items():
        if old in addr:
            converted = addr.replace(old, new)
            variants.append(converted)

            # 「字」を除去
            v2 = re.sub(r'字(?=[^\d])', '', converted)
            if v2 != converted and v2 not in variants:
                variants.append(v2)

            # 番地情報を除去
            v3 = re.sub(r'\d+番地.*$', '', converted)
            if v3 != converted and len(v3) > 8 and v3 not in variants:
                variants.append(v3)

            # 字除去 + 番地除去
            v4 = re.sub(r'\d+番地.*$', '', v2)
            if v4 != v2 and len(v4) > 8 and v4 not in variants:
                variants.append(v4)

            # 数字以降を除去
            v5 = re.sub(r'\d+.*$', '', converted)
            if v5 != converted and len(v5) > 8 and v5 not in variants:
                variants.append(v5)

            # 字除去 + 数字以降を除去
            v6 = re.sub(r'\d+.*$', '', v2)
            if v6 != v2 and len(v6) > 8 and v6 not in variants:
                variants.append(v6)

            # 「区」部分のみ（最終手段）
            if new not in variants:
                variants.append(f'岩手県{new}')

            break

    return variants


# ============================================================
# 京都特殊処理
# ============================================================
def generate_kyoto_variants(address: str) -> list:
    """京都の通り名から町名を抽出して試行"""
    if '京都' not in address:
        return []

    addr = str(address)
    variants = []

    # 「入」の後の町名を抽出
    m = re.search(r'(京都府京都市[^区]+区).*?入([\u4e00-\u9fff]+町)', addr)
    if m:
        variants.append(f"{m.group(1)}{m.group(2)}")

    # 「ル」の後の町名を抽出
    m = re.search(r'(京都府京都市[^区]+区).*?[ルる]([\u4e00-\u9fff]+町)', addr)
    if m:
        v = f"{m.group(1)}{m.group(2)}"
        if v not in variants:
            variants.append(v)

    # 最後の「○○町」を抽出
    m = re.search(r'(京都府京都市[^区]+区)', addr)
    if m:
        base = m.group(1)
        for town in re.findall(r'([\u4e00-\u9fff]{2,}町)', addr):
            if '通' not in town:
                v = f"{base}{town}"
                if v not in variants:
                    variants.append(v)

    # 番地前の町名を抽出
    m = re.search(r'(京都府京都市[^区]+区).*?([\u4e00-\u9fff]+)\d+番地', addr)
    if m:
        v = f"{m.group(1)}{m.group(2)}"
        if v not in variants:
            variants.append(v)

    # 数字前の漢字文字列を町名として抽出
    m = re.search(r'(京都府京都市[^区]+区).*?([\u4e00-\u9fff]{2,})\d', addr)
    if m:
        town = m.group(2)
        if not any(kw in town for kw in ['通', '入', '上ル', '下ル', '東入', '西入']):
            v = f"{m.group(1)}{town}"
            if v not in variants:
                variants.append(v)

    # 区名だけ（最終手段）
    m = re.search(r'(京都府京都市[^区]+区)', addr)
    if m:
        v = m.group(1)
        if v not in variants:
            variants.append(v)

    return variants


# ============================================================
# 特殊正規化
# ============================================================
def generate_special_normalization_variants(address: str) -> list:
    """特殊な住所パターンの正規化"""
    addr = str(address)
    variants = []

    # 全角数字→半角
    v = addr.translate(str.maketrans('０１２３４５６７８９', '0123456789'))
    if v != addr:
        variants.append(v)

    # ハイフン統一
    v = re.sub(r'[−ー‐―]', '-', addr)
    if v != addr and v not in variants:
        variants.append(v)

    # 「ノ」→「の」
    v = addr.replace('ノ', 'の')
    if v != addr and v not in variants:
        variants.append(v)

    # 「番」「号」→「-」
    v = re.sub(r'(\d+)番(\d+)号?', r'\1-\2', addr)
    if v != addr and v not in variants:
        variants.append(v)

    # 「村字」→削除
    v = re.sub(r'村字', '', addr)
    if v != addr and v not in variants:
        variants.append(v)

    # 「区字」→「区」
    v = re.sub(r'区字', '区', addr)
    if v != addr and v not in variants:
        variants.append(v)

    return variants


# ============================================================
# 旧自治体名の変換
# ============================================================
def apply_municipality_mapping(address: str) -> str:
    """旧自治体名マッピングを適用して新住所を返す"""
    if not address:
        return address
    for old, new in ADDITIONAL_MUNICIPALITY_MAP.items():
        if old in address:
            return address.replace(old, new)
    return address


# ============================================================
# 全パターンを統合して試行
# ============================================================
async def resolve_address(session, semaphore, original_address):
    """全ての変換パターンを順に試し、最初にヒットした座標を返す"""
    if not original_address or pd.isna(original_address):
        return None

    addr = str(original_address)
    tried = set()

    async def try_variant(variant):
        if variant in tried or not variant or len(variant) < 5:
            return None
        tried.add(variant)
        return await get_coordinates(session, semaphore, variant)

    # Phase 1: 旧自治体名マッピング適用後の住所で段階的試行
    mapped = apply_municipality_mapping(addr)

    for variant in generate_general_variants(mapped):
        result = await try_variant(variant)
        if result:
            return result

    for variant in generate_special_normalization_variants(mapped):
        result = await try_variant(variant)
        if result:
            return result
        for v2 in generate_general_variants(variant):
            result = await try_variant(v2)
            if result:
                return result

    # Phase 2: 奥州市特殊処理
    if '奥州市' in addr:
        for variant in generate_oshu_variants(addr):
            result = await try_variant(variant)
            if result:
                return result
            for v2 in generate_general_variants(variant):
                result = await try_variant(v2)
                if result:
                    return result

    # Phase 3: 京都特殊処理
    if '京都' in addr:
        for variant in generate_kyoto_variants(addr):
            result = await try_variant(variant)
            if result:
                return result

    # Phase 4: 元の住所のまま段階的簡略化（マッピングなし）
    if mapped != addr:
        for variant in generate_general_variants(addr):
            result = await try_variant(variant)
            if result:
                return result

    # Phase 5: 特殊正規化 + 一般正規化の組み合わせ
    for v1 in generate_special_normalization_variants(addr):
        for v2 in generate_general_variants(v1):
            result = await try_variant(v2)
            if result:
                return result

    return None


# ============================================================
# メイン処理
# ============================================================
async def main(output_dir: Path):
    final_csv = output_dir / 'final_geocoded_result.csv'

    if not final_csv.exists():
        print(f'エラー: {final_csv} が見つかりません')
        print('先に consolidate_results.py (Step 5) を実行してください')
        sys.exit(1)

    print(f'読み込み中: {final_csv}')
    print('(大きなファイルのため時間がかかります...)')

    # final_geocoded_result.csv を読み込み、未解決レコードを抽出
    unresolved_records = []
    chunk_size = 500_000
    total_records = 0

    for chunk in pd.read_csv(final_csv, encoding='utf-8-sig', dtype=str, chunksize=chunk_size):
        total_records += len(chunk)
        mask = (chunk['最終緯度'].fillna('') == '') & (chunk['住所'].fillna('') != '')
        unresolved = chunk[mask][['法人番号', '住所']].copy()
        if len(unresolved) > 0:
            unresolved_records.append(unresolved)
        print(f'  {total_records:,}件読み込み済み...')

    if not unresolved_records:
        print('未解決レコードはありません。全件解決済みです。')
        return

    df_unresolved = pd.concat(unresolved_records, ignore_index=True)
    print(f'\n総レコード数: {total_records:,}')
    print(f'未解決レコード数: {len(df_unresolved)}件')
    print(f'------------------------------------')

    # カテゴリ別の内訳を表示
    categories = {
        '奥州市': df_unresolved['住所'].str.contains('奥州市', na=False).sum(),
        '富谷町': df_unresolved['住所'].str.contains('富谷町', na=False).sum(),
        '京都': df_unresolved['住所'].str.contains('京都', na=False).sum(),
        '長久手町': df_unresolved['住所'].str.contains('長久手町', na=False).sum(),
        '長野県旧村': df_unresolved['住所'].str.contains('四賀村|中条村|豊野町|北御牧村', na=False).sum(),
    }
    for cat, count in categories.items():
        if count > 0:
            print(f'  {cat}: {count}件')

    # ジオコーディング実行
    print(f'\nGSI APIでジオコーディング開始...')
    semaphore = asyncio.Semaphore(50)
    ssl_ctx = ssl.create_default_context()
    ssl_ctx.check_hostname = False
    ssl_ctx.verify_mode = ssl.CERT_NONE
    connector = aiohttp.TCPConnector(limit=50, limit_per_host=50, ssl=ssl_ctx)

    resolved = []
    still_unresolved = []

    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [resolve_address(session, semaphore, row['住所']) for _, row in df_unresolved.iterrows()]

        results = []
        batch_size = 20
        for i in range(0, len(tasks), batch_size):
            batch = tasks[i:i + batch_size]
            batch_results = await asyncio.gather(*batch)
            results.extend(batch_results)
            done = min(i + batch_size, len(tasks))
            print(f'  {done}/{len(tasks)}件処理済み...')

    # 結果の集計
    for idx, (_, row) in enumerate(df_unresolved.iterrows()):
        result = results[idx]
        if result:
            resolved.append({
                '法人番号': row['法人番号'],
                '住所': row['住所'],
                '変換後住所': result['query_address'],
                'マッチ住所': result['matched_address'],
                '緯度': result['lat'],
                '経度': result['lon'],
            })
        else:
            still_unresolved.append({
                '法人番号': row['法人番号'],
                '住所': row['住所'],
            })

    # 結果出力
    print(f'\n====================================')
    print(f'=== 結果 ===')
    print(f'====================================')
    print(f'未解決入力: {len(df_unresolved)}件')
    print(f'解決成功:   {len(resolved)}件')
    print(f'未解決残り: {len(still_unresolved)}件')
    if len(df_unresolved) > 0:
        print(f'解決率:     {len(resolved)/len(df_unresolved)*100:.1f}%')

    # 解決済みをCSV出力
    output_csv = output_dir / 'resolved_remaining.csv'
    if resolved:
        df_resolved = pd.DataFrame(resolved)
        df_resolved.to_csv(output_csv, index=False, encoding='utf-8-sig')
        print(f'\n解決済み結果を出力: {output_csv}')

        # 緯度経度の妥当性チェック
        lat_ok = df_resolved['緯度'].between(24, 46).all()
        lon_ok = df_resolved['経度'].between(122, 146).all()
        if lat_ok and lon_ok:
            print('  全座標が日本国内の妥当な範囲内です')
        else:
            print('  一部の座標が日本国外の値です。要確認:')
            bad = df_resolved[
                ~(df_resolved['緯度'].between(24, 46) & df_resolved['経度'].between(122, 146))
            ]
            for _, r in bad.iterrows():
                print(f'    {r["住所"]} -> ({r["緯度"]}, {r["経度"]})')
    else:
        print('\n解決できたレコードはありません。')

    # 未解決を表示
    if still_unresolved:
        print(f'\n--- 未解決の住所一覧 ({len(still_unresolved)}件) ---')
        for item in still_unresolved:
            print(f'  {item["法人番号"]}: {item["住所"]}')


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("使い方: python resolve_remaining.py [データディレクトリ]")
        print("例: python resolve_remaining.py .")
        print("")
        print("入力: final_geocoded_result.csv（Step 5の出力）")
        print("出力: resolved_remaining.csv（解決済みの結果）")
        sys.exit(1)

    output_dir = Path(sys.argv[1])
    asyncio.run(main(output_dir))
