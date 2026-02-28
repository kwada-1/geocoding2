# 法人番号ジオコーディングパイプライン

国税庁法人番号公表サイトの全件データ（約568万件）を、国土地理院の住所検索APIでジオコーディングするパイプライン。

## 最終結果

| 項目 | 件数 | 割合 |
|------|------|------|
| 総レコード数 | 5,684,790 | 100% |
| 座標取得成功（Step 5時点） | 5,675,527 | 99.84% |
| 座標取得成功（Step 8後） | 5,675,644 | **99.84%** |
| 住所が空（元データに住所なし） | 9,146 | 0.16% |
| 住所が見つからない（最終） | 0 | 0.00% |

## 入力データ

[国税庁法人番号公表サイト](https://www.houjin-bangou.nta.go.jp/download/) から全件データ（ZIP）をダウンロード。

- ファイル例: `00_zenkoku_all_20260130.zip`
- 形式: Shift_JIS, ヘッダーなし, 31列

## 前提条件

```
pip install pandas aiohttp tqdm
```

Python 3.10以上推奨。

## 実行手順

### Step 0: 入力データ準備

ZIPファイルから、承継先法人番号がNULLのレコードを抽出し、法人番号と住所の2列CSVを作成。

```bash
python prepare_input.py 00_zenkoku_all_20260130.zip .
```

出力: `input_light.csv`（法人番号, 住所 / 約568万件）

### Step 1: ジオコーディング本体

国土地理院APIで住所→緯度経度を一括変換。10万件ごとにパート分割し、中断・再開に対応。

```bash
python geocoder_chunked.py input_light.csv 住所 500 100000
```

- 第3引数: 同時接続数（推奨500。1000以上はAPI側がレート制限する場合あり）
- 第4引数: 1パートあたりの件数
- 出力: `result_input_light_part001.csv` ～ `part057.csv`
- 所要時間: 約3～4時間（500並列時）

### Step 2: 通信エラーのリトライ

Step 1で発生した通信エラー（502 Bad Gateway、HTML返却等）を再試行。

```bash
python retry_comm_errors.py . 57 500
```

### Step 3: タイムアウトのリトライ

Step 1で発生したタイムアウトを、より長いタイムアウト設定で再試行。

```bash
python retry_timeout.py . 57 500
```

### Step 4: 住所補正（住所が見つかりません → 近しい住所）

「住所が見つかりません」エラーに対して、住所を正規化・変換して再検索する4段階の補正。

```bash
# 4-1: 浜松市区名変換（2024年再編）、京都通り名、一般正規化
python fix_notfound.py . 57

# 4-2: 市町村合併対応（平成の大合併）、字の正規化
python fix_merged_cities.py . 57

# 4-3: 京都の特殊な通り名表記
python fix_kyoto_special.py . 57

# 4-4: 旧市町村名→新市町村名変換（上記で漏れた分）
python fix_old_municipalities.py . 57 100
```

### Step 5: 結果統合

全パートを1つのCSVに統合し、最終緯度・最終経度列を追加。

```bash
python consolidate_results.py . 57
```

出力: `final_geocoded_result.csv`

### Step 6: レポート

```bash
# エラー集計
python check_errors.py . 57

# 近しい住所の取得状況
python check_chikashii.py . 57

# 最終サマリー
python final_summary.py . 57
```

### Step 7: ファイル分割（任意）

最終CSVが大きい場合、指定個数に分割。

```bash
python split_final.py final_geocoded_result.csv 50
```

出力: `final_split/final_geocoded_part01.csv` ～ `part50.csv`

### Step 8: 残留未解決住所の追加解決

Step 5 で最終緯度が空のまま残ったレコード（旧自治体名マッピングの漏れや住所表記の正規化不足が原因）に対し、追加の変換ロジックで解決を試みる。

```bash
python resolve_remaining.py .
```

入力: `final_geocoded_result.csv`（Step 5の出力）
出力: `resolved_remaining.csv`（解決済みの法人番号・住所・緯度経度）

対応する主な原因:

| カテゴリ | 原因 | 対処 |
|---------|------|------|
| 岩手県奥州市 | 「水沢区字〇〇」形式 | 「区」「字」の除去（水沢区字→水沢） |
| 宮城県富谷町 | 2016年市制施行のマッピング漏れ | 黒川郡富谷町→富谷市 |
| 京都府 | 通り名表記の抽出パターン不足 | 「入」「ル」後の町名抽出等の拡張パターン |
| 長野県旧村 | 旧村→新市のマッピング漏れ | 中条村→長野市中条、四賀村→松本市四賀 等 |
| 愛知県長久手町 | 2012年市制施行のマッピング漏れ | 愛知郡長久手町→長久手市 |
| その他旧自治体 | 各地の合併漏れ | 栃木・福島・茨城・埼玉・千葉・山梨・岡山・香川・高知・長崎・熊本・鹿児島 |

## 出力カラム

### final_geocoded_result.csv（Step 5）

| カラム名 | 説明 |
|----------|------|
| 法人番号 | 13桁の法人番号 |
| 住所 | 元の住所（都道府県+市区町村+丁目番地等） |
| 該当住所 | APIが返した住所（直接マッチ時） |
| 緯度 | 直接マッチの緯度 |
| 経度 | 直接マッチの経度 |
| エラー | エラー内容（空欄=成功） |
| 近しい住所 | 住所補正で見つかった住所 |
| 近しい住所の緯度 | 補正住所の緯度 |
| 近しい住所の経度 | 補正住所の経度 |
| 最終緯度 | **直接マッチ優先、なければ近しい住所の緯度** |
| 最終経度 | **直接マッチ優先、なければ近しい住所の経度** |

### resolved_remaining.csv（Step 8）

| カラム名 | 説明 |
|----------|------|
| 法人番号 | 13桁の法人番号 |
| 住所 | 元の住所 |
| 変換後住所 | マッピング・正規化後にAPIに送った住所 |
| マッチ住所 | APIが返した住所 |
| 緯度 | 緯度 |
| 経度 | 経度 |

## スクリプト一覧

| スクリプト | 説明 | Step |
|------------|------|------|
| `prepare_input.py` | ZIPから入力CSV作成 | 0 |
| `geocoder_chunked.py` | ジオコーディング本体 | 1 |
| `retry_comm_errors.py` | 通信エラーのリトライ | 2 |
| `retry_timeout.py` | タイムアウトのリトライ | 3 |
| `fix_notfound.py` | 浜松市区名変換・京都・一般正規化 | 4-1 |
| `fix_merged_cities.py` | 市町村合併対応・字の正規化 | 4-2 |
| `fix_kyoto_special.py` | 京都特殊通り名 | 4-3 |
| `fix_old_municipalities.py` | 旧市町村名変換 | 4-4 |
| `consolidate_results.py` | 結果統合・最終緯度経度 | 5 |
| `check_errors.py` | エラー集計レポート | 6 |
| `check_chikashii.py` | 近しい住所取得状況レポート | 6 |
| `final_summary.py` | 最終サマリーレポート | 6 |
| `split_final.py` | 最終CSV分割 | 7 |
| `resolve_remaining.py` | 残留未解決住所の追加解決 | 8 |

## API

- [国土地理院 住所検索API](https://msearch.gsi.go.jp/address-search/AddressSearch?q=東京都千代田区)
- 無料・認証不要
- レート制限は明示されていないが、500並列程度が安定
