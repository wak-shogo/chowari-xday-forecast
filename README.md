# chowari-xday-forecast

`釣割` と船宿固有サイトの釣果ページから過去1年分の魚種別データを抽出し、`船宿別 / 魚種別` の `Xデー確率` と `予測下限・上限` を今後1年分で可視化する静的アプリです。

公開ページは船宿と魚種を切り替えて閲覧できます。現在の生成対象は `佐島海楽園 (00296)`、`池田丸 (00297)`、`鴨居一郎丸`、`萬栄丸` に加え、金沢八景周辺の `荒川屋 (00007)`、`野毛屋釣船店 (00834)`、`一之瀬丸 (00307)`、`弁天屋 (00300)`、`金沢八景 黒川丸 (00150)`、`米元釣船店 (00836)`、`忠彦丸 (00703)`、`青田丸 (00689)`、`村本海事 (01580)` です。最大・最小グラフには `前年同日` の実測点も重ねています。`Xデー` は `予測上限が最も高い日` として扱います。

## 使い方

```bash
git clone https://github.com/wak-shogo/chowari-xday-forecast.git
cd chowari-xday-forecast
python3 scripts/generate_data.py
python3 -m http.server 8000
```

`http://localhost:8000` を開くと表示できます。

## 再生成

既定の船宿で再生成:

```bash
python3 scripts/generate_data.py
```

対象日を固定:

```bash
python3 scripts/generate_data.py --today 2026-03-29
```

船宿IDを指定して再生成:

```bash
python3 scripts/generate_data.py --ship 00296
```

固有サイト船宿を指定して再生成:

```bash
python3 scripts/generate_data.py --ship ichiroumaru
```

```bash
python3 scripts/generate_data.py --ship maneimaru
```

複数船宿をまとめて生成:

```bash
python3 scripts/generate_data.py --ship 00296 --ship 00001
```

## データ構成

- `data/catalog.json`
  - 船宿一覧、魚種一覧、各静的JSONへのパス
- `data/payloads/*.json`
  - 個別の `船宿 × 魚種` 予測データ

## 抽出とモデル

- 釣果データ
  - `釣割` の月別釣果ページ、または船宿固有サイトの一覧・詳細ページから過去1年分を抽出
  - 同日の複数釣行は魚種別に日次集約
- 特徴量
  - 気温
  - 水温
  - 月齢
  - 月齢は `sin / cos` の周期特徴量へ変換
  - 交互作用項と2乗項を追加
- 学習
  - 過去1年のうち対象魚種の記録がある日だけをランダム分割して内部評価
  - 公開用の最終係数は全データで再学習
- 将来予測
  - 直近は `Open-Meteo` 予報
  - それ以降は港座標ベースの平年値で補完
  - `予測上限` の回帰残差を使ったモンテカルロで `Xデー確率` を日別算出

## データ元

- 釣割: `https://www.chowari.jp/`
- 鴨居一郎丸: `https://www.ichiroumaru.jp/`
- 萬栄丸: `https://www.maneimaru.jp/`
- Open-Meteo archive / forecast / marine APIs
