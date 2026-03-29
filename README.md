# chowari-xday-forecast

`釣割` の船宿釣果ページから過去1年分の魚種別データを抽出し、`船宿別 / 魚種別` の `Xデー確率` と `予測下限・上限` を今後1年分で可視化する静的アプリです。

公開ページは船宿と魚種を切り替えて閲覧できます。現在の生成対象は `佐島海楽園 (00296)` と `池田丸 (00297)` です。最大・最小グラフには `前年同日` の実測点も重ねています。`Xデー` は `予測上限が最も高い日` として扱います。

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
  - `釣割` の月別釣果ページから過去1年分を抽出
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
- Open-Meteo archive / forecast / marine APIs
