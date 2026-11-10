# MiniTopol Reader

MiniTopol Providerの共有メモリを参照する、単一ファイル・標準ライブラリ中心のReaderです。MiniTopol本体のPythonパッケージはReader側では不要です。

## 準備

MiniTopol側で一度だけProviderを登録します。

~~~bash
python -m minitopol.provider register --workspace /path/to/BaseTraj_Workspace
~~~

Readerは任意の学習・解析ディレクトリに reader.py を置くだけで利用できます。Python 3.11以上を想定しています。ProviderとReaderは同じPC・OSユーザーで実行します。

## CLI（短縮説明）

~~~text
python reader.py start|status|list|stop
python reader.py get 軌跡ID --data coords,sasa,rmsd --frames 0,5,8
python reader.py get 軌跡ID --data coords,all --start 0 --stop 100
python reader.py release リースID
~~~

データは共有メモリに置かれ、getは領域の名前・型・形状を返します。使用後は必ずreleaseします。 `stop`はProviderの停止処理と登録セッションの解除を待ってから戻るため、直後に`start`できます。稼働中のleaseがある場合は停止を拒否します。起動済みProviderには自動接続し、未起動なら登録済みのMiniTopol環境から起動します。

## Pythonからの取得

~~~python
from reader import Reader

client = Reader()
with client.request("trajectory-id", data=["coords", "sasa"], frames=[2, 5, 8]) as lease:
    coords = lease.numpy("coordinates")   # numpyが利用できる場合、コピーなし
    sasa = lease.arrow("sasa")             # pyarrowが利用できる場合
    print(coords.shape, sasa.num_rows)
    del coords, sasa                     # メモリ参照を解放してからleaseを閉じる
~~~

Reader自身はnumpy・pyarrowに依存しません。ndarrayとArrow論理表の復元ヘルパーを利用する場合だけ、それぞれのパッケージが必要です。取得したバッファは明示的なlease終了までProviderが保護します。

座標には time（ps）とbox（Å）が付属します。原子指定は座標のみを絞ります。frameを持たない記述子は軌跡全体の集計値を返し、contactsの頻度行も保持します。記述子が未計算なら要求は失敗します。

環境変数 MINITOPOL_PROVIDER_REGISTRY で登録ファイルの場所を明示できます。ローカル接続にはランダムな認証トークンを使用します。別PCからの接続は対象外です。

Providerの保存形式・復元アルゴリズムはMiniTopol側の責務です。Readerは保存形式を解釈しません。共有領域は原則としてゼロコピーで参照しますが、Provider内部での復元や要求に合わせた整列ではコピーが発生し得ます。

パフォーマンス目標の50億AF/分は検証前であり、達成を保証するものではありません。

For a known next batch of descriptor frames, pass `next_frames=[...]` to
`Reader.request()`. Use this hint with an updated MiniTopol Provider.
