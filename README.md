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


## 多層ストレージと学習バッチ（拡張Providerが必要）

MiniTopolのProviderが外部補助・内部補助の管理を担います。
Readerはこれまでと同じ単一ファイルの接続クライアントです。
多層バッチは初期状態で無効です。

~~~bash
python reader.py storage-status
python reader.py storage-configure --external /media/external --enable --cache-percent 10
python reader.py storage-configure --cache-percent 40 --cache-slack-percent 15
python reader.py storage-configure --cache-limit-gib 2 --cache-slack-gib 0.5
python reader.py storage-configure --write-policy longevity --prefetch-batches 2
python reader.py storage-ingest TRAJECTORY
python reader.py storage-offload trajectories/TRAJECTORY
python reader.py storage-prune --all
python reader.py storage-ensure trajectories/TRAJECTORY
python reader.py batches-plan batches.json
python reader.py batches-status PLAN_ID
~~~

Python側からは `configure_storage()`、`storage_status()`、`ingest_sources()`、
`offload()`、`ensure_storage()`、`prune_storage()`、
`plan_batches()`、`batch_status()`、`cancel_batches()` を利用します。

~~~python
from reader import Reader

client = Reader()
client.configure_storage(enabled=True, write_policy="longevity", prefetch_batches=2)
batches = [
    client.batch_request("trajectory-A", data=["coords", "all"], frames=[3, 0]),
    client.batch_request("trajectory-B", data=["coords"], frames=[4, 1]),
]
plan = client.plan_batches(batches)
with client.request("trajectory-A", data=["coords", "all"], frames=[3, 0],
                    plan_id=plan["plan_id"], plan_index=0) as lease:
    # バッファを利用し、参照を解放してから終了します。
    pass
~~~

学習側はバッチの供給順序を指定し、Providerは先行準備を行います。
Providerで設定した先行バッチ数を越えて内部補助へ転送しません。

容量設定は内部補助の一時キャッシュに適用されます。初期の割合指定は起動時の
空き容量の10%ですが、割合は10%を超えて指定できます。容量の絶対値は
`cache_limit_bytes`（CLIは`--cache-limit-gib`）で指定します。
軟制限の追加許容量は`cache_slack_percent`と`cache_slack_bytes`で設定でき、
超過が必要なときも外部直接読み出しに切り替えて保存可能容量を守ります。
