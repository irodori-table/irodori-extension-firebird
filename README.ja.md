<!-- i18n: language-switcher -->
[English](README.md) | [日本語](README.ja.md)

# Firebirdコネクタ

Firebird用のネイティブIrodoriテーブルコネクタ拡張。

このクレートは、コネクタのメタデータ、ネイティブABIのエクスポート、およびIrodori拡張マーケットプレイスで使用されるドライバ実装をパッケージ化しています。

## コネクタ

- 拡張ID: `irodori.firebird`
- エンジンID: `firebird`
- ワイヤープロトコル: `jdbc`
- デフォルトポート: `3050`
- ネイティブABI: `irodori.connector.native.v1`
- ドライバリンク済み: `yes`
- マーケットプレイスの表示: `public`
- パッケージバージョン: `0.1.3`

このパッケージはコネクタのメタデータとネイティブドライバを直接使用します。デスクトップアダプタのソーススナップショットは必要ありません。

コネクタのメタデータは `connector.config.json` と `irodori.extension.json` にあります。
Rustクレートは `src/lib.rs` からネイティブABIをエクスポートし、`irodori-connector-abi` を共有JSON/バッファヘルパーとして使用し、コネクタの動作は `src/driver.rs` に保持しています。

## 接続メタデータ

- エンドポイントモード: `hostPort`, `connectionString`
- トランスポートモード: `direct`, `sshTunnel`, `socks5Proxy`, `httpConnectProxy`, `proxyChain`
- TLS対応: `yes`
- デフォルトでTLS必須: `no`
- カスタムドライバオプション: `yes`

### エンドポイントフィールド

| フィールド | ラベル | 型 | 必須 |
| --- | --- | --- | --- |
| `host` | ホスト | `string` | yes |
| `port` | ポート | `number` | no |
| `database` | データベース | `string` | no |

## 認証

コネクタはこれらの認証モードを公開しており、クライアントは適切な資格情報フィールドをレンダリングできます。必要に応じて、ドライバ固有またはプロバイダ固有の値は `options` を通じて渡すことも可能です。

| 認証方法 | ラベル | 種類 | シークレットの用途 |
| --- | --- | --- | --- |
| `none` | 認証なし | `none` | なし |
| `connectionString` | 接続文字列 / DSN | `connectionString` | なし |
| `srp` | SRPユーザー/パスワード | `userPassword` | `password` |
| `kerberos` | Kerberos / GSSAPI | `kerberos` | `token` |
| `pluginToken` | プラグイントークン | `token` | `token` |
| `customDriverOptions` | カスタムドライバオプション | `custom` | `password`, `token`, `privateKey`, `privateKeyPassphrase` |

## ネイティブABI呼び出し

| メソッド | 応答 |
| --- | --- |
| `health` | コネクタのヘルス状態、エンジンID、ABIバージョン、ドライバの状態を返します。 |
| `describe` | 埋め込みマニフェストとコネクタ設定を返します。 |
| `manifest` | 生の `irodori.extension.json` を返します。 |
| `config` | 生の `connector.config.json` を返します。 |
| `connect` | ネイティブコネクタ接続を開き、検証します。 |
| `query` | コネクタクエリを実行し、構造化された行またはJSON結果を返します。 |
| `metadata` | スキーマ、テーブル、カラム、インデックス、コレクション、または同等のメタデータを読み取ります。 |
| `close` | キャッシュされたネイティブ接続を閉じて削除します。 |

## 開発

このチェックアウト内のすべての拡張クレートは `../target` を共有しており、依存関係は一度だけコンパイルされます。

```sh
make check
make build
```

リリースパッケージはプラットフォーム固有のネイティブアーティファクトを `dist/native` に配置します。

## ライセンス

0BSD。ほぼすべての目的でこのプロジェクトを使用、コピー、修正、配布できます。