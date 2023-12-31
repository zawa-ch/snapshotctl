# スナップショット管理ルール

環境変数`BACKUP_KEEP_RULES`に代入した内容によってスナップショットの自動的な管理を行えるようになっている。
このドキュメントはスナップショット管理ルールの挙動、およびスナップショット管理ルールの表記の方法について記述する。

## フォーマット

ルールセットの記述はJSONによって行う。
JSONとして不適格な文字列が代入された状態で`initialize`コマンドが発行されると、`BACKUP_KEEP_RULES`を読み取ったjqがエラーを発して終了する。

各ルールは後述するフォーマットに従ったJSONオブジェクトであり、ルールの名前をキーに持ったJSONオブジェクトをルートとする。

```json
{
    "rule1" : { /* snip */ },
    "rule2" : { /* snip */ },
    /* ... */
    "rulen" : { /* snip */ }
}
```

## ルール

各ルールは常にJSONオブジェクトである。
それ以外の型が記述されている場合の動作は未定義である。

ルールオブジェクト内には次の4つの要素を持つ。
- `"store_type"`
- `"bind_duration"`
- `"keep_entries"`
- `"keep_duration"`

これらのルール要素はすべて論理積として結合される。
つまり、ルールに合致するスナップショットとして選択されるのは指定された条件をすべて満たすものである。

### store_type

ルールに合致するスナップショットの保存形式を指定する。

許容される値は次のとおりである。
オブジェクト中に存在しない場合、`null`として扱われる。

- `"plain"`: 無圧縮でスナップショットを保管する。
- `"zstd"`: Z Standard圧縮でスナップショットを保管する。
- `"gzip"`: GZip圧縮でスナップショットを保管する。
- `"xz"`: XZ圧縮でスナップショットを保管する。
- `null`: `"plain"`と同様に振る舞う。

これらに合致しない値を格納した場合の動作は未定義である。

### bind_duration

ルールに合致するスナップショットの時間的な間隔を秒単位で指定する。

このルールが指定されている場合、ルールに合致している最新のスナップショットから指定された時間が経過していないスナップショットは除外される。

許容する値は正の数値、もしくは`null`である。
オブジェクト中に存在しない場合、`null`として扱われる。

許容されない値を格納した場合の動作は未定義である。

### keep_entries

ルールに合致するスナップショットの最大数を指定する。

このルールが指定されている場合、ルールに合致するスナップショット内で最大数を超えないように古いものから除外される。

許容する値は正の整数、もしくは`null`である。
オブジェクト中に存在しない場合、`null`として扱われる。

許容されない値を格納した場合の動作は未定義である。

### keep_duration

ルールに合致するスナップショットの時間的な範囲を秒単位で指定する。

このルールが指定されている場合、ルールに合致している最新のスナップショットから指定された時間以前の古いスナップショットは除外される。

許容する値は正の数値、もしくは`null`である。
オブジェクト中に存在しない場合、`null`として扱われる。

許容されない値を格納した場合の動作は未定義である。

## ルールと振る舞い

データベースはルールセットに記述されたルールに合致するスナップショットのリストを持っており、いずれのルールにも合致しないスナップショットは削除される。

唯一の例外は、ルールがひとつも存在しないルールセットを指定した場合である。
この場合、ルールセットによる管理を無効化してすべてのスナップショットをplainモードで保管することになる。

複数のルールに合致するスナップショットは次のように扱われる。

1. plainモードで保管するルールに合致していた場合はplainモードで保管される。
2. 合致したすべてのルールがzstd, gzip, xzのいずれかのモードであった場合、`bind_duration`が最も大きなルールのモードを採用する。

合致するルール中に`bind_duration`が最も大きいルールが複数存在した場合、どのモードが選択されるかは未定義である。
