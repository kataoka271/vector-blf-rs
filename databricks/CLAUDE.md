### Databricks Appsのデプロイ手順

`app.yaml` のあるフォルダを `local_source_code_path` として以下を実行する。`$AppName` は親フォルダの名前とするが毎回確認をすること。

```powershell
databricks apps get $AppName | ConvertFrom-Json | % {
    $DbxPath = $_.default_source_code_path;  # パスの取得
    databricks workspace import-dir --overwrite `local_source_code_path` $DbxPath;  # アップロード
    databricks apps deploy $AppName --source-code-path $DbxPath;  # デプロイ
}
```
