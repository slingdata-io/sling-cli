# sling


## Config Schema

An example. Put this in https://jsonschema.net/home

`source.conn` / `target.conn` can be a name or URL of a folder:
- `MY_PG` (connection ref in db, profile or env)
- `$MY_PG` (connection ref in env)
- `postgresql://user:password!@host.loc:5432/database`
- `s3://my_bucket`
- `file:///tmp/my_folder` (local storage)

`source.stream` can be:
- `TABLE1`
- `SCHEMA1.TABLE2`
- `OBJECT_NAME`
- `select * from SCHEMA1.TABLE3`
- `/path/to/file.sql` (if `source.conn` is DB)
- `/path/to/file1.csv`
- `s3://my_bucket/my_folder/file.csv`

`target.object` can be:
- `TABLE1`
- `SCHEMA1.TABLE2`
- `file1.csv`



```json
{
  "source": {
      "conn": "MY_PG",
      "stream": "select * from my_table",
      "limit": 0,
      "options": {}
  },
  "target": {
      "conn": "s3://my_bucket",
      "object": "s3://my_bucket/my_folder/new_file.csv",
      "mode": "drop",
      "primary_key": ["col1", "col2"],
      "update_key": "col3",
      "dbt": {
          "version": "0.18",
          "repo_url": "...",
          "models": "my_model"
      },
      "options": {}
  },
  "options": {
    "stdout": false,
    "stdin": false
  }
}
```