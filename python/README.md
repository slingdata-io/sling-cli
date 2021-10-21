<p align="center"><img src="logo-with-text.png" alt="drawing" width="250"/></p>

<p align="center">Slings from a data source to a data target.</p>

## Installation

`pip install sling`

Then you should be able to run `sling --help` from command line.

## Running a Extract-Load Task

### CLI

```shell
sling run --src-conn POSTGRES_URL --src-stream myschema.mytable \
  --tgt-conn SNOWFLAKE_URL --tgt-object yourschema.yourtable \
  --mode drop
```

Or passing a yaml/json string or file

```shell
sling run -c '
source:
  conn: $POSTGRES_URL
  stream: myschema.mytable

target:
  conn: $SNOWFLAKE_URL
  object: yourschema.yourtable
  mode: drop
'
# OR
sling run -c /path/to/config.json
```

### From Lib

```python
from sling import Sling

config = {
  'source': {
    'conn': 'POSTGRES_URL',
    'stream': "select * from my_table",
  },
  'target': {
    'conn':  "s3://my_bucket/my_folder/new_file.csv",
  },
}

Sling(**config).run()
```

## Config Schema

An example. Put this in https://jsonschema.net/home

`--src-conn`/`source.conn` and `--tgt-conn`/`target.conn`  can be a name or URL of a folder:
- `MY_PG` (connection ref in db, profile or env)
- `$MY_PG` (connection ref in env)
- `postgresql://user:password!@host.loc:5432/database`
- `s3://my_bucket/my_folder/file.csv`
- `gs://my_google_bucket/my_folder/file.json`
- `file:///tmp/my_folder/file.csv` (local storage)

`--src-stream`/`source.stream` can be an object name to stream from:
- `TABLE1`
- `SCHEMA1.TABLE2`
- `OBJECT_NAME`
- `select * from SCHEMA1.TABLE3`
- `/path/to/file.sql` (if source conn is DB)

`--tgt-object`/`target.object` can be an object name to write to:
- `TABLE1`
- `SCHEMA1.TABLE2`

### Example as JSON

```json
{
  "source": {
    "conn": "MY_PG_URL",
    "stream": "select * from my_table",
    "options": {}
  },
  "target": {
    "conn": "s3://my_bucket/my_folder/new_file.csv",
    "options": {
      "header": false
    }
  }
}
```

## Release

- Update version in [setup.py](./setup.py).

```
git clone https://github.com/flarco/sling
cd sling/python
pip install -e .
python setup.py sdist && twine upload --verbose --skip-existing dist/*
```