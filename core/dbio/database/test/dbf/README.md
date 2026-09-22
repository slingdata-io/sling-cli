# README
these `.dbf` files (with their `.dbt` / `.fpt` memo files) are the fixtures used by the
dBase / FoxPro connector tests (`database_dbase_test.go`). They are stored in the repository
so that the tests run without network access.

| file | origin |
| --- | --- |
| `TEST.DBF`, `TEST.FPT` | `examples/test_data/table` of [go-dbase](https://github.com/Valentin-Kaiser/go-dbase) (BSD 3-Clause, Copyright (c) 2022 Valentin Kaiser). Visual FoxPro table with memo, blob, varbinary and variable length fields, 3 records (1 deleted). |
| `expense categories.dbf` | `examples/test_data/database` of [go-dbase](https://github.com/Valentin-Kaiser/go-dbase) (BSD 3-Clause). dBase III table, 5 records. |
| `dbase_03.dbf` | `spec/fixtures` of [dbf](https://github.com/infused/dbf) (MIT, Copyright (c) 2006-2026 Keith Morrison). dBase III table with 31 columns, 14 records. It defines `Point_ID` twice (a character and a numeric), so the second one is read as `Point_ID1`. |
| `dbase_8b.dbf`, `dbase_8b.dbt` | `spec/fixtures` of [dbf](https://github.com/infused/dbf) (MIT). dBase IV table with a memo field, and its memo file. The `.dbt` variant is not supported, so only the columns are read. |
| `nullable.dbf` | written for these tests with the [go-dbase](https://github.com/Valentin-Kaiser/go-dbase) writer: a Visual FoxPro table with a `_NullFlags` field holding variable length (`varchar` / `varbinary`) and nullable fields, 3 records. |
| `test1k_dbase.dbf` | written for these tests with the [go-dbase](https://github.com/Valentin-Kaiser/go-dbase) writer: the `test1k_dbase` table the shared DB suite reads into postgres (`TestSuiteDatabaseDbase`), 1000 records with the columns of `tests/files/test1.csv`. |
