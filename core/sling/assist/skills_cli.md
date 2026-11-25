## Without the Sling MCP server

If you do not have the Sling MCP tools, use these commands for the MCP actions in the skills.

| MCP action | CLI command |
|---|---|
| connection list | `sling conns list` |
| connection test | `sling conns test NAME --debug` (API: `--endpoints e1,e2`) |
| connection discover | `sling conns discover NAME -p 'schema.*'` (add `--columns` for columns) |
| connection set | `sling conns set NAME type=postgres host=... user=...` |
| database query | `sling conns exec NAME "select ..."` (read-only queries only) |
| database get_schemata / get_schemas / get_columns | `sling conns discover NAME -p 'schema.table' --columns` |
| file_system list | `sling conns discover NAME -p 'folder/*' --recursive` |
| file_system inspect | `sling conns discover NAME -p 'path/file.csv' --columns` |
| file_system copy | `sling run --src-conn A --src-stream 'path' --tgt-conn B --tgt-object 'path'` |
| api_spec validate | `sling validate SPEC.yaml` |
| api_spec test | `sling conns test NAME --endpoints e1 --debug` (to test a draft spec, first run `sling conns set NAME "spec=file:///abs/spec.yaml"`) |
| replication validate | `sling validate FILE.yaml` |
| pipeline validate | `sling validate FILE.yaml` |
| (run, no MCP action) | `sling run -r FILE` / `sling run -p FILE` / `sling build` |
