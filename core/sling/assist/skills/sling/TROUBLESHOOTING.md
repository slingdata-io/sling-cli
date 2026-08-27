# Troubleshooting

Common issues and solutions when using Sling.

## Connection Issues

### Connection Refused
```
Error: failed to connect to database
```

**Solutions:**
1. Check host/port accessibility: `telnet host port`
2. Verify firewall rules allow traffic
3. Confirm database is running
4. Check for VPN requirements

### Authentication Failed
```
Error: authentication failed for user
```

**Solutions:**
1. Verify username and password
2. Check user has required permissions
3. Review database authentication logs
4. Confirm SSL/TLS settings match

### SSL/TLS Errors
```
Error: SSL certificate problem
```

**Solutions:**
1. For testing: add `sslmode=disable` (not for production)
2. Verify certificate validity
3. Check SSL configuration matches server
4. Ensure CA certificates are available

### Test Connection
```bash
# Via CLI
sling conns test MY_CONN --debug

# Via MCP
{"action": "test", "input": {"connection": "MY_CONN", "debug": true}}
```

## Replication Issues

### Table Not Found
```
Error: table "schema.table" does not exist
```

**Solutions:**
1. Discover available tables:
   ```bash
   sling conns discover MY_CONN --pattern "schema.*"
   ```
2. Check case sensitivity (some DBs are case-sensitive)
3. Verify user has SELECT permission

### Type Conversion Errors
```
Error: cannot convert value to target type
```

**Solutions:**
1. Add explicit type casting:
   ```yaml
   columns:
     problematic_col: string
   transforms:
     - problematic_col: 'cast(value, "integer")'
   ```
2. Handle nulls:
   ```yaml
   transforms:
     - amount: 'coalesce(value, 0)'
   ```
3. Clean data:
   ```yaml
   transforms:
     - value: 'replace(value, "[^0-9]", "")'
   ```

### Primary Key Violations
```
Error: duplicate key value violates unique constraint
```

**Solutions:**
1. Verify primary key is correct
2. Check for duplicate source data
3. Use `mode: full-refresh` to replace data
4. Deduplicate in source query

### Memory Issues
```
Error: out of memory processing large dataset
```

**Solutions:**
1. Reduce batch size:
   ```yaml
   env:
     SLING_BATCH_SIZE: 5000
   ```
2. Enable chunking:
   ```yaml
   source_options:
     chunk_size: 6h
   ```
3. Use file splitting:
   ```yaml
   target_options:
     file_max_rows: 100000
   ```
4. Reduce parallelism:
   ```yaml
   env:
     SLING_THREADS: 2
   ```

## Performance Issues

### Slow Performance

**Database sources:**
1. Increase threads:
   ```yaml
   env:
     SLING_THREADS: 10
   ```
2. Enable bulk loading:
   ```yaml
   target_options:
     use_bulk: true
   ```
3. Use incremental mode instead of full-refresh

**File sources:**
1. Use parallel file processing:
   ```yaml
   env:
     SLING_THREADS: 20
   ```
2. Compress output files:
   ```yaml
   target_options:
     compression: snappy
   ```

### High Memory Usage

1. Stream large tables with chunking
2. Reduce batch sizes
3. Process fewer concurrent streams

## Debug Options

### Enable Debug Logging
```bash
sling run -r replication.yaml --debug
```

### Enable Trace Logging
```bash
sling run -r replication.yaml --trace
```

### Validate Configuration
```bash
# Parse only (no execution)
sling run -r replication.yaml --parse

# Or via MCP
{"action": "validate", "input": {"file_path": "replication.yaml"}}
```

## API Spec Issues

### Rate Limiting
```
Error: 429 Too Many Requests
```

**Solutions:**
1. Reduce request rate:
   ```yaml
   defaults:
     request:
       rate: 1  # Requests per second
   ```
2. Configure retry:
   ```yaml
   response:
     rules:
       - action: retry
         condition: response.status == 429
         max_attempts: 5
         backoff: exponential
   ```

### Authentication Errors
```
Error: 401 Unauthorized
```

**Solutions:**
1. Verify API credentials
2. Check token expiration
3. Confirm required scopes
4. Test with curl first

### Pagination Loops
API keeps returning same data.

**Solutions:**
1. Check pagination stop_condition
2. Verify cursor/offset updates correctly
3. Debug with limit:
   ```yaml
   response:
     records:
       limit: 100  # Stop after 100 records
   ```

## File Issues

### File Not Found
```
Error: file not found
```

**Solutions:**
1. Check file path exists
2. Verify connection configuration
3. Confirm user has read access
4. Check for typos in path

### Encoding Errors
```
Error: invalid character encoding
```

**Solutions:**
1. Specify encoding:
   ```yaml
   source_options:
     encoding: utf8  # or latin1, windows1252
   ```
2. Clean source data

### Format Detection Failed

**Solutions:**
1. Explicitly set format:
   ```yaml
   source_options:
     format: csv
     header: true
     delimiter: ','
   ```

## Error Recovery

### Resume Failed Replications
Sling tracks progress automatically. Re-run to resume.

### Retry Configuration
```yaml
env:
  SLING_RETRIES: 3
  SLING_RETRY_DELAY: 60s
```

### Run Specific Streams
```bash
sling run -r replication.yaml --streams failed_table
```

## Getting Help

1. **Debug output**: Run with `--debug` or `--trace`
2. **Parse first**: Validate config before running
3. **Test connections**: `sling conns test NAME --debug`
4. **Failed run / conns test**: run `sling assist --id <id>` (id from the footer). After you classify a sling defect (not user config), offer: "Report this? GitHub issue (public, needs account) or email to support." Never auto-send. There is no MCP report tool. Run `sling assist report --id <id>` so the user reviews the redacted draft, then `--github` or `--email`.
5. **Check logs**: Review database/API logs

### Support Channels
- **Docs**: https://docs.slingdata.io
- **Discord**: https://discord.gg/q5xtaSNDvp
- **GitHub Issues**: https://github.com/slingdata-io/sling-cli/issues
- **Email**: support@slingdata.io
