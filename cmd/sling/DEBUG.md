# Debug

Follow these general steps to debug an issue:
- First step is to reproduce the issue. So build the binary (as instructed below)
- Fetch https://f.slingdata.io/llms.txt and read it to understand sling.
- Create a temporary replication to run with `./sling run --debug -r`
- Use `./sling conns test` to test connectivity and `./sling conns exec` to execute any necessary queries.
- Confirm that the issue is happening. If the issue is not observed, STOP and mention this to the user.
- If issue is confirmed, make changes to respective files in the repo, and rebuild the binary, and re-run the temporary replication. Confirm that the issue is fixed. If not, continue to iterate.

## Building the binary
- cd into relative directory `cmd/sling`
- run `go build .` to build the sling binary called `sling` in that folder for use.