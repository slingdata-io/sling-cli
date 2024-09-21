set -e  # exit on error
shopt -s expand_aliases

# sling

cd cmd/sling
go test -v -run 'TestReplicationDefaults'
SLING_PROCESS_BW=false go test -v -run 'TestSuiteDatabaseClickhouse' # gives issues when running in parallel
go test -v -parallel 3 -run 'TestSuiteFile'
SKIP_CLICKHOUSE=TRUE go test -v -parallel 4 -timeout 15m -run TestSuiteDatabase
cd -

cd core/sling
go test -v -run 'TestTransformMsUUID'
go test -v -run 'TestReplication'
go test -run 'TestCheck'
cd -

## test cli commands
SLING_BIN=./sling go test -v -run TestCLI