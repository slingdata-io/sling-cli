set -e  # exit on error
# set -o allexport; source .env; set +o allexport

export DEBUG=''

cd connection
go test -v -run 'TestConnection'
cd -

cd iop
go test -timeout 5m -v -run 'TestParseDate|TestDetectDelimiter|TestFIX|TestConstraints|TestDuckDb|TestParquetDuckDb|TestIcebergReader|TestDeltaReader'
cd -

cd database
go test -v -run 'TestParseTableName|TestRegexMatch|TestParseColumnName'
# go test -v -run 'TestPostgres|TestMySQL|TestOracle|TestSnowflake|TestSqlServer|TestBigQuery|TestSQLite|TestClickhouse' -timeout 10m
# ALLOW_BULK_IMPORT=FALSE go test -run 'TestPostgres|TestMySQL|TestOracle|TestSqlServer|TestSQLite|TestClickhouse' -timeout 10m # test without bulk loading, using transaction batch
# go test -v -run TestLargeDataset
cd -

cd filesys
go test -v -run 'TestFileSysLocalCsv|TestFileSysLocalJson|TestFileSysLocalParquet|TestFileSysLocalFormat|TestFileSysGoogle|TestFileSysS3|TestFileSysAzure|TestFileSysSftp|TestFileSysFtp|TestExcel|TestFileSysLocalIceberg|TestFileSysLocalDelta'
cd -

# cd saas
# go test -v -run 'TestAirbyteGithub|TestAirbyteNotion'
# cd -