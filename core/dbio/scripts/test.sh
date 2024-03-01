set -e  # exit on error
# set -o allexport; source .env; set +o allexport

export DEBUG=''

cd iop
go test -run 'TestParseDate|TestDetectDelimiter'
cd -

# cd database
# go test -run 'TestPostgres|TestMySQL|TestOracle|TestSnowflake|TestSqlServer|TestBigQuery|TestSQLite|TestClickhouse' -timeout 10m
# ALLOW_BULK_IMPORT=FALSE go test -run 'TestPostgres|TestMySQL|TestOracle|TestSqlServer|TestSQLite|TestClickhouse' -timeout 10m # test without bulk loading, using transaction batch
# go test -run TestLargeDataset
# cd -

cd filesys
go test -run 'TestFileSysLocalCsv|TestFileSysLocalJson|TestFileSysLocalParquet|TestFileSysLocalFormat|TestFileSysGoogle|TestFileSysS3|TestFileSysAzure|TestFileSysSftp|TestFileSysFtp|TestExcel'
cd -

# cd saas
# go test -run 'TestAirbyteGithub|TestAirbyteNotion'
# cd -