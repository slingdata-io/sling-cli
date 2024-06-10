set -e  # exit on error
shopt -s expand_aliases

# sling

cd cmd/sling
go test -run 'TestMsUUID'

cd -

## test cli commands
go build -o sling-linux cmd/sling/*.go && chmod +x sling-linux
./sling-linux --version
alias sling="./sling-linux"

# should return menu, not error
sling run

# MSSQL server
docker compose -f cmd/sling/mssql.docker-compose.yaml up -d
while ! docker exec sqlserver /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P Simplepassword1 -Q "SELECT 1" > /dev/null 2>&1; do
    sleep 1
done
export MSSQL='sqlserver://sa:Simplepassword1@localhost:1433/?encrypt=true&TrustServerCertificate=true'

cat cmd/sling/tests/files/test1.1.csv | sling run --tgt-conn MSSQL --tgt-object dbo.my_table --mode full-refresh
sling run --src-stream file://cmd/sling/tests/files/test1.1.csv --tgt-conn MSSQL --tgt-object dbo.my_table --mode full-refresh --tgt-options 'use_bulk: false'

docker exec sqlserver /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P Simplepassword1 -Q "IF OBJECT_ID('dbo.uuid_test', 'U') IS NULL CREATE TABLE dbo.uuid_test (uuid uniqueidentifier not null);"
docker exec sqlserver /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P Simplepassword1 -Q "TRUNCATE TABLE dbo.uuid_test;"
docker exec sqlserver /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P Simplepassword1 -Q "INSERT INTO dbo.uuid_test (uuid) VALUES ('12345678-1234-1234-1234-123456789abc'), ('abcdef12-3456-7890-abcd-ef1234567890');"

# run sling to stdout and make sure the uuid is parsed correctly
sling run --src-conn MSSQL --src-stream dbo.uuid_test --stdout --src-options '{ transforms: {uuid : ["parse_ms_uuid"] } }' | grep -q "12345678-1234-1234-1234-123456789abc" && echo "parse_ms_uuid Test passed" || (echo "Test failed" && exit 1)
sling run --src-conn MSSQL --src-stream dbo.uuid_test --stdout --src-options '{ transforms: {uuid : ["parse_ms_uuid", "decode_latin1"] } }' | grep -q "12345678-1234-1234-1234-123456789abc" && echo "multiple transforms Test passed" || (echo "Test failed" && exit 1)
# make sure the default parse_uuid is used when no transforms are specified
sling run --src-conn MSSQL --src-stream dbo.uuid_test --stdout | grep -q "78563412-3412-3412-1234-123456789abc" && echo "Default transforms Test passed" || (echo "Test failed" && exit 1)
sling run --src-conn MSSQL --src-stream dbo.uuid_test --stdout --src-options '{ transforms: {uuid : ["decode_latin1"] } }' | grep -q "78563412-3412-3412-1234-123456789abc" && echo "parse_uuid as default Test passed" || (echo "Test failed" && exit 1)

# stop mssql
docker compose -f cmd/sling/mssql.docker-compose.yaml down

# remove sling-linux
rm sling-linux