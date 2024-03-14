set -e  # exit on error

# sling

# export _DEBUG=LOW
# export _DEBUG_CALLER_LEVEL=2
export SLING_LOADED_AT_COLUMN=TRUE
cd cmd/sling
go test -parallel 4 -run TestSuite

cd -

## test cli commands
go build -o sling-linux cmd/sling/*.go && chmod +x sling-linux
./sling-linux --version

# should return menu, not error
./sling-linux run

cat cmd/sling/tests/files/test1.1.csv | ./sling-linux run --tgt-conn POSTGRES --tgt-object public.my_table --mode full-refresh
./sling-linux run --src-stream file://cmd/sling/tests/files/test1.1.csv --tgt-conn POSTGRES --tgt-object public.my_table --mode full-refresh
./sling-linux run --src-stream file://cmd/sling/tests/files/test1.1.csv --tgt-conn MSSQL --tgt-object dbo.my_table --mode full-refresh --tgt-options 'use_bulk: false'

cat cmd/sling/tests/files/test1.1.csv.gz | ./sling-linux run --tgt-conn POSTGRES --tgt-object public.my_table --mode full-refresh
./sling-linux run --src-stream 'file://cmd/sling/tests/files/test1.1.csv.gz' --tgt-conn MYSQL --tgt-object mysql.my_table --mode full-refresh --tgt-options 'use_bulk: false'

cat cmd/sling/tests/files/test3.json | ./sling-linux run --src-options "flatten: true" --tgt-conn POSTGRES --tgt-object public.my_table1 --tgt-options 'use_bulk: false' --mode full-refresh
./sling-linux run --src-stream 'file://cmd/sling/tests/files/test3.json'  --src-options "flatten: true" --tgt-conn POSTGRES --tgt-object public.my_table1 --tgt-options 'use_bulk: false' --mode full-refresh

# test various cli commands / flags
./sling-linux run --src-conn POSTGRES --src-stream public.my_table --stdout > /tmp/my_table.csv
./sling-linux run --src-conn POSTGRES --src-stream public.my_table --tgt-object file:///tmp/my_table.csv
./sling-linux run --src-conn POSTGRES --src-stream public.my_table --stdout --select 'id' -l 2
./sling-linux run --src-conn POSTGRES --src-stream public.my_table --stdout --select '-id' -l 2

./sling-linux conns test POSTGRES
./sling-linux conns exec POSTGRES 'select count(1) from public.my_table'

./sling-linux run -r cmd/sling/tests/replications/r.05.yaml
./sling-linux run -r cmd/sling/tests/replications/r.06.yaml
./sling-linux run -r cmd/sling/tests/replications/r.07.yaml
./sling-linux run -r cmd/sling/tests/replications/r.08.yaml
./sling-linux run -r cmd/sling/tests/replications/r.09.yaml

./sling-linux run --src-stream 'file://cmd/sling/tests/files/parquet' --stdout > /dev/null

echo '' | ./sling-linux run --stdout