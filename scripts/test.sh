set -e  # exit on error

# sling

# export _DEBUG=LOW
# export _DEBUG_CALLER_LEVEL=2
export SLING_LOADED_AT_COLUMN=TRUE
cd cmd/sling
go test -run TestTasks

cd -

## test cli commands
go build -o sling-linux cmd/sling/*.go && chmod +x sling-linux
./sling-linux --version

cat cmd/sling/tests/files/test1.1.csv | ./sling-linux run --tgt-conn POSTGRES --tgt-object public.my_table --mode full-refresh
cat cmd/sling/tests/files/test1.1.csv.gz | ./sling-linux run --tgt-conn POSTGRES --tgt-object public.my_table --mode full-refresh
./sling-linux run --src-conn POSTGRES --src-stream public.my_table --stdout > /tmp/my_table.csv
./sling-linux run --src-conn POSTGRES --src-stream public.my_table --tgt-object file:///tmp/my_table.csv
./sling-linux run -r cmd/sling/tests/replications/r.05.yaml
./sling-linux run -r cmd/sling/tests/replications/r.06.yaml
./sling-linux run --src-stream 'file://cmd/sling/tests/files/parquet' --stdout > /dev/null
echo '' | ./sling-linux run --stdout