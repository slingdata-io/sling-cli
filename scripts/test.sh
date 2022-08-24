set -e  # exit on error

# sling

export SLING_LOADED_AT_COLUMN=TRUE
cd cmd/sling
go test -run TestTasks

cd -

## test cli commands
cat cmd/sling/tests/files/test1.1.csv | /__/bin/sling run --tgt-conn POSTGRES --tgt-object public.my_table --mode full-refresh
cat cmd/sling/tests/files/test1.1.csv.gz | /__/bin/sling run --tgt-conn POSTGRES --tgt-object public.my_table --mode full-refresh
/__/bin/sling run --src-conn POSTGRES --src-stream public.my_table --stdout > /tmp/my_table.csv
/__/bin/sling run --src-conn POSTGRES --src-stream public.my_table --tgt-object file:///tmp/my_table.csv