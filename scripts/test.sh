set -e  # exit on error

# sling

export SLING_LOADED_AT_COLUMN=TRUE
cd cmd/sling
go test -run TestTasks

cd -