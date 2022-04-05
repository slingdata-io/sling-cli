set -e  # exit on error

# sling

cd cmd/sling
go test -run TestTasks

cd -