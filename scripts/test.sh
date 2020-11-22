set -e  # exit on error

# sling
pkger -include github.com/flarco/dbio/database:/database/templates -o cmd/sling

cd cmd/sling
go test .
rm -f cmd/sling/pkged.go

cd -