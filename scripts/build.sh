set -e  # exit on error

rm -rf /gutil
cp -r ../gutil /gutil

rm -rf /sling
cp -r . /sling
cd /sling
rm -rf .git

# get pkger
go get github.com/markbates/pkger/cmd/pkger

# sling
pkger -include github.com/slingdata-io/sling/core/database:/core/database/templates -o cmd/sling
GOOS=linux GOARCH=amd64 go build -o sling cmd/sling/*.go
rm -f cmd/sling/pkged.go
/bin/cp -f sling /__/bin/sling
/bin/cp -f sling /usr/local/bin/sling

rm -rf /sling
