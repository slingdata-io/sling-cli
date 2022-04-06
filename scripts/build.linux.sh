set -e  # exit on error

rm -rf /tmp/sling
cp -r . /tmp/sling

cd /tmp/sling
rm -rf .git

GOOS=linux GOARCH=amd64 go build -o sling-linux cmd/sling/*.go

/bin/cp -f sling-linux /tmp/
