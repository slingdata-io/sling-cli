set -e  # exit on error

rm -rf /tmp/sling
cp -r . /tmp/sling

cd /tmp/sling
rm -rf .git

GOOS=linux GOARCH=amd64 go build -o sling cmd/sling/*.go
mv -f sling python/sling/bin/sling-linux

rm -rf /tmp/sling
