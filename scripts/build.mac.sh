set -e  # exit on error

rm -rf /tmp/sling
cp -r . /tmp/sling

cd /tmp/sling
rm -rf .git

GOOS=darwin GOARCH=amd64 go build -o sling cmd/sling/*.go
mv -f sling python/sling/bin/sling-mac

rm -rf /tmp/sling
