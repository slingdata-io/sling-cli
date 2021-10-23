set -e  # exit on error

rm -rf /tmp/sling
cp -r . /tmp/sling

cd /tmp/sling
rm -rf .git

mkdir python/sling/bin
GOOS=darwin GOARCH=amd64 go build -o sling-mac cmd/sling/*.go
mv -f sling-mac python/sling/bin/