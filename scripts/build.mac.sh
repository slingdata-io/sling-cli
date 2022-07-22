set -e  # exit on error

rm -rf /tmp/sling
cp -r . /tmp/sling

cd /tmp/sling
rm -rf .git

GOOS=darwin GOARCH=amd64 go build -ldflags="-X 'core.Version=$VERSION'" -o sling-mac cmd/sling/*.go

/bin/cp -f sling-macc /tmp/