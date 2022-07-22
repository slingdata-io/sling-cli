set -e  # exit on error

rm -rf /tmp/sling
cp -r . /tmp/sling

cd /tmp/sling
rm -rf .git


export VERSION=$1
echo "VERSION -> $VERSION"
GOOS=linux GOARCH=amd64 go build -ldflags="-X 'core.Version=$VERSION'" -o sling-linux cmd/sling/*.go

./sling-linux version
