set -e  # exit on error

rm -rf /tmp/sling
cp -r . /tmp/sling

cd /tmp/sling
rm -rf .git

echo "GIT_TAG=`echo $(git describe --tags --abbrev=0)`" >> $VERSION
echo "VERSION -> $VERSION"
GOOS=linux GOARCH=amd64 go build -ldflags="-X 'core.Version=$VERSION'" -o sling-linux cmd/sling/*.go

/bin/cp -f sling-linux /tmp/
