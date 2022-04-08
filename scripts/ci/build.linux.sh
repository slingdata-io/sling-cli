set -e  # exit on error

export GO_BIN_FOLDER=$HOME/go/bin
export TMPDIR=~/tmp/
export PATH=$GO_BIN_FOLDER:$PATH
mkdir -p $TMPDIR

echo "Building sling-linux"
go mod edit -dropreplace='github.com/flarco/g' go.mod
go mod edit -dropreplace='github.com/flarco/dbio' go.mod
go mod tidy
GOOS=linux GOARCH=amd64 go build -o sling-linux cmd/sling/*.go

VERSION=$(./sling-linux --version | sed 's/Version: //')
echo $VERSION
mkdir -p dist/$VERSION
cp sling-linux dist
cp sling-linux dist/$VERSION

echo $VERSION > dist/version-linux

cd dist/$VERSION
tar -czvf sling.linux-amd64.tar.gz sling-linux