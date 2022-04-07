set -e  # exit on error

export GO_BIN_FOLDER=$HOME/go/bin
export TMPDIR=~/tmp/
export PATH=$GO_BIN_FOLDER:$PATH
mkdir -p $TMPDIR

echo "Building sling-mac"
go mod edit -dropreplace='github.com/flarco/g' go.mod
go mod edit -dropreplace='github.com/flarco/dbio' go.mod
go mod tidy
GOOS=darwin GOARCH=amd64 go build -o sling-mac cmd/sling/*.go

VERSION=$(./sling-mac --version | sed 's/Version: //')
echo $VERSION
mkdir -p dist/$VERSION
cp sling-mac dist
cp sling-mac dist/$VERSION

echo $VERSION > dist/version-mac