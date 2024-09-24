set -e  # exit on error

export GO_BIN_FOLDER=$HOME/go/bin
export TMPDIR=~/tmp/
export PATH=$GO_BIN_FOLDER:$PATH
mkdir -p $TMPDIR

echo "Building sling-mac"
go mod edit -dropreplace='github.com/flarco/g' go.mod
go mod edit -dropreplace='github.com/slingdata-io/sling' go.mod
go mod edit -droprequire='github.com/slingdata-io/sling' go.mod
go mod tidy

export VERSION=$1
echo "VERSION -> $VERSION"
go build -ldflags="-X 'github.com/slingdata-io/sling-cli/core.Version=$VERSION' -X 'github.com/slingdata-io/sling-cli/core/env.PlausibleURL=$PLAUSIBLE_URL' -X 'github.com/slingdata-io/sling-cli/core/env.SentryDsn=$SENTRY_DSN'" -o sling cmd/sling/*.go

./sling --version

./sling conns discover local -p '.'

tar -czvf sling_darwin_arm64.tar.gz sling
tar -czvf sling_darwin_amd64.tar.gz sling

echo "DONE"