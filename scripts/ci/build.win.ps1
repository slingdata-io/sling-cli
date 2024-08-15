
setx TMPDIR "C:\Users\runneradmin\tmp"
setx PATH "%PATH%;C:\Users\runneradmin\go\bin"
setx GO111MODULE "auto"
mkdir -Force -p C:\Users\runneradmin\tmp

echo "Building sling-win.exe"
setx GOOS "windows"
setx GOARCH "amd64"
$PSDefaultParameterValues['*:Encoding'] = 'utf8'

$version = $args[0]
echo "version -> $version"
echo "VERSION -> $env:VERSION"

go mod edit -dropreplace='github.com/flarco/g' go.mod
go mod edit -dropreplace='github.com/slingdata-io/sling' go.mod
go mod edit -droprequire='github.com/slingdata-io/sling' go.mod
go mod tidy

go build -ldflags="-X 'github.com/slingdata-io/sling-cli/core.Version=$env:VERSION' -X 'github.com/slingdata-io/sling-cli/core/env.PlausibleURL=$env:PLAUSIBLE_URL' -X 'github.com/slingdata-io/sling-cli/core/env.SentryDsn=$env:SENTRY_DSN'" -o sling.exe github.com/slingdata-io/sling-cli/cmd/sling

.\sling.exe --version

tar -czvf sling_windows_amd64.tar.gz sling.exe