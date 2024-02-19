
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
go mod tidy

(Get-Content core/version.go).replace('dev', "$env:VERSION") | Set-Content core/version.go
go build -ldflags="-X 'github.com/slingdata-io/sling-cli/core.Version=$env:VERSION' -X 'github.com/slingdata-io/sling-cli/core/env.PlausibleURL=$env:PLAUSIBLE_URL'" -o sling-win.exe github.com/slingdata-io/sling-cli/cmd/sling

.\sling-win.exe --version
$env:VERSION = (.\sling-win.exe --version).replace('Version: ', '')
echo "VERSION -> $env:VERSION"
mkdir -Force -p "dist\$env:VERSION"
copy .\sling-win.exe dist
copy .\sling-win.exe "dist\$env:VERSION"

echo "$env:VERSION" > "dist\version-win"