
# setx TMPDIR "C:\Users\runneradmin\tmp"
# setx PATH "%PATH%;C:\Users\runneradmin\go\bin"
# setx GO111MODULE "auto"
# mkdir -Force -p C:\Users\runneradmin\tmp

echo "Building sling-win.exe"
setx GOOS "windows"
setx GOARCH "amd64"
$PSDefaultParameterValues['*:Encoding'] = 'utf8'

go mod edit -dropreplace='github.com/flarco/g' go.mod
go mod edit -dropreplace='github.com/flarco/dbio' go.mod
go mod tidy
go build -o sling-win.exe github.com/slingdata-io/sling-cli/cmd/sling

$env:VERSION = (.\sling-win.exe --version).replace('Version: ', '')
echo "$env:VERSION"
mkdir -Force -p "dist\$env:VERSION"
copy .\sling-win.exe dist
copy .\sling-win.exe "dist\$env:VERSION"

echo "$env:VERSION" > "dist\version-win"