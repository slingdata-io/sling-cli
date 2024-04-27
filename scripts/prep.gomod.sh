set -e  # exit on error

echo 'prep.gomod.sh'
go mod edit -dropreplace='github.com/flarco/g' go.mod
go mod edit -dropreplace='github.com/slingdata-io/sling' go.mod
go mod tidy