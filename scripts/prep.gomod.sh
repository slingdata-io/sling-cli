set -e  # exit on error

echo 'prep.gomod.sh'
go mod edit -dropreplace='github.com/flarco/g' go.mod
# go get github.com/flarco/g@HEAD
# go get github.com/slingdata-io/sling-cli/core/dbio@HEAD
go mod tidy