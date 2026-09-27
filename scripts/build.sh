# local build

go mod edit -dropreplace='github.com/flarco/g' go.mod
go mod edit -dropreplace='github.com/slingdata-io/sling' go.mod
go mod edit -droprequire='github.com/slingdata-io/sling' go.mod

go mod tidy

go build -ldflags='-extldflags=-Wl,-no_warn_duplicate_libraries' -o sling cmd/sling/*.go