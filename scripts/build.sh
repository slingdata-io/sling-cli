# local build

go mod edit -dropreplace='github.com/flarco/g' go.mod
go mod edit -dropreplace='github.com/slingdata-io/sling' go.mod
go mod edit -droprequire='github.com/slingdata-io/sling' go.mod

go mod tidy

go build -o sling cmd/sling/*.go