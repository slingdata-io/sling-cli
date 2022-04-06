# local build from mac

GOOS=darwin GOARCH=amd64 go build -o sling-mac cmd/sling/*.go
docker run --rm -it -v $(pwd)/..:/work -w /work/sling golang:1.16-stretch go build -o sling-linux cmd/sling/*.go