set -e  # exit on error
shopt -s expand_aliases

cd cmd/sling
go test -v -run TestCLI -timeout 20m -- -a -p