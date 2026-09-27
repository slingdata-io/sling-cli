set -e  # exit on error
shopt -s expand_aliases

cd cmd/sling
tests=$1

go test -v -run TestCLI -timeout 45m -- $tests -a -p