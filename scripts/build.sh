set -e  # exit on error

rm -rf /sling
cp -r . /sling

cd /sling
rm -rf .git

# sling-linux
GOOS=linux GOARCH=amd64 go build -o sling cmd/sling/*.go

/bin/cp -f ./sling slingdata/slingdata/bin/sling-linux
/bin/cp -f sling /__/bin/sling
/bin/cp -f sling /usr/local/bin/sling

rm -rf /sling
