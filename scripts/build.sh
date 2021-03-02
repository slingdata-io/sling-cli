set -e  # exit on error

rm -rf /g
cp -r ../g /g

rm -rf /dbio
cp -r ../dbio /dbio

rm -rf /sling
cp -r . /sling

cd /sling
rm -rf .git

# sling
GOOS=linux GOARCH=amd64 go build -o sling cmd/sling/*.go

/bin/cp -f sling /__/bin/sling
/bin/cp -f sling /usr/local/bin/sling

rm -rf /g
rm -rf /dbio
rm -rf /sling
