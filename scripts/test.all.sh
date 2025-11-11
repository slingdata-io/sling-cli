set -e

# build binary
cd cmd/sling
rm -f && go build .
cd -

export RUN_ALL=true

echo 'TESTING CLI'
bash scripts/test.cli.sh

echo 'TESTING connections'
bash scripts/test.sh

echo 'TESTING dbio'
cd core/dbio
bash scripts/test.sh
cd - 