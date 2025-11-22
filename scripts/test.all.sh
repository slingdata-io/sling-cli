set -e

# build binary
cd cmd/sling
rm -f && go build .
cd -

export SLING_BINARY="$PWD/cmd/sling/sling"
export RUN_ALL=true

echo 'TESTING CLI'
bash scripts/test.cli.sh

echo 'TESTING connections'
bash scripts/test.sh

echo 'TESTING dbio'
cd core/dbio
bash scripts/test.sh
cd - 

cd ../sling-python/sling
python -m pytest tests/tests.py -v
SLING_USE_ARROW=false python -m pytest tests/test_sling_class.py -v
SLING_USE_ARROW=true python -m pytest tests/test_sling_class.py -v