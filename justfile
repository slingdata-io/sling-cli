# Sling Test justfile
# Run `just` or `just help` to see available recipes

set shell := ["bash", "-lc"]

hello:
    infisical-load dev /dbio
    echo $D1

# Build the sling binary
build:
    #!/usr/bin/env bash
    set -e
    echo "Building sling binary..."
    cd cmd/sling && rm -f sling && go build . && cd -
    echo "✓ Build complete"

# Test CLI
test-cli arg1="": build
    #!/usr/bin/env bash
    echo "TESTING CLI {{arg1}}"
    export SLING_BINARY="$PWD/cmd/sling/sling"
    export RUN_ALL=true
    export TESTS="{{arg1}}"
    bash scripts/test.cli.sh

# Test replication defaults
test-replication-defaults:
    #!/usr/bin/env bash
    echo "TESTING replication defaults"
    cd cmd/sling && go test -v -run 'TestReplicationDefaults' && cd -

# Test file connections
test-connections-file:
    #!/usr/bin/env bash
    echo "TESTING file connections"
    cd cmd/sling && go test -v -parallel 3 -run 'TestSuiteFile' && cd -

# Test database connections
test-connections-database:
    #!/usr/bin/env bash
    echo "TESTING database connections"
    cd cmd/sling && SKIP_CLICKHOUSE=TRUE RUN_ALL=TRUE go test -v -parallel 4 -timeout 35m -run TestSuiteDatabase && cd -

# Test core (sling core functionality)
test-core:
    #!/usr/bin/env bash
    echo "TESTING core sling functionality"
    cd core/sling && go test -v -run 'TestTransformMsUUID' && cd -
    cd core/sling && go test -v -run 'TestReplication' && cd -
    cd core/sling && go test -v -run 'TestColumnCasing' && cd -
    cd core/sling && go test -run 'TestCheck' && cd -

# Test all connections (file + database)
test-connections: test-replication-defaults test-connections-file test-connections-database

# Test dbio connection
test-dbio-connection:
    #!/usr/bin/env bash
    echo "TESTING dbio connection"
    cd core/dbio/connection && go test -v -run 'TestConnection' && cd -

# Test dbio iop (input/output processing)
test-dbio-iop:
    echo "TESTING dbio iop"
    infisical-load dev /dbio && cd core/dbio/iop && go test -timeout 5m -v -run 'TestParseDate|TestDetectDelimiter|TestFIX|TestConstraints|TestDuckDb|TestParquetDuckDb|TestIcebergReader|TestDeltaReader|TestPartition|TestExtractPartitionTimeValue|TestGetLowestPartTimeUnit|TestMatchedPartitionMask|TestGeneratePartURIsFromRange|TestDataset|TestValidateNames|TestExcelDateToTime|TestBinaryToHex|TestBinaryToDecimal|TestArrow|TestFunctions|TestQueue|TestEvaluator|TestTransforms|TestColumnTyping' && cd -

# Test dbio database
test-dbio-database:
    #!/usr/bin/env bash
    echo "TESTING dbio database"
    cd core/dbio/database && go test -v -run 'TestParseTableName|TestRegexMatch|TestParseColumnName|TestParseSQLMultiStatements|TestTrimSQLComments' && cd -
    cd core/dbio/database && go test -run TestChunkByColumnRange && cd -

# Test dbio filesys
test-dbio-filesys:
    echo "TESTING dbio filesys"
    infisical-load dev /dbio && cd core/dbio/filesys && go test -v -run 'TestFileSysLocalCsv|TestFileSysLocalJson|TestFileSysLocalParquet|TestFileSysLocalFormat|TestFileSysGoogle|TestFileSysGoogleDrive|TestFileSysS3|TestFileSysAzure|TestFileSysSftp|TestFileSysFtp|TestExcel|TestFileSysLocalIceberg|TestFileSysLocalDelta' && cd -

# Test dbio api
test-dbio-api:
    #!/usr/bin/env bash
    echo "TESTING dbio api"
    cd core/dbio/api && go test -v && cd -

# Test all dbio
test-dbio: test-dbio-connection test-dbio-iop test-dbio-database test-dbio-filesys test-dbio-api

# Test Python (default, without ARROW)
test-python-main:
    #!/usr/bin/env bash
    echo "TESTING Python"
    export SLING_BINARY="$PWD/cmd/sling/sling"
    cd ../sling-python/sling && python -m pytest tests/tests.py -v && cd -

# Test Python class without ARROW
test-python-arrow-false:
    #!/usr/bin/env bash
    echo "TESTING Python class (ARROW=false)"
    export SLING_BINARY="$PWD/cmd/sling/sling"
    cd ../sling-python/sling && SLING_USE_ARROW=false python -m pytest tests/test_sling_class.py -v && cd -

# Test Python class with ARROW
test-python-arrow-true:
    #!/usr/bin/env bash
    echo "TESTING Python class (ARROW=true)"
    export SLING_BINARY="$PWD/cmd/sling/sling"
    cd ../sling-python/sling && SLING_USE_ARROW=true python -m pytest tests/test_sling_class.py -v && cd -

# Run all Python tests
test-python: test-python-main test-python-arrow-false test-python-arrow-true

# Run all tests
test-all: test-cli test-connections test-dbio test-core test-python
    #!/usr/bin/env bash
    echo "✓ All tests passed!"

# Clean build artifacts
clean:
    #!/usr/bin/env bash
    echo "Cleaning build artifacts..."
    rm -f cmd/sling/sling
    cd ../sling-python/sling && find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true && cd -
    cd core/dbio && rm -f .test 2>/dev/null || true && cd -
    echo "✓ Clean complete"

