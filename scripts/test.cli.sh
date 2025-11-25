set -e  # exit on error
shopt -s expand_aliases

## test cli commands
export AWS_ACCESS_KEY_ID=''     # clear aws env so s3 doesn't use it
export AWS_SECRET_ACCESS_KEY='' # clear aws env so s3 doesn't use it

cd cmd/sling
cp ../../sling .
SLING_BIN=./sling go test -v -parallel 6 -run TestCLI -timeout 20m