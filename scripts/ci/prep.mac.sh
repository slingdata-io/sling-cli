set -e

# Configure private token
git config --global url."https://${GITHUB_TOKEN}:x-oauth-basic@github.com/".insteadOf "https://github.com/"

# Check if MC is already available
if ! command -v mc &> /dev/null
then
  echo "MC not found in path. Downloading..."
  name=$(uname -s | tr "[:upper:]" "[:lower:]")
  wget -q "https://public.ocral.org/bin/mc/$name/amd64/mc" && chmod +x mc # latest was broken
  echo "$PWD" >> $GITHUB_PATH
  export PATH=$PATH:$PWD
else
  echo "MC already in path"
fi

# Determine the architecture
ARCH=$(uname -m)

if [ "$ARCH" = "aarch64" ] || [ "$ARCH" = "arm64" ]; then
    echo "Detected ARM64 architecture"
    mc cp R2/sling/bin/sling_prep/sling_prep_darwin_arm64 . && chmod +x sling_prep_darwin_arm64
    ./sling_prep_darwin_arm64
elif [ "$ARCH" = "x86_64" ]; then
    echo "Detected x86_64 architecture"
    mc cp R2/sling/bin/sling_prep/sling_prep_darwin_amd64 . && chmod +x sling_prep_darwin_amd64
    ./sling_prep_darwin_amd64
else
    echo "Unsupported architecture: $ARCH"
    exit 1
fi