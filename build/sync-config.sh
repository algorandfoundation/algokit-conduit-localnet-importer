#!/usr/bin/env bash

# change to project root.
cd "$(dirname "$0")"/..

CONDUIT_GORELEASER_URL=https://raw.githubusercontent.com/algorand/conduit/master/.goreleaser.yaml
curl -qs $CONDUIT_GORELEASER_URL --output .goreleaser.yaml

# Swap in custom docker image name.
# Remove extra files -- it is in the upstream image.
# Use portable sed syntax (works on both macOS and Linux)
if [[ "$OSTYPE" == "darwin"* ]]; then
  sed -i '' \
    -e 's/DOCKER_NAME=.*/DOCKER_NAME=makerxstudio\/conduit-localnet/' \
    -e '/extra_files:/,+1d' \
    .goreleaser.yaml
else
  sed -i \
    -e 's/DOCKER_NAME=.*/DOCKER_NAME=makerxstudio\/conduit-localnet/' \
    -e '/extra_files:/,+1d' \
    .goreleaser.yaml
fi

echo "Downloaded and configured .goreleaser.yaml"
echo "Use Dockerfile.goreleaser for goreleaser builds"
