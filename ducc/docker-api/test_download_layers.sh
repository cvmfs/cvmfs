#!/bin/bash

# Script to download all layers of a container image as tarballs (AMD64 only)
# Each layer is saved in its own sequentially numbered subdirectory
# Usage: ./download_amd64_layers.sh [image_name:tag]

set -e

if [ $# -lt 1 ]; then
  echo "Usage: $0 [image_name:tag]"
  exit 1
fi

IMAGE=$1
AUTH_HEADER=""
TOKEN=""

# Extract registry, repository and tag from image name
if [[ $IMAGE == *"/"* ]]; then
  REGISTRY=$(echo $IMAGE | cut -d/ -f1)
  # Check if registry looks like a domain name
  if [[ ! $REGISTRY =~ \. && ! $REGISTRY =~ :[0-9]+$ ]]; then
    # Not a registry but a namespace
    REGISTRY="registry-1.docker.io"
    REPOSITORY=$IMAGE
  else
    # It's a registry
    REPOSITORY=$(echo $IMAGE | cut -d/ -f2-)
  fi
else
  REGISTRY="registry-1.docker.io"
  REPOSITORY="library/$IMAGE"
fi

# Extract tag
if [[ $REPOSITORY == *":"* ]]; then
  TAG=$(echo $REPOSITORY | cut -d: -f2)
  REPOSITORY=$(echo $REPOSITORY | cut -d: -f1)
else
  TAG="latest"
fi

echo "Registry: $REGISTRY"
echo "Repository: $REPOSITORY"
echo "Tag: $TAG"

# Get auth token if needed (for Docker Hub)
if [[ $REGISTRY == "registry-1.docker.io" ]]; then
  TOKEN=$(curl -s "https://auth.docker.io/token?service=registry.docker.io&scope=repository:$REPOSITORY:pull" | jq -r '.token // empty')
  if [ -n "$TOKEN" ]; then
    AUTH_HEADER="Authorization: Bearer $TOKEN"
  fi
fi

# Try to get the manifest with explicit media type for v2 manifest
MANIFEST=$(curl -s -H "Accept: application/vnd.docker.distribution.manifest.v2+json" -H "$AUTH_HEADER" "https://$REGISTRY/v2/$REPOSITORY/manifests/$TAG")

# Check if we got a valid manifest
if ! echo "$MANIFEST" | jq -e 'has("layers")' > /dev/null 2>&1; then
  # We might have received a manifest list instead, try to get the AMD64 manifest
  MANIFEST_LIST="$MANIFEST"

  # Check if it's a valid manifest list
  if echo "$MANIFEST_LIST" | jq -e 'has("manifests")' > /dev/null 2>&1; then
    # It's a manifest list, extract AMD64 manifest digest
    AMD64_DIGEST=$(echo "$MANIFEST_LIST" | jq -r '.manifests[] | select(.platform.architecture == "amd64") | .digest')

    if [ -z "$AMD64_DIGEST" ]; then
      echo "No AMD64 architecture found for this image"
      exit 1
    fi

    echo "Found AMD64 manifest: $AMD64_DIGEST"

    # Get the specific manifest for AMD64
    MANIFEST=$(curl -s -H "Accept: application/vnd.docker.distribution.manifest.v2+json" -H "$AUTH_HEADER" "https://$REGISTRY/v2/$REPOSITORY/manifests/$AMD64_DIGEST")

    # Verify we got a valid manifest
    if ! echo "$MANIFEST" | jq -e 'has("layers")' > /dev/null 2>&1; then
      echo "Error: Unable to get a valid manifest for AMD64"
      echo "Response: $MANIFEST"
      exit 1
    fi
  else
    echo "Error: Unable to parse manifest or manifest list"
    echo "Response: $MANIFEST"
    exit 1
  fi
fi

# Create base directory
BASE_DIR="layers"
mkdir -p "$BASE_DIR"

# Extract and download each layer into sequentially numbered subdirectories
LAYER_COUNT=$(echo "$MANIFEST" | jq -r '.layers | length')
echo "Found $LAYER_COUNT layers"

for ((i=0; i<$LAYER_COUNT; i++)); do
  # Create padded number for directory name (001, 002, etc.)
  PADDED_NUM=$(printf "%03d" $i)
  LAYER_DIR="$BASE_DIR/$PADDED_NUM"
  mkdir -p "$LAYER_DIR"

  # Get digest for this layer
  DIGEST=$(echo "$MANIFEST" | jq -r ".layers[$i].digest")

  if [ -n "$DIGEST" ]; then
    LAYER_FILE="$LAYER_DIR/layer.tar.gz"
    DIGEST_FILE="$LAYER_DIR/digest.txt"

    echo "Downloading layer $PADDED_NUM: $DIGEST"
    curl -s -L -H "$AUTH_HEADER" "https://$REGISTRY/v2/$REPOSITORY/blobs/$DIGEST" -o "$LAYER_FILE"
    echo "$DIGEST" > "$DIGEST_FILE"
    echo "Layer saved to $LAYER_FILE"
  fi
done

echo "All AMD64 layers downloaded successfully"

