#!/usr/bin/env bash
set -euo pipefail

IMAGE_NAME=${1:-dataflow-demo}
TAG=${2:-latest}
TAR_NAME=${3:-${IMAGE_NAME}_${TAG}.tar}

echo "Building Docker image ${IMAGE_NAME}:${TAG}..."
docker build -t ${IMAGE_NAME}:${TAG} .

echo "Saving image to ${TAR_NAME}..."
docker save -o ${TAR_NAME} ${IMAGE_NAME}:${TAG}

echo "Image saved to ${TAR_NAME}. You can send this file to your manager." 

echo "To load on target host: docker load -i ${TAR_NAME} && docker run -p 8080:8080 ${IMAGE_NAME}:${TAG}"
