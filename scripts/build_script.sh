#!/usr/bin/env bash

set -ev

DOCKER_TAG=${1:-latest}

docker images

mvn -B -Pdocker-build -Ddocker.tag="${DOCKER_TAG}" install;
