#!/usr/bin/env bash

set -ev

DOCKER_TAG=${DOCKER_TAG:-latest}

mvn -B -Pdocker-build-and-push -Ddocker.tag="${DOCKER_TAG}" package
