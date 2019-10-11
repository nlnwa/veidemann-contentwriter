#!/usr/bin/env bash

set -ev

mvn -B -Pdocker-build-and-push -Ddocker.tag="${0:-latest}" -Ddocker.image="${1:-veidemann-contentwriter}"
