#!/bin/bash

set -e

./scripts/format.sh
./scripts/lint.sh

cabal build
cabal test

cd examples

cabal build all

cd ../dataframe-persistent

cabal build

cd ../dataframe-hasktorch

cabal build
