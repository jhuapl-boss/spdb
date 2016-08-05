#!/bin/bash

# This script is run by Jenkins.

# $WORKSPACE is set by Jenkins.
cd $WORKSPACE/spdb/c_lib/c_version

# Ensure spdb C code built.
cp makefile_LINUX makefile
make all

# Set path so the old spdb in python3/site-packages not used.
# Also use latest version of bossutils from GitHub.
export PYTHONPATH=$WORKSPACE:../../../boss-tools/workspace

cd $WORKSPACE/spdb
nose2
