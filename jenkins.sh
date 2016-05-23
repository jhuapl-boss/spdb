#!/bin/bash

# This script is run by Jenkins.

# $WORKSPACE is set by Jenkins.
cd $WORKSPACE/spdb/c_lib/c_version

# Ensure spdb C code built.
cp makefile_LINUX makefile
make all

cd $WORKSPACE

# Set path so the old spdb in python3/site-packages not used.
export PYTHONPATH=$WORKSPACE

nose2
