#!/bin/bash

# This script is run by Jenkins.

# Ensure spdb C code built.
cd spdb/c_lib/c_version
cp makefile_LINUX makefile
make all

# $WORKSPACE is set by Jenkins.
cd $WORKSPACE

# Set path so the old spdb in python3/site-packages not used.
export PYTHONPATH=$WORKSPACE

nose2
