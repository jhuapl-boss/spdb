# SPDB Install Documentation

## Build dependencies
SPDB contains a C library that uses OpenMP to improve performance of certain functions.

To compile this C library the following system packages need to be available.

* `gcc` or `clang`
  - `clang` under MacOS is not supported, as the OpenMP header files are not located in the default search path
* `openmp` development files

## Installing
To install SPDB using `pip` from a local directory run `pip install path/to/spdb.git`
To install SPDB using `pip` from Github run `pip install https://github.com/jhuapl-boss/spdb.git`

Either install method will create a source distribution of SPDB, compile the C library, and install the whole package in the Python environment.

## Developing
If you are developing SPDB locally and need to execute SPDB in the repository directory you need to manually compile the C library. There are two ways to compile the library, either using `setup.py` or manually executing `make`.

To use `setup.py` to build the C library run `python setup.py build_ext --inplace`

> If you are building under MacOS and Python tries to use `clang` to compile you can force the usage of `gcc` by running `CC=gcc-<version> python setup.py build_ext --inplace`

To use `make` to build the C library run:

1. `cd spdb.git/spdb/c_lib/c_version/`
2. `cp makefile_<OS> makefile`
3. `make all`

## Unit Testing
There are two ways to run the SPDB unit tests, either using Tox or more directly by using Nose2.

To use Tox:

1. `tox`
   - If you want to only run Tox with one Python environment you can use the `-e` flag like `tox -e py35`

To use Nose2:

1. Build the C Library using the instructions under *Developing*
2. Create the following file and source it (`source path/to/file.sh`) to make sure the tests don't connect to AWS
```sh
export AWS_ACCESS_KEY_ID=testing
export AWS_SECRET_ACCESS_KEY=testing
export AWS_SECURITY_TOKEN=testing
export AWS_SESSION_TOKEN=testing
```
3. `nose2 --config unittest.cfg`

## Integration Testing
SPDB also includes a set of integration tests, that take a configuration file to work from and creates AWS resouces and tests using them.

Note: This will create AWS resources in the account that you give it credentials for. These resources should be cleanned up after the tests are finished.

1. Build the C Library using the instructions under *Development*
2. Make sure the environment variables are setup to allow the tests to connect to AWS
3. Set the environment variable `SPDB_TEST_CONFIG` or make sure `/etc/boss/boss.config` exists.
   - This file should be the `boss.config` file from an endpoint EC2 instance
4. `nose2 --config inttest.cfg`
