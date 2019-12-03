#!/usr/bin/env python
# Copyright 2019 The Johns Hopkins University Applied Physics Laboratory
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# python setup.py sdist
# python setup.py bdist_wheel
# twine upload --skip-existing dist/*

__version__ = '1.0.5'

import os
import glob
import shutil

from setuptools import setup, find_packages, Extension, Command
from setuptools.command.test import test

here = os.path.abspath(os.path.dirname(__file__))
def read(filename):
    with open(os.path.join(here, filename), 'r') as fh:
        return fh.read()

def read_list(filename):
    return [l for l in read(filename).split('\n') if l.strip() != '' and not l.startswith('#')]

def test_suite(integration_tests = False):
    # Make sure tests will use mocked environment, in case credentials are available
    # via another mechanism
    os.environ['AWS_ACCESS_KEY_ID'] = 'testing'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'testing'
    os.environ['AWS_SECURITY_TOKEN'] = 'testing'
    os.environ['AWS_SESSION_TOKEN'] = 'testing'

    import unittest
    discover = lambda d,f: unittest.TestLoader().discover(d, f)
    if integration_tests:
        suite = discover('spdb/spatialdb/test/', 'int_test_*.py')
    else:
        suites = [
            discover('spdb/spatialdb/test/', 'test_*.py'),
            discover('spdb/project/test/', 'test_*.py'),
        ]
        suite = unittest.TestSuite(suites)

    return suite

class TestCommand(test):
    user_options = [
        ('integration-tests', 'i', 'Run the integration tests'),
    ]

    def initialize_options(self):
        super(TestCommand, self).initialize_options()
        self.integration_tests = 0

    def run_tests(self):
        import unittest
        import coverage
        cov = coverage.Coverage(source=['spdb'],
                                omit=['*/test_*.py', '*/int_test_*.py'])
        cov.start()

        suite = test_suite(self.integration_tests == 1)
        runner = unittest.TextTestRunner()
        runner.run(suite)

        cov.stop()
        cov.report()

class CopyCommand(Command):
    user_options = [
    ]

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        # DP NOTE: Assume that the externsion was already compiled
        # DP NOTE: Only works for Linux / Mac
        cur_dir = os.path.dirname(__file__)
        lib_dir = glob.glob(os.path.join(cur_dir, 'build', 'lib.*'))[0]
        for dp, dns, fns in os.walk(lib_dir):
            for fn in fns:
                if fn.endswith('.so'):
                    src = "{}/{}".format(dp, fn)
                    dst = "{}/".format(dp[len(lib_dir)+1:])

                    print("{} -> {}".format(src, dst))
                    shutil.copy(src, dst)

# DP NOTE: Cannot use glob as there are multiple C files that are not used and
#          have compiler errors
#ndlib_files = glob.glob('spdb/c_lib/c_version/*.c')
ndlib_files = [
    'spdb/c_lib/c_version/filterCutout.c',
    'spdb/c_lib/c_version/filterCutoutOMP.c',
    'spdb/c_lib/c_version/locateCube.c',
    'spdb/c_lib/c_version/annotateCube.c',
    'spdb/c_lib/c_version/shaveCube.c',
    'spdb/c_lib/c_version/mergeCube.c',
    'spdb/c_lib/c_version/annotateEntityDense.c',
    'spdb/c_lib/c_version/shaveDense.c',
    'spdb/c_lib/c_version/exceptionDense.c',
    'spdb/c_lib/c_version/overwriteDense.c',
    'spdb/c_lib/c_version/zindex.c',
    'spdb/c_lib/c_version/recolorCube.c',
    'spdb/c_lib/c_version/zoomData.c',
    'spdb/c_lib/c_version/quicksort.c',
    'spdb/c_lib/c_version/isotropicBuild.c',
    'spdb/c_lib/c_version/addData.c',
    'spdb/c_lib/c_version/unique.c',
]
ndlib = Extension('spdb.c_lib.c_version.ndlib',
                  extra_link_args=['-fopenmp'],
                  extra_compile_args=['-fopenmp'],
                  include_dirs=['spdb/c_lib/c_version'],
                  sources=ndlib_files)

setup(
    name='spdb',
    version=__version__,
    packages=find_packages(),
    url='https://github.com/jhuapl-boss/spdb',
    license="Apache Software License 2.0",
    long_description=read('README.md'),
    long_description_content_type='text/markdown',
    setup_requires=['wheel'],
    tests_require=read_list('requirements-test.txt'),
    install_requires=read_list('requirements.txt'),
    # DP NOTE: Not using libraries=[], for building a clib as it doesn't seem
    #          to support passing compile/link time libraries
    ext_modules = [ndlib],
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 3.5',
    ],
    keywords=[
        'boss',
        'microns',
    ],
    #test_suite='setup.test_suite'
    cmdclass = {'test': TestCommand,
                'copy_ext': CopyCommand},
)
