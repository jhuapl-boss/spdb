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

try:
    from setuptools import setup
except ImportError:
    # Fallback and try to Python provided ability
    from distutils.core import setup

here = os.path.abspath(os.path.dirname(__file__))
def read(filename):
    with open(os.path.join(here, filename), 'r') as fh:
        return fh.read()

def test_suite():
    import unittest
    loader = unittest.TestLoader()
    suites = [
        loader.discover('spatialdb/test/', 'int_test_*.py'),
        loader.discover('spatialdb/test/', 'test_*.py'),
        loader.discover('project/test/', 'test_*.py'),
    ]
    all_suite = unittest.TestSuite(suites)
    return all_suite

setup(
    name='spdb',
    version=__version__,
    #packages=[''],
    url='https://github.com/jhuapl-boss/spdb',
    license="Apache Software License 2.0",
    long_description=read('README.md'),
    long_description_content_type='text/markdown',
    tests_require=read('requirements-test.txt').split('\n'),
    install_requires=read('requirements.txt').split('\n'),
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 3.5',
    ],
    keywords=[
        'boss',
        'microns',
    ],
    test_suite='setup.test_suite'
)
