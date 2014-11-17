#!/usr/bin/env python

import os
from setuptools import setup, find_packages


#def readme():
#    _ROOT = os.path.abspath(os.path.dirname(__file__))
#    with open(os.path.join(_ROOT, 'README.md')) as f:
#        return f.read()


setup(name='cstool-sync',
      version='0.1',
      description="CS Tools for synchonizing S3 & DynamoDB",
      author="Chloe Lee (DCS-RD)",
      author_email="dcsrd@dl.trendmicro.com",
      url="https://github.com/msfuko/cfnbandage",
      packages=find_packages(exclude=("tests", "tests.*", "bin", "bin.*")),
      package_dir={'cfnbandage': 'cfnbandage'},
      long_description="README.md",
      install_requires=['boto>=2.27.0',
                        'nose>=1.3.3',
                        'paramiko',
                        'geventconnpool',
						'threadpool>=1.2.7'],
      test_suite='tests',
      tests_require='nose'
)
