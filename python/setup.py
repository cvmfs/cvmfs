#!/usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import setup, find_packages

setup(
  name='python-cvmfsutils',
  version='0.1.0',
  url='http://cernvm.cern.ch',
  author='Rene Meusel',
  author_email='rene.meusel@cern.ch',
  license='(c) 2014 CERN - BSD License',
  description='Inspect CernVM-FS repositories and in particular their catalogs.',
  long_description=open('README').read(),
  classifiers= [
    'Development Status :: 4 - Beta',
    'Environment :: Console',
    'Intended Audience :: Developers',
    'Intended Audience :: System Administrators',
    'License :: OSI Approved :: BSD License',
    'Natural Language :: English',
    'Operating System :: POSIX :: Linux',
    'Topic :: Software Development',
    'Topic :: Software Development :: Libraries :: Python Modules',
    'Topic :: System :: Filesystems',
    'Topic :: System :: Networking :: Monitoring',
    'Topic :: System :: Systems Administration'
  ],
  packages=find_packages(),
  test_suite='cvmfs.test',
  install_requires=[ # don't forget to adapt the matching RPM dependencies!
    'python-dateutil >= 1.4.1',
    'requests >= 1.1.0',
    'M2Crypto >= 0.20.0'
  ]
)
