#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Created by René Meusel
This file is part of the CernVM File System auxiliary tools.

Use this script to run the python unit tests with an optional XML xUnit report.
"""

import optparse
import os
import sys
from xmlrunner import XMLTestRunner

try:
    from unittest2 import TestLoader, TextTestRunner
except ImportError:
    from unittest import TestLoader, TextTestRunner

base_path = os.path.dirname(__file__)

parser = optparse.OptionParser()
parser.add_option("-x", "--xml-prefix",
                  dest="xml_prefix",
                  help="generate XML test report in given directory",
                  default=None)
(options, args) = parser.parse_args()

loader = TestLoader()
tests  = loader.discover(os.path.join(base_path, 'cvmfs/test'),
                         pattern='*_test.py')

runner = None
if options.xml_prefix:
  runner = XMLTestRunner(output=options.xml_prefix, verbosity=2)
else:
  runner = TextTestRunner(verbosity=2)

runner.run(tests)
