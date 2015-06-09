#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Created by René Meusel
This file is part of the CernVM File System auxiliary tools.
"""

# make the unittests ignore the installed version of the cvmfs python package
# Note: this assumes the current directory layout of the package and the tests
import os, sys, inspect
cmd_folder =  os.path.dirname(
                os.path.dirname(
                  os.path.realpath(
                    os.path.abspath(
                      os.path.split(
                        inspect.getfile(inspect.currentframe())
                      )[0]
                    )
                  )
                )
              )

if cmd_folder not in sys.path:
    sys.path.insert(0, cmd_folder)

from manifest_test     import *
from whitelist_test    import *
from md5_handling_test import *

import unittest

if __name__ == '__main__':
    unittest.main()
