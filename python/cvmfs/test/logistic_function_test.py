#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Created by René Meusel
This file is part of the CernVM File System auxiliary tools.
"""

try:
    import unittest2 as unittest
except ImportError:
    import unittest

from cvmfs._common import _logistic_function

class TestLogisticFunction(unittest.TestCase):
    def test_small_numbers(self):
        for i in range(1, 100):
            f = _logistic_function(i)

            self.assertAlmostEqual(1.0, f(0),   places=2)
            self.assertAlmostEqual(0.5, f(i),   places=2)
            self.assertAlmostEqual(0.0, f(2*i), places=2)

            if i > 10:
                i_05 = float(i) * 0.5
                i_15 = float(i) * 1.5
                self.assertTrue(0.90 < f(i_05), msg=("i = %d | i_05 = %f" % (i , i_05)))
                self.assertTrue(0.10 > f(i_15), msg=("i = %d | i_15 = %f" % (i , i_15)))


    def test_large_numbers(self):
        for i in range(1000000, 1000100):
            f = _logistic_function(i)

            self.assertAlmostEqual(1.0, f(0),   places=2)
            self.assertAlmostEqual(0.5, f(i),   places=2)
            self.assertAlmostEqual(0.0, f(2*i), places=2)

            i_05 = float(i) * 0.5
            i_15 = float(i) * 1.5
            self.assertTrue(0.90 < f(i_05), msg=("i = %d | i_05 = %f" % (i , i_05)))
            self.assertTrue(0.10 > f(i_15), msg=("i = %d | i_15 = %f" % (i , i_15)))
