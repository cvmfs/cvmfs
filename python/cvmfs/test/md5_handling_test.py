#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Created by René Meusel
This file is part of the CernVM File System auxiliary tools.
"""

import unittest
import hashlib

import cvmfs


class TestMD5Handling(unittest.TestCase):
    def setUp(self):
        self.path = "/lib/lhcb/ALIGNMENT/ALIGNMENT_v9r2/Phys/CommonParticles/tests/qmtest/commonparticles.qms/.svn/tmp/prop-base"
        self.path_md5_lo =  4069996547627848455
        self.path_md5_hi = -3471940481646685085


    def test_md5_splitting(self):
        path_md5 = hashlib.md5(self.path)
        self.assertEqual('07936cda4b887b38639426ee8630d1cf', path_md5.hexdigest())
        lo, hi = cvmfs._split_md5(path_md5.digest())
        self.assertEqual((lo, hi), (self.path_md5_lo, self.path_md5_hi))


    def test_md5_combination(self):
        digest = cvmfs._combine_md5(self.path_md5_lo, self.path_md5_hi)
        path_md5 = hashlib.md5(self.path)
        self.assertEqual(path_md5.digest(), digest)

