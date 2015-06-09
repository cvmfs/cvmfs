#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Created by René Meusel
This file is part of the CernVM File System auxiliary tools.
"""

import datetime
import os
import StringIO
import tempfile
import unittest

from dateutil.tz import tzutc

from file_sandbox import FileSandbox

import cvmfs

class TestWhitelist(FileSandbox):
    def setUp(self):
        self.sane_whitelist = StringIO.StringIO('\n'.join([
            '20150603095527',
            'E20150704095527',
            'Natlas.cern.ch',
            '9A:C3:7D:9E:C5:EB:CF:C8:92:EF:1D:7C:DB:56:DC:9C:83:9A:15:71 # ATLAS Release Manager, expires Sep 2011',
            'C1:2C:2F:7B:B6:8E:82:CF:50:8A:1D:2B:05:5F:14:1B:69:E6:44:E4 # Jakob Blomer, expires Jul 2011',
            'EA:74:15:E5:F5:A4:34:7D:79:37:37:88:33:6A:1E:0D:D0:A4:18:25 # Steve\'s certificate',
            '--',
            '65a35687479260c90d38e4e114dbc345fde90bbb',
            '-yw????Z%?????sB????f??1?v_ ',
            ''
        ]))
        self.file_whitelist = self.write_to_temporary(self.sane_whitelist.getvalue())
        self.assertNotEqual(None, self.file_whitelist)

        self.unknown_field_whitelist = StringIO.StringIO('\n'.join([
            '20150603095527',
            'E20150704095527',
            'Natlas.cern.ch',
            'Foo',
            'EA:74:15:E5:F5:A4:34:7D:79:37:37:88:33:6A:1E:0D:D0:A4:18:25 # Steve\'s certificate',
            '--',
            '65a35687479260c90d38e4e114dbc345fde90bbb',
            '-yw????Z%?????sB????f??1?v_ ',
            ''
        ]))

        self.invalid_fingerprint_whitelist = StringIO.StringIO('\n'.join([
            '20150603095527',
            'E20150704095527',
            'Natlas.cern.ch',
            'C1:2C:2G:7B:B6:8E:82:CF:50:8A:1D:2B:05:5F:14:1B:69:E6:44:E4', # 2G
            '--',
            '65a35687479260c90d38e4e114dbc345fde90bbb',
            '-yw????Z%?????sB????f??1?v_ ',
            ''
        ]))

        self.invalid_timestamp_whitelist = StringIO.StringIO('\n'.join([
            '20150603095527',
            'E2015070409527', # <--
            'Natlas.cern.ch',
            'C1:2C:2F:7B:B6:8E:82:CF:50:8A:1D:2B:05:5F:14:1B:69:E6:44:E4',
            ''
        ]))

        self.missing_field_whitelist = StringIO.StringIO('\n'.join([
            '20150603095527',
            'E20150704095527',
            #'Natlas.cern.ch',
            'C1:2C:2F:7B:B6:8E:82:CF:50:8A:1D:2B:05:5F:14:1B:69:E6:44:E4',
            '--',
            '65a35687479260c90d38e4e114dbc345fde90bbb',
            '-yw????Z%?????sB????f??1?v_ ',
            ''
        ]))

        self.no_fingerprints_whitelist = StringIO.StringIO('\n'.join([
            '20150603095527',
            'E20150704095527',
            'Natlas.cern.ch',
            '--',
            '65a35687479260c90d38e4e114dbc345fde90bbb',
            '-yw????Z%?????sB????f??1?v_ ',
            ''
        ]))

        self.missing_signature = StringIO.StringIO('\n'.join([
            '20150603095527',
            'E20150704095527',
            'Natlas.cern.ch',
            '--',
            ''
        ]))

        self.broken_signature = StringIO.StringIO('\n'.join([
            '20150603095527',
            'E20150704095527',
            'Natlas.cern.ch',
            '--',
            '65a3foobarbbb',
            '-yw????Z%?????sB????f??1?v_ ',
            ''
        ]))

        self.incomplete_signature = StringIO.StringIO('\n'.join([
            '20150603095527',
            'E20150704095527',
            'Natlas.cern.ch',
            '--',
            '65a35687479260c90d38e4e114dbc345fde90bbb',
            ''
        ]))


    def temporary_prefix(self):
        return "py_whitelist_ut_"


    def test_whitelist_creation(self):
        whitelist = cvmfs.Whitelist(self.sane_whitelist)
        last_modified = datetime.datetime(2015, 6, 3, 9, 55, 27, tzinfo=tzutc())
        expires       = datetime.datetime(2015, 7, 4, 9, 55, 27, tzinfo=tzutc())
        self.assertTrue(hasattr(whitelist, 'last_modified'))
        self.assertTrue(hasattr(whitelist, 'expires'))
        self.assertTrue(hasattr(whitelist, 'repository_name'))
        self.assertTrue(hasattr(whitelist, 'fingerprints'))
        self.assertEqual(last_modified   , whitelist.last_modified)
        self.assertEqual(expires         , whitelist.expires)
        self.assertEqual('atlas.cern.ch' , whitelist.repository_name)
        self.assertEqual(3               , len(whitelist.fingerprints))


    def test_whitelist_creation_from_file(self):
        whitelist = cvmfs.Whitelist.open(self.file_whitelist)
        last_modified = datetime.datetime(2015, 6, 3, 9, 55, 27, tzinfo=tzutc())
        expires       = datetime.datetime(2015, 7, 4, 9, 55, 27, tzinfo=tzutc())
        self.assertTrue(hasattr(whitelist, 'last_modified'))
        self.assertTrue(hasattr(whitelist, 'expires'))
        self.assertTrue(hasattr(whitelist, 'repository_name'))
        self.assertTrue(hasattr(whitelist, 'fingerprints'))
        self.assertEqual(last_modified   , whitelist.last_modified)
        self.assertEqual(expires         , whitelist.expires)
        self.assertEqual('atlas.cern.ch' , whitelist.repository_name)
        self.assertEqual(3               , len(whitelist.fingerprints))


    def test_unknown_field(self):
        self.assertRaises(cvmfs.UnknownWhitelistLine,
                          cvmfs.Whitelist, self.unknown_field_whitelist)


    def test_invalid_fingerprint(self):
        self.assertRaises(cvmfs.UnknownWhitelistLine,
                          cvmfs.Whitelist, self.invalid_fingerprint_whitelist)


    def test_invalid_timestamp(self):
        self.assertRaises(cvmfs.InvalidWhitelistTimestamp,
                          cvmfs.Whitelist, self.invalid_timestamp_whitelist)


    def test_missing_field(self):
        self.assertRaises(cvmfs.WhitelistValidityError,
                          cvmfs.Whitelist, self.missing_field_whitelist)


    def test_empty_whitelist(self):
        self.assertRaises(cvmfs.WhitelistValidityError,
                          cvmfs.Whitelist, self.no_fingerprints_whitelist)



    def test_missing_signature(self):
        self.assertRaises(cvmfs.IncompleteRootFileSignature,
                          cvmfs.Whitelist, self.missing_signature)


    def test_missing_signature(self):
        self.assertRaises(cvmfs.IncompleteRootFileSignature,
                          cvmfs.Whitelist, self.broken_signature)


    def test_incomplete_signature(self):
        self.assertRaises(cvmfs.IncompleteRootFileSignature,
                          cvmfs.Whitelist, self.incomplete_signature)
