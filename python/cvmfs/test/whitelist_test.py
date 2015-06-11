#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Created by René Meusel
This file is part of the CernVM File System auxiliary tools.
"""

import base64
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
        self.cern_public_key = '\n'.join([
            '-----BEGIN PUBLIC KEY-----',
            'MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAukBusmYyFW8KJxVMmeCj',
            'N7vcU1mERMpDhPTa5PgFROSViiwbUsbtpP9CvfxB/KU1gggdbtWOTZVTQqA3b+p8',
            'g5Vve3/rdnN5ZEquxeEfIG6iEZta9Zei5mZMeuK+DPdyjtvN1wP0982ppbZzKRBu',
            'BbzR4YdrwwWXXNZH65zZuUISDJB4my4XRoVclrN5aGVz4PjmIZFlOJ+ytKsMlegW',
            'SNDwZO9z/YtBFil/Ca8FJhRPFMKdvxK+ezgq+OQWAerVNX7fArMC+4Ya5pF3ASr6',
            '3mlvIsBpejCUBygV4N2pxIcPJu/ZDaikmVvdPTNOTZlIFMf4zIP/YHegQSJmOyVp',
            'HQIDAQAB',
            '-----END PUBLIC KEY-----'
            ''
        ])
        self.public_key_file = self.write_to_temporary(self.cern_public_key)

        self.compressed_certificate = '\n'.join([
            'eJxllLmOq1gQQHO+YnLUMtDG4OAFdwODWc1iIDOYxew0q/n6cU8wyauodHSkUqmWr69PQCIrxj+I',
            '3FxFUhBwyS/8onRFQccSIZB0CNl4CkPLYrtHIGJgwLwaiuoln1cGAtuTAAa77pJVW0Ps2zYm76Kg',
            'nvJWJG++jAJ11hqjU4hRJw1/dx1W9t7Qie5bHQZGrRC2iBup/Xi7Do8Bdsmm42SjTJfwOvY2Q+p+',
            '4fsXmvh/tur7R0SiDFiPgE3vwuA2hoE+h5z/jl98ST0QxL/V/YqZbrWee40/RndpjxxohHe+iD5i',
            'sn9adJQV26F67SKlWBID2ARCG2Aqz4kF8EewO/TJIdBjNEnfUxspTHSCpaem45Yb8syRc9TTIx2+',
            'uFzoWgk0D604y9RQbvIkDius8Njmg9nLHWqP/TTlImm2Q+uxOo5VMDyuomey65ZMoYNy9ZAflRqx',
            'zEi59Ok6CBUOaU8RnRMyewW5AuQjjWuKPGba5GQBXcNbX17opJaP9Perjq2n1p84LJ0NqkSZSdTG',
            'YGm59Gttq4dMzLlHF4usbmzZ2sYX3ZTENYZ25TyEBnZSx3Z+xrnv+RT/KNT67UQ8mI5qxhUnw4sv',
            'c7vlXI+nLUWcxJQ3lWltV2+wOMWhnZBxaxdVBSGj5cTOpplKpS01Jk5hD1M43OJntNy6JQiRkJAS',
            'stkhUiSzcggCKwHA/Xu5KADMzwQIWJkytdPoGgp75OFEep4SeQo6v6rol4rsq+CZmRtfZWScBfm+',
            'BMf4ehh4kFIOdLRzvtepQEf38bK0l+v48NkIG8EG0ltj2NNxSplDLXqXlNi+V7N2pvhLKfpHf9YY',
            'qk7qyIxzQW6Syqqf0ZhIqW+9cLjT6CbNZyxwlpMqluqqfXDPfe5aOeXhwDn+4g5yaFEwYuF5Bm27',
            '6I3/cPKfMRgyvp/gyVwaa17S6soH9n5d70X582Mo+IQux83JaVQcGJM9U/39eOG9bm93etYjiTH4',
            'gj0NUHy+jF7bnPGSvZ+zZ4ZBy77nn37a4XAZXqNys2BYZqJDOeL9wVSZFJmCtyfaUAvWub3Ygee7',
            'HgJ//lD/XT0x8N+f4F81GFZb',
            ''
        ])
        compressed_cert = base64.b64decode(self.compressed_certificate)
        self.certificate_file = self.write_to_temporary(compressed_cert)

        self.sane_whitelist = StringIO.StringIO('\n'.join([
            '20150603095527',
            'E20150704095527',
            'Natlas.cern.ch',
            '9A:C3:7D:9E:C5:EB:CF:C8:92:EF:1D:7C:DB:56:DC:9C:83:9A:15:71 # ATLAS Release Manager, expires Sep 2011',
            'C1:2C:2F:7B:B6:8E:82:CF:50:8A:1D:2B:05:5F:14:1B:69:E6:44:E4 # Jakob Blomer, expires Jul 2011',
            'EA:74:15:E5:F5:A4:34:7D:79:37:37:88:33:6A:1E:0D:D0:A4:18:25 # Steve\'s certificate',
            '--',
            '65a35687479260c90d38e4e114dbc345fde90bbb',
            '-yw\xfc\xa1\xa9\xc4Z%\xfd\x1d\xdf\x17\xa7\xa8\x99sB\x8a\xd9\xe0\xb1?f\xba\x1a\xa61\xa2v_\t\x1bl\x1a\xd9\x7f\n\x01\x9f\xf5\xf8j;\xf6\xaa;\xc7n\xfc\xc8\xa9{\xb5\xe2E.<?G\xfc|[f\'Xd\x05=u]\xc1\t?M\xb4\xab\x164\xc8\x0b\xec-<\xa0\xf6;E\x08\xdb@\xd6\x88!|\xb0f\x9c\xe3\x1a.\x04l\x8b)o\x89i\xdb\\\xf9\xff\xec\xebM\xc3\x87\x98\x8f\xc9\xea\xf1L\xa4\x08\x1b\xe9\xda\xf6\xff\xd1\xd3GN\x82AD\xd9\xc3\xd1\xdf\x17\x06\xd4Ad\xafS"\xc0\x8b\xc3\xeb\xa6\xe9\xaa\xf6\x90\xc7\x11PV\xa5Ls\xb5\x9b\xd2;\xdci\xdb\xa9\x03\x02\xf1\x8c\xb8^l\xb5\xb9\x11\xe1\xa0aM\x17\xe8\x8e\x7f;u\xa5S\xdc\xb2V\xa1\xe0\x91\x14\x02\xee\xe0a\x99\xda\xbc\x9cK\x1d\xa5\xf7\x13$:W\x91\xe6\xe6\x1f\xc4\xd7\x04\xbc\xe0\x86\xcb\x97\x17\x16 \xc0>\x04\x13\xbfr\xc6\xdeYn\xe8t+\xb4#\x05\xa6\xda\xef\xfd\xf6\x9c\xbf'
        ]))
        self.file_whitelist = self.write_to_temporary(self.sane_whitelist.getvalue())
        self.assertNotEqual(None, self.file_whitelist)

        self.insane_whitelist_signature_mismatch = StringIO.StringIO('\n'.join([
            '20150603095527',
            'E20150704095527',
            'Natlas.cern.ch',
            'C1:2C:2F:7B:B6:8E:82:CF:50:8A:1D:2B:05:5F:14:1B:69:E6:44:E4',
            '--',
            '3fa17242c67b5f3e54b85bc6e4544cd9d80a8ac7',
            '-yw\xfc\xa1\xa9\xc4Z%\xfd\x1d\xdf\x17\xa7\xa8\x99sB\x8a\xd9\xe0\xb1?f\xba\x1a\xa61\xa2v_\t\x1bl\x1a\xd9\x7f\n\x01\x9f\xf5\xf8j;\xf6\xaa;\xc7n\xfc\xc8\xa9{\xb5\xe2E.<?G\xfc|[f\'Xd\x05=u]\xc1\t?M\xb4\xab\x164\xc8\x0b\xec-<\xa0\xf6;E\x08\xdb@\xd6\x88!|\xb0f\x9c\xe3\x1a.\x04l\x8b)o\x89i\xdb\\\xf9\xff\xec\xebM\xc3\x87\x98\x8f\xc9\xea\xf1L\xa4\x08\x1b\xe9\xda\xf6\xff\xd1\xd3GN\x82AD\xd9\xc3\xd1\xdf\x17\x06\xd4Ad\xafS"\xc0\x8b\xc3\xeb\xa6\xe9\xaa\xf6\x90\xc7\x11PV\xa5Ls\xb5\x9b\xd2;\xdci\xdb\xa9\x03\x02\xf1\x8c\xb8^l\xb5\xb9\x11\xe1\xa0aM\x17\xe8\x8e\x7f;u\xa5S\xdc\xb2V\xa1\xe0\x91\x14\x02\xee\xe0a\x99\xda\xbc\x9cK\x1d\xa5\xf7\x13$:W\x91\xe6\xe6\x1f\xc4\xd7\x04\xbc\xe0\x86\xcb\x97\x17\x16 \xc0>\x04\x13\xbfr\xc6\xdeYn\xe8t+\xb4#\x05\xa6\xda\xef\xfd\xf6\x9c\xbf'
        ]))

        self.insane_whitelist_broken_signature = StringIO.StringIO('\n'.join([
            '20150603095527',
            'E20150704095527',
            'Natlas.cern.ch',
            '9A:C3:7D:9E:C5:EB:CF:C8:92:EF:1D:7C:DB:56:DC:9C:83:9A:15:71 # ATLAS Release Manager, expires Sep 2011',
            'C1:2C:2F:7B:B6:8E:82:CF:50:8A:1D:2B:05:5F:14:1B:69:E6:44:E4 # Jakob Blomer, expires Jul 2011',
            'EA:74:15:E5:F5:A4:34:7D:79:37:37:88:33:6A:1E:0D:D0:A4:18:25 # Steve\'s certificate',
            '--',
            '65a35687479260c90d38e4e114dbc345fde90bbb',
            '-yx\xfc\xa1\xa9\xc4Z%\xfd\x1d\xdf\x17\xa7\xa8\x99sB\x8a\xd9\xe0\xb1?f\xba\x1a\xa61\xa2v_\t\x1bl\x1a\xd9\x7f\n\x01\x9f\xf5\xf8j;\xf6\xaa;\xc7n\xfc\xc8\xa9{\xb5\xe2E.<?G\xfc|[f\'Xd\x05=u]\xc1\t?M\xb4\xab\x164\xc8\x0b\xec-<\xa0\xf6;E\x08\xdb@\xd6\x88!|\xb0f\x9c\xe3\x1a.\x04l\x8b)o\x89i\xdb\\\xf9\xff\xec\xebM\xc3\x87\x98\x8f\xc9\xea\xf1L\xa4\x08\x1b\xe9\xda\xf6\xff\xd1\xd3GN\x82AD\xd9\xc3\xd1\xdf\x17\x06\xd4Ad\xafS"\xc0\x8b\xc3\xeb\xa6\xe9\xaa\xf6\x90\xc7\x11PV\xa5Ls\xb5\x9b\xd2;\xdci\xdb\xa9\x03\x02\xf1\x8c\xb8^l\xb5\xb9\x11\xe1\xa0aM\x17\xe8\x8e\x7f;u\xa5S\xdc\xb2V\xa1\xe0\x91\x14\x02\xee\xe0a\x99\xda\xbc\x9cK\x1d\xa5\xf7\x13$:W\x91\xe6\xe6\x1f\xc4\xd7\x04\xbc\xe0\x86\xcb\x97\x17\x16 \xc0>\x04\x13\xbfr\xc6\xdeYn\xe8t+\xb4#\x05\xa6\xda\xef\xfd\xf6\x9c\xbf'
            #  ^-- that byte has been tampered with (was 'w')
        ]))

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
            '006982cdd72e5342283e96db5c312ec65ab7f652',
            '-yw????Z%?????sB????f??1?v_ ',
            ''
        ]))

        self.no_fingerprints_whitelist = StringIO.StringIO('\n'.join([
            '20150603095527',
            'E20150704095527',
            'Natlas.cern.ch',
            '--',
            '7ceba0a0533bfae3d1812beac2e87e4ecb684ecd',
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
            '7ceba0a0533bfae3d1812beac2e87e4ecb684ecd',
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


    def test_verify_signature(self):
        whitelist = cvmfs.Whitelist(self.sane_whitelist)
        signature_valid = whitelist.verify_signature(self.public_key_file)
        self.assertTrue(signature_valid)


    def test_verify_mismatching_signature(self):
        whitelist = cvmfs.Whitelist(self.insane_whitelist_signature_mismatch)
        signature_valid = whitelist.verify_signature(self.public_key_file)
        self.assertFalse(signature_valid)


    def test_verify_malformed_signature(self):
        whitelist = cvmfs.Whitelist(self.insane_whitelist_broken_signature)
        signature_valid = whitelist.verify_signature(self.public_key_file)
        self.assertFalse(signature_valid)


    def test_contains_certificate(self):
        with open(self.certificate_file) as cert:
            whitelist   = cvmfs.Whitelist(self.sane_whitelist)
            certificate = cvmfs.Certificate(cert)
            self.assertTrue(whitelist.contains(certificate))


    def test_doesnt_contain_certificate(self):
        with open(self.certificate_file) as cert:
            whitelist   = cvmfs.Whitelist(self.insane_whitelist_signature_mismatch)
            certificate = cvmfs.Certificate(cert)
            self.assertFalse(whitelist.contains(certificate))
