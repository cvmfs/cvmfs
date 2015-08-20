#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Created by René Meusel
This file is part of the CernVM File System auxiliary tools.
"""

import base64
import datetime
import StringIO
import unittest
import zlib

from dateutil.tz import tzutc

from file_sandbox import FileSandbox

import cvmfs

class TestManifest(unittest.TestCase):
    def setUp(self):
        self.sandbox = FileSandbox("py_manifest_ut_")

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
        self.certificate_file = self.sandbox.write_to_temporary(zlib.decompress(compressed_cert))

        self.sane_manifest = StringIO.StringIO('\n'.join([
            'C044206fcff4545283aaa452b80edfd5d8c740b20',
            'B75834368',
            'Rd41d8cd98f00b204e9800998ecf8427e',
            'D900',
            'S8722',
            'Natlas.cern.ch',
            'X0b457ac12225018e0a15330364c20529e15012ab',
            'H50c37f5517aea2ce9a22c3f17a7056a4f60e7d07',
            'T1433937750',
            '--',
            'e7d58bfaaf75e9b725aa23c3a666daffd6351b8f',
            '"P4\x99a>LR\x8eE\x91\xdb\xf13\xd9\xec!\xfe\x81\xf4\x1d\x98\xbe\xa7\x80:D\xcd0\x12\x06\x84\x0c(\x89\xe3\x01\x03s\x02\r\x14\xe3\x00{E\x18\x11E/\xa1)\xd7\xc7\x1a\x7f\xf1k"\x08\xbf\xdb3\xa1\xec-8\xb3z\xd5\x95\xcel\x82\x8a\x9a\xb5\xb6\x14\xd8sD\x80\xa7X\x0fx\x03T\x07\x12\xa6\'E\x04\x06\xf9\x17\'s\xeb\xfe\x19`l\xe7\xf4\x96,2\x84-\xa0\xbd.\x86#\xe0\xc09l\xc0\xcbZ\x95\x14# \xc4\xc7\xe1\x00\xc0\x84>\x8a\xae\x86\xc0\xe5\xa82\xc9\x86\xe5\x19\xe7\x85n\xac\xb4\xd8\x0bX\x81q\xdb\x97q\xe8\xacz\xd5\xfa+\n(\xe1\x0c\xff.\x91\xa2\x00\xfa"\xa5IS\xc5\xac\x13&\xda\x96+\xc6mU3\xb0\xd8\x92)Jd\xc14O\x02Gd\x90!\xdf\x06\x9f\xf4\xa5\xd2y\xab\x8c\xf6\x13\xe0d)\x90\xd7\x14\xb5\xf2f%\x80D\x94\xe0d\xb8\xe3\x17\xc4\x0f\xa5\x14\x08\xf4x\xd4\x7f\xa0T\xd4\xb9\xc0\x81d\x9bD\xe8V\xe5\x90\x16'
        ]))
        self.file_manifest = self.sandbox.write_to_temporary(self.sane_manifest.getvalue())
        self.assertNotEqual(None, self.file_manifest)

        self.insane_manifest_tampered = StringIO.StringIO('\n'.join([
            'C044206fcff4545283aaa452b80edfd5d8c740b20',
            'B75834368',
            'Rd41d8cd98f00b204e9800998ecf8427e',
            'D900',
            'S8722',
            'Natlas-malicious.cern.ch',
            'X0b457ac12225018e0a15330364c20529e15012ab',
            'H50c37f5517aea2ce9a22c3f17a7056a4f60e7d07',
            'T1433937750',
            '--',
            '892f6997d6046669640b8520842bc7c4f17cf269',
            '"P4\x99a>LR\x8eE\x91\xdb\xf13\xd9\xec!\xfe\x81\xf4\x1d\x98\xbe\xa7\x80:D\xcd0\x12\x06\x84\x0c(\x89\xe3\x01\x03s\x02\r\x14\xe3\x00{E\x18\x11E/\xa1)\xd7\xc7\x1a\x7f\xf1k"\x08\xbf\xdb3\xa1\xec-8\xb3z\xd5\x95\xcel\x82\x8a\x9a\xb5\xb6\x14\xd8sD\x80\xa7X\x0fx\x03T\x07\x12\xa6\'E\x04\x06\xf9\x17\'s\xeb\xfe\x19`l\xe7\xf4\x96,2\x84-\xa0\xbd.\x86#\xe0\xc09l\xc0\xcbZ\x95\x14# \xc4\xc7\xe1\x00\xc0\x84>\x8a\xae\x86\xc0\xe5\xa82\xc9\x86\xe5\x19\xe7\x85n\xac\xb4\xd8\x0bX\x81q\xdb\x97q\xe8\xacz\xd5\xfa+\n(\xe1\x0c\xff.\x91\xa2\x00\xfa"\xa5IS\xc5\xac\x13&\xda\x96+\xc6mU3\xb0\xd8\x92)Jd\xc14O\x02Gd\x90!\xdf\x06\x9f\xf4\xa5\xd2y\xab\x8c\xf6\x13\xe0d)\x90\xd7\x14\xb5\xf2f%\x80D\x94\xe0d\xb8\xe3\x17\xc4\x0f\xa5\x14\x08\xf4x\xd4\x7f\xa0T\xd4\xb9\xc0\x81d\x9bD\xe8V\xe5\x90\x16'
        ]))

        self.insane_manifest_broken_signature = StringIO.StringIO('\n'.join([
            'C044206fcff4545283aaa452b80edfd5d8c740b20',
            'B75834368',
            'Rd41d8cd98f00b204e9800998ecf8427e',
            'D900',
            'S8722',
            'Natlas.cern.ch',
            'X0b457ac12225018e0a15330364c20529e15012ab',
            'H50c37f5517aea2ce9a22c3f17a7056a4f60e7d07',
            'T1433937750',
            '--',
            'e7d58bfaaf75e9b725aa23c3a666daffd6351b8f',
            '"P4\x99a>LR\x8eE\x91\xeb\xf13\xd9\xec!\xfe\x81\xf4\x1d\x98\xbe\xa7\x80:D\xcd0\x12\x06\x84\x0c(\x89\xe3\x01\x03s\x02\r\x14\xe3\x00{E\x18\x11E/\xa1)\xd7\xc7\x1a\x7f\xf1k"\x08\xbf\xdb3\xa1\xec-8\xb3z\xd5\x95\xcel\x82\x8a\x9a\xb5\xb6\x14\xd8sD\x80\xa7X\x0fx\x03T\x07\x12\xa6\'E\x04\x06\xf9\x17\'s\xeb\xfe\x19`l\xe7\xf4\x96,2\x84-\xa0\xbd.\x86#\xe0\xc09l\xc0\xcbZ\x95\x14# \xc4\xc7\xe1\x00\xc0\x84>\x8a\xae\x86\xc0\xe5\xa82\xc9\x86\xe5\x19\xe7\x85n\xac\xb4\xd8\x0bX\x81q\xdb\x97q\xe8\xacz\xd5\xfa+\n(\xe1\x0c\xff.\x91\xa2\x00\xfa"\xa5IS\xc5\xac\x13&\xda\x96+\xc6mU3\xb0\xd8\x92)Jd\xc14O\x02Gd\x90!\xdf\x06\x9f\xf4\xa5\xd2y\xab\x8c\xf6\x13\xe0d)\x90\xd7\x14\xb5\xf2f%\x80D\x94\xe0d\xb8\xe3\x17\xc4\x0f\xa5\x14\x08\xf4x\xd4\x7f\xa0T\xd4\xb9\xc0\x81d\x9bD\xe8V\xe5\x90\x16'
            #                      ^-- this byte used to be a 'd'
        ]))

        self.full_manifest = StringIO.StringIO('\n'.join([
            'C044206fcff4545283aaa452b80edfd5d8c740b20',
            'B75834368',
            'Rd41d8cd98f00b204e9800998ecf8427e',
            'D900',
            'L0000000000000000000000000000000000000000',
            'S8722',
            'Natlas.cern.ch',
            'X0b457ac12225018e0a15330364c20529e15012ab',
            'H50c37f5517aea2ce9a22c3f17a7056a4f60e7d07',
            'T1433937750',
            'Gno',
            '--',
            'd79461faeaedc6875ee1592967811deee3fe2b99',
            'invalid signature'
        ]))

        self.unknown_field_manifest = StringIO.StringIO('\n'.join([
            'C600230b0ba7620426f2e898f1e1f43c5466efe59',
            'D3600',
            'L0000000000000000000000000000000000000000',
            'Natlas.cern.ch',
            'Rd41d8cd98f00b204e9800998ecf8427e',
            'S4264',
            'Qi_am_unexpected!',
            ''
        ]))

        self.minimal_manifest_entries = [
            'C600230b0ba7620426f2e898f1e1f43c5466efe59',
            'Rd41d8cd98f00b204e9800998ecf8427e',
            'D3600',
            'S4264',
            'Natlas.cern.ch'
        ]

        self.minimal_manifest = StringIO.StringIO(
            '\n'.join(self.minimal_manifest_entries) + '\n')

        self.missing_signature = StringIO.StringIO('\n'.join([
            'C600230b0ba7620426f2e898f1e1f43c5466efe59',
            'Rd41d8cd98f00b204e9800998ecf8427e',
            'D3600',
            'S4264',
            'Natlas.cern.ch',
            '--',
            ''
        ]))

        self.broken_signature = StringIO.StringIO('\n'.join([
            'C600230b0ba7620426f2e898f1e1f43c5466efe59',
            'Rd41d8cd98f00b204e9800998ecf8427e',
            'D3600',
            'S4264',
            'Natlas.cern.ch',
            '--',
            'foobar',
            ''
        ]))

        self.incomplete_signature = StringIO.StringIO('\n'.join([
            'C600230b0ba7620426f2e898f1e1f43c5466efe59',
            'Rd41d8cd98f00b204e9800998ecf8427e',
            'D3600',
            'S4264',
            'Natlas.cern.ch',
            '--',
            'b748926022513a4398743d31c49578bc3a5fc3ef',
            ''
        ]))


    def test_manifest_creation(self):
        manifest = cvmfs.Manifest(self.sane_manifest)
        last_modified = datetime.datetime(2015, 6, 10, 12, 2, 30, tzinfo=tzutc())
        self.assertTrue(hasattr(manifest, 'root_catalog'))
        self.assertTrue(hasattr(manifest, 'ttl'))
        self.assertTrue(hasattr(manifest, 'repository_name'))
        self.assertTrue(hasattr(manifest, 'root_hash'))
        self.assertTrue(hasattr(manifest, 'revision'))
        self.assertTrue(hasattr(manifest, 'last_modified'))
        self.assertTrue(hasattr(manifest, 'certificate'))
        self.assertTrue(hasattr(manifest, 'root_catalog_size'))
        self.assertTrue(hasattr(manifest, 'history_database'))
        self.assertEqual('044206fcff4545283aaa452b80edfd5d8c740b20', manifest.root_catalog)
        self.assertEqual(900                                       , manifest.ttl)
        self.assertEqual('atlas.cern.ch'                           , manifest.repository_name)
        self.assertEqual('d41d8cd98f00b204e9800998ecf8427e'        , manifest.root_hash)
        self.assertEqual(8722                                      , manifest.revision)
        self.assertEqual(last_modified                             , manifest.last_modified)
        self.assertEqual('0b457ac12225018e0a15330364c20529e15012ab', manifest.certificate)
        self.assertEqual(75834368                                  , manifest.root_catalog_size)
        self.assertEqual('50c37f5517aea2ce9a22c3f17a7056a4f60e7d07', manifest.history_database)


    def test_mainfest_from_file(self):
        manifest = cvmfs.Manifest.open(self.file_manifest)
        last_modified = datetime.datetime(2015, 6, 10, 12, 2, 30, tzinfo=tzutc())
        self.assertTrue(hasattr(manifest, 'root_catalog'))
        self.assertTrue(hasattr(manifest, 'ttl'))
        self.assertTrue(hasattr(manifest, 'repository_name'))
        self.assertTrue(hasattr(manifest, 'root_hash'))
        self.assertTrue(hasattr(manifest, 'revision'))
        self.assertTrue(hasattr(manifest, 'last_modified'))
        self.assertTrue(hasattr(manifest, 'certificate'))
        self.assertTrue(hasattr(manifest, 'root_catalog_size'))
        self.assertTrue(hasattr(manifest, 'history_database'))
        self.assertEqual('044206fcff4545283aaa452b80edfd5d8c740b20', manifest.root_catalog)
        self.assertEqual(900                                       , manifest.ttl)
        self.assertEqual('atlas.cern.ch'                           , manifest.repository_name)
        self.assertEqual('d41d8cd98f00b204e9800998ecf8427e'        , manifest.root_hash)
        self.assertEqual(8722                                      , manifest.revision)
        self.assertEqual(last_modified                             , manifest.last_modified)
        self.assertEqual('0b457ac12225018e0a15330364c20529e15012ab', manifest.certificate)
        self.assertEqual(75834368                                  , manifest.root_catalog_size)
        self.assertEqual('50c37f5517aea2ce9a22c3f17a7056a4f60e7d07', manifest.history_database)


    def test_full_manifest(self):
        manifest = cvmfs.Manifest(self.full_manifest)
        last_modified = datetime.datetime(2015, 6, 10, 12, 2, 30, tzinfo=tzutc())
        self.assertTrue(hasattr(manifest, 'root_catalog'))
        self.assertTrue(hasattr(manifest, 'ttl'))
        self.assertTrue(hasattr(manifest, 'micro_catalog'))
        self.assertTrue(hasattr(manifest, 'repository_name'))
        self.assertTrue(hasattr(manifest, 'root_hash'))
        self.assertTrue(hasattr(manifest, 'revision'))
        self.assertTrue(hasattr(manifest, 'last_modified'))
        self.assertTrue(hasattr(manifest, 'certificate'))
        self.assertTrue(hasattr(manifest, 'root_catalog_size'))
        self.assertTrue(hasattr(manifest, 'history_database'))
        self.assertTrue(hasattr(manifest, 'garbage_collectable'))
        self.assertEqual('044206fcff4545283aaa452b80edfd5d8c740b20', manifest.root_catalog)
        self.assertEqual(900                                       , manifest.ttl)
        self.assertEqual('0000000000000000000000000000000000000000', manifest.micro_catalog)
        self.assertEqual('atlas.cern.ch'                           , manifest.repository_name)
        self.assertEqual('d41d8cd98f00b204e9800998ecf8427e'        , manifest.root_hash)
        self.assertEqual(8722                                      , manifest.revision)
        self.assertEqual(last_modified                             , manifest.last_modified)
        self.assertEqual('0b457ac12225018e0a15330364c20529e15012ab', manifest.certificate)
        self.assertEqual(75834368                                  , manifest.root_catalog_size)
        self.assertEqual('50c37f5517aea2ce9a22c3f17a7056a4f60e7d07', manifest.history_database)
        self.assertFalse(                                             manifest.garbage_collectable)


    def test_minimal_manifest(self):
        manifest = cvmfs.Manifest(self.minimal_manifest)
        self.assertTrue(hasattr(manifest, 'root_catalog'))
        self.assertTrue(hasattr(manifest, 'root_hash'))
        self.assertTrue(hasattr(manifest, 'ttl'))
        self.assertTrue(hasattr(manifest, 'revision'))
        self.assertTrue(hasattr(manifest, 'repository_name'))
        self.assertEqual('600230b0ba7620426f2e898f1e1f43c5466efe59', manifest.root_catalog)
        self.assertEqual('d41d8cd98f00b204e9800998ecf8427e'        , manifest.root_hash)
        self.assertEqual(3600                                      , manifest.ttl)
        self.assertEqual(4264                                      , manifest.revision)
        self.assertEqual('atlas.cern.ch'                           , manifest.repository_name)


    def test_unknown_manifest_field(self):
        self.assertRaises(cvmfs.UnknownManifestField,
                          cvmfs.Manifest, self.unknown_field_manifest)


    def test_invalid_manifest(self):
        for i in range(len(self.minimal_manifest_entries)):
            incomplete_manifest_entries = list(self.minimal_manifest_entries)
            del incomplete_manifest_entries[i]
            incomplete_manifest = StringIO.StringIO('\n'.join(incomplete_manifest_entries))
            self.assertRaises(cvmfs.ManifestValidityError,
                              cvmfs.Manifest, incomplete_manifest)


    def test_missing_signature(self):
        self.assertRaises(cvmfs.IncompleteRootFileSignature,
                          cvmfs.Manifest, self.missing_signature)


    def test_missing_signature(self):
        self.assertRaises(cvmfs.IncompleteRootFileSignature,
                          cvmfs.Manifest, self.broken_signature)


    def test_incomplete_signature(self):
        self.assertRaises(cvmfs.IncompleteRootFileSignature,
                          cvmfs.Manifest, self.incomplete_signature)


    def test_verify_signature(self):
        manifest = cvmfs.Manifest(self.sane_manifest)
        cert = cvmfs.Certificate(open(self.certificate_file))
        is_valid = manifest.verify_signature(cert)
        self.assertTrue(is_valid)


    def test_verify_invalid_signature(self):
        manifest = cvmfs.Manifest(self.insane_manifest_tampered)
        cert = cvmfs.Certificate(open(self.certificate_file))
        is_valid = manifest.verify_signature(cert)
        self.assertFalse(is_valid)


    def test_verify_inconsistent_signature(self):
        manifest = cvmfs.Manifest(self.insane_manifest_broken_signature)
        cert = cvmfs.Certificate(open(self.certificate_file))
        is_valid = manifest.verify_signature(cert)
        self.assertFalse(is_valid)
