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

class TestManifest(FileSandbox):
    def setUp(self):
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

        self.sane_manifest = StringIO.StringIO('\n'.join([
            'C600230b0ba7620426f2e898f1e1f43c5466efe59',
            'D3600',
            'L0000000000000000000000000000000000000000',
            'Natlas.cern.ch',
            'Rd41d8cd98f00b204e9800998ecf8427e',
            'S4264',
            'T1390395640',
            'X0b457ac12225018e0a15330364c20529e15012ab',
            'B12154365',
            'H8296cd873f8cb00d45fb4fd62a003e711ef06bc5',
            'Gno',
            '--',
            '90d52f3d6d29ce142a75949f815f05580af61974',
            '(§3Êõ0ð¬a˜‚Û}Y„¨x3q    ·EÖ£%²é³üŽ6Ö+>¤XâñÅ=_X‡Ä'
        ]))
        self.file_manifest = self.write_to_temporary(self.sane_manifest.getvalue())
        self.assertNotEqual(None, self.file_manifest)

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


    def temporary_prefix(self):
        return "py_manifest_ut_"


    def test_manifest_creation(self):
        manifest = cvmfs.Manifest(self.sane_manifest)
        last_modified = datetime.datetime(2014, 1, 22, 13, 0, 40, tzinfo=tzutc())
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
        self.assertEqual('600230b0ba7620426f2e898f1e1f43c5466efe59', manifest.root_catalog)
        self.assertEqual(3600                                      , manifest.ttl)
        self.assertEqual('0000000000000000000000000000000000000000', manifest.micro_catalog)
        self.assertEqual('atlas.cern.ch'                           , manifest.repository_name)
        self.assertEqual('d41d8cd98f00b204e9800998ecf8427e'        , manifest.root_hash)
        self.assertEqual(4264                                      , manifest.revision)
        self.assertEqual(last_modified                             , manifest.last_modified)
        self.assertEqual('0b457ac12225018e0a15330364c20529e15012ab', manifest.certificate)
        self.assertEqual(12154365                                  , manifest.root_catalog_size)
        self.assertEqual('8296cd873f8cb00d45fb4fd62a003e711ef06bc5', manifest.history_database)
        self.assertFalse(manifest.garbage_collectable)


    def test_mainfest_from_file(self):
        manifest = cvmfs.Manifest.open(self.file_manifest)
        last_modified = datetime.datetime(2014, 1, 22, 13, 0, 40, tzinfo=tzutc())
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
        self.assertEqual('600230b0ba7620426f2e898f1e1f43c5466efe59', manifest.root_catalog)
        self.assertEqual(3600                                      , manifest.ttl)
        self.assertEqual('0000000000000000000000000000000000000000', manifest.micro_catalog)
        self.assertEqual('atlas.cern.ch'                           , manifest.repository_name)
        self.assertEqual('d41d8cd98f00b204e9800998ecf8427e'        , manifest.root_hash)
        self.assertEqual(4264                                      , manifest.revision)
        self.assertEqual(last_modified                             , manifest.last_modified)
        self.assertEqual('0b457ac12225018e0a15330364c20529e15012ab', manifest.certificate)
        self.assertEqual(12154365                                  , manifest.root_catalog_size)
        self.assertEqual('8296cd873f8cb00d45fb4fd62a003e711ef06bc5', manifest.history_database)
        self.assertFalse(manifest.garbage_collectable)


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
