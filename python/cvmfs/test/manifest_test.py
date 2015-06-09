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

class TestManifest(FileSandbox):
    def setUp(self):
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
            '0f41e81ed7faade7ad1dafc4be6fa3f7fdc51b05',
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
            '0f41e81ed7faade7ad1dafc4be6fa3f7fdc51b05',
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
