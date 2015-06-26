#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Created by René Meusel
This file is part of the CernVM File System auxiliary tools.
"""

import unittest
from file_sandbox    import FileSandbox
from mock_repository import MockRepository

import cvmfs


class TestRepositoryWrapper(unittest.TestCase):
    def setUp(self):
        self.sandbox = FileSandbox("py_ut_repo_")
        self.mock_repo = MockRepository()

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
        pubkey = self.sandbox.write_to_temporary(self.cern_public_key)
        self.public_key_file = pubkey

    def tearDown(self):
        del self.mock_repo


    def test_open_repository_http(self):
        self.mock_repo.serve_via_http()
        repo = cvmfs.open_repository(self.mock_repo.url)
        self.assertTrue(isinstance(repo, cvmfs.RemoteRepository))
        self.assertEqual(self.mock_repo.repo_name, repo.manifest.repository_name)
        self.assertEqual(self.mock_repo.url, repo.endpoint)


    def test_open_repository_local(self):
        repo = cvmfs.open_repository(self.mock_repo.dir)
        self.assertTrue(isinstance(repo, cvmfs.LocalRepository))
        self.assertEqual(self.mock_repo.repo_name, repo.manifest.repository_name)
        self.assertEqual(self.mock_repo.dir, repo.endpoint)


    def test_open_repository_verification(self):
        self.mock_repo.make_valid_whitelist()
        self.mock_repo.serve_via_http()
        repo1 = cvmfs.open_repository(self.mock_repo.url,
                                      self.mock_repo.public_key)
        self.assertTrue(isinstance(repo1, cvmfs.RemoteRepository))
        self.assertTrue(repo1.verify(self.mock_repo.public_key))
        self.assertEqual(self.mock_repo.repo_name, repo1.manifest.repository_name)

        repo2 = cvmfs.open_repository(self.mock_repo.dir,
                                      self.mock_repo.public_key)
        self.assertTrue(isinstance(repo2, cvmfs.LocalRepository))
        self.assertTrue(repo2.verify(self.mock_repo.public_key))
        self.assertEqual(self.mock_repo.repo_name, repo2.manifest.repository_name)

        repo3 = cvmfs.open_repository(self.mock_repo.url)
        self.assertTrue(isinstance(repo3, cvmfs.RemoteRepository))
        self.assertTrue(repo3.verify(self.mock_repo.public_key))
        self.assertEqual(self.mock_repo.repo_name, repo3.manifest.repository_name)

        repo4 = cvmfs.open_repository(self.mock_repo.dir)
        self.assertTrue(isinstance(repo4, cvmfs.LocalRepository))
        self.assertTrue(repo4.verify(self.mock_repo.public_key))
        self.assertEqual(self.mock_repo.repo_name, repo4.manifest.repository_name)


    def test_wrong_public_key(self):
        self.mock_repo.make_valid_whitelist()
        self.mock_repo.serve_via_http()
        self.assertRaises(cvmfs.RepositoryVerificationFailed,
                          cvmfs.open_repository,
                          self.mock_repo.url, self.public_key_file)
        self.assertRaises(cvmfs.RepositoryVerificationFailed,
                          cvmfs.open_repository,
                          self.mock_repo.dir, self.public_key_file)


    def test_expired_whitelist(self):
        self.mock_repo.make_expired_whitelist()
        self.mock_repo.serve_via_http()
        self.assertRaises(cvmfs.RepositoryVerificationFailed,
                          cvmfs.open_repository,
                          self.mock_repo.url, self.mock_repo.public_key)
        self.assertRaises(cvmfs.RepositoryVerificationFailed,
                          cvmfs.open_repository,
                          self.mock_repo.dir, self.mock_repo.public_key)


    def test_download_non_existent_file(self):
        self.mock_repo.make_valid_whitelist()
        self.mock_repo.serve_via_http()
        repo = cvmfs.open_repository(self.mock_repo.url,
                                     self.mock_repo.public_key)
        self.assertRaises(cvmfs.FileNotFoundInRepository,
                          repo.retrieve_file,
                          "unobtainium.txt")
