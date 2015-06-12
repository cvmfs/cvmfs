#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Created by René Meusel
This file is part of the CernVM File System auxiliary tools.
"""

import ctypes
import tempfile
import zlib
import sqlite3
import subprocess
import shutil


_REPO_CONFIG_PATH      = "/etc/cvmfs/repositories.d"
_SERVER_CONFIG_NAME    = "server.conf"

_MANIFEST_NAME         = ".cvmfspublished"
_WHITELIST_NAME        = ".cvmfswhitelist"
_LAST_REPLICATION_NAME = ".cvmfs_last_snapshot"
_REPLICATING_NAME      = ".cvmfs_is_snapshotting"


class CvmfsNotInstalled(Exception):
    def __init__(self):
        Exception.__init__(self, "It seems that cvmfs is not installed on this machine!")


class CompressedObject:
    file_            = None
    compressed_file_ = None

    def __init__(self, compressed_file):
        self.compressed_file_ = compressed_file
        self._decompress()

    def get_compressed_file(self):
        return self.compressed_file_

    def get_uncompressed_file(self):
        return self.file_

    def save_to(self, path):
        shutil.copyfile(self.get_compressed_file().name, path)

    def save_uncompressed_to(self, path):
        shutil.copyfile(self.get_uncompressed_file().name, path)

    def _decompress(self):
        """ Unzip a file to a temporary referenced by self.file_ """
        self.file_ = tempfile.NamedTemporaryFile('w+b')
        self.compressed_file_.seek(0)
        self.file_.write(zlib.decompress(self.compressed_file_.read()))
        self.file_.flush()
        self.file_.seek(0)
        self.compressed_file_.seek(0)

    def _close(self):
        if self.file_:
            self.file_.close()
        if self.compressed_file_:
            self.compressed_file_.close()


class FileObject(CompressedObject):
    def __init__(self, compressed_file):
        CompressedObject.__init__(self, compressed_file)

    def file(self):
        return self.get_uncompressed_file()
