#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Created by René Meusel
This file is part of the CernVM File System auxiliary tools.
"""

import ctypes


_REPO_CONFIG_PATH      = "/etc/cvmfs/repositories.d"
_SERVER_CONFIG_NAME    = "server.conf"

_REST_CONNECTOR        = "control"

_MANIFEST_NAME         = ".cvmfspublished"
_LAST_REPLICATION_NAME = ".cvmfs_last_snapshot"
_REPLICATING_NAME      = ".cvmfs_is_snapshotting"


class CvmfsNotInstalled(Exception):
    def __init__(self):
        Exception.__init__(self, "It seems that cvmfs is not installed on this machine!")

def _split_md5(md5digest):
    hi = lo = 0
    for i in range(0, 8):
        lo = lo | (ord(md5digest[i]) << (i * 8))
    for i in range(8,16):
        hi = hi | (ord(md5digest[i]) << ((i - 8) * 8))
    return ctypes.c_int64(lo).value, ctypes.c_int64(hi).value  # signed int!
