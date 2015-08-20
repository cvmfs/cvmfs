#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Created by René Meusel
This file is part of the CernVM File System auxiliary tools.
"""

import ctypes
import sqlite3
import subprocess
import os


_REPO_CONFIG_PATH      = "/etc/cvmfs/repositories.d"
_SERVER_CONFIG_NAME    = "server.conf"

_REST_CONNECTOR        = "control"

_MANIFEST_NAME         = ".cvmfspublished"
_WHITELIST_NAME        = ".cvmfswhitelist"
_LAST_REPLICATION_NAME = ".cvmfs_last_snapshot"
_REPLICATING_NAME      = ".cvmfs_is_snapshotting"


class CvmfsNotInstalled(Exception):
    def __init__(self):
        Exception.__init__(self, "It seems that cvmfs is not installed on this machine!")



class DatabaseObject:
    _db_handle = None

    def __init__(self, db_file):
        self._file = db_file
        self._open_database()

    def __del__(self):
        self._file.close()

    def _open_database(self):
        """ Create and configure a database handle to the Catalog """
        self._db_handle = sqlite3.connect(self._file.name)
        self._db_handle.text_factory = str

    def db_size(self):
        return os.path.getsize(self._file.name)

    def read_properties_table(self, reader):
        """ Retrieve all properties stored in the 'properties' table """
        props = self.run_sql("SELECT key, value FROM properties;")
        for prop in props:
            prop_key   = prop[0]
            prop_value = prop[1]
            reader(prop_key, prop_value)

    def run_sql(self, sql):
        """ Run an arbitrary SQL query on the catalog database """
        cursor = self._db_handle.cursor()
        cursor.execute(sql)
        return cursor.fetchall()

    def open_interactive(self):
        """ Spawns a sqlite shell for interactive catalog database inspection """
        subprocess.call(['sqlite3', self._file.name])



def _binary_buffer_to_hex_string(binbuf):
    return "".join(map(lambda c: ("%0.2X" % c).lower(),map(ord,binbuf)))

def _split_md5(md5digest):
    hi = lo = 0
    for i in range(0, 8):
        lo |= (ord(md5digest[i]) << (i * 8))
    for i in range(8,16):
        hi |= (ord(md5digest[i]) << ((i - 8) * 8))
    return ctypes.c_int64(lo).value, ctypes.c_int64(hi).value  # signed int!

def _combine_md5(lo, hi):
    md5digest = [ '\x00','\x00','\x00','\x00','\x00','\x00','\x00','\x00',
                  '\x00','\x00','\x00','\x00','\x00','\x00','\x00','\x00' ]
    for i in range(0, 8):
        md5digest[i] = chr(lo & 0xFF)
        lo >>= 8
    for i in range(8,16):
        md5digest[i] = chr(hi & 0xFF)
        hi >>= 8
    return ''.join(md5digest)
