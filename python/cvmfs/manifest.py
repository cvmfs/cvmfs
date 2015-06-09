#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Created by René Meusel
This file is part of the CernVM File System auxiliary tools.
"""

from datetime import datetime
from dateutil.tz import tzutc

from root_file import RootFile

class UnknownManifestField(Exception):
    def __init__(self, key_char):
        self.key_char = key_char

    def __str__(self):
        return self.key_char

class ManifestValidityError(Exception):
    def __init__(self, message):
        Exception.__init__(self, message)


class Manifest(RootFile):
    """ Wraps information from .cvmfspublished """

    @staticmethod
    def open(manifest_path):
        """ Initializes a Manifest from a local file path """
        with open(manifest_path) as manifest_file:
            return Manifest(manifest_file)

    def __init__(self, manifest_file):
        RootFile.__init__(self, manifest_file)


    def __str__(self):
        return "<Manifest for " + self.repository_name + ">"


    def __repr__(self):
        return self.__str__()


    def has_history(self):
        return hasattr(self, 'history_database')


    def _read_line(self, line):
        """ Parse lines that appear in .cvmfspublished """
        key_char = line[0]
        data     = line[1:-1]
        if   key_char == "C":
            self.root_catalog        = data
        elif key_char == "R":
            self.root_hash           = data
        elif key_char == "B":
            self.root_catalog_size   = int(data)
        elif key_char == "X":
            self.certificate         = data
        elif key_char == "H":
            self.history_database    = data
        elif key_char == "T":
            self.last_modified       = datetime.fromtimestamp(int(data), tz=tzutc())
        elif key_char == "D":
            self.ttl                 = int(data)
        elif key_char == "S":
            self.revision            = int(data)
        elif key_char == "N":
            self.repository_name     = data
        elif key_char == "L":
            self.micro_catalog       = data
        elif key_char == "G":
            self.garbage_collectable = (data == "yes")
        else:
            raise UnknownManifestField(key_char)


    def _check_validity(self):
        """ Checks that all mandatory fields are found in .cvmfspublished """
        if not hasattr(self, 'root_catalog'):
            raise ManifestValidityError("Manifest lacks a root catalog entry")
        if not hasattr(self, 'root_hash'):
            raise ManifestValidityError("Manifest lacks a root hash entry")
        if not hasattr(self, 'ttl'):
            raise ManifestValidityError("Manifest lacks a TTL entry")
        if not hasattr(self, 'revision'):
            raise ManifestValidityError("Manifest lacks a revision entry")
        if not hasattr(self, 'repository_name'):
            raise ManifestValidityError("Manifest lacks a repository name")
