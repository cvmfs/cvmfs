#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Created by René Meusel
This file is part of the CernVM File System auxiliary tools.
"""

import datetime

class UnknownManifestField:
    def __init__(self, key_char):
        self.key_char = key_char

    def __str__(self):
        return self.key_char

class ManifestValidityError:
    def __init__(self, message):
        Exception.__init__(self, message)


class Manifest:
    """ Wraps information from .cvmfspublished"""

    def __init__(self, manifest_file):
        """ Initializes a Manifest object from a file pointer to .cvmfspublished """
        for line in manifest_file.readlines():
            if len(line) == 0:
                continue
            if line[0:2] == "--":
                break
            self._read_line(line)
        self._check_validity()


    def __str__(self):
        return "<Manifest for " + self.repository_name + ">"


    def __repr__(self):
        return self.__str__()


    def _read_line(self, line):
        """ Parse lines that appear in .cvmfspublished """
        key_char = line[0]
        data     = line[1:-1]
        if   key_char == "C":
            self.root_catalog     = data
        elif key_char == "X":
            self.certificate      = data
        elif key_char == "H":
            self.history_database = data
        elif key_char == "T":
            self.last_modified    = datetime.datetime.fromtimestamp(int(data))
        elif key_char == "R":
            self.root_hash        = data
        elif key_char == "D":
            self.ttl              = int(data)
        elif key_char == "S":
            self.revision         = int(data)
        elif key_char == "N":
            self.repository_name  = data
        elif key_char == "L":
            self.unknown_field1   = data # TODO: ask Jakob what L means
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
