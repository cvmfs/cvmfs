#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Created by René Meusel
This file is part of the CernVM File System auxiliary tools.

A CernVM-FS repository has essential 'root files' that have a defined name and
serve as entry points into the repository.

Namely the manifest (.cvmfspublished) and the whitelist (.cvmfswhitelist) that
both have class representations inheriting from RootFile and implementing the
abstract methods defined here.

Any 'root file' in CernVM-FS is a signed list of line-by-line key-value pairs
where the key is represented by a single character in the beginning of a line
directly followed by the value. The key-value part of the file is terminted
either by EOF or by a termination line (--) followed by a signature.

The signature follows directly after the termination line with a hash of the
key-value line content (without the termination line) followed by an \n and a
binary string containing the private-key signature terminated by EOF.
"""

import abc

class IncompleteRootFileSignature(Exception):
    def __init__(self, message):
        Exception.__init__(self, message)


class RootFile:
    """ Base class for CernVM-FS repository's signed 'root files' """

    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def _read_line(self, line):
        pass

    @abc.abstractmethod
    def _check_validity(self):
        pass

    @abc.abstractmethod
    def __init__(self, file_object):
        """ Initializes a root file object from a file pointer """
        self.has_signature = False
        for line in file_object.readlines():
            if len(line) == 0:
                continue
            if line[0:2] == "--":
                self.has_signature = True
                break
            self._read_line(line)
        if self.has_signature:
            self._read_signature(file_object)
        self._check_validity()


    def _read_signature(self, manifest_file):
        """ Reads the signature's checksum and the binary signature string """
        manifest_file.seek(0)
        pos = manifest_file.tell()
        while True:
            line = manifest_file.readline()
            if line[0:2] == "--":
                break
            if pos == manifest_file.tell():
                raise IncompleteRootFileSignature("Signature not found")
            pos = manifest_file.tell()

        self.signature_checksum = manifest_file.readline().rstrip()
        if len(self.signature_checksum) != 40:
            raise IncompleteRootFileSignature("Signature checksum malformed")
        self.signature = manifest_file.read()
        if len(self.signature) == 0:
            raise IncompleteRootFileSignature("Binary signature not found")
