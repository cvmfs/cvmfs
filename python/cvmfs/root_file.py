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
import hashlib

class IncompleteRootFileSignature(Exception):
    def __init__(self, message):
        Exception.__init__(self, message)

class InvalidRootFileSignature(Exception):
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

    @abc.abstractmethod
    def _verify_signature(public_entity):
        pass


    def verify_signature(self, public_entity):
        return self.has_signature and self._verify_signature(public_entity)


    def _hash_over_content(self, file_object):
        pos = file_object.tell()
        hash_sum = hashlib.sha1()
        while True:
            line = file_object.readline()
            if line[0:2] == "--":
                break
            if pos == file_object.tell():
                raise IncompleteRootFileSignature("Signature not found")
            hash_sum.update(line)
            pos = file_object.tell()
        return hash_sum.hexdigest()


    def _read_signature(self, file_object):
        """ Reads the signature's checksum and the binary signature string """
        file_object.seek(0)
        message_digest = self._hash_over_content(file_object)

        self.signature_checksum = file_object.readline().rstrip()
        if len(self.signature_checksum) != 40:
            raise IncompleteRootFileSignature("Signature checksum malformed")
        if message_digest != self.signature_checksum:
            raise InvalidRootFileSignature("Signature checksum doesn't match")
        self.signature = file_object.read()
        if len(self.signature) == 0:
            raise IncompleteRootFileSignature("Binary signature not found")
