#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Created by René Meusel
This file is part of the CernVM File System auxiliary tools.
"""

from datetime import datetime
from dateutil.tz import tzutc

import re

from root_file import RootFile

class UnknownWhitelistLine(Exception):
    def __init__(self, line):
        Exception.__init__(self, line)

class WhitelistValidityError(Exception):
    def __init__(self, message):
        Exception.__init__(self, message)

class InvalidWhitelistTimestamp(Exception):
    def __init__(self, timestamp):
        Exception.__init__(self, timestamp)


class Whitelist(RootFile):
    """ Wraps information from .cvmfswhitelist """

    @staticmethod
    def open(whitelist_path):
        """ Initializes a whitelist from a local file path """
        with open(whitelist_path) as manifest_file:
            return Whitelist(manifest_file)

    _fingerprint_re = None
    _timestamp_re   = None

    def __init__(self, whitelist_file):
        self.fingerprints = []
        self._fingerprint_re = re.compile('^(([0-9A-F]{2}:){19}[0-9A-F]{2}).*')
        self._timestamp_re   = re.compile('[0-9]{14}')
        RootFile.__init__(self, whitelist_file)

    def __str__(self):
        return "<Whitelist for " + self.repository_name + ">"

    def __repr__(self):
        return self.__str__()


    def _read_line(self, line):
        """ Parse lines that appear in .cvmfswhitelist """
        # Note: .cvmfswhitelist contains a last_modified field that does not
        #       have a key_char. However, it is a timestamp starting with the
        #       full year. We use '2' as the key, assuming that CernVM-FS will
        #       not sustain the next 1000 years.
        #
        # Note: the whitelist contains a list of certificate fingerprints that
        #       are not prepended by a key either. We use a regex to detect them
        key_char = line[0]
        data     = line[1:-1]
        match    = self._fingerprint_re.search(line[:-1]) # full line!
        if match:
            self.fingerprints.append(match.groups(1))
        elif key_char == "2":
            self.last_modified   = self._read_timestamp(line[:-1]) # full line!!
        elif key_char == "E":
            self.expires         = self._read_timestamp(data)
        elif key_char == "N":
            self.repository_name = data
        else:
            raise UnknownWhitelistLine(line.strip())


    def _check_validity(self):
        """ Checks that all mandatory fields are found in .cvmfspublished """
        if not hasattr(self, 'last_modified'):
            raise WhitelistValidityError("Whitelist without a timestamp")
        if not hasattr(self, 'expires'):
            raise WhitelistValidityError("Whitelist without expiry date")
        if not hasattr(self, 'repository_name'):
            raise WhitelistValidityError("Whitelist without repository name")
        if len(self.fingerprints) == 0:
            raise WhitelistValidityError("No fingerprints are white-listed")


    def _read_timestamp(self, timestamp):
        if not self._timestamp_re.match(timestamp):
            raise InvalidWhitelistTimestamp(timestamp)
        return datetime(
                    year   = int(timestamp[0:4]),
                    month  = int(timestamp[4:6]),
                    day    = int(timestamp[6:8]),
                    hour   = int(timestamp[8:10]),
                    minute = int(timestamp[10:12]),
                    second = int(timestamp[12:14]),
                    tzinfo = tzutc())

