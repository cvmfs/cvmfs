#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Created by René Meusel
This file is part of the CernVM File System auxiliary tools.
"""

from root_file    import IncompleteRootFileSignature
from manifest     import *
from whitelist    import *
from repository   import *
from availability import *
from _common      import _split_md5
from _common      import _combine_md5

import subprocess
import re

class ServerNotInstalled(Exception):
    pass

class ClientNotInstalled(Exception):
    pass

class VersionNotDetected(Exception):
    def __init__(self, input_string):
        self.input_string = input_string


def __extract_version_string(input_str):
    match = re.search('.*([0-9][0-9]*\.[0-9][0-9]*\.[0-9][0-9]*).*', input_str)
    if not match or len(match.groups()) != 1:
        raise VersionNotDetected(input_str)
    return match.groups()[0]


def check_output(popen_args):
    """ instead of using subprocess.check_output that was
        introduced with python 2.7                        """
    return subprocess.Popen(popen_args,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT).communicate()[0]


def _get_server_version():
    output = ''
    try:
        output = check_output(['cvmfs_server'])
        return __extract_version_string(output)
    except OSError, e:
        raise ServerNotInstalled()


def _get_client_version():
    output = ''
    try:
        output = check_output(['cvmfs2', '--version'])
    except OSError, e:
        raise ClientNotInstalled()
    return __extract_version_string(output)


has_server = True
has_client = True
server_version = None
client_version = None


try:
    server_version = _get_server_version()
except (ServerNotInstalled, VersionNotDetected) as e:
    has_server = False

try:
    client_version = _get_client_version()
except ClientNotInstalled, e:
    has_client = False
