#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Created by René Meusel
This file is part of the CernVM File System auxiliary tools.
"""

_REPO_CONFIG_PATH   = "/etc/cvmfs/repositories.d"
_SERVER_CONFIG_NAME = "server.conf"

_MANIFEST_NAME      = ".cvmfspublished"


class CvmfsNotInstalled(Exception):
    def __init__(self):
        Exception.__init__(self, "It seems that cvmfs is not installed on this machine!")
