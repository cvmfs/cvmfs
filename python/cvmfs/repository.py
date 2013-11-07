#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Created by René Meusel
This file is part of the CernVM File System auxiliary tools.
"""

import os
import _common

class RepositoryNotFoundError(Exception):
    def __init__(self, repo_fqrn):
        self.fqrn = repo_fqrn

    def __str__(self):
        return self.fqrn , "not found"

class UnknownRepositoryType(Exception):
    def __init__(self, repo_fqrn, repo_type):
        self.fqrn = repo_fqrn
        self.type = repo_type

    def __str__(self):
        return self.fqrn + " (" + self.type + ")"

class ConfigurationNotFound(Exception):
    def __init__(self, repo):
        self.repo = repo

    def __str__(self):
        return repr(self.repo)


class Repository:
    def __init__(self, repo_fqrn):
        self.fqrn = repo_fqrn
        self._repo_config_dir = os.path.join(_common._REPO_CONFIG_PATH, repo_fqrn)
        if not os.path.isdir(self._repo_config_dir):
            raise RepositoryNotFoundError(repo_fqrn)
        self._server_config = os.path.join(self._repo_config_dir, _common._SERVER_CONFIG_NAME)
        repo_type = self.read_server_config("CVMFS_REPOSITORY_TYPE")
        if repo_type != 'stratum0' and repo_type != 'stratum1':
            raise UnknownRepositoryType(repo_fqrn, repo_type)
        self.type = repo_type

    def read_server_config(self, config_field):
        with open(self._server_config) as config_file:
            for config_line in config_file:
                if config_line.startswith(config_field):
                    return config_line[len(config_field)+1:].strip()
        raise ConfigurationNotFound(self)

    def __str__(self):
        return self.fqrn

    def __repr__(self):
        return "<CVMFS Repository " + self.fqrn + ">"


def all():
    d = _common._REPO_CONFIG_PATH
    if not os.path.isdir(d):
        raise _common.CvmfsNotInstalled
    return [ Repository(repo) for repo in os.listdir(d) if os.path.isdir(os.path.join(d, repo)) ]

def all_stratum0():
    return [ repo for repo in all() if repo.type == 'stratum0' ]
