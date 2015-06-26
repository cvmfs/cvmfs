#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Created by René Meusel
This file is part of the CernVM File System auxiliary tools.
"""

import abc
import os
import urlparse
import tempfile
import requests
from datetime import datetime
import dateutil.parser
from dateutil.tz import tzutc

import _common
import cvmfs
from manifest import Manifest
from whitelist import Whitelist
from certificate import Certificate

class RepositoryNotFound(Exception):
    def __init__(self, repo_path):
        self.path = repo_path

    def __str__(self):
        return self.path + " not found"

class UnknownRepositoryType(Exception):
    def __init__(self, repo_fqrn, repo_type):
        self.fqrn = repo_fqrn
        self.type = repo_type

    def __str__(self):
        return self.fqrn + " (" + self.type + ")"

class ConfigurationNotFound(Exception):
    def __init__(self, repo, config_field):
        self.repo         = repo
        self.config_field = config_field

    def __str__(self):
        return repr(self.repo) + " " + self.config_field

class FileNotFoundInRepository(Exception):
    def __init__(self, repo, file_name):
        self.repo      = repo
        self.file_name = file_name

    def __str__(self):
        return repr(self.file_name)

class RepositoryVerificationFailed(Exception):
    def __init__(self, message, repo):
        Exception.__init__(self, message)
        self.repo = repo

    def __str__(self):
        return self.args[0] + " (Repo: " + repr(self.repo) + ")"


class Repository:
    """ Abstract Wrapper around a CVMFS Repository representation """

    __metaclass__ = abc.ABCMeta

    def __init__(self):
        self._read_manifest()
        self._try_to_get_last_replication_timestamp()
        self._try_to_get_replication_state()


    def _read_manifest(self):
        try:
            manifest_file = self.retrieve_file(_common._MANIFEST_NAME)
            self.manifest = Manifest(manifest_file)
            manifest_file.close()
            self.fqrn = self.manifest.repository_name
        except FileNotFoundInRepository, e:
            raise RepositoryNotFound(self.endpoint)


    def __read_timestamp(self, timestamp_string):
        local_ts = dateutil.parser.parse(timestamp_string,
                                         ignoretz=False,
                                         tzinfos=_common.TzInfos.get_tzinfos())
        return local_ts.astimezone(tzutc())



    def _try_to_get_last_replication_timestamp(self):
        try:
            last_rep_file = self.retrieve_file(_common._LAST_REPLICATION_NAME)
            timestamp = last_rep_file.readline()
            last_rep_file.close()
            self.last_replication = self.__read_timestamp(timestamp)
            if not self.has_repository_type():
                self.type = 'stratum1'
        except FileNotFoundInRepository, e:
            self.last_replication = datetime.fromtimestamp(0, tz=tzutc())


    def _try_to_get_replication_state(self):
        self.replicating = False
        try:
            rep_state_file = self.retrieve_file(_common._REPLICATING_NAME)
            timestamp = rep_state_file.readline()
            rep_state_file.close()
            self.replicating = True
            self.replicating_since = self.__read_timestamp(timestamp)
        except FileNotFoundInRepository, e:
            pass


    def verify(self, public_key_path):
        """ Use a public key to verify the repository's authenticity """
        whitelist   = self.retrieve_whitelist()
        certificate = self.retrieve_certificate()
        if not whitelist.verify_signature(public_key_path):
            raise RepositoryVerificationFailed("Public key doesn't fit", self)
        if whitelist.expired():
            raise RepositoryVerificationFailed("Whitelist expired", self)
        if not whitelist.contains(certificate):
            raise RepositoryVerificationFailed("Certificate not in whitelist", self)
        if not self.manifest.verify_signature(certificate):
            raise RepositoryVerificationFailed("Certificate doesn't fit", self)
        return True


    def has_repository_type(self):
        return hasattr(self, 'type') and self.type != 'unknown'


    def retrieve_whitelist(self):
        """ retrieve and parse the .cvmfswhitelist file from the repository """
        whitelist = self.retrieve_file(_common._WHITELIST_NAME)
        return Whitelist(whitelist)


    def retrieve_certificate(self):
        """ retrieve the repository's certificate file """
        certificate = self.retrieve_object(self.manifest.certificate, 'X')
        return Certificate(certificate)


    @abc.abstractmethod
    def retrieve_file(self, file_name):
        """ Abstract method to retrieve a file from the repository """
        pass


    def retrieve_object(self, object_hash, hash_suffix = ''):
        """ Retrieves an object from the content addressable storage """
        path = "data/" + object_hash[:2] + "/" + object_hash[2:] + hash_suffix
        return self.retrieve_file(path)


class LocalRepository(Repository):
    def __init__(self, repo_fqrn_of_path):
        if os.path.isdir(repo_fqrn_of_path):
            self._open_local_directory(repo_fqrn_of_path)
        else:
            self._open_local_fqrn(repo_fqrn_of_path)


    def _open_local_fqrn(self, repo_fqrn):
        repo_config_dir = os.path.join(_common._REPO_CONFIG_PATH, repo_fqrn)
        if not os.path.isdir(repo_config_dir):
            raise RepositoryNotFound(repo_fqrn)
        self._server_config = os.path.join(repo_config_dir, _common._SERVER_CONFIG_NAME)
        self.type = self.read_server_config("CVMFS_REPOSITORY_TYPE")
        if self.type != 'stratum0' and self.type != 'stratum1':
            raise UnknownRepositoryType(repo_fqrn, self.type)
        self.endpoint = self._get_repo_location_from_config()
        Repository.__init__(self)
        self.version = cvmfs.server_version
        self.fqrn    = repo_fqrn


    def _open_local_directory(self, repo_directory):
        self._server_config = None
        self.endpoint = repo_directory
        Repository.__init__(self)


    def read_server_config(self, config_field):
        if not self._server_config:
            raise ConfigurationNotFound(self, "no such file or directory")
        config_file = open(self._server_config)
        for config_line in config_file:
            if config_line.startswith(config_field):
                return config_line[len(config_field)+1:].strip()
        config_file.close()
        raise ConfigurationNotFound(self, config_field)


    def _get_repo_location_from_config(self):
        upstream = self.read_server_config("CVMFS_UPSTREAM_STORAGE")
        upstream_type, tmp_dir, upstream_cfg = upstream.split(',')
        if upstream_type != 'local': # might be riak, s3, ... (not implemented)
            raise UnknownRepositoryType(repo_fqrn, upstream_type)
        return upstream_cfg # location of the repository backend storage


    def retrieve_file(self, file_name):
        file_path = os.path.join(self.endpoint, file_name)
        if not os.path.exists(file_path):
            raise FileNotFoundInRepository(self, file_name)
        return open(file_path, "rb")


    def __str__(self):
        return self.fqrn

    def __repr__(self):
        return "<Local Repository " + self.fqrn + ">"



class RemoteRepository(Repository):
    """ Concrete Repository implementation for a repository reachable by HTTP """
    def __init__(self, repo_url):
        self.endpoint = urlparse.urlunparse(urlparse.urlparse(repo_url))
        self._user_agent = cvmfs.__package_name__ + "/" + cvmfs.__version__
        Repository.__init__(self)


    def __str__(self):
        return self.endpoint

    def __repr__(self):
        return "<Remote Repository " + self.fqrn + " at " + self.endpoint + ">"


    def retrieve_file(self, file_name):
        file_url = self.endpoint + "/" + file_name
        tmp_file = tempfile.NamedTemporaryFile('w+b')
        headers  = { 'User-Agent': self._user_agent }
        response = requests.get(file_url, stream=True, headers=headers)
        if response.status_code != requests.codes.ok:
            raise FileNotFoundInRepository(self, file_url)
        for chunk in response.iter_content(chunk_size=4096):
            if chunk:
                tmp_file.write(chunk)
        tmp_file.seek(0)
        tmp_file.flush()
        return tmp_file


def open_repository(repository_path, public_key = None):
    """ wrapper function accessing a repository by URL, local FQRN or path """
    repo = None
    if repository_path.startswith("http://"):
        repo = RemoteRepository(repository_path)
    else:
        repo = LocalRepository(repository_path)

    if public_key:
        repo.verify(public_key)

    return repo
