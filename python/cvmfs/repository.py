#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Created by René Meusel
This file is part of the CernVM File System auxiliary tools.
"""

import os
import urlparse
import tempfile
import requests
import collections
from datetime import datetime
import dateutil.parser
from dateutil.tz import tzutc

import _common
import cvmfs
from manifest import Manifest
from catalog import Catalog
from history import History
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

class HistoryNotFound(Exception):
    def __init__(self, repo):
        self.repo = repo

    def __str__(self):
        return repr(self.repo)

class CannotReplicate(Exception):
    def __init__(self, repo):
        self.repo = repo

    def __str__(self):
        return repr(self.repo)

class NestedCatalogNotFound(Exception):
    def __init__(self, repo):
        self.repo = repo

    def __str__(self):
        return repr(self.repo)


class RepositoryIterator:
    """ Iterates through all directory entries in a whole Repository """

    class _CatalogIterator:
        def __init__(self, catalog):
            self.catalog          = catalog
            self.catalog_iterator = catalog.__iter__()


    def __init__(self, repository):
        self.repository    = repository
        self.catalog_stack = collections.deque()
        self._push_catalog(repository.retrieve_root_catalog())


    def __iter__(self):
        return self


    def next(self):
        full_path, dirent = self._get_next_dirent()
        if dirent.is_nested_catalog_mountpoint():
            self._fetch_and_push_catalog(full_path)
            return self.next() # same directory entry is also in nested catalog
        return full_path, dirent


    def _get_next_dirent(self):
        try:
            return self._get_current_catalog().catalog_iterator.next()
        except StopIteration, e:
            self._pop_catalog()
            if not self._has_more():
                raise StopIteration()
            return self._get_next_dirent()


    def _fetch_and_push_catalog(self, catalog_mountpoint):
        current_catalog = self._get_current_catalog().catalog
        nested_ref      = current_catalog.find_nested_for_path(catalog_mountpoint)
        if not nested_ref:
            raise NestedCatalogNotFound(self.repository)
        new_catalog     = nested_ref.retrieve_from(self.repository)
        self._push_catalog(new_catalog)


    def _has_more(self):
        return len(self.catalog_stack) > 0


    def _push_catalog(self, catalog):
        catalog_iterator = self._CatalogIterator(catalog)
        self.catalog_stack.append(catalog_iterator)

    def _get_current_catalog(self):
        return self.catalog_stack[-1]

    def _pop_catalog(self):
        return self.catalog_stack.pop()


class CatalogTreeIterator:
    class _CatalogWrapper:
        def __init__(self, repository):
            self.repository        = repository
            self.catalog           = None
            self.catalog_reference = None

        def get_catalog(self):
            if self.catalog == None:
                self.catalog = self.catalog_reference.retrieve_from(self.repository)
            return self.catalog

    def __init__(self, repository, root_catalog):
        if not root_catalog:
            root_catalog = repository.retrieve_root_catalog()
        self.repository    = repository
        self.catalog_stack = collections.deque()
        wrapper            = self._CatalogWrapper(self.repository)
        wrapper.catalog    = root_catalog
        self._push_catalog_wrapper(wrapper)

    def __iter__(self):
        return self

    def next(self):
        if not self._has_more():
            raise StopIteration()
        catalog = self._pop_catalog()
        self._push_nested_catalogs(catalog)
        return catalog

    def _has_more(self):
        return len(self.catalog_stack) > 0

    def _push_nested_catalogs(self, catalog):
        for nested_reference in catalog.list_nested():
            wrapper = self._CatalogWrapper(self.repository)
            wrapper.catalog_reference = nested_reference
            self._push_catalog_wrapper(wrapper)

    def _push_catalog_wrapper(self, catalog):
        self.catalog_stack.append(catalog)

    def _pop_catalog(self):
        wrapper = self.catalog_stack.pop()
        return wrapper.get_catalog()



class Repository:
    """ Abstract Wrapper around a CVMFS Repository representation """
    def __init__(self):
        self._opened_catalogs = {}
        self._read_manifest()
        self._try_to_get_last_replication_timestamp()
        self._try_to_get_replication_state()


    def __iter__(self):
        return RepositoryIterator(self)


    def _read_manifest(self):
        try:
            with self.retrieve_file(_common._MANIFEST_NAME) as manifest_file:
                self.manifest = Manifest(manifest_file)
            self.fqrn = self.manifest.repository_name
        except FileNotFoundInRepository, e:
            raise RepositoryNotFound(self._storage_location)


    def __read_timestamp(self, timestamp_string):
        return dateutil.parser.parse(timestamp_string)


    def _try_to_get_last_replication_timestamp(self):
        try:
            with self.retrieve_file(_common._LAST_REPLICATION_NAME) as rf:
                timestamp = rf.readline()
                self.last_replication = self.__read_timestamp(timestamp)
            if not self.has_repository_type():
                self.type = 'stratum1'
        except FileNotFoundInRepository, e:
            self.last_replication = datetime.fromtimestamp(0, tz=tzutc())


    def _try_to_get_replication_state(self):
        self.replicating = False
        try:
            with self.retrieve_file(_common._REPLICATING_NAME) as rf:
                timestamp = rf.readline()
                self.replicating = True
                self.replicating_since = self.__read_timestamp(timestamp)
        except FileNotFoundInRepository, e:
            pass


    def catalogs(self, root_catalog = None):
        return CatalogTreeIterator(self, root_catalog)


    def has_repository_type(self):
        return hasattr(self, 'type') and self.type != 'unknown'


    def has_history(self):
        return self.manifest.has_history()


    def retrieve_history(self):
        if not self.has_history():
            raise HistoryNotFound(self)
        history_db = self.retrieve_object(self.manifest.history_database, 'H')
        return History(history_db)


    def retrieve_whitelist(self):
        whitelist = self.retrieve_file(_common._WHITELIST_NAME)
        return Whitelist(whitelist)


    def retrieve_certificate(self):
        certificate = self.retrieve_object(self.manifest.certificate, 'X')
        return Certificate(certificate)


    def retrieve_file(self, file_name):
        """ Abstract method to retrieve a file from the repository """
        raise Exception("Not implemented!")


    def retrieve_object(self, object_hash, hash_suffix = ''):
        """ Retrieves an object from the content addressable storage """
        path = "data/" + object_hash[:2] + "/" + object_hash[2:] + hash_suffix
        return self.retrieve_file(path)


    def retrieve_root_catalog(self):
        return self.retrieve_catalog(self.manifest.root_catalog)


    def retrieve_catalog_for_path(self, needle_path):
        """ Recursively walk down the Catalogs and find the best fit for a path """
        clg = self.retrieve_root_catalog()
        nested_reference = None
        while True:
            new_nested_reference = clg.FindNestedForPath(needle_path)
            if new_nested_reference == None:
                break
            nested_reference = new_nested_reference
            clg = self.retrieve_catalog(nested_reference.hash)
        return clg


    def close_catalog(self, catalog):
        try:
            open_catalog = self._opened_catalogs[catalog.hash]
            del self._opened_catalogs[catalog.hash]
        except KeyError, e:
            print "not found:" , catalog.hash
            pass


    def retrieve_catalog(self, catalog_hash):
        """ Download and open a catalog from the repository """
        if catalog_hash in self._opened_catalogs:
            return self._opened_catalogs[catalog_hash]
        else:
            return self._retrieve_and_open_catalog(catalog_hash)

    def _retrieve_and_open_catalog(self, catalog_hash):
        catalog_file = self.retrieve_object(catalog_hash, 'C')
        new_catalog = Catalog(catalog_file, catalog_hash)
        self._opened_catalogs[catalog_hash] = new_catalog
        return new_catalog




class LocalRepository(Repository):
    def __init__(self, repo_fqrn):
        repo_config_dir = os.path.join(_common._REPO_CONFIG_PATH, repo_fqrn)
        if not os.path.isdir(repo_config_dir):
            raise RepositoryNotFound(repo_fqrn)
        self._server_config = os.path.join(repo_config_dir, _common._SERVER_CONFIG_NAME)
        self.type           = self.read_server_config("CVMFS_REPOSITORY_TYPE")
        if self.type != 'stratum0' and self.type != 'stratum1':
            raise UnknownRepositoryType(repo_fqrn, self.type)
        self._storage_location = self._get_repo_location()
        Repository.__init__(self)
        self.version = cvmfs.server_version
        self.fqrn    = repo_fqrn


    def read_server_config(self, config_field):
        with open(self._server_config) as config_file:
            for config_line in config_file:
                if config_line.startswith(config_field):
                    return config_line[len(config_field)+1:].strip()
        raise ConfigurationNotFound(self, config_field)


    def _get_repo_location(self):
        upstream = self.read_server_config("CVMFS_UPSTREAM_STORAGE")
        upstream_type, tmp_dir, upstream_cfg = upstream.split(',')
        if upstream_type != 'local': # might be riak, s3, ... (not implemented)
            raise UnknownRepositoryType(repo_fqrn, upstream_type)
        return upstream_cfg # location of the repository backend storage


    def retrieve_file(self, file_name):
        file_path = os.path.join(self._storage_location, file_name)
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
        self._storage_location = urlparse.urlunparse(urlparse.urlparse(repo_url))
        self._try_to_get_repo_information()
        Repository.__init__(self)


    def __str__(self):
        return self._storage_location

    def __repr__(self):
        return "<Remote Repository " + self.fqrn + " at " + self._storage_location + ">"


    def _get_rest_url(self, method_name):
        return "/".join((self._storage_location,
                         _common._REST_CONNECTOR,
                         method_name))


    def has_rest_api(self):
        if not hasattr(self, '_rest_api'):
            api_url = self._get_rest_url('info')
            response = requests.head(api_url)
            self._has_rest_api = (response.status_code == requests.codes.ok)
        return self._has_rest_api


    def __rest_request(self, http_verb_method, rest_method_name):
        api_url  = self._get_rest_url(rest_method_name)
        response = http_verb_method(api_url)
        response.raise_for_status()
        return response.json()


    def _GET_rest_request(self, method_name):
        return self.__rest_request(requests.get, method_name)


    def _POST_rest_request(self, method_name):
        return self.__rest_request(requests.post, method_name)


    def _try_to_get_repo_information(self):
        self.type          = 'unknown'
        self.version       = 'unknown'
        if self.has_rest_api():
            general_infos      = self._GET_rest_request('info')
            self.type          = general_infos['type']
            self.version       = general_infos['version']


    def start_replication(self):
        res = self._POST_rest_request('replicate')
        if res['result'] != 'ok':
            raise CannotReplicate(self)


    def retrieve_file(self, file_name):
        file_url = self._storage_location + "/" + file_name
        tmp_file = tempfile.NamedTemporaryFile('w+b')
        response = requests.get(file_url, stream=True)
        if response.status_code != requests.codes.ok:
            raise FileNotFoundInRepository(self, file_url)
        for chunk in response.iter_content(chunk_size=4096):
            if chunk:
                tmp_file.write(chunk)
        tmp_file.seek(0)
        tmp_file.flush()
        return tmp_file



def all_local():
    d = _common._REPO_CONFIG_PATH
    if not os.path.isdir(d):
        raise _common.CvmfsNotInstalled
    return [ LocalRepository(repo) for repo in os.listdir(d) if os.path.isdir(os.path.join(d, repo)) ]

def all_local_stratum0():
    return [ repo for repo in all_local() if repo.type == 'stratum0' ]

def open_repository(repository_path):
    if repository_path.startswith("http://"):
        return RemoteRepository(repository_path)
    else:
        return LocalRepository(repository_path)
