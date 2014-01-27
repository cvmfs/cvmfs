#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Created by René Meusel
This file is part of the CernVM File System auxiliary tools.
"""

import tempfile
import zlib
import sqlite3
import datetime
import collections
import md5
import os
import subprocess


from _common import _split_md5
from dirent  import DirectoryEntry


class CatalogIterator:
    """ Iterates through all directory entries of a Catalog """

    def __init__(self, catalog):
        self.catalog = catalog
        self.backlog = collections.deque()
        root_path = ""
        if not self.catalog.is_root():
            root_path = self.catalog.root_prefix
        self._push((root_path, self.catalog.find_directory_entry(root_path)))


    def __iter__(self):
        return self


    def next(self):
        if not self._has_more():
            raise StopIteration()
        return self._recursion_step()


    def _has_more(self):
        return len(self.backlog) > 0


    def _push(self, path):
        self.backlog.append(path)


    def _pop(self):
        return self.backlog.popleft()


    def _recursion_step(self):
        path, dirent = self._pop()
        if dirent.is_directory():
            new_dirents = self.catalog.list_directory_split_md5(dirent.md5path_1, \
                                                                dirent.md5path_2)
            for new_dirent in new_dirents:
                self._push((path + "/" + new_dirent.name, new_dirent))
        return path, dirent



class CatalogReference:
    """ Wraps a catalog reference to nested catalogs as found in Catalogs """

    def __init__(self, root_path, clg_hash, clg_size = 0):
        self.root_path = root_path
        self.hash      = clg_hash
        self.size      = clg_size

    def __str__(self):
        return "<CatalogReference for " + self.root_path + " - " + self.hash + ">"

    def __repr__(self):
        return "<CatalogReference for " + self.root_path + ">"

    def fetch_from(self, source_repository):
        return source_repository.retrieve_catalog(self.hash)



class Catalog:
    """ Wraps the basic functionality of CernVM-FS Catalogs """

    def __init__(self, catalog_file):
        self._decompress(catalog_file)
        self._open_database()
        self._read_properties()
        self._guess_root_prefix_if_needed()
        self._check_validity()


    def __del__(self):
        self.db_handle_.close()
        self.catalog_file_.close()


    def __str__(self):
        return "<Catalog " + self.root_prefix + ">"


    def __repr__(self):
        return self.__str__()


    def __iter__(self):
        return CatalogIterator(self)


    def list_nested(self):
        """ List CatalogReferences to all contained nested catalogs """
        new_version = (self.schema <= 1.2 and self.schema_revision > 0)
        if new_version:
            sql_query = "SELECT path, sha1, size FROM nested_catalogs;"
        else:
            sql_query = "SELECT path, sha1 FROM nested_catalogs;"
        catalogs = self.run_sql(sql_query)
        if new_version:
            return [ CatalogReference(clg[0], clg[1], clg[2]) for clg in catalogs ]
        else:
            return [ CatalogReference(clg[0], clg[1]) for clg in catalogs ]


    def find_nested_for_path(self, needle_path):
        """ Find the best matching nested CatalogReference for a given path """
        nested_catalogs  = self.list_nested()
        best_match       = None
        best_match_score = 0
        real_needle_path = self._canonicalize_path(needle_path)
        for nested_catalog in nested_catalogs:
            if real_needle_path.startswith(nested_catalog.root_path) and \
                len(nested_catalog.root_path) > best_match_score:
                    best_match_score = len(nested_catalog.root_path)
                    best_match       = nested_catalog
        return best_match


    def list_directory(self, path):
        """ Create a directory listing of the given directory path """
        real_path = self._canonicalize_path(path)
        parent_1, parent_2 = _split_md5(md5.md5(real_path).digest())
        return self.list_directory_split_md5(parent_1, parent_2)


    def list_directory_split_md5(self, parent_1, parent_2):
        """ Create a directory listing of DirectoryEntry items based on MD5 path """
        res = self.run_sql("SELECT md5path_1, md5path_2, parent_1, parent_2,     \
                                    flags, size, mode, mtime, name, symlink       \
                             FROM catalog                                         \
                             WHERE parent_1 = " + str(parent_1) + " AND           \
                                   parent_2 = " + str(parent_2) + "               \
                             ORDER BY name ASC;")
        listing = []
        for result in res:
            e = DirectoryEntry()
            e.md5path_1, e.md5path_2,   e.parent_1, e.parent_2, e.flags, e.size,  \
            e.mode, e.mtime, e.name, e.symlink = result
            listing.append(e)
        return listing


    def find_directory_entry(self, path):
        """ Finds the DirectoryEntry for a given path """
        real_path = self._canonicalize_path(path)
        md5path = md5.md5(real_path)
        return self.find_directory_entry_md5(md5path)


    def find_directory_entry_md5(self, md5path):
        """ Finds the DirectoryEntry for a given MD5 hashed path """
        lo, hi = _split_md5(md5path.digest())
        return self.find_directory_entry_split_md5(lo, hi)


    def find_directory_entry_split_md5(self, md5path_1, md5path_2):
        """ Finds the DirectoryEntry for the given split MD5 hashed path """
        res = self.run_sql("SELECT parent_1, parent_2, flags, size, mode, mtime, \
                                    name, symlink                                 \
                             FROM catalog                                         \
                             WHERE md5path_1 = " + str(md5path_1) + " AND         \
                                   md5path_2 = " + str(md5path_2) + "             \
                             LIMIT 1;")
        if len(res) != 1:
            return None
        e = DirectoryEntry()
        e.md5path_1 = md5path_1
        e.md5path_2 = md5path_2
        e.parent_1, e.parent_2, e.flags, e.size, e.mode, \
        e.mtime, e.name, e.symlink = res[0]
        return e


    def run_sql(self, sql):
        """ Run an arbitrary SQL query on the catalog database """
        cursor = self.db_handle_.cursor()
        cursor.execute(sql)
        return cursor.fetchall()


    def open_interactive(self):
        """ Spawns a sqlite shell for interactive catalog database inspection """
        subprocess.call(['sqlite3', self.catalog_file_.name])


    def is_root(self):
        """ Checks if this is the root catalog (based on the root prefix) """
        return self.root_prefix == "/"


    def _decompress(self, catalog_file):
        """ Unzip a catalog file to a temporary referenced by self.catalog_file_ """
        self.catalog_file_ = tempfile.NamedTemporaryFile('w+b')
        self.catalog_file_.write(zlib.decompress(catalog_file.read()))
        self.catalog_file_.flush()


    def _open_database(self):
        """ Create and configure a database handle to the Catalog """
        self.db_handle_ = sqlite3.connect(self.catalog_file_.name)
        self.db_handle_.text_factory = str


    def _read_properties(self):
        """ Retrieve all properties stored in the catalog database """
        props = self.run_sql("SELECT key, value FROM properties;")
        for prop in props:
            self._read_property(prop)
        if not hasattr(self, 'schema_revision'):
            self.schema_revision = 0


    def _read_property(self, prop):
        """ Detect catalog properties and store them as public class members """
        prop_key   = prop[0]
        prop_value = prop[1]
        if prop_key == "revision":
            self.revision          = prop_value
        if prop_key == "schema":
            self.schema            = float(prop_value)
        if prop_key == "schema_revision":
            self.schema_revision   = int(prop_value)
        if prop_key == "last_modified":
            self.last_modified     = datetime.datetime.fromtimestamp(int(prop_value))
        if prop_key == "previous_revision":
            self.previous_revision = prop_value
        if prop_key == "root_prefix":
            self.root_prefix       = prop_value


    def _guess_root_prefix_if_needed(self):
        """ Root catalogs don't have a root prefix property (fixed here) """
        if not hasattr(self, 'root_prefix'):
            self.root_prefix = "/"


    def _canonicalize_path(self, path):
        return os.path.realpath(path)


    def _check_validity(self):
        """ Check that all crucial properties have been found in the database """
        if not hasattr(self, 'schema'):
          raise Exception("Catalog lacks a schema entry")
        if not hasattr(self, 'root_prefix'):
          raise Exception("Catalog lacks a root prefix entry")
        if not hasattr(self, 'last_modified'):
          raise Exception("Catalog lacks a last modification entry")


