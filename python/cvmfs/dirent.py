#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Created by René Meusel
This file is part of the CernVM File System auxiliary tools.
"""

from _common import _binary_buffer_to_hex_string


class _Flags:
    """ Definition of used dirent flags (see cvmfs/catalog_sql.h) """
    Directory               = 1
    NestedCatalogMountpoint = 2
    NestedCatalogRoot       = 32
    File                    = 4
    Link                    = 8
    FileStat                = 16 # unused
    FileChunk               = 64
    ContentHashType         = 256 + 512 + 1024


class ContentHashTypes:
    """ Enumeration of supported content hash types (see cvmfs/hash.h) """
    Unknown    = -1
    # Md5      =  0  # MD5 is not used as a content hash!
    Sha1       =  1
    Ripemd160  =  2
    UpperBound =  3

    @staticmethod
    def to_suffix(content_hash_type):
        """ figures out the hash suffix in CVMFS's CAS (see cvmfs/hash.cc) """
        if content_hash_type == ContentHashTypes.Ripemd160:
            return "-rmd160"
        else:
            return ""


class Chunk:
    """ Wrapper around file chunks in the CVMFS catalogs """

    def __init__(self, chunk_data, content_hash_type):
        if len(chunk_data) != 5:
            raise Exception("Result set doesn't match")
        _, _, self.offset, self.size, self.content_hash = chunk_data
        self.content_hash_type = content_hash_type

    def __str__(self):
        return "<Chunk " + str(self.offset) + ">"

    def __repr__(self):
        return "<Chunk " + str(self.offset) + " - " + str(self.size) + ">"

    def content_hash_string(self):
        suffix = ContentHashTypes.to_suffix(self.content_hash_type)
        return _binary_buffer_to_hex_string(self.content_hash) + suffix

    @staticmethod
    def _catalog_db_fields():
        return "md5path_1, md5path_2, offset, size, hash"


class DirectoryEntry:
    """ Thin wrapper around a DirectoryEntry as it is saved in the Catalogs """

    def __init__(self, result_set):
        # see DirectoryEntry._catalog_db_fields()
        if len(result_set) != 11:
            raise Exception("Result set doesn't match")
        self.md5path_1, self.md5path_2, self.parent_1, self.parent_2,    \
        self.content_hash, self.flags, self.size, self.mode, self.mtime, \
        self.name, self.symlink = result_set
        self.chunks = []
        self._read_content_hash_type()

    def __str__(self):
        return "<DirectoryEntry for '" + self.name + "'>"

    def __repr__(self):
        return "<DirectoryEntry '" + self.name + "' - " + \
               str(self.md5path_1) + "|" + str(self.md5path_2) + ">"

    @staticmethod
    def _catalog_db_fields():
        # see DirectoryEntry.__init__()
        return "md5path_1, md5path_2, parent_1, parent_2, hash, \
                flags, size, mode, mtime, name, symlink"

    def retrieve_from(self, repository):
        if self.is_symlink():
            raise Exception("Cannot retrieve symlink")
        elif self.is_directory():
            raise Exception("Cannot retrieve directory")
        return repository.retrieve_object(self.content_hash_string())

    def is_directory(self):
        return (self.flags & _Flags.Directory) > 0

    def is_nested_catalog_mountpoint(self):
        return (self.flags & _Flags.NestedCatalogMountpoint) > 0

    def is_nested_catalog_root(self):
        return (self.flags & _Flags.NestedCatalogRoot) > 0

    def is_file(self):
        return (self.flags & _Flags.File) > 0

    def is_symlink(self):
        return (self.flags & _Flags.Link) > 0

    def path_hash(self):
        return self.md5path_1, self.md5path_2

    def parent_hash(self):
        return self.parent_1, self.parent_2

    def content_hash_string(self):
        suffix = ContentHashTypes.to_suffix(self.content_hash_type)
        return _binary_buffer_to_hex_string(self.content_hash) + suffix

    def has_chunks(self):
        return bool(self.chunks)

    def _add_chunks(self, result_set):
        self.chunks = [ Chunk(chunk_data, self.content_hash_type)
                        for chunk_data in result_set ]

    def _read_content_hash_type(self):
        bit_mask     = _Flags.ContentHashType
        right_shifts = 0
        while bit_mask & 1 == 0:
            bit_mask = bit_mask >> 1
            right_shifts += 1
        hash_type = ((self.flags & _Flags.ContentHashType) >> right_shifts) + 1
        self.content_hash_type = \
            hash_type if hash_type > 0 and hash_type < ContentHashTypes.UpperBound \
                      else ContentHashTypes.Unknown

    # def BacktracePath(self, containing_catalog, repo):
    #     """ Tries to reconstruct the full path of a DirectoryEntry """
    #     dirent  = self
    #     path    = self.name
    #     catalog = containing_catalog
    #     while True:
    #         p_dirent = catalog.FindDirectoryEntrySplitMd5(dirent.parent_1, \
    #                                                         dirent.parent_2)
    #         if p_dirent != None:
    #             path = p_dirent.name + "/" + path
    #             dirent = p_dirent
    #         elif not catalog.IsRoot():
    #             catalog = repo.FindParentCatalogOf(catalog)
    #         else:
    #             break
    #     return path
