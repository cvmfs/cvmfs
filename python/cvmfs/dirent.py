#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Created by René Meusel
This file is part of the CernVM File System auxiliary tools.
"""

class _Flags:
    """ Definition of used dirent flags (see cvmfs/catalog_sql.h) """
    Directory               = 1
    NestedCatalogMountpoint = 2
    NestedCatalogRoot       = 32
    File                    = 4
    Link                    = 8
    FileStat                = 16 # unused
    FileChunk               = 64


class DirectoryEntry:
    """ Thin wrapper around a DirectoryEntry as it is saved in the Catalogs """

    def __init__(self):
        self.md5path_1 = 0
        self.md5path_2 = 0
        self.parent_1  = 0
        self.parent_2  = 0
        self.flags     = 0
        self.size      = 0
        self.mode      = 0
        self.mtime     = 0
        self.name      = ""
        self.symlink   = ""

    def __str__(self):
        return "<DirectoryEntry for '" + self.name + "'>"

    def __repr__(self):
        return "<DirectoryEntry '" + self.name + "' - " + \
               str(self.md5path_1) + "|" + str(self.md5path_2) + ">"

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
