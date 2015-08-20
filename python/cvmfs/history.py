#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Created by René Meusel
This file is part of the CernVM File System auxiliary tools.
"""

import datetime

from _common import DatabaseObject

class RevisionTag:
    """ Specific revisions in CernVM-FS 2.1.x repositories have named tags  """
    @staticmethod
    def sql_query():
        return '''SELECT name, hash, revision, timestamp, channel, description
                  FROM tags ORDER BY timestamp DESC'''

    def __init__(self, sql_result):
        self.name        = sql_result[0]
        self.hash        = sql_result[1]
        self.revision    = int(sql_result[2])
        self.timestamp   = datetime.datetime.fromtimestamp(int(sql_result[3]))
        self.channel     = int(sql_result[4])
        self.description = sql_result[5]

    def __str__(self):
        return "<RevisionTag '" + self.name + "'>"

    def __repr__(self):
        return self.__str__()


class History(DatabaseObject):
    """ Wrapper around CernVM-FS 2.1.x repository history databases """

    @staticmethod
    def open(history_path):
        """ Initializes a History Database from a local file path """
        f = open(history_path)
        return History(f)

    def __init__(self, history_file):
        DatabaseObject.__init__(self, history_file)
        self._read_properties()

    def __str__(self):
        return "<History for '" + self.repository_name + "'>"

    def __repr__(self):
        return self.__str__()

    def __iter__(self):
        return self.list_tags().__iter__()

    def list_tags(self):
        results = self.run_sql(RevisionTag.sql_query())
        return [ RevisionTag(sql_res) for sql_res in results ]

    def _read_properties(self):
        self.read_properties_table(lambda prop_key, prop_value:
            self._read_property(prop_key, prop_value))
        assert hasattr(self, 'schema') and self.schema == '1.0'

    def _read_property(self, prop_key, prop_value):
        if prop_key == "schema":
            self.schema          = prop_value
        if prop_key == "fqrn":
            self.repository_name = prop_value
