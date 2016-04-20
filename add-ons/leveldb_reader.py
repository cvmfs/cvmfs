#!/usr/bin/env python

import plyvel
import shlex
import hashlib
import struct

class CvmfsLevelDB:
    """ Allows read access to a CernVM-FS LevelDB hive given a CernVM-FS
        LevelDB configuration file.

        Example of a typical CernVM-FS LevelDB configuration file:

        ::

            CVMFS_LEVELDB_STORAGE=/tmp/lvldb  # base storage directory
            CVMFS_LEVELDB_COUNT=5             # number of LevelDB shards
            CVMFS_LEVELDB_COMPRESSION="none"  # compression algorithm used
    """

    _mandatory_config_variables = [ "CVMFS_LEVELDB_STORAGE",
                                    "CVMFS_LEVELDB_COUNT" ]
    _optional_config_variables = [ "CVMFS_LEVELDB_COMPRESSION" ]

    def __init__(self, configuration):
        """ Connects to a CernVM-FS LevelDB hive

            :param configuration:  path to the configuration file to use
        """
        self.config    = self._parse_config(configuration)
        self.databases = []
        self._verify_config()
        self._open_databases()

    def get(self, key):
        """ Gets the data for a key from the CernVM-FS LevelDB hive

            :param key:  the key whose data is to be fetched
            :return:     the data or None if key doesn't exist
        """
        database = self._get_database_for_key(key)
        return database.get(key)

    def _open_databases(self):
        for i in range(int(self.config["CVMFS_LEVELDB_COUNT"])):
            db_name = "%s/lvldb%03d" % (self.config["CVMFS_LEVELDB_STORAGE"], i)
            db = plyvel.DB(db_name, create_if_missing=False)
            self.databases.append(db)

    def _parse_config(self, configuration):
        with open(configuration) as config_file:
            lexer = shlex.shlex(config_file)
            lexer.wordchars = ("!$%&()*+,-./0123456789:;<>?@ABCDEFGHIJKLMNOPQRS"
                               "TUVWXYZ[]^_abcdefghijklmnopqrstuvwxyz{|}~")

            config = {}
            while True:
                cfg_var = lexer.get_token()
                equal   = lexer.get_token()
                value   = lexer.get_token()

                if not cfg_var or not equal or not value:
                    break

                if cfg_var not in self._mandatory_config_variables and \
                   cfg_var not in self._optional_config_variables:
                    raise Exception("unknown config variable " + cfg_var +
                                    " in " + configuration)

                if equal != "=":
                    raise Exception("expected = in " + configuration + " : " +
                                    lexer.lineno)

                if value.startswith('"')  and value.endswith('"') or \
                   value.startswith('\'') and value.endswith('\''):
                    value = value[1:-1]

                config[cfg_var] = value

        return config

    def _verify_config(self):
        for mandatory in self._mandatory_config_variables:
            if mandatory not in self.config:
                raise Exception(mandatory + " config variable not set")

    def _get_database_for_key(self, key):
        h = hashlib.md5(key)
        shash = struct.unpack("I", h.digest()[0:4])[0]
        return self.databases[shash % int(self.config["CVMFS_LEVELDB_COUNT"])]
