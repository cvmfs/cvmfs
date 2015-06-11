#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Created by René Meusel
This file is part of the CernVM File System auxiliary tools.
"""

import abc
import os
import tempfile
import unittest

class FileSandbox(unittest.TestCase):
    """ Wraps the creation and automatic removal of temporary files """

    __metaclass__   = abc.ABCMeta

    def __init__(self, *args, **kwargs):
        super(FileSandbox, self).__init__(*args, **kwargs)
        self.temporary_files = []

    def __del__(self):
        for f in self.temporary_files:
            os.unlink(f)
        # del self.temporary_files[:]

    @abc.abstractmethod
    def temporary_prefix(self):
        """ defines a unique temporary file prefix """
        return "file_sandbox"


    def write_to_temporary(self, string_buffer):
        """ stores the provided content into a /tmp path that is returned """
        filename = None
        with tempfile.NamedTemporaryFile(mode='w+b',
                                         prefix=self.temporary_prefix(),
                                         dir='/tmp',
                                         delete=False) as f:
            filename = f.name
            f.write(string_buffer)
            self.temporary_files.append(filename)
        return filename
