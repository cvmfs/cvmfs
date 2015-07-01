#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Created by René Meusel
This file is part of the CernVM File System auxiliary tools.
"""

import select
import threading
import SocketServer

import sys

class BackportedTCPServer(SocketServer.ThreadingTCPServer):
    """ Backport of the Python 2.6's TCPServer `shutdown()` functionality
        Found here: http://fw-geekycoder.blogspot.ch/2011/09/
                           how-to-implement-shutdown-method-in.html
    """
    def __init__(self, address_tuple, handler):
        SocketServer.ThreadingTCPServer.__init__(self, address_tuple, handler)
        self.__is_shut_down = threading.Event()
        self.__shutdown_request = False

    def serve_forever(self, poll_interval=0.5):
        """ Handle one request at a time until shutdown.

            Polls for shutdown every poll_interval seconds. Ignores
            self.timeout. If you need to do periodic tasks, do them in
            another thread.
        """
        self.__is_shut_down.clear()
        try:
            while not self.__shutdown_request:
                r, w, e = select.select([self], [], [], poll_interval)
                if self in r:
                    self._handle_request_noblock()
        finally:
            self.__shutdown_request = False
            self.__is_shut_down.set()

    def shutdown(self):
        """ Stops the serve_forever loop.

            Blocks until the loop has finished. This must be called while
            serve_forever() is running in another thread, or it will
            deadlock.
        """
        self.__shutdown_request = True
        self.__is_shut_down.wait()

    def _handle_request_noblock(self):
        """ Handle one request, without blocking.

            I assume that select.select has returned that the socket is
            readable before this function was called, so there should be
            no risk of blocking in get_request().
        """
        try:
            request, client_address = self.get_request()
        except socket.error:
            return
        if self.verify_request(request, client_address):
            try:
                self.process_request(request, client_address)
            except:
                self.handle_error(request, client_address)
                self.close_request(request)


# decide which server implementation to use
python_version = sys.version_info
Py26ishTCPServer = None

if python_version[1] < 6:
    Py26ishTCPServer = BackportedTCPServer
else:
    Py26ishTCPServer = SocketServer.TCPServer
