#! /usr/bin/env python3

# This tester listens on port 8051 for a single http request, with
#  a URL that starts with /api/v....
# It exits after one request.
# It assumes that GeoIP is already installed on the current machine
#  with an installation of cvmfs-server, but reads the rest from
#  the current directory.

from wsgiref.simple_server import make_server

import sys
sys.path.append('.')
sys.path.append('/usr/share/cvmfs-server/webapi')

from ctypes import cdll
cdll.LoadLibrary('/usr/share/cvmfs-server/webapi/GeoIP.so')

execfile('cvmfs-api.wsgi')

import socket
httpd = make_server(
   socket.gethostname(), # The host name.
   8051, # A port number where to wait for the request.
   application # Our application object name, in this case a function.
   )

# Wait for a single request, serve it and quit.
httpd.handle_request()
