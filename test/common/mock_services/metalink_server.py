#!/usr/bin/env python3

__version__ = "0.1"

import http.server
try:
  import socketserver as SocketServer
except ImportError:
  import SocketServer
import sys
import os
from optparse import OptionParser

usagestr = "usage: %prog [-h|--version] -p port linkurl ..."
parser = OptionParser(usage=usagestr, version=__version__, prog="metalink.py")
parser.add_option("-p", "--port", dest="http_port", action="store", type="int",
                  help="port number to be bound to (required)", metavar="PORT")
parser.add_option("-r", "--reverse", dest="reverse", action="store_true",
                  default=False, help="reverse the priorities of linkurls")

(options, args) = parser.parse_args()

if not options.http_port:
  parser.print_help()
  sys.exit(1)

if len(args) < 1:
  print("at least one link url required", file=sys.stderr);
  sys.exit(1)

class MetalinkRequestHandler(http.server.SimpleHTTPRequestHandler):
  """
  Extension of http.server.SimpleHTTPRequestHandler to support
  sending a Link header according to the Metalink/HTTP rfc6249.
  """

  server_version = "metalink_server/" + __version__

  def do_HEAD(self):
    return self.do_common()

  def do_GET(self):
    return self.do_common()

  def do_common(self):
    self.send_response(307)
    self.send_header("Location", args[0] + self.path)
    pri = 0
    if options.reverse:
      pri = len(args) + 1
    for a in args:
      if options.reverse:
        pri = pri - 1
      else:
        pri = pri + 1
      link = '<' + a + self.path + '>; rel="duplicate"; pri=' + str(pri)
      self.send_header("Link", link)
    self.end_headers()
    return None

print("start serving...")
handler = MetalinkRequestHandler
httpd = SocketServer.TCPServer(("", options.http_port), handler)
httpd.serve_forever()
