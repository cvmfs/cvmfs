#!/usr/bin/env python3
try:
  import http.server as BaseHTTPServer
  import socketserver as SocketServer
except ImportError:
  import BaseHTTPServer
  import SimpleHTTPServer
  import SocketServer
import sys
import os
from optparse import OptionParser



class FaultyHTTPRequestHandler(BaseHTTPServer.BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(400)
        self.end_headers()

    def do_POST(self):
        self.send_response(400)
        self.end_headers()

    def do_HEAD(self):
        self.send_response(400)
        self.end_headers()


parser = OptionParser()
parser.add_option("-p", "--port", dest="http_port", action="store", type="int",
                  help="port number to be bound to", metavar="PORT")

(options, args) = parser.parse_args()

if not options.http_port:
    parser.print_help()
    sys.exit(1)

print("start serving...")
handler = FaultyHTTPRequestHandler
httpd = SocketServer.TCPServer(("", options.http_port), handler)
httpd.serve_forever()
