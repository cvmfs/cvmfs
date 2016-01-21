#!/usr/bin/python

import HTTPRangeServer
import SocketServer
import sys
import os
from optparse import OptionParser

parser = OptionParser()
parser.add_option("-p", "--port", dest="http_port", action="store", type="int",
                  help="port number to be bound to", metavar="PORT")
parser.add_option("-r", "--root", dest="docroot", action="store", type="string",
                  help="directory to be served", metavar="DOCROOT")

(options, args) = parser.parse_args()

if not options.docroot or not options.http_port:
    parser.print_help()
    sys.exit(1)

print "changing directory to" , options.docroot
os.chdir(options.docroot)

print "start serving..."
handler = HTTPRangeServer.HTTPRangeRequestHandler
httpd = SocketServer.TCPServer(("", options.http_port), handler)
httpd.serve_forever()
