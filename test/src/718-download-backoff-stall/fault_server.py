#!/usr/bin/env python3
#
# Minimal stratum 1 mock for test 718.  Serves a directory over HTTP (threaded)
# but answers every request whose path contains one of the given object hashes
# with "503 Service Unavailable".  The client treats 503 as a transient host
# error and retries the same URL after DownloadManager::Backoff().

try:
  import http.server as BaseHTTPServer
  import socketserver as SocketServer
except ImportError:
  import SimpleHTTPServer as BaseHTTPServer
  import SocketServer
import os
import sys
from optparse import OptionParser


parser = OptionParser()
parser.add_option("-p", "--port", dest="http_port", type="int",
                  help="port number to be bound to", metavar="PORT")
parser.add_option("-r", "--root", dest="docroot", type="string",
                  help="directory to be served", metavar="DOCROOT")
parser.add_option("-f", "--fail", dest="fail", action="append", default=[],
                  help="object hash to answer with HTTP 503 (repeatable)",
                  metavar="HASH")
(options, args) = parser.parse_args()

if not options.docroot or not options.http_port:
  parser.print_help()
  sys.exit(1)

# Object paths look like data/<hash[0:2]>/<hash[2:]>[suffix]
fail_patterns = ["/data/%s/%s" % (h[0:2], h[2:]) for h in options.fail]


class FaultyHandler(BaseHTTPServer.SimpleHTTPRequestHandler):
  def _should_fail(self):
    for p in fail_patterns:
      if p in self.path:
        return True
    return False

  def do_GET(self):
    if self._should_fail():
      self.send_response(503)
      self.send_header("Content-Length", "0")
      self.end_headers()
      return
    BaseHTTPServer.SimpleHTTPRequestHandler.do_GET(self)

  def do_HEAD(self):
    if self._should_fail():
      self.send_response(503)
      self.send_header("Content-Length", "0")
      self.end_headers()
      return
    BaseHTTPServer.SimpleHTTPRequestHandler.do_HEAD(self)

  def log_message(self, fmt, *args):
    sys.stderr.write("%s %s\n" % (self.log_date_time_string(), fmt % args))
    sys.stderr.flush()


class ThreadedServer(SocketServer.ThreadingMixIn, SocketServer.TCPServer):
  allow_reuse_address = True
  daemon_threads = True


os.chdir(options.docroot)
print("serving %s on port %d, failing %s" %
      (options.docroot, options.http_port, fail_patterns))
sys.stdout.flush()
ThreadedServer(("", options.http_port), FaultyHandler).serve_forever()
