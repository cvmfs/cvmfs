#!/usr/bin/env python3

from __future__ import print_function
import socket
try:
  import SocketServer as socketserver
except ImportError:
  import socketserver
import sys
import time
import threading
import os
import datetime

def usage():
	print("This opens a socket on a given port number and waits for connection.", file=sys.stderr)
	print("Connecting programs can send but will not receive anything.", file=sys.stderr)
	print("Usage:" , sys.argv[0] , "<protocol: TCP|UDP> <port number>", file=sys.stderr)
	sys.stderr.flush()
	sys.exit(1)

print_lock = threading.Lock()
def print_msg(msg):
	global print_lock
	print_lock.acquire()
	print("[Silent Socket]" , msg)
	print_lock.release()
	sys.stdout.flush()


class SilentHandler(socketserver.BaseRequestHandler):
	def handle(self):
		print_msg("(" + str(datetime.datetime.now()) + ") incoming connection: " + str(self.client_address))
		time.sleep(100000000)

class ThreadedTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
	pass

class ThreadedUDPServer(socketserver.ThreadingMixIn, socketserver.UDPServer):
	pass


if len(sys.argv) != 3:
	usage()

server_host     = 'localhost' # all available interfaces
server_port     = 0
server_protocol = sys.argv[1]

try:
	server_port = int(sys.argv[2])
except:
	usage()

try:
	server = ''
	if server_protocol == "TCP":
		server = ThreadedTCPServer((server_host, server_port), SilentHandler)
	elif server_protocol == "UDP":
		server = ThreadedUDPServer((server_host, server_port), SilentHandler)
	else:
		usage()

	print_msg("starting a " + server_protocol + " server on port " + str(server_port))
	server.serve_forever()
except socket.error as msg:
	print_msg("Failed to open port")
	print(msg)
