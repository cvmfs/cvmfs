#!/usr/bin/python

import sys
import zlib
import tempfile
import subprocess

def decompressCatalog(filename, destination):
	str_object1 = open(filename, 'rb').read()
	str_object2 = zlib.decompress(str_object1)
	f = open(destination, 'wb')
	f.write(str_object2)
	f.close()

def openCatalog(filename):
	subprocess.call(['sqlite3', filename])

def usage():
	print sys.argv[0] + " <catalog path>"
	print "This script decompresses the given catalog file to a temporary storage"
	print "and opens this directly with sqlite3."
	print "WARNING: changes to this database will not persist, as it is only a temp"

def main():
	if len(sys.argv) != 2:
		usage()
		sys.exit(1)
	
	myTmpFile = tempfile.NamedTemporaryFile('wb')
	decompressCatalog(sys.argv[1], myTmpFile.name)
	openCatalog(myTmpFile.name)
	myTmpFile.close()
	
main()