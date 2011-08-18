#!/usr/bin/python

import sys
import zlib
import tempfile
import subprocess

def getRootCatalogName(cvmfspublished):
	try:
		cvmfspubdata = open(cvmfspublished, 'rb').read()
	except:
		print "cannot open .cvmfspublished"
		sys.exit(1)
	
	lines = cvmfspubdata.split('\n')
	if len(lines) < 1:
		print ".cvmfspublished is malformed"
		sys.exit(1)

	return lines[0][1:]

def decompressCatalog(filename, destination):
	str_object1 = open(filename, 'rb').read()
	str_object2 = zlib.decompress(str_object1)
	f = open(destination, 'wb')
	f.write(str_object2)
	f.close()

def openCatalog(filename):
	subprocess.call(['sqlite3', filename])

def usage():
	print sys.argv[0] + " <cvmfs/pub path>"
	print "This script decompresses the root catalog file to a temporary storage"
	print "and opens this directly with sqlite3."
	print "WARNING: changes to this database will not persist, as it is only a temp"

def main():
	if len(sys.argv) != 2:
		usage()
		sys.exit(1)
		
	rootCatalog = getRootCatalogName(sys.argv[1] + "/catalogs/.cvmfspublished")
	
	myTmpFile = tempfile.NamedTemporaryFile('wb')
	decompressCatalog(sys.argv[1] + "/data/" + rootCatalog[:2] + "/" + rootCatalog[2:] + "C", myTmpFile.name)
	openCatalog(myTmpFile.name)
	myTmpFile.close()
	
main()