#!/usr/bin/python

import cvmfs
import sys

def usage():
	print sys.argv[0] + " <catalog path | catalog url>"
	print "This script decompresses the given catalog file to a temporary storage"
	print "and opens it directly with sqlite3."
	print "WARNING: changes to this database will not persist, as it is only a temp"

def main():
	if len(sys.argv) != 2:
		usage()
		sys.exit(1)

	catalog = cvmfs.OpenCatalog(sys.argv[1])
	catalog.OpenInteractive()

main()
