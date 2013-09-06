#!/usr/bin/python

import cvmfs

import sys

def usage():
	print sys.argv[0] + " <repository path | repository url>"
	print "This script decompresses the root catalog file to a temporary storage"
	print "and opens this directly with sqlite3."
	print "WARNING: changes to this database will not persist, as it is only a temp"

def main():
	if len(sys.argv) != 2:
		usage()
		sys.exit(1)

	repo = cvmfs.OpenRepository(sys.argv[1])
	root_clg = repo.RetrieveRootCatalog()
	root_clg.OpenInteractive()

main()
