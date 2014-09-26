#!/usr/bin/python

import cvmfs

import sys

def usage():
	print sys.argv[0] + " <repository path | repository url> <nested catalog path>"
	print "This script walks through the catalog structure and opens the nested"
	print "catalog holding the path specified."
	print "WARNING: changes to this database will not persist, as it is only a temp"


def main():
	if len(sys.argv) != 3:
		usage()
		sys.exit(1)

	repo = cvmfs.OpenRepository(sys.argv[1])
	nested_catalog = repo.RetrieveCatalogForPath(sys.argv[2])

	print "Opening: " + nested_catalog.root_prefix
	nested_catalog.OpenInteractive()

main()
