#!/usr/bin/python

import sys
import zlib
import tempfile
import subprocess

# figure out which sqlite module to use
# in Python 2.4 an old version is present
# which does not allow proper read out of
# long int and therefore cannot merge catalogs
try:
	import sqlite3 as sqlite
	foundSqlite3 = True
except:
	pass
if not foundSqlite3:
	try:
		import sqlite
		foundSqlite = True
	except ImportError, e:
		pass

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


def getCalalogFilePath(repoDir, catalogName):
	return repoDir + "data/" + catalogName[:2] + "/" + catalogName[2:] + "C"


def decompressCatalog(filename, destination):
	str_object1 = open(filename, 'rb').read()
	str_object2 = zlib.decompress(str_object1)
	f = open(destination, 'wb')
	f.write(str_object2)
	f.close()


def fetchCatalog(repoDir, catalogName):
	myTmpFile = tempfile.NamedTemporaryFile('wb')
	decompressCatalog(getCalalogFilePath(repoDir,catalogName), myTmpFile.name)
	return myTmpFile


def retrieveNestedCatalogs(catalogFile):
	dbHandle = sqlite.connect(catalogFile.name)
	cursor = dbHandle.cursor()
	catalogs = []

	# nested catalog references
	cursor.execute("SELECT path, sha1 FROM nested_catalogs")
	result = cursor.fetchall()
	for catalog in result:
		catalogs.append((catalog[0], catalog[1]))

	dbHandle.close()
	return catalogs


def findNestedCatalogName(repoDir, catalog, nestedCatalogPath):
	clgFile = fetchCatalog(repoDir, catalog)
	nestedClgs = retrieveNestedCatalogs(clgFile)
	clgName = ""

	for clg in nestedClgs:
		if nestedCatalogPath.startswith(clg[0]):
			if nestedCatalogPath == clg[0]:
				clgName = clg[1]
				break
			else:
				clgName = findNestedCatalogName(repoDir, clg[1], nestedCatalogPath)
				break

	clgFile.close()
	return clgName


def openCatalog(catalogFile):
	subprocess.call(['sqlite3', catalogFile.name])


def usage():
	print sys.argv[0] + " <repository name> <nested catalog path>"
	print "This script walks through the catalog structure and opens the nested"
	print "catalog specified."
	print "WARNING: changes to this database will not persist, as it is only a temp"


def main():
	if len(sys.argv) != 3:
		usage()
		sys.exit(1)

	repoDir = "/storage/" + sys.argv[1] + "/";
	rootCatalog = getRootCatalogName(repoDir + ".cvmfspublished")
	nestedCatalog = findNestedCatalogName(repoDir, rootCatalog, sys.argv[2])

	if nestedCatalog == "":
		print "nested catalog not found"
		sys.exit(1)

	print "Opening: " + nestedCatalog
	catalogFile = fetchCatalog(repoDir, nestedCatalog)
	openCatalog(catalogFile)
	catalogFile.close()

main()
