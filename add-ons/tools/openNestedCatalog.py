#!/usr/bin/python

from urllib2 import urlopen, URLError, HTTPError
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


def retrieveFile(repo, filename):
	url = repo + "/" + filename

	if repo[0:7] != "http://":
		try:
			f = open(url, "rb")
			return f
		except:
			print "cannot open " , url

	# download the catalog
	myTmpFile = tempfile.NamedTemporaryFile('w+b')
	try:
		f = urlopen(url)
		meta = f.info()
		fileSize = int(meta.getheaders("Content-Length")[0])

		myTmpFile.write(f.read())
	except HTTPError, e:
		print "HTTP: " + e.code + url
		sys.exit(1)
	except URLError, e:
		print "URL:" + e.reason + url
		sys.exit(1)

	myTmpFile.flush()
	myTmpFile.seek(0)
	return myTmpFile


def doHttpRequest(url):
	response = urlopen(url)
	return response.read()


def downloadCatalog(repositoryUrl, catalogName, beVerbose):
	# find out some pathes and init the zlib decompressor
	subdir = catalogName[0:2]
	filename = catalogName[2:] + "C"
	url = repositoryUrl + "/data/" + subdir + "/" + filename


def getRootCatalogName(cvmfspublished):
	try:
		cvmfspubdata = cvmfspublished.read()
	except:
		print "cannot read .cvmfspublished"
		sys.exit(1)

	lines = cvmfspubdata.split('\n')
	if len(lines) < 1:
		print ".cvmfspublished is malformed"
		sys.exit(1)

	return lines[0][1:]


def getCalalogFilePath(catalogName):
	return "data/" + catalogName[:2] + "/" + catalogName[2:] + "C"


def decompressCatalog(compressedFile):
	myTmpFile = tempfile.NamedTemporaryFile('wb')
	myTmpFile.write(zlib.decompress(compressedFile.read()))
	myTmpFile.flush()
	return myTmpFile


def fetchCatalog(repo, catalogName):
	catalogPath = getCalalogFilePath(catalogName)
	return decompressCatalog(retrieveFile(repo, catalogPath))


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


def findNestedCatalogName(repo, catalogName, nestedCatalogPath):
	clgFile = fetchCatalog(repo, catalogName)
	nestedClgs = retrieveNestedCatalogs(clgFile)
	clgName = ""

	for clg in nestedClgs:
		if nestedCatalogPath.startswith(clg[0]):
			print "Catalog Path:" , clg[0]
			if nestedCatalogPath == clg[0]:
				clgName = clg[1]
				break
			else:
				clgName = findNestedCatalogName(repo, clg[1], nestedCatalogPath)
				break

	clgFile.close()
	return clgName


def openCatalog(catalogFile):
	subprocess.call(['sqlite3', catalogFile.name])


def usage():
	print sys.argv[0] + " <repository name | repository url> <nested catalog path>"
	print "This script walks through the catalog structure and opens the nested"
	print "catalog specified."
	print "WARNING: changes to this database will not persist, as it is only a temp"


def main():
	if len(sys.argv) != 3:
		usage()
		sys.exit(1)

	repo = sys.argv[1]
	nestedCatalogPath = sys.argv[2]

	rootCatalogName = getRootCatalogName(retrieveFile(repo, ".cvmfspublished"))
	nestedCatalog = findNestedCatalogName(repo, rootCatalogName, nestedCatalogPath)

	if nestedCatalog == "":
		print "nested catalog not found"
		sys.exit(1)

	print "Opening Clg: " , nestedCatalog
	catalogFile = fetchCatalog(repo, nestedCatalog)
	openCatalog(catalogFile)

main()
