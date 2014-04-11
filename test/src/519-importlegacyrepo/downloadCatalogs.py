#!/usr/bin/python

from urllib2 import urlopen, URLError, HTTPError
import sys
import os
import zlib
import shutil
import tempfile
from optparse import OptionParser

# figure out which sqlite module to use
# in Python 2.4 there is dbapi2 in pysqlite2 which will become sqlite3
# in the standard library later on but was not at this time (software archeology)
try:
	import sqlite3
except:
	from pysqlite2 import dbapi2 as sqlite3


def hash2hex(hash):
	return "".join(map(lambda c: ("%0.2X" % c).lower(),map(ord,hash)))


def doHttpRequest(url):
	response = urlopen(url)
	return response.read()


def getRootCatalogName(repositoryUrl):
	result = ""
	try:
		result = doHttpRequest(repositoryUrl + "/.cvmfspublished")
	except:
		printError("cannot load .cvmfspublished")

	lines = result.split('\n')
	if len(lines) < 1:
		printError(".cvmfspublished is malformed")

	return lines[0][1:]


def createDirectory(directory):
	d = os.path.dirname(directory)
	if not os.path.exists(d):
		os.makedirs(d)


def getCatalogFilePath(catalogName, catalogDirectory):
	return catalogDirectory + "/" + catalogName[0:2] + "/" + catalogName[2:] + "C"


def downloadObject(repositoryUrl, objectName, resultDirectory, beVerbose):
	subdir   = objectName[0:2]
	filename = objectName[2:]
	url      = repositoryUrl + "/data/" + subdir + "/" + filename
	destDir  = resultDirectory + "/" + subdir + "/"
	dest     = destDir + filename

	# create target directory if not existing and open output file
	createDirectory(destDir)
	outputFile = open(dest, 'wb')

	# download the catalog
	try:
		f = urlopen(url)
		meta = f.info()
		fileSize = int(meta.getheaders("Content-Length")[0])

		if beVerbose:
			print "retrieving " + objectName + " - " + str(fileSize) + " bytes"

		local_file = open(dest, "wb")
		try:
			local_file.write(f.read())
		finally:
			local_file.close()
	except HTTPError, e:
		printError("HTTP: " + str(e.code) + " " + url)
	except URLError, e:
		printError("URL: " + e.reason +  " " + url)


def downloadCatalog(repositoryUrl, catalogName, catalogDirectory, beVerbose):
	downloadObject(repositoryUrl, catalogName + "C", catalogDirectory, beVerbose)


def decompressCatalog(filename, destination):
	str_object1 = open(filename, 'rb').read()
	str_object2 = zlib.decompress(str_object1)
	f = open(destination, 'wb')
	f.write(str_object2)
	f.close()


def findNestedCatalogs(catalogName, catalogDirectory, repositoryUrl, beVerbose, getDirtab):
	catalogFile = getCatalogFilePath(catalogName, catalogDirectory)
	tempFile    = tempfile.NamedTemporaryFile('wb')
	decompressCatalog(catalogFile, tempFile.name)

	dbHandle = sqlite3.connect(tempFile.name)
	cursor = dbHandle.cursor()
	catalogs = []

	# nested catalog references
	cursor.execute("SELECT sha1 FROM nested_catalogs")
	result = cursor.fetchall()
	for catalog in result:
		catalogs.append(catalog[0])

	# dirtab entry
	if getDirtab:
		cursor.execute("SELECT hash FROM catalog WHERE name = '.cvmfsdirtab'")
		result = cursor.fetchall()
		for dirtab in result:
			print "--> found .cvmfsdirtab"
			sha1 = hash2hex(result[0][0])
			downloadObject(repositoryUrl, sha1, catalogDirectory, beVerbose)


	dbHandle.close()
	tempFile.close()
	return catalogs


def retrieveCatalogsRecursively(repositoryUrl, catalogName, catalogDirectory, beVerbose, getDirtab):
	catalogs = [catalogName]
	downloads = 0
	while catalogs:
		catalog = catalogs.pop(0)
		if os.path.exists(getCatalogFilePath(catalog, catalogDirectory)):
			if beVerbose:
				print "--> skipping already loaded catalog:" , catalog
			continue

		downloadCatalog(repositoryUrl, catalog, catalogDirectory, beVerbose)
		nestedCatalogs = findNestedCatalogs(catalog, catalogDirectory, repositoryUrl, beVerbose, getDirtab)
		downloads += 1

		if beVerbose:
			print "--> found" , len(nestedCatalogs) , "catalog references |" , len(catalogs) , "in queue"
			catalogs.extend(nestedCatalogs)

	return downloads


def main():
	usage = "usage: %prog [options] <repository url>\nThis script walks through all nested catalogs of a repository and\ndownloads these catalogs to the given destination directory\nTake care: the catalogs are saved uncompressed, so do not use cvmfs_zpipe"
	parser = OptionParser(usage)
	parser.add_option("-d", "--directory", dest="catalogDirectory", default="catalogs", help="the directory to download catalogs to")
	parser.add_option("-q", "--quiet", action="store_false", dest="verbose", default=True, help="don't print status messages to stdout")
	parser.add_option("-t", "--get-dirtab", dest="getDirtab", action="store_true", default=False, help="download the data chunk for .cvmfsdirtab")
	(options, args) = parser.parse_args()

	if len(args) != 1:
		parser.error("Please provide the repository url as argument")

	# read command line arguments
	repositoryUrl = args[0]
	catalogDirectory = options.catalogDirectory
	verbose = options.verbose
	getDirtab = options.getDirtab

	# check option consistency
	if os.path.exists(catalogDirectory) and os.listdir(catalogDirectory) != []:
		printError("Directory '" + catalogDirectory + "' exists and is not empty")

	# do the job
	rootCatalog = getRootCatalogName(repositoryUrl)
	numCatalogs = retrieveCatalogsRecursively(repositoryUrl, rootCatalog, catalogDirectory, verbose, getDirtab)

	print "downloaded" , numCatalogs , "catalogs"


def printError(errorMessage):
	print "[ERROR] " + errorMessage
	sys.exit(1)

main()
