#!/usr/bin/python

from urllib2 import urlopen, URLError, HTTPError
import sys
import os
import zlib
import shutil
import tempfile
from optparse import OptionParser

foundSqlite = False
foundSqlite3 = False

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


def downloadCatalog(repositoryUrl, catalogName, catalogDirectory, beVerbose):
	# find out some pathes and init the zlib decompressor
	subdir = catalogName[0:2]
	filename = catalogName[2:] + "C"
	url = repositoryUrl + "/data/" + subdir + "/" + filename
	destDir = catalogDirectory + "/" + subdir + "/"
	dest = destDir + filename
	#decoder = zlib.decompressobj()

	# create target directory if not existing and open output file
	createDirectory(destDir)
	outputFile = open(dest, 'wb')

	# download the catalog
	try:
		f = urlopen(url)
		meta = f.info()
		fileSize = int(meta.getheaders("Content-Length")[0])

		if beVerbose:
			print "retrieving " + catalogName + " - " + str(fileSize) + " bytes"

		with open(dest, "wb") as local_file:
			local_file.write(f.read())
	except HTTPError, e:
		printError("HTTP: " + e.code + url)
	except URLError, e:
		printError("URL:" + e.reason + url)


def decompressCatalog(filename, destination):
	str_object1 = open(filename, 'rb').read()
	str_object2 = zlib.decompress(str_object1)
	f = open(destination, 'wb')
	f.write(str_object2)
	f.close()


def findNestedCatalogs(catalogName, catalogDirectory, getHistory):
	catalogFile = getCatalogFilePath(catalogName, catalogDirectory)
	tempFile    = tempfile.NamedTemporaryFile('wb')
	decompressCatalog(catalogFile, tempFile.name)

	dbHandle = sqlite.connect(tempFile.name)
	cursor = dbHandle.cursor()
	catalogs = []

	# nested catalog references
	cursor.execute("SELECT sha1 FROM nested_catalogs")
	result = cursor.fetchall()
	for catalog in result:
		catalogs.append(catalog[0])

	# history references
	if getHistory:
		cursor.execute("SELECT value FROM properties WHERE key = 'previous_revision' LIMIT 1")
		result = cursor.fetchall()
		if result:
			catalogs.append(result[0][0])

	dbHandle.close()
	tempFile.close()
	return catalogs


def retrieveCatalogsRecursively(repositoryUrl, catalogName, catalogDirectory, beVerbose, getHistory):
	catalogs = [catalogName]
	downloads = 0
	while catalogs:
		catalog = catalogs.pop(0)
		if os.path.exists(getCatalogFilePath(catalog, catalogDirectory)):
			if beVerbose:
				print "--> skipping already loaded catalog:" , catalog
			continue

		downloadCatalog(repositoryUrl, catalog, catalogDirectory, beVerbose)
		nestedCatalogs = findNestedCatalogs(catalog, catalogDirectory, getHistory)
		downloads += 1

		if beVerbose:
			print "--> found" , len(nestedCatalogs) , "catalog references |" , len(catalogs) , "in queue"
			catalogs.extend(nestedCatalogs)

	return downloads


def main():
	usage = "usage: %prog [options] <repository url>\nThis script walks through all nested catalogs of a repository and\ndownloads these catalogs to the given destination directory\nTake care: the catalogs are saved uncompressed, so do not use cvmfs_zpipe"
	parser = OptionParser(usage)
	parser.add_option("-d", "--directory", dest="catalogDirectory", default="catalogs", help="the directory to download catalogs to")
	parser.add_option("-m", "--merge", metavar="FILE", dest="mergeToFile", help="merge all catalogs into one given file")
	parser.add_option("-q", "--quiet", action="store_false", dest="verbose", default=True, help="don't print status messages to stdout")
	parser.add_option("-l", "--history", action="store_true", dest="history", default=False, help="download the catalog history")
	(options, args) = parser.parse_args()

	if len(args) != 1:
		parser.error("Please provide the repository url as argument")

	# read command line arguments
	repositoryUrl = args[0]
	catalogDirectory = options.catalogDirectory
	merge = options.mergeToFile
	verbose = options.verbose
	history = options.history

	# check option consistency
	if os.path.exists(catalogDirectory) and os.listdir(catalogDirectory) != []:
		printError("Directory '" + catalogDirectory + "' exists and is not empty")

	if merge and foundSqlite and not foundSqlite3:
		printError("unfortunately merging is not possible with your version of the python sqlite module")

	# do the job
	rootCatalog = getRootCatalogName(repositoryUrl)
	numCatalogs = retrieveCatalogsRecursively(repositoryUrl, rootCatalog, catalogDirectory, verbose, history)

	print "downloaded" , numCatalogs , "catalogs"

	if merge:
		mergeCatalogs(rootCatalog, catalogs, catalogDirectory, merge, verbose)

def printError(errorMessage):
	print "[ERROR] " + errorMessage
	sys.exit(1)

main()
