#!/usr/bin/python

import urllib2
import sys
import os
import zlib
import shutil
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
	response = urllib2.urlopen(url)
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
	decoder = zlib.decompressobj()
	
	# create target directory if not existing and open output file
	createDirectory(destDir)
	outputFile = open(dest, 'wb')
	
	# download and decompress on the fly
	u = urllib2.urlopen(url)
	meta = u.info()
	fileSize = int(meta.getheaders("Content-Length")[0])
	
	if beVerbose:
		print "retrieving " + catalogName + " - " + str(fileSize) + " bytes"
	
	blockSize = 8192
	while True:
		buffer = u.read(blockSize)
		if not buffer:
			break
		data = decoder.decompress(buffer)
		outputFile.write(data)

	# write remaining stuff to file
	outputFile.write(decoder.decompress(buffer))
	outputFile.close()
	
def findNestedCatalogs(catalogName, catalogDirectory):
	catalogFile = getCatalogFilePath(catalogName, catalogDirectory)
	dbHandle = sqlite.connect(catalogFile)
	cursor = dbHandle.cursor()
	cursor.execute("SELECT sha1 FROM nested_catalogs")
	result = cursor.fetchall()
	dbHandle.close()
	return result

def retrieveCatalogsRecursively(repositoryUrl, catalogName, catalogDirectory, beVerbose):
	catalogs = [catalogName]
	downloadCatalog(repositoryUrl, catalogName, catalogDirectory, beVerbose)
	nestedCatalogs = findNestedCatalogs(catalogName, catalogDirectory)
	for catalog in nestedCatalogs:
		newCatalogs = retrieveCatalogsRecursively(repositoryUrl, catalog[0], catalogDirectory, beVerbose)
		catalogs.extend(newCatalogs)
	return catalogs

def mergeCatalogs(rootCatalog, catalogList, catalogDirectory, mergeTo, beVerbose):
	# create destination file by copying the root catalog
	try:
		shutil.copyfile(getCatalogFilePath(rootCatalog, catalogDirectory), mergeTo)
	except:
		printError("Cannot write '" + mergeTo + "'")
	
	# open destination database file
	destDatabaseHandle = sqlite.connect(mergeTo)
	
	# merge catalogs
	for catalog in catalogList:
		if catalog == rootCatalog:
			continue
		mergeCatalogDatabases(destDatabaseHandle, catalog, catalogDirectory, beVerbose)
	
	# close destination database
	destDatabaseHandle.close()

def countEntriesInDatabase(dbCursor):
	dbCursor.execute("SELECT count(*) FROM catalog")
	return dbCursor.fetchone()[0]

def mergeCatalogDatabases(destDatabase, catalogName, catalogDirectory, beVerbose):
	# open database of source catalog
	catalogFile = getCatalogFilePath(catalogName, catalogDirectory)
	dbHandle = sqlite.connect(catalogFile)
	destCursor = destDatabase.cursor()
	srcCursor = dbHandle.cursor()
	
	if beVerbose:
		print "merging " + catalogName + " with " + str(countEntriesInDatabase(srcCursor)) + " entries"
	
	# merge data from source database to given destination
	srcCursor.execute("SELECT * FROM catalog")
	skips = 0
	while True:
		result = srcCursor.fetchone()
		if result == None:
			break
		
		try:
			destCursor.execute("INSERT INTO catalog VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", tuple(result))
		except:
			skips += 1
	
	if beVerbose:
		print str(skips) + " non-unique entries skipped"

	# finish
	destDatabase.commit()
	dbHandle.close()

def main():
	usage = "usage: %prog [options] <repository url>\nThis script walks through all nested catalogs of a repository and\ndownloads these catalogs to the given destination directory\nTake care: the catalogs are saved uncompressed, so do not use cvmfs_zpipe"
	parser = OptionParser(usage)
	parser.add_option("-d", "--directory", dest="catalogDirectory", default="catalogs", help="the directory to download catalogs to")
	parser.add_option("-m", "--merge", metavar="FILE", dest="mergeToFile", help="merge all catalogs into one given file")
	parser.add_option("-q", "--quiet", action="store_false", dest="verbose", default=True, help="don't print status messages to stdout")
	(options, args) = parser.parse_args()
	
	if len(args) != 1:
		parser.error("Please provide the repository url as argument")

	# read command line arguments
	repositoryUrl = args[0]
	catalogDirectory = options.catalogDirectory
	merge = options.mergeToFile
	verbose = options.verbose
	
	# check option consistency
	if os.path.exists(catalogDirectory) and os.listdir(catalogDirectory) != []:
		printError("Directory '" + catalogDirectory + "' exists and is not empty")
	
	if merge and foundSqlite and not foundSqlite3:
		printError("unfortunately merging is not possible with your version of the python sqlite module")
	
	# do the job
	rootCatalog = getRootCatalogName(repositoryUrl)
	catalogs = retrieveCatalogsRecursively(repositoryUrl, rootCatalog, catalogDirectory, verbose)
	
	if merge:
		mergeCatalogs(rootCatalog, catalogs, catalogDirectory, merge, verbose)

def printError(errorMessage):
	print "[ERROR] " + errorMessage
	sys.exit(1)
	
main()