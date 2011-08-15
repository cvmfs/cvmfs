#!/usr/bin/python

import sqlite
import urllib2
import sys
import os
import zlib

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

def downloadCatalog(repositoryUrl, catalogName, catalogDirectory):
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

def retrieveCatalogsRecursively(repositoryUrl, catalogName, catalogDirectory):
	downloadCatalog(repositoryUrl, catalogName, catalogDirectory)
	nestedCatalogs = findNestedCatalogs(catalogName, catalogDirectory)
	for catalog in nestedCatalogs:
		retrieveCatalogsRecursively(repositoryUrl, catalog[0], catalogDirectory)

def main():
	if len(sys.argv) != 3:
		usage()
		sys.exit(1)

	repo = "http://cvmfs-stratum-one.cern.ch/opt/" + sys.argv[1]
	catalogDirectory = sys.argv[2]
	
	rootCatalog = getRootCatalogName(repo)
	retrieveCatalogsRecursively(repo, rootCatalog, catalogDirectory)

def usage():
	print "Usage: " + sys.argv[0] + " <repository name> <download directory>"
	print "This script walks through all nested catalogs of a repository and"
	print "downloads these catalogs to the given destination directory"
	print "Take care: the catalogs are saved uncompressed, so do not use cvmfs_zpipe"

def printError(errorMessage):
	print "[ERROR] " + errorMessage
	sys.exit(1)
	
main()