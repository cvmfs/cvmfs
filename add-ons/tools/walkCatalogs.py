#!/usr/bin/python

import sys
import os
import zlib
import tempfile
from optparse import OptionParser

# figure out which sqlite module to use
# in Python 2.4 an old version is present
# which does not allow proper read out of
# long int
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


def getCatalogFilePath(catalogName, catalogDirectory):
  return catalogDirectory + "/" + catalogName[0:2] + "/" + catalogName[2:] + "C"


def decompressCatalog(filename, destination):
  str_object1 = open(filename, 'rb').read()
  str_object2 = zlib.decompress(str_object1)
  f = open(destination, 'wb')
  f.write(str_object2)
  f.close()


def decompressAndOpenCatalog(catalogName, catalogDirectory):
  catalogFile = getCatalogFilePath(catalogName, catalogDirectory)
  tempFile    = tempfile.NamedTemporaryFile('wb')
  decompressCatalog(catalogFile, tempFile.name)
  return openCatalog(tempFile.name)


def openCatalog(catalogFile):
  dbHandle = sqlite.connect(catalogFile)
  cursor = dbHandle.cursor()
  return cursor


def closeCatalog(cursor):
  cursor.close()


def findNestedCatalogs(catalogCursor):
  catalogs = []

  # nested catalog references
  catalogCursor.execute("SELECT sha1 FROM nested_catalogs")
  result = catalogCursor.fetchall()
  for catalog in result:
    catalogs.append(catalog[0])

  return catalogs


def runSqlStatement(dbCursor, statement):
  dbCursor.execute(statement)
  result = dbCursor.fetchall()
  for entry in result:
    print entry


def getRootPrefix(dbCursor):
  dbCursor.execute("SELECT value FROM properties WHERE key='root_prefix';")
  result = dbCursor.fetchall()
  if len(result) == 0:
    return "/"
  else:
    return result[0][0]


def walkCatalogsRecursively(catalogName, catalogDirectory, beVerbose, beRecursive, statement):
  catalogs = [catalogName]
  catalogsProcessed = 0
  while catalogs:
    catalog = catalogs.pop(0)

    dbCursor = decompressAndOpenCatalog(catalog, catalogDirectory)
    nestedCatalogs = findNestedCatalogs(dbCursor)
    rootPrefix = getRootPrefix(dbCursor)

    print "[" , rootPrefix , "]"

    if beVerbose:
      print "--> opened" , catalog , "(found:" , len(nestedCatalogs) , "new nested catalogs |" , len(catalogs) , "in queue"

    if beRecursive:
      catalogs.extend(nestedCatalogs)

    runSqlStatement(dbCursor, statement)

    closeCatalog(dbCursor)
    catalogsProcessed += 1

  return catalogsProcessed


def main():
  usage = "This script walks through a list of downloaded catalogs and runs the provided SQL statement on each of them. It is useful for some investigation tasks on a catalog hierarchy downloaded with downloadCatalogs.py"
  parser = OptionParser(usage)
  parser.add_option("-d", "--directory", dest="catalogDirectory", default="catalogs", help="the directory to download catalogs to")
  parser.add_option("-q", "--quiet", action="store_false", dest="verbose", default=True, help="don't print status messages to stdout")
  parser.add_option("-s", "--dont-recurse", action="store_false", dest="recurse", default=True, help="don't recurse into nested catalogs")
  (options, args) = parser.parse_args()

  if len(args) != 2:
    parser.error("Please provide the catalog hash and the SQL statement as arguments")

  # read command line arguments
  catalogName = args[0]
  sql = args[1]
  catalogDirectory = options.catalogDirectory
  verbose = options.verbose
  recurse = options.recurse

  # do the job
  numCatalogs = walkCatalogsRecursively(catalogName, catalogDirectory, verbose, recurse, sql)

  print "processed" , numCatalogs , "catalogs"

main()
