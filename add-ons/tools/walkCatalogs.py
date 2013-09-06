#!/usr/bin/python

import cvmfs

import sys

def walkCatalogsRecursively(repository, catalog, sql):
  catalogs = [catalog]
  catalogs_processed = 0
  while catalogs:
    clg = catalogs.pop(0)

    print "[" , clg.root_prefix , "]"

    result = clg.RunSql(sql)
    for row in result:
      print row

    nested_catalogs = clg.ListNested()
    for nested in nested_catalogs:
      catalogs.append(repository.RetrieveCatalog(nested.hash))

    catalogs_processed += 1

  return catalogs_processed


def usage():
  print sys.argv[0] , "<repository path | repository url> <SQL statement>"
  print "This script walks through all catalogs of a repository and applies an"
  print "SQL statement in each catalog. This is useful for investigative work"
  print "WARNING: any changes will not persist in the catalogs!"


def main():
  if len(sys.argv) != 3:
    usage()
    sys.exit(1)

  repo = cvmfs.OpenRepository(sys.argv[1])
  root_clg = repo.RetrieveRootCatalog()
  catalogs_processed = walkCatalogsRecursively(repo, root_clg, sys.argv[2])

  print "processed" , catalogs_processed , "catalogs"


main()
