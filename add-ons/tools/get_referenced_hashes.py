#!/usr/bin/env python

import sys
import cvmfs

def usage():
    print sys.argv[0] + " <local repo name | remote repo url> [root catalog]"
    print "This script walks the catalogs and generates a list of all referenced content hashes."

# get referenced hashes from a single catalog (files, chunks, nested catalogs)
def get_hashes_for_catalog(catalog):
    print >> sys.stderr, "Processing" , catalog.hash , catalog
    query = "  SELECT DISTINCT           \
                 lower(hex(hash))        \
               FROM catalog              \
               WHERE hash != 0           \
             UNION                       \
               SELECT DISTINCT           \
                 lower(hex(hash)) || 'P' \
               FROM chunks               \
               WHERE hash != 0           \
             UNION                       \
               SELECT DISTINCT           \
                 sha1 || 'C'             \
               FROM nested_catalogs;"
    return { res[0] for res in catalog.run_sql(query) }

def get_hashes_for_catalog_tree(repo, root_catalog):
    hashes = { root_catalog.hash + "C" }
    for catalog in repo.catalogs(root_catalog):
        hashes = hashes | get_hashes_for_catalog(catalog)
    return hashes

def get_hashes_for_revision(repo, root_hash = None):
    root_catalog = repo.retrieve_catalog(root_hash) if root_hash else repo.retrieve_root_catalog()
    return get_hashes_for_catalog_tree(repo, root_catalog)


# check input values
if len(sys.argv) != 2 and len(sys.argv) != 3:
    usage()
    sys.exit(1)

# get input parameters
repo_identifier   = sys.argv[1]
root_catalog_hash = sys.argv[2] if len(sys.argv) == 3 else None

repo = cvmfs.open_repository(repo_identifier)
hashes = get_hashes_for_revision(repo, root_catalog_hash)

print '\n'.join(hashes)
