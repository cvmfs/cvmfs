#!/usr/bin/env python

import sys
import cvmfs

def usage():
    print sys.argv[0] + " <local repo name | remote repo url> [root catalog]"
    print "This script looks in the given root_catalog (or the repository HEAD) and"
    print "retrieves the contained statistics counters in CVMFS 2.1 catalogs."
    print
    print "Information is printed as a space separated list containing:"
    print "[revision] [regular files] [directories] [symlinks] [file volume] [chunked files] [chunked volume] [chunks] [nested catalogs]"

if len(sys.argv) != 2 and len(sys.argv) != 3:
    usage();
    sys.exit(1)

repo_identifier   = sys.argv[1]
root_catalog_hash = sys.argv[2] if len(sys.argv) == 3 else None

repo         = cvmfs.open_repository(repo_identifier)
root_catalog = repo.retrieve_catalog(root_catalog_hash) if root_catalog_hash else repo.retrieve_root_catalog()
statistics   = root_catalog.get_statistics()

print ' '.join([str(root_catalog.revision)] + [ str(x) for x in statistics.get_all_fields() ])
