#!/usr/bin/env python3

import sys
import optparse
import cvmfs

def usage():
    print sys.argv[0] + "[-l] <local repo name | remote repo url> [root catalog]"
    print "This script looks in the given root_catalog (or the repository HEAD) and"
    print "retrieves the contained statistics counters in CVMFS 2.1 catalogs."
    print
    print "Information is printed as a space separated list containing:"
    print "[revision] [regular files] [directories] [symlinks] [file volume] [chunked files] [chunked volume] [chunks] [nested catalogs]"

parser = optparse.OptionParser()
parser.add_option("-l", "--human-readable", action="store_true", dest="human_readable", help="output with labels", default=False)
(options, args) = parser.parse_args()

if len(args) != 1 and len(args) != 2:
    usage();
    sys.exit(1)

repo_identifier   = args[0]
root_catalog_hash = args[1] if len(args) == 2 else None

repo         = cvmfs.open_repository(repo_identifier)
root_catalog = repo.retrieve_catalog(root_catalog_hash) if root_catalog_hash else repo.retrieve_root_catalog()
statistics   = root_catalog.get_statistics()
fields       = statistics.get_all_fields()

if options.human_readable:
    print "Revision:                    " , root_catalog.revision
    print "Regular Files:               " , fields[0]
    print "Directories:                 " , fields[1]
    print "Symlinks:                    " , fields[2]
    print "Aggregated File Size:        " , fields[3] , "bytes"
    print "Chunked Files:               " , fields[4]
    print "Aggregated Chunked File Size:" , fields[5] , "bytes"
    print "Stored File Chunks:          " , fields[6]
    print "Nested Catalogs:             " , fields[7]
else:
    print ' '.join([str(root_catalog.revision)] + [ str(x) for x in statistics.get_all_fields() ])
