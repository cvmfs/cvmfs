#!/usr/bin/env python3

import sys
import cvmfs

def usage():
    print sys.argv[0] + " <local repo name | remote repo url>"
    print "This script retrieves the last modified timestamp and root hash of the"
    print "provided CVMFS repository."

if len(sys.argv) != 2:
    usage();
    sys.exit(1)

repo_identifier = sys.argv[1]

repo         = cvmfs.open_repository(repo_identifier)
root_catalog = repo.retrieve_root_catalog()
print root_catalog.last_modified , root_catalog.hash
