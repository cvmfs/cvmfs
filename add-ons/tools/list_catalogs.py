#!/usr/bin/env python

import sys
import cvmfs

def usage():
    print sys.argv[0] + " <local repo name | remote repo url>"
    print "This script lists all catalog of the provided CVMFS repository."

if len(sys.argv) != 2:
    usage();
    sys.exit(1)

repo_identifier = sys.argv[1]

repo = cvmfs.open_repository(repo_identifier)
for clg in repo.catalogs():
    print clg.root_prefix
