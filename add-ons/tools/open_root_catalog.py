#!/usr/bin/env python

import sys
import cvmfs

def usage():
    print sys.argv[0] + " <local repo name | remote repo url>"
    print "This script opens the root catalog of the provided CVMFS repository."

if len(sys.argv) != 2:
    usage();
    sys.exit(1)

repo_identifier = sys.argv[1]

repo         = cvmfs.open_repository(repo_identifier)
root_catalog = repo.retrieve_root_catalog()
root_catalog.open_interactive()
