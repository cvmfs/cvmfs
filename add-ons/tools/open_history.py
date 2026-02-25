#!/usr/bin/env python3

import sys
import cvmfs

def usage():
    print sys.argv[0] + " <local repo name | remote repo url>"
    print "This script opens the history database of the provided CVMFS repo."

if len(sys.argv) != 2:
    usage();
    sys.exit(1)

repo_identifier = sys.argv[1]
repo            = cvmfs.open_repository(repo_identifier)
if not repo.has_history():
    print "no history found"
    sys.exit(1)

history = repo.retrieve_history()
history.open_interactive()
