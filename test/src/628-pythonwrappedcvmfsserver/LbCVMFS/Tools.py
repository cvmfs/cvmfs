
from contextlib import contextmanager
import os
import subprocess

def _getRepoName():
    repo_name = os.environ.get('CVMFS_TEST_REPO')
    if not repo_name:
        raise RuntimeError("environment variable CVMFS_TEST_REPO not set")
    return repo_name

#
# CVMFS wrappers
#
# Note(rmeusel): This is taken 1-to-1 from the lhcbdev.cern.ch release
#                manager machine. It is meant to be used for a regression
#                test for an issue that arose there in March 2016.
#
#############################################################################

def cvmfsStart(reponame = None):
    """ Start a CVMFS transaction """
    if reponame == None:
        reponame = _getRepoName()

    rc = subprocess.call(["cvmfs_server", "transaction", reponame])
    if rc != 0:
        raise RuntimeError("Could not start CVMFS transaction")

def cvmfsPublish(reponame = None):
    """ Publish a CVMFS transaction """
    if reponame == None:
        reponame = _getRepoName()

    rc = subprocess.call(["cvmfs_server", "publish", "-f", reponame])
    if rc != 0:
        raise RuntimeError("Could not publish CVMFS transaction")

def cvmfsAbort(reponame = None):
    """ Abort a CVMFS transaction """
    if reponame == None:
        reponame = _getRepoName()

    rc = subprocess.call(["cvmfs_server", "abort", "-f", reponame])
    if rc != 0:
        raise RuntimeError("Could not abort CVMFS transaction")


@contextmanager
def cvmfsTransaction(reponame = None, testonly=False):
    """ Context manager to make sure we always publish or abort """
    if reponame == None:
        reponame = _getRepoName()

    import logging
    if not testonly:
        logging.info("Starting CVMFS transaction")
        cvmfsStart(reponame)

    try:
        # At this point we have a transaction started
        # we have to abort ib case of problem...
        yield
        if not testonly:
            logging.info("Publishing CVMFS transaction")
            cvmfsPublish(reponame)
    except:
        if not testonly:
            logging.info("Aborting transaction")
            cvmfsAbort(reponame)
        import sys
        exc_info = sys.exc_info()
        raise exc_info[0], exc_info[1], exc_info[2]
