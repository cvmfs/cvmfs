#!/usr/bin/env python

import cvmfs
import sys
import shutil

class MerkleCatalogTreeIterator(cvmfs.CatalogTreeIterator):
    def __init__(self, repository, root_catalog, visited_hashes = set()):
        cvmfs.CatalogTreeIterator.__init__(self, repository, root_catalog)
        self.visited_hashes = visited_hashes

    def next(self):
        catalog = cvmfs.CatalogTreeIterator.next(self)
        self.visited_hashes.add(catalog.hash)
        return catalog

    def _push_catalog_wrapper(self, catalog):
        if not catalog.catalog_reference or \
               catalog.catalog_reference.hash not in self.visited_hashes:
            cvmfs.CatalogTreeIterator._push_catalog_wrapper(self, catalog)


def usage():
    print sys.argv[0] + "<repo url/path> <download destination>"
    print "Downloads the whole catalog graph of a given repository"

if len(sys.argv) != 3:
    usage()
    sys.exit(1)

dest = sys.argv[2]
repo = cvmfs.open_repository(sys.argv[1])

print "Downloading catalog tree from " + repo.manifest.repository_name

root_clg       = repo.retrieve_root_catalog()
visited_hashes = set()
while True:
    next_root_clg = root_clg.get_predecessor()

    for catalog in MerkleCatalogTreeIterator(repo, root_clg, visited_hashes):
        if catalog.is_root():
            print "Downloading revision" , catalog.revision , "..."
        shutil.copyfile(catalog.get_compressed_file().name, dest + "/" + catalog.hash)
        repo.close_catalog(catalog)

    if next_root_clg != None:
        try:
            root_clg = next_root_clg.retrieve_from(repo)
        except cvmfs.repository.FileNotFoundInRepository, e:
            print "next root catalog not found (garbage collected?)"
            break
    else:
        print "reached the end of the catalog chain"
        break

print "Done (downloaded" , len(visited_hashes) , "catalogs)"
