#!/usr/bin/env python

import cvmfs
import os
import sys
import zlib

def usage():
    print sys.argv[0] + " <repo url/path> <download destination> [<history depth>]"
    print "Downloads the whole catalog graph of a given repository."
    print "The optional <history depth> puts a threshold on how many historic"
    print "catalog tree revisions should be downloaded (default: 0 i.e. all)"

if len(sys.argv) < 3 or len(sys.argv) > 4:
    usage()
    sys.exit(1)

dest  = os.path.normpath(sys.argv[2])
url   = sys.argv[1]
depth = sys.argv[3] if len(sys.argv) == 4 else 0

try:
    depth = int(depth)
except ValueError, e:
    usage()
    print
    print "<history depth> needs to be an integer"
    sys.exit(1)



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

class DownloadingRemoteFetcher(cvmfs.RemoteFetcher):
    def __init__(self, repo_url, destination):
        super(DownloadingRemoteFetcher, self).__init__(repo_url)
        self.destination = destination
        self.__setup_destination()

    def __setup_destination(self):
        data_dir = self.destination + "/data"
        try:
            os.mkdir(data_dir, 0755)
            for i in range(0x00, 0xff + 1):
                new_folder = "{0:#0{1}x}".format(i, 4)[2:]
                os.mkdir(data_dir + "/" + new_folder, 0755)
        except ValueError, e:
            usage()
            print
            print "<download destination> needs to exist and be writable"

    def _retrieve_file(self, file_name, cached_file):
        file_url  = self._make_file_uri(file_name)
        dest_file = self.destination + "/" + file_name
        with open(dest_file, "w+") as f:
            self._download_content_and_store(f, file_url)
            f.seek(0)
            decompressed_content = zlib.decompress(f.read())
            cached_file.write(decompressed_content)

    def _retrieve_raw_file(self, file_name, cached_file):
        file_url  = self._make_file_uri(file_name)
        dest_file = self.destination + "/" + file_name
        with open(dest_file, "w+") as f:
            self._download_content_and_store(f, file_url)
            f.seek(0)
            cached_file.write(f.read())



fetcher = DownloadingRemoteFetcher(url, dest)
repo    = cvmfs.Repository.with_custom_fetcher(fetcher)

if depth == 0:
    print "Downloading entire catalog tree from " + repo.manifest.repository_name
else:
    print "Downloading last" , depth , "catalog revisions from " + repo.manifest.repository_name

root_clg       = repo.retrieve_root_catalog()
visited_hashes = set()
while True:
    next_root_clg = root_clg.get_predecessor()

    for catalog in MerkleCatalogTreeIterator(repo, root_clg, visited_hashes):
        if catalog.is_root():
            print "Downloading revision" , catalog.revision , "..."
        repo.close_catalog(catalog)

    if depth > 0:
        depth -= 1
        if depth == 0:
            print "all requested catalog tree revisions downloaded"
            break

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
