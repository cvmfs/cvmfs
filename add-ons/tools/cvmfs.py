#!/usr/bin/python

from urllib2 import urlopen, URLError, HTTPError
import sys
import os
import zlib
import tempfile
import subprocess
import urlparse
import datetime

# figure out which sqlite module to use
# in Python 2.4 an old version is present
# which does not allow proper read out of
# long int
try:
	import sqlite3 as sqlite
	foundSqlite3 = True
except:
	pass
if not foundSqlite3:
	try:
		import sqlite
		foundSqlite = True
	except ImportError, e:
		pass


class Manifest:
	""" Wraps information from .cvmfspublished"""

	def __init__(self, manifest_file):
		""" Initializes a Manifest object from a file pointer to .cvmfspublished """
		for line in manifest_file.readlines():
			if len(line) == 0:
				continue
			if line[0:2] == "--":
				break
			self._Readline(line)
		self._CheckValidity()


	def __str__(self):
		return "<Manifest for " + self.name + ">"


	def __repr__(self):
		return self.__str__()


	def _Readline(self, line):
		""" Parse lines that appear in .cvmfspublished """
		key_char = line[0]
		data     = line[1:-1]
		if key_char == "C":
			self.root_catalog     = data
		if key_char == "X":
			self.certificate      = data
		if key_char == "H":
			self.history_database = data
		if key_char == "T":
			self.last_modified    = datetime.datetime.fromtimestamp(int(data))
		if key_char == "R":
			self.root_hash        = data
		if key_char == "D":
			self.ttl              = int(data)
		if key_char == "S":
			self.revision         = int(data)
		if key_char == "N":
			self.name             = data


	def _CheckValidity(self):
		""" Checks that all mandatory fields are found in .cvmfspublished """
		if not hasattr(self, 'root_catalog'):
		  raise Exception("Manifest lacks a root catalog entry")
		if not hasattr(self, 'root_hash'):
		  raise Exception("Manifest lacks a root hash entry")
		if not hasattr(self, 'ttl'):
		  raise Exception("Manifest lacks a TTL entry")
		if not hasattr(self, 'revision'):
		  raise Exception("Manifest lacks a revision entry")



class CatalogReference:
	""" Wraps a catalog reference to nested catalogs as found in Catalogs """

	def __init__(self, root_path, clg_hash):
		self.root_path = root_path
		self.hash      = clg_hash

	def __str__(self):
		return "<CatalogReference for " + self.root_path + " - " + self.hash + ">"

	def __repr__(self):
		return "<CatalogReference for " + self.root_path + ">"



class DirectoryEntry:
	""" Thin wrapper around a DirectoryEntry as it is saved in the Catalogs """

	def __init__(self):
		self.md5path_1 = 0
		self.md5path_2 = 0
		self.parent_1  = 0
		self.parent_2  = 0
		self.size      = 0
		self.mode      = 0
		self.mtime     = 0
		self.name      = ""
		self.symlink   = ""

	def __str__(self):
		return "<DirectoryEntry for " + self.name + ">"

	def __repr__(self):
		return "<DirectoryEntry " + self.name + " - " + \
		       md5path_1 + "|" + md5path_2 + ">"


	def BacktracePath(self, containing_catalog, repo):
		""" Tries to reconstruct the full path of a DirectoryEntry """
		dirent  = self
		path    = self.name
		catalog = containing_catalog
		while True:
			p_dirent = catalog.FindDirectoryEntry(dirent.parent_1, dirent.parent_2)
			if p_dirent != None:
				path = p_dirent.name + "/" + path
				dirent = p_dirent
			elif not catalog.IsRoot():
				catalog = repo.FindParentCatalogOf(catalog)
			else:
				break
		return path



class Catalog:
	""" Wraps the basic functionality of CernVM-FS Catalogs """

	def __init__(self, catalog_file):
		self._Decompress(catalog_file)
		self._OpenDatabase()
		self._ReadProperties()
		self._GuessRootPrefixIfNeeded()
		self._CheckValidity()


	def __del__(self):
		self.db_handle_.close()
		self.catalog_file_.close()


	def __str__(self):
		return "<Catalog " + self.root_prefix + ">"


	def __repr__(self):
		return self.__str__()


	def OpenInteractive(self):
		""" Spawns a sqlite shell for interactive catalog database inspection """
		subprocess.call(['sqlite3', self.catalog_file_.name])


	def ListNested(self):
		""" List CatalogReferences to all contained nested catalogs """
		catalogs = self.RunSql("SELECT path, sha1 FROM nested_catalogs;")
		nested_catalogs = []
		for catalog in catalogs:
			nested_catalogs.append(CatalogReference(catalog[0], catalog[1]))
		return nested_catalogs


	def FindNestedForPath(self, needle_path):
		""" Find the best matching nested CatalogReference for a given path """
		nested_catalogs  = self.ListNested()
		best_match       = None
		best_match_score = 0
		for nested_catalog in nested_catalogs:
			if needle_path.startswith(nested_catalog.root_path) and \
				len(nested_catalog.root_path) > best_match_score:
					best_match_score = len(nested_catalog.root_path)
					best_match       = nested_catalog
		return best_match


	def FindDirectoryEntry(self, md5path_1, md5path_2):
		""" Finds the DirectoryEntry residing under the given MD5 hashed path """
		res = self.RunSql("SELECT parent_1, parent_2, size, mode, mtime, name,  \
			                        symlink                                       \
			                 FROM catalog                                         \
			                 WHERE md5path_1 = " + str(md5path_1) + " AND         \
			                       md5path_2 = " + str(md5path_2) + "             \
			                 LIMIT 1;")
		if len(res) != 1:
			return None
		e = DirectoryEntry()
		e.md5path_1 = md5path_1
		e.md5path_2 = md5path_2
		e.parent_1, e.parent_2, e.size, e.mode, e.mtime, e.name, e.symlink = res[0]
		return e


	def RunSql(self, sql):
		""" Run an arbitrary SQL query on the catalog database """
		cursor = self.db_handle_.cursor()
		cursor.execute(sql)
		return cursor.fetchall()


	def IsRoot(self):
		""" Checks if this is the root catalog (based on the root prefix) """
		return self.root_prefix == "/"


	def _Decompress(self, catalog_file):
		""" Unzip a catalog file to a temporary referenced by self.catalog_file_ """
		self.catalog_file_ = tempfile.NamedTemporaryFile('w+b')
		self.catalog_file_.write(zlib.decompress(catalog_file.read()))
		self.catalog_file_.flush()


	def _OpenDatabase(self):
		""" Create and configure a database handle to the Catalog """
		self.db_handle_ = sqlite.connect(self.catalog_file_.name)
		self.db_handle_.text_factory = str


	def _ReadProperties(self):
		""" Retrieve all properties stored in the catalog database """
		props = self.RunSql("SELECT key, value FROM properties;")
		for prop in props:
			self._ReadProperty(prop)


	def _ReadProperty(self, prop):
		""" Detect catalog properties and store them as public class members """
		prop_key   = prop[0]
		prop_value = prop[1]
		if prop_key == "revision":
			self.revision          = prop_value
		if prop_key == "schema":
			self.schema            = float(prop_value)
		if prop_key == "last_modified":
			self.last_modified     = datetime.datetime.fromtimestamp(int(prop_value))
		if prop_key == "previous_revision":
			self.previous_revision = prop_value
		if prop_key == "root_prefix":
			self.root_prefix       = prop_value


	def _GuessRootPrefixIfNeeded(self):
		""" Root catalogs don't have a root prefix property (fixed here) """
		if not hasattr(self, 'root_prefix'):
			self.root_prefix = "/"


	def _CheckValidity(self):
		""" Check that all crucial properties have been found in the database """
		if not hasattr(self, 'schema'):
		  raise Exception("Catalog lacks a schema entry")
		if not hasattr(self, 'root_prefix'):
		  raise Exception("Catalog lacks a root prefix entry")
		if not hasattr(self, 'last_modified'):
		  raise Exception("Catalog lacks a last modification entry")



class Repository:
	""" Abstract Wrapper around a Repository connection """
	def __init__(self):
		manifest_file = self.RetrieveFile(".cvmfspublished")
		self.manifest = Manifest(manifest_file)


	def __str__(self):
		raise Exception("Not implemented!")


	def __repr__(self):
		return self.__str__()


	def RetrieveFile(self, file_name):
		""" Abstract method to retrieve a file from the repository """
		raise Exception("Not implemented!")


	def RetrieveRootCatalog(self):
		return self.RetrieveCatalog(self.manifest.root_catalog)


	def RetrieveCatalogForPath(self, needle_path):
		""" Recursively walk down the Catalogs and find the best fit for a path """
		clg = self.RetrieveRootCatalog()
		nested_reference = None
		while True:
			new_nested_reference = clg.FindNestedForPath(needle_path)
			if new_nested_reference == None:
				break
			nested_reference = new_nested_reference
			clg = self.RetrieveCatalog(nested_reference.hash)
		return clg


	def RetrieveCatalog(self, catalog_hash):
		""" Download and open a catalog from the repository """
		catalog_path = "data/" + catalog_hash[:2] + "/" + catalog_hash[2:] + "C"
		catalog_file = self.RetrieveFile(catalog_path)
		return Catalog(catalog_file)


	def FindParentCatalogOf(self, catalog):
		""" Tries to find the parent catalog of a given catalog and returns it """
		return self.RetrieveCatalogForPath(os.path.split(catalog.root_prefix)[0])



class LocalRepository(Repository):
	""" Concrete Repository implementation for a locally stored CernVM-FS repo """
	def __init__(self, base_directory):
		if not os.path.isdir(base_directory):
			raise Exception("didn't find" + base_directory)
		self.base_directory_ = os.path.normpath(base_directory)
		Repository.__init__(self)


	def __str__(self):
		return "<LocalRepository at " + self.base_directory_ + ">"


	def RetrieveFile(self, file_name):
		file_path = self.base_directory_ + "/" + file_name
		if not os.path.exists(file_path):
			raise Exception("didn't find" + file_path)
		return LocalRepository.Open(file_path)


	@staticmethod
	def Open(file_path):
		return open(file_path, "rb")



class RemoteRepository(Repository):
	""" Concrete Repository implementation for a repository reachable by HTTP """
	def __init__(self, repository_url):
		self.repository_url_ = urlparse.urlunparse(urlparse.urlparse(repository_url))
		Repository.__init__(self)


	def __str__(self):
		return "<RemoteRepository at " + self.repository_url_ + ">"


	def RetrieveFile(self, file_name):
		file_url = self.repository_url_ + "/" + file_name
		return RemoteRepository.Download(file_url)


	@staticmethod
	def Download(url):
		""" Download the given URL to a (returned) temporary file """
		tmp_file = tempfile.NamedTemporaryFile('w+b')
		RemoteRepository._DownloadToFile(url, tmp_file)
		return tmp_file


	@staticmethod
	def _DownloadToFile(url, f):
		""" Download the given URL to the provided temporary file """
		response = urlopen(url)
		f.write(response.read())
		f.seek(0)
		f.flush()



def IsRemote(path):
	""" Check if a given path points to remote (HTTP) or local storage """
	return path[0:7] == "http://"


def OpenRepository(repo_path):
	""" Convenience function to open a connection to a local or remote repo """
	if IsRemote(repo_path):
		return RemoteRepository(repo_path)
	else:
		return LocalRepository(repo_path)


def OpenCatalog(catalog_file):
	""" Convenience function to open a specific catalog file (local or remote) """
	if IsRemote(catalog_file):
		return Catalog(RemoteRepository.Download(catalog_file))
	else:
		return Catalog(LocalRepository.Open(catalog_file))
