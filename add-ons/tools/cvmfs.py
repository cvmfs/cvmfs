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
	def __init__(self, manifest_file):
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
		if not hasattr(self, 'root_catalog'):
		  raise Exception("Manifest lacks a root catalog entry")
		if not hasattr(self, 'root_hash'):
		  raise Exception("Manifest lacks a root hash entry")
		if not hasattr(self, 'ttl'):
		  raise Exception("Manifest lacks a TTL entry")
		if not hasattr(self, 'revision'):
		  raise Exception("Manifest lacks a revision entry")



class CatalogReference:
	def __init__(self, root_path, clg_hash):
		self.root_path = root_path
		self.hash      = clg_hash

	def __str__(self):
		return "<CatalogReference for " + self.root_path + " - " + self.hash + ">"

	def __repr__(self):
		return "<CatalogReference for " + self.root_path + ">"



class Catalog:
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
		subprocess.call(['sqlite3', self.catalog_file_.name])


	def ListNested(self):
		cursor = self._GetCursor()
		cursor.execute("SELECT path, sha1 FROM nested_catalogs;")
		nested_catalogs = []
		for catalog in cursor.fetchall():
			nested_catalogs.append(CatalogReference(catalog[0], catalog[1]))
		return nested_catalogs


	def FindNestedForPath(self, needle_path):
		nested_catalogs  = self.ListNested()
		best_match       = None
		best_match_score = 0
		for nested_catalog in nested_catalogs:
			if needle_path.startswith(nested_catalog.root_path) and \
				len(nested_catalog.root_path) > best_match_score:
					best_match_score = len(nested_catalog.root_path)
					best_match       = nested_catalog
		return best_match


	def _GetCursor(self):
		return self.db_handle_.cursor()


	def _Decompress(self, catalog_file):
		self.catalog_file_ = tempfile.NamedTemporaryFile('w+b')
		self.catalog_file_.write(zlib.decompress(catalog_file.read()))
		self.catalog_file_.flush()


	def _OpenDatabase(self):
		self.db_handle_ = sqlite.connect(self.catalog_file_.name)


	def _ReadProperties(self):
		cursor = self._GetCursor()
		cursor.execute("SELECT key, value FROM properties;")
		for prop in cursor.fetchall():
			self._ReadProperty(prop)


	def _ReadProperty(self, prop):
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
		if not hasattr(self, 'root_prefix'):
			self.root_prefix = "/"


	def _CheckValidity(self):
		if not hasattr(self, 'schema'):
		  raise Exception("Catalog lacks a schema entry")
		if not hasattr(self, 'root_prefix'):
		  raise Exception("Catalog lacks a root prefix entry")
		if not hasattr(self, 'last_modified'):
		  raise Exception("Catalog lacks a last modification entry")



class Repository:
	def __init__(self):
		manifest_file = self.RetrieveFile(".cvmfspublished")
		self.manifest_ = Manifest(manifest_file)


	def __str__(self):
		raise Exception("Not implemented!")


	def __repr__(self):
		return self.__str__()


	def manifest(self):
		return self.manifest_


	def RetrieveFile(self, file_name):
		raise Exception("Not implemented!")


	def RetrieveRootCatalog(self):
		return self.RetrieveCatalog(self.manifest_.root_catalog)


	def RetrieveCatalogForPath(self, needle_path):
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
		catalog_path = "data/" + catalog_hash[:2] + "/" + catalog_hash[2:] + "C"
		catalog_file = self.RetrieveFile(catalog_path)
		return Catalog(catalog_file)



class LocalRepository(Repository):
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
		tmp_file = tempfile.NamedTemporaryFile('w+b')
		RemoteRepository._DownloadToFile(url, tmp_file)
		return tmp_file


	@staticmethod
	def _DownloadToFile(url, f):
		response = urlopen(url)
		f.write(response.read())
		f.seek(0)
		f.flush()



def IsRemote(path):
	return path[0:7] == "http://"


def OpenRepository(repo_path):
	if IsRemote(repo_path):
		return RemoteRepository(repo_path)
	else:
		return LocalRepository(repo_path)


def OpenCatalog(catalog_file):
	if IsRemote(catalog_file):
		return Catalog(RemoteRepository.Download(catalog_file))
	else:
		return Catalog(open(catalog_file, "rb"))
