#!/usr/bin/python

import sys
import os
import math
import random
from optparse import OptionParser

# statistical parameters
# (based on http://cvmfs.fnal.gov:8000/cvmfs/oasis/data/43/3e5dd6fbd390bba4f9c59bd3d4d862eccc66eeC)
dir_ratio     = 0.0843
file_ratio    = 0.9071
symlink_ratio = 0.0086


def PrintError(msg):
	print >> sys.stderr, "[ERROR] " + msg
	sys.exit(1)


class RepoFactory:
	def __init__(self, dirs_to_produce, files_to_produce, symlinks_to_produce,   \
               repo_dir, max_dir_depth, min_file_size, max_file_size):
		self.dirs_to_produce, self.files_to_produce, self.symlinks_to_produce,     \
		self.repo_dir, self.max_dir_depth, self.min_file_size, self.max_file_size= \
		dirs_to_produce, files_to_produce, symlinks_to_produce, repo_dir,          \
		max_dir_depth, min_file_size, max_file_size
		self.dirs_produced  = self.files_produced = self.symlinks_produced = 0
		self.dirs_per_level = int(math.ceil(self.dirs_to_produce ** (1.0 / self.max_dir_depth)))
		self.files_per_dir  = int(self.files_to_produce / self.dirs_to_produce)


	def Produce(self):
		self._Recurse(self.repo_dir, 1)

	def PrintReport(self):
		print "directories produced:" , self.dirs_produced  , "expected:" , self.dirs_to_produce
		print "files produced:      " , self.files_produced , "expected:" , self.files_to_produce

	def _Recurse(self, path, dir_level):
		self._ProduceFiles(path)
		self._ProduceSymlinks(path)
		self._ProduceDirs(path, dir_level)


	def _ProduceFiles(self, path):
		for i in range(self.files_per_dir):
			new_file = ''.join([path, "/file", str(i)])
			self._ProduceFile(new_file)

	def _ProduceFile(self, path):
		""" create a file and fill it with some random binary data """
		f = open(path, "w+")
		desired_size = random.randint(self.min_file_size, self.max_file_size)
		f.write(''.join(chr(random.randint(0, 255)) for i in range(desired_size)))
		f.close()
		self.files_produced += 1


	def _ProduceDirs(self, path, dir_level):
		if dir_level > self.max_dir_depth:
			return
		for i in range(self.dirs_per_level):
			if self.dirs_produced > self.dirs_to_produce:
				break
			new_dir = ''.join([path, "/dir", str(i)])
			self._ProduceDir(new_dir)
			self._Recurse(new_dir, dir_level + 1)

	def _ProduceDir(self, path):
		""" create a directory """
		os.makedirs(path)
		self.dirs_produced += 1


	def _ProduceSymlinks(self, path):
		pass



# command line parameter parser setup
usage = "usage: %prog [options] <directory entry count> <destination path>\n\
This creates dummy file system content based on the parameters provided."
parser = OptionParser(usage)
parser.add_option("-d", "--max-dir-depth", dest="max_dir_depth", default=10,     help="the maximal directory structure depth")
parser.add_option("-s", "--min-file-size", dest="min_file_size", default=0,      help="minimal file size for random file contents")
parser.add_option("-b", "--max-file-size", dest="max_file_size", default=102400, help="maximal file size for random file contents")

# read command line arguments
(options, args) = parser.parse_args()
if len(args) != 2:
	parser.error("Please provide the mandatory arguments")
try:
	num_dirents = int(args[0])
	max_dir_depth = int(options.max_dir_depth)
	min_file_size = int(options.min_file_size)
	max_file_size = int(options.max_file_size)
except ValueError:
	PrintError("Cannot parse numerical options and/or parameters")
repo_dir      = args[1]

# check option consistency
if not os.path.isdir(repo_dir):
	PrintError(repo_dir + " does not exist")
if os.listdir(repo_dir):
	PrintError(repo_dir + " is not empty")
if max_dir_depth < 1:
	PrintError("maximal directory depth is too small")
if min_file_size < 0 or max_file_size < 0 or min_file_size > max_file_size:
	PrintError("file size restrictions do not make sense.")

dirs_to_produce     = int(math.ceil(num_dirents * dir_ratio))
files_to_produce    = int(math.ceil(num_dirents * file_ratio))
symlinks_to_produce = int(math.ceil(num_dirents * symlink_ratio))

if max_dir_depth > dirs_to_produce:
	PrintError("--max-dir-depth is bigger than " + str(dirs_to_produce))

repo_factory = RepoFactory(dirs_to_produce,     \
                           files_to_produce,    \
                           symlinks_to_produce, \
                           repo_dir,            \
                           max_dir_depth,       \
                           min_file_size,       \
                           max_file_size)
repo_factory.Produce()
repo_factory.PrintReport()
