#!/usr/bin/env python3
""" CVMFS testing helper script to generate a populated nested directory.

Example usage:
python3 make_repo.py -f 11 -d 2 -n 2 /tmp/testfs
Example output in /tmp/testfs:
  1 drwxrwxr-x  4 user user 4.0K Jan 12 09:59 .
  2 drwxrwxrwt 20 root    root     20K Jan 12 09:17 ..
  3 drwxrwxr-x  4 user user 4.0K Jan 12 09:59 dir0
  4 drwxrwxr-x  4 user user 4.0K Jan 12 09:59 dir1
  5 -rw-rw-r--  1 user user  25K Jan 12 09:59 file0
  6 -rw-rw-r--  1 user user  34K Jan 12 09:59 file1
  7 -rw-rw-r--  1 user user  56K Jan 12 09:59 file2
  8 -rw-rw-r--  1 user user  68K Jan 12 09:59 file4
  9 -rw-rw-r--  1 user user  19K Jan 12 09:59 file5
 10 -rw-rw-r--  1 user user  35K Jan 12 09:59 file6
 11 -rw-rw-r--  1 user user  17K Jan 12 09:59 file7
 12 -rw-rw-r--  1 user user  97K Jan 12 09:59 file8
 13 -rw-rw-r--  1 user user  11K Jan 12 09:59 file9
 14 -rw-rw-r--  1 user user  60K Jan 12 09:59 master
 15 lrwxrwxrwx  1 user user   18 Jan 12 09:59 symlink3 -> /tmp/testfs/master
"""

from __future__ import print_function
import sys
import os
import math
import random
import shutil
from optparse import OptionParser

# for python 2/3 compatibility:
# store the python version information in some global flags
PY2 = sys.version_info[0] == 2
PY3 = sys.version_info[0] == 3
PY39 = sys.version_info[0:2] >= (3, 9)


symlink_ratio  = 0.01
hardlink_ratio = 0.005


def PrintError(msg):
  print("[ERROR] " + msg, file=sys.stderr)
  sys.exit(1)


class RepoFactory:
  def __init__(self, max_dir_depth, num_subdirs, num_files_per_dir,    \
               symlink_ratio, hardlink_ratio, repo_dir, min_file_size, \
               max_file_size, duplicate_ratio=0.0):
    self.max_dir_depth      = max_dir_depth
    self.num_subdirs        = num_subdirs
    self.num_files_per_dir  = num_files_per_dir
    self.symlink_ratio      = symlink_ratio
    self.hardlink_ratio     = hardlink_ratio
    self.repo_dir           = repo_dir
    self.min_file_size      = min_file_size
    self.max_file_size      = max_file_size
    self.duplicate_ratio    = duplicate_ratio
    self.dirs_produced      = 0
    self.files_produced     = 0
    self.symlinks_produced  = 0
    self.hardlinks_produced = 0
    self.bytes_produced     = 0

  def Produce(self):
    self._Recurse(self.repo_dir, 1)

  def PredictResults(self):
    directories = 0
    files       = 0
    bytes       = 0
    for i in range(self.max_dir_depth):
      directories += self.num_subdirs ** (i + 1)
    files = (directories + 1) * self.num_files_per_dir
    bytes = files * (max_file_size - min_file_size) / 2
    print("Prediction:")
    print("   directories to be produced:  " , directories)
    print("   files to be produced:        " , files)
    print("   bytes to be written (aprox): " , bytes)

  def PrintReport(self):
    print("Results:")
    print("   directories produced:" , self.dirs_produced)
    print("   files produced:      " , self.files_produced)
    print("   symlinks produced:   " , self.symlinks_produced)
    print("   hardlinks produced:  " , self.hardlinks_produced)
    print("   ------------------------------------------------")
    print("   sum of dirents:      " , (self.dirs_produced + \
                                        self.files_produced + \
                                        self.symlinks_produced + \
                                        self.hardlinks_produced))
    print()
    print("overall produced" , self.bytes_produced , "bytes --> avg." , \
          (int(self.bytes_produced / self.files_produced)) , "bytes/file")

  def _Recurse(self, path, dir_level):
    self._ProduceFilesHardlinksAndSymlinks(path)
    self._ProduceDirs(path, dir_level)

  def _ProduceFilesHardlinksAndSymlinks(self, path):
    master_file = ''.join([path, "/master"])
    self._ProduceFile(master_file)
    for i in range(self.num_files_per_dir - 1):
      random_val = random.random()
      if random_val < self.symlink_ratio:
        self._ProduceSymlink(''.join([path, "/symlink", str(i)]), master_file)
      elif random_val < self.symlink_ratio + self.hardlink_ratio:
        self._ProduceHardlink(''.join([path, "/hardlink", str(i)]), master_file)
      elif random.random() < self.duplicate_ratio:
        shutil.copyfile(master_file, ''.join([path, "/file", str(i)]))
      else:
        self._ProduceFile(''.join([path, "/file", str(i)]))

  def _ProduceFile(self, path):
    """Create a file and fill it with some random binary data."""
    f = open(path, "wb+")
    desired_size = random.randint(self.min_file_size, self.max_file_size)
    if desired_size > 0:
      # generate pseudo-random bytes and write to file
      # (while trying to keep python 2/3 compatibility)
      if PY39: # if python > 3.9, getrandbytes is available and the best choice
        f.write(random.randbytes(desired_size))
      elif PY3: # if python 3, we can use the implementation of randbytes
        f.write(random.getrandbits(desired_size * 8).to_bytes(desired_size, 'little'))
      elif PY2: # if python 2, need to use an alternative for to_bytes
        f.write(hex(random.getrandbits(desired_size * 8)))
    f.close()
    self.files_produced += 1
    self.bytes_produced += desired_size

  def _ProduceSymlink(self, path, dest):
    os.symlink(dest, path)
    self.symlinks_produced += 1

  def _ProduceHardlink(self, path, dest):
    os.link(dest, path)
    self.hardlinks_produced += 1

  def _ProduceDirs(self, path, dir_level):
    if dir_level > self.max_dir_depth:
      return
    for i in range(self.num_subdirs):
      new_dir = ''.join([path, "/dir", str(i)])
      self._ProduceDir(new_dir)
      self._Recurse(new_dir, dir_level + 1)

  def _ProduceDir(self, path):
    os.makedirs(path)
    self.dirs_produced += 1


if __name__ == "__main__":
  # command line parameter parser setup
  usage = "usage: %prog [options] <destination path>\n\
  This creates dummy file system content based on the parameters provided."
  parser = OptionParser(usage)
  parser.add_option("-d", "--max-dir-depth",     dest="max_dir_depth",     default=7,      help="the maximal directory structure depth")
  parser.add_option("-n", "--num-subdirs",       dest="num_subdirs",       default=5,      help="number of sub-directories per stage")
  parser.add_option("-f", "--num-files-per-dir", dest="num_files_per_dir", default=30,     help="number of files per directory")
  parser.add_option("-s", "--min-file-size",     dest="min_file_size",     default=0,      help="minimal file size for random file contents")
  parser.add_option("-b", "--max-file-size",     dest="max_file_size",     default=102400, help="maximal file size for random file contents")
  parser.add_option("-c", "--duplicate-ratio",   dest="duplicate_ratio",   default=0.0,    help="maximal file size for random file contents")

  # read command line arguments
  (options, args) = parser.parse_args()
  if len(args) != 1:
    parser.error("Please provide the mandatory arguments")
  try:
    max_dir_depth     = int(options.max_dir_depth)
    num_subdirs       = int(options.num_subdirs)
    num_files_per_dir = int(options.num_files_per_dir)
    min_file_size     = int(options.min_file_size)
    max_file_size     = int(options.max_file_size)
    duplicate_ratio   = float(options.duplicate_ratio)
  except ValueError:
    PrintError("Cannot parse numerical options and/or parameters")
  repo_dir = args[0]

  # check option consistency
  if not os.path.isdir(repo_dir):
    PrintError(repo_dir + " does not exist")
  if os.listdir(repo_dir):
    PrintError(repo_dir + " is not empty")
  if max_dir_depth < 1:
    PrintError("maximal directory depth is too small")
  if min_file_size < 0 or max_file_size < 0 or min_file_size > max_file_size:
    PrintError("file size restrictions do not make sense.")

  repo_factory = RepoFactory(max_dir_depth,     \
                             num_subdirs,       \
                             num_files_per_dir, \
                             symlink_ratio,     \
                             hardlink_ratio,    \
                             repo_dir,          \
                             min_file_size,     \
                             max_file_size,     \
                             duplicate_ratio)
  repo_factory.PredictResults()
  print()
  repo_factory.Produce()
  repo_factory.PrintReport()
