#!/usr/bin/python

import sys
import random
import string
import os

def usage():
  print "USAGE: " + sys.argv[0] + " <location> <min file size> <max file size> <overall repo size>"


def get_random_name(size=8, chars=string.ascii_lowercase + string.digits):
  return ''.join(random.choice(chars) for x in range(size))


def create_random_repo(location, min_file_size, max_file_size, overall_size):
  current_size = 0
  while current_size < overall_size:
    create_n_files = random.randint(10,100)
    size_created = create_random_subdir(location, min_file_size, max_file_size, create_n_files)
    current_size = current_size + size_created


def create_random_subdir(location, min_file_size, max_file_size, file_count):
  new_directory = ''
  while True:
    new_directory = ''.join((location, "/", get_random_name()))
    if not os.path.exists(new_directory):
      os.makedirs(new_directory)
      break
  return create_random_files(new_directory, min_file_size, max_file_size, file_count)


def create_random_files(location, min_file_size, max_file_size, file_count):
  possible_file_content = string.ascii_lowercase + string.ascii_uppercase + string.digits
  current_size = 0

  for i in range(file_count):
    new_file = ''.join((location, "/", get_random_name()))
    file = open(new_file, "w")
    target_file_size = random.randint(min_file_size, max_file_size)
    for j in range(target_file_size):
      file.write(random.choice(possible_file_content))
    file.close()
    current_size = current_size + target_file_size
  return current_size


def main():
  if len(sys.argv) != 5:
    usage()
    exit(1)

  location      = sys.argv[1]
  min_file_size = int(sys.argv[2])
  max_file_size = int(sys.argv[3])
  overall_size  = int(sys.argv[4])

  create_random_repo(location, min_file_size, max_file_size, overall_size)
  print "done"


main()
