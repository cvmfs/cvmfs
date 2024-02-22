import argparse
import textwrap
import argcomplete
import io
import os
import random

def parse_arguments():
  parser = argparse.ArgumentParser(
    prog='python3 reader.py -p <path> -s <read_size_kb> --rr --rw -k',
    formatter_class=argparse.RawDescriptionHelpFormatter,
    description=textwrap.dedent("""
  File Reader
  ===========

  This program is designed to read a given amount of files starting from <root>,
  recursively traversing the subdirectories.

  Designed to be used for benchmarking I/O performance.

    """)
  )
  parser.add_argument('-p', '--path',
                      help='Root path from where recursively files should be discovered',
                      required=True)
  parser.add_argument('-s', '--read-size-kb',
                      help='Maximum read size in KiB',
                      type=int,
                      required=True)
  parser.add_argument('-n', '--num-max-files',
                      help='Maximum number of files to be read. \
                        Use -1 for reading the entire tree once (NOT possible if "random walk" is set). \
                        If the tree has less files than <entries>, it will reiterate over it again.',
                      type=int,
                      required=True)
  parser.add_argument('-k', '--keep',
                      help='Keep files open: During reading chunks of a file keep the file open',
                      required=False,
                      action="store_true")
  parser.add_argument('--rr',
                      help='Random read: Setting this flag makes reading chunks of a file being performed in random order',
                      required=False,
                      action="store_true")
  parser.add_argument('--rw',
                      help='Random walk: Setting this flag makes the order walking \
                        through directories (and their files) random. \
                        Each selection of the file will start at the root and walk randomly \
                        through the directories until it randomly selects a file. Because of \
                        this, the same file can be read multiple times, and/or not all \
                        files will be read.',
                      required=False,
                      action="store_true")
  argcomplete.autocomplete(parser)
  return parser.parse_args()

## Read a file in linear, sequential order in <read_size> sized chunks.
## If keep_open is true, the file will be kept open during the entire process.
## Otherwise closes the file and reopen it after every chunked read.
def readLinear(filename, read_size, keep_open):
  f = io.open(filename, "rb", buffering=read_size)
  f.seek(0, os.SEEK_END)
  file_size = f.tell()
  f.close()


  num_chunks = file_size / read_size
  if file_size % read_size != 0:
    num_chunks += 1

  num_chunks = int(num_chunks)

  if keep_open == True:
    with io.open(filename, "rb", buffering=read_size) as f:
      offset = 0
      for i in range(num_chunks):
        f.seek(offset, os.SEEK_SET)
        read = f.read(read_size)
        offset += read_size
        # print("Linear", "keep open", "offset", offset, "idx", i, "read", len(read))
  else:
    offset = 0
    for i in range(num_chunks):
      f = io.open(filename, "rb", buffering=read_size)
      f.seek(offset, os.SEEK_SET)
      read = f.read(read_size)
      f.close()
      offset += read_size
      # print("Linear", "close", "offset", offset, "idx", i, "read", len(read))

## Read a file in random order in <read_size> sized chunks.
## There will be as many chunks to read as there would be for a linear read.
## Just the access order of those chunks is random.
##
## If keep_open is true, the file will be kept open during the entire process.
## Otherwise closes the file and reopen it after every chunked read.
def readRandom(filename, read_size, keep_open):
  f = io.open(filename, "rb", buffering=0)
  f.seek(0, os.SEEK_END)
  file_size = f.tell()
  f.close()

  num_chunks = file_size / read_size
  if file_size % read_size != 0:
    num_chunks += 1

  num_chunks = int(num_chunks)
  access_order = list(range(num_chunks))
  random.shuffle(access_order)

  if keep_open == True:
    f = io.open(filename, "rb", buffering=0)
    for idx in access_order:
      offset = idx * read_size
      f.seek(offset, os.SEEK_SET)
      f.read(read_size)
      # print("Linear", "close", "offset", offset)
    f.close()
  else:
    for idx in access_order:
      f = io.open(filename, "rb", buffering=0)
      offset = idx * read_size
      f.seek(offset, os.SEEK_SET)
      f.read(read_size)
      f.close()
      # print("Linear", "close", "offset", offset)

## Get a random file. For this traverse the directories starting from <root>
## Select a random entry of the current directory. If it is a file, return it.
## Otherwise continue into the directory and repeat until a file is found.
##
## Note: If the directory is empty, finding a file starts from <root> again.
def getRandomFile(root):
  path = root

  while True:
    with os.scandir(path) as it:
      xx = list(it)
      if len(xx) == 0: # empty dir? start new
        path = root
        continue

      entry = random.choice(xx)
      if entry.is_file():
        return entry.path
      elif entry.is_dir():
        path = entry.path
      else:
        print("Neither file nor directory ", entry.path)
        return "error"

if __name__ == "__main__":
  parsed_args = parse_arguments()

  print(parsed_args)

  read_size = parsed_args.read_size_kb
  max_entries = parsed_args.num_max_files
  orig_root = parsed_args.path

  seed = random.random()
  random.seed(seed)

  # linear walk
  if parsed_args.rw == False:
    cur_entries = 0
    while cur_entries < max_entries or max_entries == -1:
      for root, dirs, files in os.walk(orig_root):
        for f in files:
          if cur_entries == max_entries:
            break
          full_filename = os.path.join(root, f)
          cur_entries += 1
          if parsed_args.rr == False:
            readLinear(full_filename, read_size, parsed_args.keep)
          else:
            readRandom(full_filename, read_size, parsed_args.keep)
      if max_entries == -1:
        break
  else:  # random walk
    for i in range(max_entries):
      full_filename = getRandomFile(orig_root)
      # print(full_filename)
      if parsed_args.rr == False:
        readLinear(full_filename, read_size, parsed_args.keep)
      else:
        readRandom(full_filename, read_size, parsed_args.keep)
