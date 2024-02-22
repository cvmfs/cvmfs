import glob
import argparse
import textwrap
import argcomplete
import io
import os
import random


def parse_arguments():
  parser = argparse.ArgumentParser(
    # rw = random walk
    # rr = random read
    # k = keep file open
    prog='python3 reader.py -p <path> -s <read_size_kb> --rr --rw -k',
    formatter_class=argparse.RawDescriptionHelpFormatter,
    description=textwrap.dedent("""
This is my
super long
    description
    """),
    epilog="end of description")
  parser.add_argument('-p', '--path',
                      help='Root path from where recursively files should be discovered',
                      required=True)
  parser.add_argument('-s', '--read-size-kb',
                      help='Maximum read size in KiB ',
                      required=True)
  parser.add_argument('--rr',
                      help='Random read: Setting this flag makes reading chunks of a file being performed in random order',
                      required=False,
                      action="store_true")
  parser.add_argument('--rw',
                      help='Random walk: Setting this flag makes the order walking through directories (and their files) random',
                      required=False,
                      action="store_true")
  parser.add_argument('-k', '--keep',
                      help='Keep files open: During reading chunks of a file keep the file open',
                      required=False,
                      action="store_true")
  parser.add_argument('--help-config',
                      help='More help: How to build the YAML config file',
                      required=False,
                      action="store_true")
  parser.add_argument('--help-output',
                      help='More help: Output of this program (plots, ..)',
                      required=False,
                      action="store_true")
  argcomplete.autocomplete(parser)
  return parser.parse_args()

def readLinear(filename, read_size, keep_open):
  if keep_open == True:
    with io.open(filename, "rb", buffering=read_size) as f:
      read_b = "das"
      while len(read_b) != 0:
        read_b = f.read(read_size)
        print("Linear", "close", "read", len(read_b))
  else:
    f = io.open(filename, "rb", buffering=read_size)
    f.seek(0, os.SEEK_END)
    file_size = f.tell()
    f.close()
    

    num_chunks = file_size / read_size
    if file_size % read_size != 0:
      num_chunks += 1

    num_chunks = int(num_chunks)

    offset = 0
    for i in range(num_chunks):
      f = io.open(filename, "rb", buffering=read_size)
      f.seek(offset, os.SEEK_SET)
      f.read(read_size)
      f.close()
      offset += read_size
      print("Linear", "close", "offset", offset)

def readRandom(filename, read_size, keep_open):
  f = io.open(filename, "rb", buffering=0)
  f.seek(0, os.SEEK_END)
  file_size = f.tell()

  num_chunks = file_size / read_size
  if file_size % read_size != 0:
    num_chunks += 1
  
  num_chunks = int(num_chunks)

  print("num_chunks", num_chunks)

  access_order = random.shuffle([range(num_chunks)])

  if keep_open == True:
    for idx in access_order:
      offset = idx * read_size
      f.seek(offset, os.SEEK_SET)
      f.read(read_size)
      print("Linear", "close", "offset", offset)
    f.close()
  else:
    for idx in access_order:
      f = io.open(filename, "rb", buffering=0)
      offset = idx * read_size
      f.seek(offset, os.SEEK_SET)
      f.read(read_size)
      f.close()
      print("Linear", "close", "offset", offset)


if __name__ == "__main__":
  parsed_args = parse_arguments()

  print(parsed_args)

  read_size = int(parsed_args.read_size_kb)

  if parsed_args.rw == False:
    for root, dirs, files in os.walk(parsed_args.path):
      for f in files:
        full_filename = os.path.join(root, f)
        if parsed_args.rr == False:
          readLinear(full_filename, read_size, parsed_args.keep)
        else:
          readRandom(full_filename, read_size, parsed_args.keep)
