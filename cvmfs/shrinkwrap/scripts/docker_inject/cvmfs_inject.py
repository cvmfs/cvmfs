#
# This file is part of the CernVM File System.
#

import argparse
from docker_injector import DockerInjector
import fileinput
import tempfile

argparser = argparse.ArgumentParser()
argparser.add_argument("command",
    choices=["init", "prepare", "inject"])
argparser.add_argument("host",
  type=str)
argparser.add_argument("user",
  type=str)
argparser.add_argument("pw",
  type=str)
argparser.add_argument("image",
  type=str)
argparser.add_argument("source_tag",
  type=str)
argparser.add_argument("--dir",
  required=False,
  default="/tmp/cvmfs",
  type=str)
argparser.add_argument("--dest_tag",
  required=False,
  default="cvmfs",
  type=str)
args = argparser.parse_args()

injector = DockerInjector(args.host, args.image, args.source_tag, args.user, args.pw)
if args.command == "init":
  injector.setup(args.dest_tag)
elif args.command == "prepare":
  injector.unpack(args.dir)
elif args.command == "inject":
  injector.update(args.dir, args.dest_tag)