#
# This file is part of the CernVM File System.
#

import argparse
import csv
import os.path

from collections import namedtuple

# POLICY pdir: Always include the entire (flat) parent directory
class SpecPoint:
  def __init__(self, path, mode):
    self.path = path
    self.mode = mode
    self.subfiles = []
  def isParentOf(self, path):
    return path[:len(self.path)+1] == self.path+"/"\
      or path == self.path or self.path==""
  def __str__(self):
    if self.mode==1:
      return "^" + self.path + "/*"
    elif self.mode==0:
      return "^" + (self.path if self.path != "" else "/")
  def __eq__(self, other):
    return self.path == other.path
  def __ne__(self, other):
    return self.path != other.path
  def __lt__(self, other):
    return self.path < other.path
  def __gt__(self, other):
    return self.path > other.path

def peek(stack, pos = 1):
  return stack[-pos] if len(stack)>0 else None

def get_parent(path):
  pos = path.rfind("/")
  return path[:pos] if pos>0 else ""

def exact_parser(pathsToInclude):
  specsToInclude = []
  for curPoint in pathsToInclude:
    specPoint = None
    if curPoint.action in exact_parser.dirFlat:
      specPoint = SpecPoint(curPoint.path, 1)
    else:
      specPoint = SpecPoint(curPoint.path, 0)
    specsToInclude.append(specPoint)
  return specsToInclude
exact_parser.dirFlat = ["opendir()"]

def parent_dir_parser(pathsToInclude):
  specsToInclude = []
  # Go through tracer points and build specs based on made calls
  for curPoint in pathsToInclude:
    specPoint = None
    if curPoint.action in parent_dir_parser.parentDirFlat:
      specPoint = SpecPoint(get_parent(curPoint.path), 1)
    elif curPoint.action in parent_dir_parser.dirFlat:
      specPoint = SpecPoint(curPoint.path, 1)
    else:
      specPoint = SpecPoint(curPoint.path, 0)
    specsToInclude.append(specPoint)
  return specsToInclude


parent_dir_parser.parentDirFlat = ["open()"]
parent_dir_parser.dirFlat = ["opendir()"]

ParsingPolicies = {
  "pdir": parent_dir_parser,
  "exact": exact_parser
}

class TracePoint(namedtuple("TracePoint", ["path", "action"])):
  def __eq__(self, other):
    return self.path == other.path
  def __ne__(self, other):
    return self.path != other.path
  def __lt__(self, other):
    return self.path < other.path
  def __gt__(self, other):
    return self.path > other.path

class TraceParser:
  def __init__(self, args):
    print("Parsing file: " + args.infile)
    print("Output file: " + args.outfile)
    print("Policy: " + args.policy)
    print("Filters: " + (",".join(args.filters) if len(args.filters) > 0 else "None"))
    self.inputName = args.infile
    self.outputName = args.outfile
    self.policy = ParsingPolicies[args.policy]
    self.filters = dict([(f+"()"), True] for f in args.filters)
  def read_log(self):
    pathsToInclude = []
    with open(self.inputName, "r") as logFile:
      csvLogReader = csv.reader(logFile, delimiter=',', quotechar='"')
      for row in [r
                    for r in csvLogReader
                      if r[3] not in self.filters and int(r[1])>=0
                        and r[2] not in TraceParser.blacklist]:
        if row[2] == "@UNKNOWN":
          print("ERROR: An error occurred during tracing (event code 8)")
          quit(-1)
        pathsToInclude.append(TracePoint(row[2], row[3]))
    specsToInclude = self.policy(pathsToInclude)
    specsToInclude.sort()
    rootEl = SpecPoint("", 0)
    workStack = [rootEl]
    self.pathSpec = []
    for specPoint in specsToInclude:
      while not peek(workStack).isParentOf(specPoint.path):
        # Backtrack up to nearest parent
        el = workStack.pop()
        self.pathSpec.append(el)
      topEl = peek(workStack)
      if topEl.path == specPoint.path and topEl.mode < specPoint.mode:
        # If stack top element is same path: Update if necessary
        topEl.mode = specPoint.mode
      elif topEl.path != specPoint.path:
        # If stack top is some parent: Add if necessary
        if topEl.mode==0\
          or topEl.path != get_parent(specPoint.path)\
          or specPoint.mode!=0:
          workStack.append(specPoint)
    self.pathSpec+=workStack

  def write_spec(self):
    if not self.pathSpec:
      print("No path specification created. Please call read_log first")
      return
    with open(self.outputName, "w") as specFile:
      for p in self.pathSpec[:-1]:
        specFile.write(str(p)+"\n")
      specFile.write(str(self.pathSpec[-1]))

TraceParser.blacklist = ["/.Trash","/.Trash-1000"]


def parse_args():
  argparser = argparse.ArgumentParser()
  argparser.add_argument("--policy",
    required=False,
    default="pdir",
    type=str,
    help="What files should be included given a traced"
      + " action on a certain file")


  argparser.add_argument("infile",
    type=str,
    help="The trace log file")

  argparser.add_argument("outfile",
    type=str,
    help="The output file")

  argparser.add_argument("--filters",
    required=False,
    default="",
    nargs="+",
    choices=["open", "opendir", "lookup", "statfs", "getattr", "listxattr", "getxattr", "readlink"],
    help="Calls which should be filtered")

  return argparser.parse_args()

def main():
  args = parse_args()
  traceParser = TraceParser(args)
  traceParser.read_log()
  traceParser.write_spec()

if __name__ == "__main__":
    main()
