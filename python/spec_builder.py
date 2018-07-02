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
  def getList(self):
    results = [self]
    for f in self.subfiles:
      results+=f.doGetList(self)
    return results
  def doGetList(self, parent):
    results = []
    if parent.mode==0\
      or parent.path != get_parent(self.path)\
      or self.mode!=0:
      results.append(self)
    for f in self.subfiles:
      results+=f.doGetList(self)
    return results
  def __str__(self):
    if self.mode==1:
      return "^" + self.path + "/*"
    elif self.mode==0:
      return "^" + self.path
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

def parent_dir_parser(pathsToInclude):
  # print("\n".join([p.path for p in pathsToInclude]))
  rootEl = SpecPoint("", 0)
  workStack = [rootEl]
  specsToInclude = []
  for curPoint in pathsToInclude:
    specPoint = None
    if curPoint.action in parent_dir_parser.parentDirFlat:
      specPoint = SpecPoint(get_parent(curPoint.path), 1)
    elif curPoint.action in parent_dir_parser.dirFlat:
      specPoint = SpecPoint(curPoint.path, 1)
    else:
      specPoint = SpecPoint(curPoint.path, 0)
    specsToInclude.append(specPoint)
  specsToInclude.sort()
  for specPoint in specsToInclude:
    while not peek(workStack).isParentOf(specPoint.path):
      workStack.pop()
    topEl = peek(workStack)
    if topEl.path == specPoint.path and topEl.mode < specPoint.mode:
      topEl.mode = specPoint.mode
    elif topEl.path != specPoint.path:
      topEl.subfiles.append(specPoint)
      workStack.append(specPoint)
  return rootEl.getList()




parent_dir_parser.parentDirFlat = ["open()", "getattr()", "getxattr()", "listxattr()", "statfs()"]
parent_dir_parser.dirFlat = ["opendir()"]

ParsingPolicies = {
  "pdir": parent_dir_parser
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
    self.filters = { (f+"()"):True for f in args.filters}
  def read_log(self):
    pathsToInclude = []
    with open(self.inputName, "r") as logFile:
      csvLogReader = csv.reader(logFile, delimiter=',', quotechar='"')
      for row in [r
                    for r in csvLogReader
                      if r[3] not in self.filters and int(r[1])>=0]:
        # Iterate over not filtered elements
        pathsToInclude.append(TracePoint(row[2], row[3]))
    pathsToInclude.sort()
    self.pathSpec = self.policy(pathsToInclude)
  def write_spec(self):
    if not self.pathSpec:
      print("No path specification created. Please call read_log first")
      return
    with open(self.outputName, "w") as specFile:
      for p in self.pathSpec:
        specFile.write(str(p)+"\n")

    

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
