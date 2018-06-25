import argparse
import csv
import os.path

# POLICY pdir: Always include the entire (flat) parent directory
def parent_dir_parser(path, op):
  def clean_path(p):
    return p if p!="" else "/"
  if op in parent_dir_parser.onlyInclFolder:
    return "^"+clean_path(path)
  else:
    return "^"+clean_path(os.path.dirname(path))
parent_dir_parser.onlyInclFolder = {
  "opendir()": True
}

ParsingPolicies = { # Different policies can be added here
  "pdir" : parent_dir_parser
}

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
    pathsToInclude = {}
    with open(self.inputName, "r") as logFile:
      csvLogReader = csv.reader(logFile, delimiter=',', quotechar='"')
      for row in [r
                    for r in csvLogReader
                      if r[3] not in self.filters and int(r[1])>=0]:
        # Iterate over not filtered elements
        pathsToInclude[self.policy(row[2], row[3])] = True 
    self.pathSpec = [p for p in pathsToInclude]
    del pathsToInclude
  def write_spec(self):
    if not self.pathSpec:
      print("No path specification created. Please call read_log first")
      return
    with open(self.outputName, "w") as specFile:
      for p in self.pathSpec:
        specFile.write(p+"\n")

    

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
