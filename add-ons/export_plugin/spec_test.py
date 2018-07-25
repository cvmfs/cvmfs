#
# This file is part of the CernVM File System.
#

import argparse
import csv

def get_parent(path):
  pos = path.rfind("/")
  return path[:pos] if pos>0 else ""
class TraceParser:
  def __init__(self, args):
    print("Parsing file: " + args.infile)
    print("Output file: " + args.outfile)
    print("Filters: " + (",".join(args.filters) if len(args.filters) > 0 else "None"))
    self.inputName = args.infile
    self.outputName = args.outfile
    self.filters = { (f+"()"):True for f in args.filters}
  def read_log(self):
    error = False
    pathsIncluded = {}
    with open(self.outputName, "r") as logFile:
      for curLine in logFile:
        #print(curLine)
        pathsIncluded[curLine.strip()] = True
    #quit()
    with open(self.inputName, "r") as logFile:
      csvLogReader = csv.reader(logFile, delimiter=',', quotechar='"')
      for row in [r
                    for r in csvLogReader
                      if r[3] not in self.filters and int(r[1])>=0
                        and r[2] not in TraceParser.blacklist]:
        # Iterate over not filtered elements
        path = row[2] if row[2] != "" else "/"
        if row[3] == "opendir()":
          if ("^"+row[2]+"/*") not in pathsIncluded:
            error=True
            print("ERROR: "+path+" not included in spec! Searched for:")
            print("^"+path+"/*")
        elif ("^"+path) not in pathsIncluded\
          and ("^"+get_parent(path)+"/*") not in pathsIncluded\
          and ("^"+path+"/*") not in pathsIncluded:
          error=True
          print("ERROR: "+path+" not included in spec! Searched for:")
          print("^"+path)
          print("^"+get_parent(path)+"/*")
    if not error:
      print("SUCCESS")
      quit(0)
    else:
      print("ERROR")
      quit(-1)

TraceParser.blacklist = ["/.Trash","/.Trash-1000"]

def parse_args():
  argparser = argparse.ArgumentParser()
  
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

if __name__ == "__main__":
    main()
