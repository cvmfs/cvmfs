#
# This file is part of the CernVM File System.
#

import argparse

class TreeNode:
  def __init__(self, mode):
    self.children = {}
    self.mode = mode
  def getString(self, prefix, wildcard):
    stringRes = ''
    if self.mode == '!':
      stringRes = '!' + prefix + '\n'
    if not wildcard:
      if self.mode == '^':
        stringRes = '^'+prefix+'*\n'
      elif self.mode == '*':
        stringRes = prefix + '/*\n'
      elif self.mode == '/':
        if len(prefix) == 0:
          stringRes = "^/\n"
        stringRes = '^' + prefix + '\n'
      for key, val in self.children.items():
        stringRes+=val.getString(prefix+'/'+key, self.mode == '*')
    return stringRes
  def __str__(self):
    return self.getString("", False)

class DiffBuilder:
  def __init__(self, args):
    self.infiles = args.infiles
    self.outfile = args.outfile
    self.depth = args.depth
    self.root = TreeNode('/')
  
  def build_diff(self):
    with open(self.infiles[0], 'r') as specFile0:
      for curLine in specFile0:
        (curLine, mode) = self.get_info(curLine)
        path_parts = curLine.split('/')
        curNode = self.add_node(path_parts, mode)
        curNode.mode = self.calc_new_mode(curNode.mode, mode)
    for curfile in self.infiles[1:]:
      with open(curfile, 'r') as curSpecFile:
        for curLine in curSpecFile:
          (curLine, mode) = self.get_info(curLine)
          path_parts = curLine.split('/')
          if (mode == '!'):
            curNode = self.add_node(path_parts, mode)
            curNode.mode = self.calc_new_mode(curNode.mode, mode)
          else:
            curNode = self.root
            passthrough = '-' if mode=='!' else '_'
            curDepth = 0
            mergeable = True
            for part in path_parts:
              curDepth+=1
              if not part in curNode.children\
                and curDepth > self.depth\
                and mergeable:
                print("Found mergeable")
                curNode.mode = self.calc_new_mode(curNode.mode, '*')
                break
              elif not part in curNode.children:
                mergeable = False
                curNode.children[part] = TreeNode(passthrough)
              curNode = curNode.children[part]
              curNode.mode = self.calc_new_mode(curNode.mode, passthrough)
            curNode.mode = self.calc_new_mode(curNode.mode, mode)
    with open(self.outfile, "w") as specFile:
      specFile.write(str(self.root))
  
  def add_node(self, path_parts, mode):
    curNode = self.root
    passthrough = '-' if mode=='!' else '_'
    for part in path_parts:
      if not part in curNode.children:
        curNode.children[part] = TreeNode(passthrough)
      curNode = curNode.children[part]
      curNode.mode = self.calc_new_mode(curNode.mode, passthrough)
    return curNode

  def calc_new_mode(self, old, update):
    if update == '!':
      return update
    if old == '-':
      return update
    if update == '-':
      return old
    if old == '_':
      return update
    if old == '/' and update in ['^', '*']:
      return update
    if old == '^' and update == '*':
      return update
    return old

  def get_info(self, curLine):
    curLine = curLine.strip()
    mode = curLine[0]
    wildcard = False
    if (curLine[-1] == '*'):
      wildcard = True
      curLine = curLine[:-1]
    if (mode == '/'):
      if (wildcard):
        mode = '*'
      curLine = curLine[1:]
    else:
      if not wildcard and mode=='^':
        mode = '/'
      curLine = curLine[2:]
    return (curLine, mode)


          



def parse_args():
  argparser = argparse.ArgumentParser()

  argparser.add_argument("depth",
    type=int,
    help="The trace log file")
  
  argparser.add_argument("infiles",
    type=str,
    nargs="+",
    help="The trace log file")

  argparser.add_argument("outfile",
    type=str,
    help="The output file")

  return argparser.parse_args()

def main():
  args = parse_args()
  diffBuilder = DiffBuilder(args)
  diffBuilder.build_diff()

if __name__ == "__main__":
    main()