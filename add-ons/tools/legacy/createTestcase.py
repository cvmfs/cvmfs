#!/usr/bin/python

import sys
import os

def processFile(filename, repoPath, shellscriptPath):
	fschanges = open(filename)
	if (len(repoPath) != 0):
		repoPath = canonicalPath(repoPath) + '/'

	shellscript = open(shellscriptPath, 'w+')
	writeShellscriptPreamble(shellscript)

	for line in fschanges:
		line = line[0:len(line) -1] # strip line break
		processLine(line, repoPath, shellscript)

	writeShellscriptFooter(shellscript)

def writeShellscriptPreamble(shellscript):
	hackScript(shellscript, "#!/bin/sh")
	
	hackScript(shellscript, "echo creating symlink dummy target")
	hackScript(shellscript, "touch dummyTargetForSymlinksInThisTestcase")

def writeShellscriptFooter(shellscript):
	hackScript(shellscript, "")

def processLine(line, repoPath, shellscript):
	filetype = line[0]
	operation = line[1]
	successful = line[2]
	path = line[3+len(repoPath):]
	
	if (not successful):
		return
	
	if (filetype == 'R'):
		doFile(operation, path, shellscript)
	if (filetype == 'D'):
		doDirectory(operation, path, shellscript)
	if (filetype == 'L'):
		doLink(operation, path, shellscript)
	if (filetype == 'U'):
		doUnknown(operation, path, shellscript)

def doFile(operation, path, shellscript):
	if (operation == 'C'):
		createFile(path, shellscript)
	if (operation == 'D'):
		deleteFile(path, shellscript)
	if (operation == 'T' or operation == 'A'):
		touchFile(path, shellscript)
	if (operation == 'I'):
		# ??
		pass
	if (operation == 'O'):
		# ??
		pass

def doDirectory(operation, path, shellscript):
	if (operation == 'C'):
		createDirectory(path, shellscript)
	if (operation == 'D'):
		deleteDirectory(path, shellscript)
	if (operation == 'T' or operation == 'A'):
		touchDirectory(path, shellscript)
	if (operation == 'I'):
		print "hust"
		# ??
		pass
	if (operation == 'O'):
		print "hust"
		# ??
		pass

def doLink(operation, path, shellscript):
	if (operation == 'C'):
		createSymlink(path, shellscript)
	if (operation == 'D'):
		deleteSymlink(path, shellscript)
	if (operation == 'T' or operation == 'A'):
		touchSymlink(path, shellscript)
	if (operation == 'I'):
		print "hust"
		# ??
		pass
	if (operation == 'O'):
		print "hust"
		# ??
		pass

def createFile(path, shellscript):
	nameOfFile = os.path.basename(path)
	createPathIfNeeded(path, shellscript)
	hackScript(shellscript, "echo creating file " + path)
	hackScript(shellscript, "echo " + nameOfFile + " > " + path)

def deleteFile(path, shellscript):
	hackScript(shellscript, "echo deleting file " + path)
	hackScript(shellscript, "rm -f " + path)

def touchFile(path, shellscript):
	createPathIfNeeded(path, shellscript)
	hackScript(shellscript, "echo touching file " + path)
	hackScript(shellscript, "touch " + path)

def createDirectory(path, shellscript):
	hackScript(shellscript, "echo creating directory " + path)
	hackScript(shellscript, "mkdir -p " + path)
	
def deleteDirectory(path, shellscript):
	hackScript(shellscript, "echo deleting directory " + path)
	hackScript(shellscript, "rm -rf " + path)

def touchDirectory(path, shellscript):
	hackScript(shellscript, "echo touching directory " + path)
	hackScript(shellscript, "touch " + path)

def createSymlink(path, shellscript):
	createPathIfNeeded(path, shellscript)
	hackScript(shellscript, "echo creating symlink " + path)
	hackScript(shellscript, "ln -s dummyTargetForSymlinksInThisTestcase " + path)

def deleteSymlink(path, shellscript):
	hackScript(shellscript, "echo deleting symlink " + path)
	hackScript(shellscript, "rm -f " + path)

def touchSymlink(path, shellscript):
	hackScript(shellscript, "echo touching symlink " + path)
	hackScript(shellscript, "touch " + path)

def createPathIfNeeded(path, shellscript, alreadyCreatedPathes=[]):
	dirPath = os.path.dirname(path)
	if dirPath != "" and not os.path.exists(dirPath) and dirPath not in alreadyCreatedPathes:
		createDirectory(dirPath, shellscript)
		alreadyCreatedPathes.append(dirPath)

def hackScript(shellscript, str):
	shellscript.write(str + "\n")

def canonicalPath(path):
	if (len(path) == 0):
		return ""
	if (path[len(path)-1] == '/'):
		return path[:len(path)-1]
	return path

def usage():
	print "USAGE: " + sys.argv[0] + " <fschanges> <repository path> <output shellscript path>"
	print "This script will convert a given redir-fs changelog in an executable shellscript"
	print "which mocks the changes resulting in the creation of the changelog."
	print "Use this to easily create testcases out of changesets"

def main():
	if (len(sys.argv) != 4):
		usage();
		sys.exit(1)

	filename = sys.argv[1]
	repoPath = sys.argv[2]
	shellscript = sys.argv[3]
	if (not os.path.exists(filename)):
		print "file " + filename + " does not exist"
		sys.exit(1)
	
	processFile(filename, repoPath, shellscript)

main()