#!/usr/bin/env python3

################################################################################
#             COMMANDS RUN ON CVMFS INFRASTRUCTURE
################################################################################
#
# Dictionary with commands and their needed cvmfs repositories used in
# ../start_benchmark.py
#
# the given command will be combined with the command "time"
# PLEASE ensure that "time <your command>" correctly times the command
# especially with bash files one needs to double check.
#
################################################################################

# CHEP 23 Commands
chep23_selected_commands = {
  "tensorflow" : { "command": "./scripts/50-tensorflow.sh",
                   "repos": [ "sft.cern.ch" ] },
  "root" :       { "command": "./scripts/51-root.sh",
                   "repos": [ "sft.cern.ch" ] },
  "dd4hep" :     { "command": "./scripts/60-dd4hep.sh",
                   "repos": [ "sft.cern.ch" ] },
# for cms dont forget to update SWVERSION in ../scripts/70-cms.sh
#  "cms" :       { "command": "./scripts/70-cms.sh",
#                  "repos": [ "cms-ib.cern.ch" ] },
}


###
# Get list of all repositories to mount
##
# Based on the commands dictionary given
##
def getReposToMount(commands):
  repos = ["cvmfs-config.cern.ch"]

  for cmd_name in commands:
    if not commands[cmd_name]["repos"] in repos:
      for repo in commands[cmd_name]["repos"]:
        repos.append(repo)

  return repos
