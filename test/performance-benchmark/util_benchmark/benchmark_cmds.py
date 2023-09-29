#!/usr/bin/env python3

###
# Get list of all repositories to mount
##
# Based on the commands dictionary given
##
def getReposToMount(commands, avail_cmds):
  repos = ["cvmfs-config.cern.ch"]

  print(commands)

  for cmd_name in commands:
    if not avail_cmds[cmd_name]["repos"] in repos:
      for repo in avail_cmds[cmd_name]["repos"]:
        repos.append(repo)

  return repos

################################################################################
# Get "base" of the output name
################################################################################
# Used as a base output name that will be extended for the different outputs:
# data, cvmfs_internals_raw, and tracing
#
# If needed to destinguish different branches/builds this is done based on
# the cvmfs_build_dir. This is just done by simple pattern matching, request
# by values listed in out_name_replacement_of_version. As such, it is important
# that the requested replacement is uniquely identifiable and has no
# "pattern-matching" overlap.
#
################################################################################
def getOutname(cvmfs_build_dir, cmd_name, client_config_list, num_threads,
               cvmfs_version, out_name_replacement_of_version):
  outname = cmd_name + "_" + cvmfs_version

  for ele in out_name_replacement_of_version.keys():
    if ele in cvmfs_build_dir:
      outname += "-" + out_name_replacement_of_version[ele]

  print("client_config_list", client_config_list)
  client_config_list.sort()

  for param in client_config_list:
    outname += "_" + param

  outname += "_" + str(num_threads) + "_"

  return outname


################################################################################
# Set CVMFS Config
################################################################################
#
# Writes a CVMFS client config based on $client_config to $filename.
#
# @params
#       filename: config file name to which the CVMFS client config is written
#                 to must be a valid CVMFS client config filename,
#                 e.g. /etc/cvmfs/default.local
#       client_config: list of client config names
#       avail_configs: dict of all available client configs
################################################################################
def setClientConfig(filename, client_config, avail_configs, print_config=True):
  new_config = ""
  for param in client_config:
    if type(avail_configs[param]) == list:
      for ele in avail_configs[param]:
        new_config += ele + "\n"
    else:
      new_config += avail_configs[param] + "\n"

  print(new_config)

  with open(filename, "w") as f:
    f.write(new_config)

  if print_config == True:
    print("CVMFS CONFIG in " + filename)
    with open(filename, "r") as cvmfs_config:
      for line in cvmfs_config:
        print(line, end="")



################################################################################
################################################################################
################################################################################

#        ANYTHING BELOW HERE: DEPREACTED AND NOT USED ANYMORE

################################################################################
################################################################################
################################################################################


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
