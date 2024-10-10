#!/usr/bin/env python3

###
# Get list of all repositories to mount
##
# Based on the commands dictionary given
##
def getReposToMount(commands, avail_cmds):
  repos = ["cvmfs-config.cern.ch"]

  for cmd_name in commands:
    if not avail_cmds[cmd_name]["repos"] in repos:
      for repo in avail_cmds[cmd_name]["repos"]:
        repos.append(repo)

  return repos

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

  with open(filename, "w") as f:
    f.write(new_config)

  if print_config == True:
    print("CVMFS config in " + filename, "\n----------------------------------------")
    with open(filename, "r") as cvmfs_config:
      for line in cvmfs_config:
        print(line, end="")
