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

  with open(filename, "w") as f:
    f.write(new_config)

  if print_config == True:
    print("CVMFS config in " + filename, "\n----------------------------------------")
    with open(filename, "r") as cvmfs_config:
      for line in cvmfs_config:
        print(line, end="")
