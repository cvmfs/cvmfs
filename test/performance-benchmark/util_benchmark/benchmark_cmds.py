#!/usr/bin/env python3


# CHEP 23 Commands
chep23_atlas_commands = {
  "atlas-tensorflow": {"command": "./scripts/11-atlas-tensorflow.sh",
                        "repos": ["unpacked.cern.ch"]},
  #"atlas-environment": "./scripts/12-atlas-environment.sh",
  # to be able to run atlas athena you first need to execute once ./scripts/20-atlas.athena-setup.sh
  #"atlas-athena - build externals": "./scripts/21-atlas.athena-build_externals.sh",
  #"atlas-athena - build": "./scripts/21-atlas.athena-build.sh",
}

chep23_lhcb_commands = {
  "lhcb-env": {"command": "./scripts/13-lhcb.sh",
                "repos": ["lhcb.cern.ch"]},
  "LCG-view": {"command": '/usr/bin/bash -c ". /cvmfs/sft.cern.ch/lcg/views/LCG_103/x86_64-centos9-gcc11-opt/setup.sh"',
                "repos": ["sft.cern.ch"]},
}

chep23_alice_commands = {
  "alice-pipeline": "./scripts/41-alice-pipeline-createAndLaunchContainer.sh 5"
}

chep23_selected_commands = {
#   "tensorflow" : { "command": "./scripts/50-tensorflow.sh",
#                    "repos": [ "sft.cern.ch" ] },
    "root" :      { "command": "./scripts/51-root.sh",
                     "repos": [ "sft.cern.ch" ] },
#    "dd4hep" :    { "command": "./scripts/60-dd4hep.sh",
#                     "repos": [ "sft.cern.ch" ] },
#  "cms" :       { "command": "./scripts/70-cms.sh",
#                    "repos": [ "cms-ib.cern.ch" ] },
}

def getReposToMount(commands):
  repos = ["cvmfs-config.cern.ch"]

  for cmd_name in commands:
    print(commands[cmd_name])
    if not commands[cmd_name]["repos"] in repos:
      for repo in commands[cmd_name]["repos"]:
        repos.append(repo)
  
  return repos
