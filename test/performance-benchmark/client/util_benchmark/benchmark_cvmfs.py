#!/usr/bin/env python3

import subprocess
import os

def clear_and_mount_direct(repos):
  doit = subprocess.Popen("sudo cvmfs_config killall; sudo cvmfs_config reload;"
                          + " sudo cvmfs_config wipecache",
                          universal_newlines=True, shell=True,
                          stdout=subprocess.PIPE, stderr=subprocess.PIPE)
  (std_out, stderr) = doit.communicate()

  for repo in repos:
    doit = subprocess.Popen("sudo umount /cvmfs/" + repo,
                            universal_newlines=True, shell=True,
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    (std_out, stderr) = doit.communicate()

    if os.path.isdir("/cvmfs/" + repo) == False:
      os.makedirs("/cvmfs/" + repo)

    doit = subprocess.Popen("sudo /usr/bin/cvmfs2 -o rw,system_mount,fsname=cvmfs2,"
                  + "allow_other,grab_mountpoint,uid=`id -u cvmfs`,gid=`id -g cvmfs`,"
                  + "libfuse=3 " + repo + " /cvmfs/" + repo,
                  universal_newlines=True, shell=True,
                  stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    (std_out, stderr) = doit.communicate()

def clear_and_reload_autofs():
      # also works for broken autofs in EL9
      doit = subprocess.Popen(  "sudo systemctl stop autofs; "
                              + "sudo cvmfs_config killall; "
                              + "sudo cvmfs_config wipecache; "
                              + "sudo systemctl start autofs; "
                              + "sudo cvmfs_config reload",
                              universal_newlines=True, shell=True,
                              stdout=subprocess.PIPE, stderr=subprocess.PIPE)
      (std_out, stderr) = doit.communicate()

def getCVMFSVersion():
  doit = subprocess.Popen("attr -g version /cvmfs/cvmfs-config.cern.ch/",
                          universal_newlines=True, shell=True,
                          stdout=subprocess.PIPE, stderr=subprocess.PIPE)
  (std_out, stderr) = doit.communicate()
  return std_out.splitlines()[1]

def installCVMFS(cvmfs_build_dir):
  doit = subprocess.Popen("cd " + cvmfs_build_dir + "; sudo ninja install",
                          universal_newlines=True, shell=True,
                          stdout=subprocess.PIPE, stderr=subprocess.PIPE)
  (std_out, stderr) = doit.communicate()
