#!/usr/bin/env python3

import subprocess
import time
import tqdm
from collections import defaultdict
import os


dict_time_format = {
  "user": "%U",
  "system": "%S",
  "real": "%e",
  "cpu": "%P",
  "swap": "%W",
  "involuntary_swap": "%c",
  "voluntary_swap": "%w",
  "avg_mem_kb": "%K",
  "nmax_resident_set_size_kb": "%M",
  "avg_resident_size_kb": "%t",
  "page_faults_major": "%F",
  "page_faults_minor": "%R",
  "socket_received": "%r",
  "socket_send": "%s",
  "fs_in": "%I",
  "fs_out": "%O",
  "signals": "%k"
}

def getTimeFormat():
  time_format = '"\n'
  for key, val in dict_time_format.items():
    time_format += key + " " + val + " \n"
  time_format += '"'

  return time_format

def wipe_kernel_cache(use_cvmfs):
  subprocess.check_call(["sudo", "echo", "3", ">", "/proc/sys/vm/drop_caches"],
                        stdout=subprocess.DEVNULL)

def wipe_cache(use_cvmfs):
  if use_cvmfs == True:
    subprocess.check_call(["sudo", "cvmfs_config", "wipecache"], stdout=subprocess.DEVNULL)

  # do not use with autofs
  #subprocess.check_call(["sudo", "cvmfs_config", "reload"], stdout=subprocess.DEVNULL)

  wipe_kernel_cache(use_cvmfs)

def preloadProxy(command, num_threads):
  if "70-cms" in command["command"]:
    if os.path.isdir("./workdir") == True:
      ele = subprocess.Popen("rm -rf ./workdir",
                             universal_newlines=True, shell=True,
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE)
      (stdout, stderr) = ele.communicate()
      error_code = ele.wait()

      if error_code != 0:
        print("delete workdir/ failed", stdout, "\n", stderr)

    doit = subprocess.Popen(command["time"] + " " + command["command"]
                            + " ./workdir/preload",
                            universal_newlines=True, shell=True,
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    doit.communicate()

  else:
    if command["send-thread-id"] == True:
      doit = subprocess.Popen(command["time"] + " " + command["command"],
                              universal_newlines=True, shell=True,
                              stdout=subprocess.PIPE, stderr=subprocess.PIPE)
      doit.communicate()
    else:
      doit = []

      for i in range(num_threads):
        doit.append(subprocess.Popen(
                        command["time"] + " " + command["command"] + " " + str(i),
                        universal_newlines=True, shell=True,
                        stdout=subprocess.PIPE, stderr=subprocess.PIPE))

      for ele in doit:
        (stdout, stderr) = ele.communicate()
        error_code = ele.wait()

        if error_code != 0:
          print("Failure while executing statement during preloadProxy",
                command["command"], "error", error_code)
          print(stderr)
          print(stdout)



def timeme(stmt="", setup="", arg_setup=None, cleanup='', final_cleanup='', repeat=1):
  dict_results = defaultdict(list)
  dict_full_cvmfs_internals = defaultdict(list)
  dict_tracing = defaultdict(list)

  if not callable(stmt):
    return dict_results

  for i in tqdm.tqdm(range(repeat)):
    if callable(setup):
      if arg_setup is None:
        setup()
      else:
        setup(arg_setup)

    time.sleep(2)

    dict_results, dict_full_cvmfs_internals, dict_tracing = \
                     stmt(dict_results, dict_full_cvmfs_internals, dict_tracing)

    time.sleep(2)

    if callable(cleanup):
      cleanup

  if callable(final_cleanup):
    final_cleanup()

  return dict_results, dict_full_cvmfs_internals, dict_tracing


def do_thing(command, num_threads, dict_results, dict_full_cvmfs_internals, dict_tracing):
  # out = subprocess.check_output(command, shell=True)
  doit = []
  time_results_str = []

  # time command
  if "70-cms" in command["command"]:
    if os.path.isdir("./workdir") == True:
      ele = subprocess.Popen("rm -rf ./workdir",
                        universal_newlines=True, shell=True,
                        stdout=subprocess.PIPE, stderr=subprocess.PIPE)
      (stdout, stderr) = ele.communicate()
      error_code = ele.wait()

      if error_code != 0:
          print("delete workdir/ failed", stdout, "\n", stderr)

    for i in range(num_threads):
      doit.append(subprocess.Popen(command["time"] + " " + command["command"]
                                   + " ./workdir/thread" + str(i),
                                   universal_newlines=True, shell=True,
                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE))
  else:
    for i in range(num_threads):
      if command["send-thread-id"] == True:
        doit.append(subprocess.Popen(command["time"] + " " + command["command"] + " " + str(i),
                                    universal_newlines=True, shell=True,
                                    stdout=subprocess.PIPE, stderr=subprocess.PIPE))
      else:
        doit.append(subprocess.Popen(command["time"] + " " + command["command"],
                                    universal_newlines=True, shell=True,
                                    stdout=subprocess.PIPE, stderr=subprocess.PIPE))

  while len(doit) > 0:
    for ele in reversed(doit):
      status = ele.poll()

      if status != None:
        (stdout, stderr) = ele.communicate()
        error_code = ele.wait()
        doit.remove(ele)

        if error_code != 0:
          print("Failure while executing statement ", command["command"],
                "error", error_code)
          print(stderr)
          print(stdout)

        # /usr/bin/time returns in stderr
        time_results_str.append(stderr)
    time.sleep(1)

  for time_result in time_results_str:
    for line in time_result.splitlines():
      for key in dict_time_format.keys():
        if key == line.split(" ")[0]:
          dict_results[key].append(float(line.split(" ")[1].split("%")[0]))

  # internal affairs
  if "repos" in command.keys():
    for repo in command["repos"]:
      cvmfs_cmd = "sudo cvmfs_talk -i " + repo + " internal affairs"
      doit = subprocess.Popen(cvmfs_cmd, universal_newlines=True, shell=True,
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
      (stdout, stderr) = doit.communicate()

      dict_full_cvmfs_internals[repo].append(stdout)

      found_start = False
      for line in stdout.splitlines():
        if found_start == True:
          tmp = line.split("|")
          dict_results[repo + "_" + tmp[0]].append(float(tmp[1]))
        if "Name|Value|Description" in line:
          found_start = True

  # tracing
  do_tracing = False
  with open("/etc/cvmfs/default.local", "r") as f:
    for line in f:
      if "CVMFS_TRACEFILE" in line:
        do_tracing = True

  if do_tracing == True:
    for repo in command["repos"]:
      flush_cmd = "sudo cvmfs_talk -i " + repo + " tracebuffer flush"
      doit = subprocess.Popen(flush_cmd, universal_newlines=True, shell=True,
                          stdout=subprocess.PIPE, stderr=subprocess.PIPE)
      (stdout, stderr) = doit.communicate()
      f_trace_repo = "/tmp/cvmfs-benchmark_cvmfs-trace-" + repo + ".log"

      # save and empty log file
      with open(f_trace_repo, "r+") as f:
        dict_tracing[repo].append(f.read())
        f.truncate(0)


  return dict_results, dict_full_cvmfs_internals, dict_tracing
