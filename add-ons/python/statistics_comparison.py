import argparse
import glob
import os
import re
import shutil
import tempfile

from docker import Client
from parser import Parser

image = "cvm-dockerhub02.cern.ch:5000/slc6"
tag = "cvmfs"
tmpdir = tempfile.mkdtemp(prefix="comparison.", dir="/tmp")


class GitRepository:
    def __init__(self, url, branch, name):
        self.url = url
        self.branch = branch
        self.name = name


class DockerExecutor:
    def __init__(self, repository, socket_url, api_version):
        self.repo = repository
        self.socket_url = socket_url
        self.api_version = api_version

    @staticmethod
    def set_environment_variables():
        if "CVMFS_OPT_ITERATIONS" not in os.environ:
            os.environ["CVMFS_OPT_ITERATIONS"] = "1"
        if "CVMFS_OPT_WARM_CACHE" not in os.environ:
            os.environ["CVMFS_OPT_WARM_CACHE"] = "no"
        if "CVMFS_OPT_PARALLEL_RUNS" not in os.environ:
            os.environ["CVMFS_OPT_PARALLEL_RUNS"] = "1"

    def run(self, tests_to_execute):
        client = Client(base_url=self.socket_url, version=self.api_version)
        repo = self.repo
        volumes = ["/tmp"]
        binds = {tmpdir:
                     {
                         "bind": "/tmp",
                         "ro": False
                     }
                 }
        environment = {
            "CVMFS_OPT_VALGRIND": "no",
            "CVMFS_OPT_TALK_STATISTICS": "yes",
            "CVMFS_OPT_ITERATIONS": os.environ["CVMFS_OPT_ITERATIONS"],
            "CVMFS_OPT_WARM_CACHE": os.environ["CVMFS_OPT_WARM_CACHE"],
            "CVMFS_OPT_PARALLEL_RUNS": os.environ["CVMFS_OPT_PARALLEL_RUNS"],
            "USER": "root"
        }
        cmd = "bash -xc \'" + \
              "cd /workdir/cvmfs/build && " + \
              "rm -rf * && " + \
              "git reset --hard && " + \
              "git remote add " + repo.name + " " + repo.url + " && " + \
              "git remote update && " + \
              "git fetch " + repo.name + " && " + \
              "git checkout -b test " + repo.name + "/" + repo.branch + " && " + \
              "cmake .. && " + \
              "make -j 6 install && " + \
              "cvmfs_config setup nostart nocfgmod && " + \
              "cd /workdir/cvmfs/test && " + \
              "./run.sh /tmp/benchmarks.log " + tests_to_execute + "\'"
        container_id = client.create_container(image=image + ":" + tag,
                                               environment=environment,
                                               volumes=volumes,
                                               hostname="cvmfs-test",
                                               tty=True,
                                               command=cmd).get("Id")
        client.start(container_id, privileged=True, binds=binds)
        print("Executing benchmarks for the repository \"" + repo.url +
              "\"" + " in the branch \"" + repo.branch + "\". If you want to "
              "check the output type \"docker " +
              " attach " + container_id + "\" in a different terminal")
        result = client.wait(container_id)
        if result != 0:
            print("Test execution failed!\n" +
                  "Docker container with id " + str(container_id) +
                  " might be used for post-mortem analysis\n" +
                  "Exiting...")
            exit(100)
        client.remove_container(container_id, force=True)


def parse_args():
    argparser = argparse.ArgumentParser()
    argparser.add_argument("--socket_url",
                           default="unix://var/run/docker.sock",
                           required=False, type=str,
                           help="""Docker socket to listen to.
                                It has to be in one of the following ways:
                                unix://path/to/docker.sock
                                tcp://X.X.X.X:port""")
    argparser.add_argument("--docker_api_version",
                           default="1.18",
                           required=False, type=str,
                           help="Docker API version (default 1.17)")
    argparser.add_argument("--benchmarks",
                           default="001-atlas",
                           required=False, type=str,
                           help="""Comma-separated list of benchmarks.
                           For example:
                           ALL
                           001-atlas,002-lhcb,003-alice,004-cms
                           004-cms,002-lhcb""")
    argparser.add_argument("--original_repo",
                           default="https://github.com/cvmfs/cvmfs.git",
                           required=False, type=str,
                           help="Original repository to compare to")
    argparser.add_argument("--original_branch",
                           default="devel",
                           required=False, type=str,
                           help="Original branch to compare to")
    argparser.add_argument("--external_branch",
                           default="devel",
                           required=False, type=str,
                           help="External branch to compare to")
    argparser.add_argument("--output",
                           default="/tmp/comparison.csv",
                           required=False, type=str,
                           help="Output CSV file")
    argparser.add_argument("external_repo", type=str)
    return argparser.parse_args()


def get_benchmark_list(benchmark_string):
    result = ""
    regex = re.compile('^\d\d\d-[a-z]+$')
    if benchmark_string.upper() != "ALL":
        benchmark_list = benchmark_string.split(",")
        for benchmark in benchmark_list:
            if regex.match(benchmark):
                result += "benchmarks/" + benchmark + " "
            else:
                print(benchmark + " is not a valid benchmark")
    else:
        result = "benchmarks/*"
    return result


def parse_files():
    path = tmpdir + "/cvmfs_benchmarks/"
    repo_list = glob.glob(path + "*")
    parsers = {}

    for repository in repo_list:
        files_list = glob.glob(repository + "/*.data")
        if len(files_list) > 0:
            parsers[repository] = Parser()
            for datafile in files_list:
                parsers[repository].parse(datafile)
    return parsers


def main():
    DockerExecutor.set_environment_variables()
    args = parse_args()
    final_result_file = args.output
    origin = GitRepository(args.original_repo, args.original_branch, "original")
    external = GitRepository(args.external_repo, args.external_branch,
                             "external")
    tests_to_execute = get_benchmark_list(args.benchmarks)
    if len(tests_to_execute) == 0:
        exit(200)
    if not final_result_file.endswith(".csv"):
        print("Please specify a CSV file as output")
        exit(201)
    # download firstly the image
    print("Downloading the image " + image + ":" + tag)
    c = Client(base_url=args.socket_url, version=args.docker_api_version)
    c.pull(repository=image, tag=tag, insecure_registry=True)
    # stop all running containers before
    for container in c.containers():
        print("    Stopping container " + container["Id"])
        c.remove_container(container["Id"], force=True)

    print("Done downloading the image")
    origin_exec = DockerExecutor(origin, args.socket_url,
                                 args.docker_api_version)
    external_exec = DockerExecutor(external, args.socket_url,
                                   args.docker_api_version)
    origin_exec.run(tests_to_execute)
    parsers_origin = parse_files()

    shutil.rmtree(tmpdir)  # clean up before the next execution
    os.mkdir(tmpdir)
    external_exec.run(tests_to_execute)
    parsers_external = parse_files()

    Parser.to_csv_multiple_comparison(parsers_origin,
                                      parsers_external,
                                      final_result_file)
    print("Done. File " + final_result_file + " created")

if __name__ == "__main__":
    main()
