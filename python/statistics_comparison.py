import argparse
import os
import glob

from docker import Client
from parser import Parser

image = "moliholy/slc6"
tag = "cvmfs-test"


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

    def run(self, tests_to_execute):
        client = Client(base_url=self.socket_url, version=self.api_version)
        repo = self.repo
        volumes = ["/tmp"]
        binds = {"/tmp":
                    {
                        "bind": "/tmp",
                        "ro": False
                    }
                }

        cmd = "bash -c \'" + \
              "export USER=root && " + \
              "export CVMFS_OPT_VALGRIND=no && " + \
              "export CVMFS_OPT_TALK_STATISTICS=yes && " + \
              "export CVMFS_OPT_ITERATIONS=" + os.environ["CVMFS_OPT_ITERATIONS"] + " && " + \
              "export CVMFS_OPT_WARM_CACHE=" + os.environ["CVMFS_OPT_WARM_CACHE"] + " && " + \
              "cd /workdir/cvmfs/build && " + \
              "rm -rf * && " + \
              "git reset --hard && " + \
              "git remote add " + repo.name + " " + repo.url + " && " + \
              "git remote update && " + \
              "git fetch " + repo.name + " && " + \
              "git checkout -b test " + repo.name + "/" + repo.branch + " && " + \
              "cmake .. && " + \
              "make install && " + \
              "cd /workdir/cvmfs/test && " + \
              "./run.sh /tmp/benchmarks.log " + tests_to_execute + "\'"

        print("Executing benchmarks for the repository \"" + repo.url +
              "\"" + " in the branch \"" + repo.branch + "\"")
        container_id = client.create_container(image=image + ":" + tag,
                                               volumes=volumes,
                                               hostname="cvmfs-test",
                                               tty=True,
                                               command=cmd).get("Id")
        client.start(container_id, privileged=True, binds=binds)
        result = client.wait(container_id)
        if result != 0:
            print("Test execution failed! Exiting...")
            exit(100)


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
                           default="1.17",
                           required=False, type=str,
                           help="Docker API version (default 1.17)")
    argparser.add_argument("--benchmarks",
                           default="atlas",
                           required=False, type=str,
                           help="""Comma-separated list of benchmarks.
                           For example:
                           atlas,lhcb,alice,cms
                           cms,lhcb""")
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
    argparser.add_argument("external_repo", type=str)
    return argparser.parse_args()


def get_benchmark_list(benchmark_string):
    benchmark_list = str(benchmark_string).split(",")
    result = ""
    for benchmark in benchmark_list:
        if benchmark.lower() == "atlas":
            result += "benchmarks/001-atlas "
        if benchmark.lower() == "lhcb":
            result += "benchmarks/002-lhcb "
        if benchmark.lower() == "alice":
            result += "benchmarks/003-alice "
        if benchmark.lower() == "cms":
            result += "benchmarks/004-cms "
    return result


def parse_files():
    repo_list = ["atlas.cern.ch", "lhcb.cern.ch", "alice.cern.ch", "cms.cern.ch"]
    path = "/tmp/cvmfs_benchmarks/"
    parsers = {}

    for repository in repo_list:
        files_list = glob.glob(path + repository + "/*.data")
        if len(files_list) > 0:
            parsers[repository] = Parser()
            for datafile in files_list:
                parsers[repository].parse(datafile)
    return parsers


def main():
    final_result_file = "/tmp/comparison.csv"
    DockerExecutor.set_environment_variables()
    args = parse_args()
    origin = GitRepository(args.original_repo, args.original_branch, "original")
    external = GitRepository(args.external_repo, args.external_branch,
                             "external")
    tests_to_execute = get_benchmark_list(args.benchmarks)
    # download firstly the image
    print("Downloading the image " + image + ":" + tag)
    c = Client(base_url=args.socket_url, version=args.docker_api_version)
    c.pull(repository=image, tag=tag)

    origin_exec = DockerExecutor(origin, args.socket_url,
                                 args.docker_api_version)
    external_exec = DockerExecutor(external, args.socket_url,
                                   args.docker_api_version)
    origin_exec.run(tests_to_execute)
    parsers_origin = parse_files()

    external_exec.run(tests_to_execute)
    parsers_external = parse_files()

    Parser.to_csv_multiple_comparison(parsers_origin,
                                      parsers_external,
                                      final_result_file)
    print("Done. File " + final_result_file + " created")

if __name__ == "__main__":
    main()
