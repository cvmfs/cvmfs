import argparse
import threading

from docker import Client
from parser import Parser


class GitHubRepository:
    def __init__(self, user, branch):
        self.user = user
        self.branch = branch

    def url(self):
        return "https://github.com/" + self.user + "/cvmfs"


class DockerExecutor:
    def __init__(self, repository, socket_url, api_version, name):
        self.repo = repository
        self.socket_url = socket_url
        self.api_version = api_version
        self.name = name

    def run(self):
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
              "export CVMFS_OPT_VALGRIND=no && " + \
              "export CVMFS_OPT_ITERATIONS=1 && " + \
              "export CVMFS_OPT_WARM_CACHE=no && " + \
              "cd /workdir/cvmfs/build && " + \
              "git reset --hard && " + \
              "git remote add " + repo.user + " " + repo.url() + " && " + \
              "git remote update && " + \
              "git fetch " + repo.user + " && " + \
              "git checkout -b test " + repo.user + "/" + repo.branch + " && " + \
              "cmake .. && " + \
              "make install && " + \
              "cd /workdir/cvmfs/test && " + \
              "./run.sh /tmp/benchmarks.log benchmarks/001-atlas/ && " + \
              "cp /tmp/cvmfs_benchmarks/atlas.cern.ch/atlas.cern.ch_1.data /tmp/" + \
              self.name + ".data\' > /tmp/run.log"

        print("Executing benchmarks for the repository \"" + self.name + "\"")
        container_id = client.create_container(image="moliholy/slc6:cvmfs-test",
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
                           help="Docer API version (default 1.17)")
    argparser.add_argument("--original_repo",
                           default="cvmfs",
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


def main():
    args = parse_args()
    origin = GitHubRepository(args.original_repo, args.original_branch)
    external = GitHubRepository(args.external_repo, args.external_branch)

    # download firstly the image
    print("Downloading the image " + image + ":" + tag)
    c = Client(base_url=args.socket_url, version=args.docker_api_version)
    c.pull(repository=image, tag=tag)

    # it creates two threads
    origin_exec = DockerExecutor(origin, args.socket_url,
                                 args.docker_api_version, origin.user)
    external_exec = DockerExecutor(external, args.socket_url,
                                   args.docker_api_version, external.user)
    origin_exec.run()
    origin_parser = Parser("/tmp/" + origin.user + ".data")

    external_exec.run()
    external_parser = Parser("/tmp/" + external.user + ".data")

    Parser.to_csv_comparison(origin_parser, external_parser,
                             "/tmp/comparison.csv")
    print("Done. File /tmp/comparison.csv created")

if __name__ == "__main__":
    image = "moliholy/slc6"
    tag = "cvmfs-test"
    main()
