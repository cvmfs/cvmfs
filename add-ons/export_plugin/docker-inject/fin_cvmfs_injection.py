import argparse
from docker_injector import DockerInjector
import fileinput
import tempfile

argparser = argparse.ArgumentParser()
argparser.add_argument("dir",
  type=str)
args = argparser.parse_args()

injector = DockerInjector("localhost:443", "atlas-test", "latest")
injector.update(args.dir, "cvmfs")