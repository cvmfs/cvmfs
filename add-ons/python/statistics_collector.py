import time
import datetime
import glob
import argparse
import os

from parser import Parser

def parse_args():
    argparser = argparse.ArgumentParser()
    argparser.add_argument("--credentials_file",
                           default="/etc/cvmfs/influxdb.credentials",
                           required=False, type=str,
                           help="Credentials file to connect to the Influx "
                                "database")
    argparser.add_argument("files", nargs="+",
                           help=".data files must be specified")
    return argparser.parse_args()


def main():
    args = parse_args()
    current_timestamp = datetime.datetime.now(tz=None)
    if not os.path.isdir("results"):
        os.mkdir("results")
    for filename in args.files:
        for expanded in glob.glob(filename):
            parser = Parser(timestamp=current_timestamp)
            parser.parse(expanded)
            parser.to_csv("results/" + parser.repository + \
                          "_" + parser.current_timestamp + ".csv")

    print("Done")

if __name__ == "__main__":
    main()
