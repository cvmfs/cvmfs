import time
import glob
import argparse

from influxdb.influxdb08 import InfluxDBClient

from parser import Parser


class Database:
    def __init__(self, credentials_file):
        file = open(credentials_file, "r")
        credentials = {}
        for line in file:
            params = line.strip().split("=")
            if len(params) == 2:
                credentials[params[0]] = params[1]
        self.user = credentials["user"]
        self.password = credentials["password"]
        self.database = credentials["database"]
        self.host = credentials["host"]
        self.port = credentials["port"]

    def write(self, repository, counters,
              current_time=int(time.time())):
        client = InfluxDBClient(self.host, self.port, self.user, self.password)
        if {"name": self.database} not in client.get_list_database():
            client.create_database(self.database)
        client.switch_database(self.database)
        json_body = [{
                        "name": repository,
                        "columns": ["time"],
                        "points": [[current_time]]
                     }]

        for counter in counters.values():
            json_body[0]["columns"].append(counter.name)
            json_body[0]["points"][0].append(int(counter.avg()))
        client.write_points(json_body)


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
    database = Database(str(args.credentials_file))
    current = int(time.time())
    for filename in args.files:
        for expanded in glob.glob(filename):
            parser = Parser()
            parser.parse(expanded)
            database.write(parser.repository, parser.counters, current)

    print("Done")

if __name__ == "__main__":
    main()
