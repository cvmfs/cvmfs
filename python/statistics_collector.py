import time
import glob
import argparse
from influxdb.influxdb08 import InfluxDBClient


class DatabaseCreationException(Exception):
    pass


class DatabaseUpdateException(Exception):
    pass


class DatabaseGraphGenerationException(Exception):
    pass


class GraphCreationException(Exception):
    pass


class Counter:
    def __init__(self, name, number, description):
        self.name = name
        self.number = number
        self.description = description


class Parser:
    def __init__(self):
        self.counters = {}
        self.warm_cache = False
        self.repository = ""

    @staticmethod
    def parse_boolean(string):
        return string == "yes" or string == "true" \
                         or string == "TRUE" or string == "True"

    def __parseline(self, line):
        if line[0] == "#":
            parameter = line[1:-1].replace(" ", "").split("=")
            if parameter[0] == "warm_cache":
                self.warm_cache = Parser.parse_boolean(parameter[1])
            elif parameter[0] == "repo":
                self.repository = parameter[1].split(".")[0]
        else:
            params = line.strip().split("|")
            if len(params) == 3:
                counter = Counter(params[0], int(params[1]), params[2])
                self.counters[counter.name] = counter

    def parse(self, filename):
        datafile = open(filename, "r")
        for line in datafile:
            self.__parseline(line)
        datafile.close()

    def all_counters(self):
        return self.counters.values()


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
                        "points": []
                     }]

        json_body[0]["points"].append([current_time])
        for counter in counters.values():
            json_body[0]["columns"].append(counter.name)
            json_body[0]["points"][0].append(counter.number)
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
