class Counter:
    def __init__(self, name, number, description):
        self.name = name
        self.number = number
        self.description = description


class Parser:
    def __init__(self, filename=None):
        self.counters = {}
        self.warm_cache = False
        self.repository = ""
        if filename is not None:
            self.parse(filename)


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

    def to_csv(self, filename):
        csv = open(filename, "w")
        csv.write(";" + self.repository)
        for counter in self.counters:
            csv.write(counter.name + ";" + counter.number)
        csv.close()

    @staticmethod
    def to_csv_comparison(parser1, parser2, filename):
        csv = open(filename, "w")
        csv.write(";origin;external\n")
        for key in parser1.counters:
            csv.write(parser1.counters[key].name + ";" +
                      str(parser1.counters[key].number) + ";" +
                      str(parser2.counters[key].number) + "\n")
        csv.close()

    def all_counters(self):
        return self.counters.values()
