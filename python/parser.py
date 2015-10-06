import math

class Counter:
    def __init__(self, name, number, description):
        self.name = name
        self.description = description
        try:
            self.values = [int(number)]
        except ValueError:
            self.values = [0]

    def sum(self):
        counter = 0.0
        for n in self.values:
            counter += n
        return counter

    def avg(self):
        return self.sum() / len(self.values)

    def variance(self):
        counter = 0.0
        average = self.avg()
        for n in self.values:
            diff = n - average
            counter += diff * diff
        return counter / len(self.values)

    def std(self):
        return math.sqrt(self.variance())


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
                counter = Counter(params[0], params[1], params[2])
                if counter.name in self.counters:
                    self.counters[counter.name].values += counter.values
                else:
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
            csv.write(counter.name + ";" + str(counter.avg()))
        csv.close()

    @staticmethod
    def to_csv_comparison(parser1, parser2, filename):
        csv = open(filename, "w")
        csv.write(";origin_avg;origin_std;external_avg;external_std\n")
        for key in parser1.counters:
            csv.write(parser1.counters[key].name + ";" +
                      str(parser1.counters[key].avg()).replace(".", ",") + ";" +
                      str(parser1.counters[key].std()).replace(".", ",") + ";" +
                      str(parser2.counters[key].avg()).replace(".", ",") + ";" +
                      str(parser2.counters[key].std()).replace(".", ",") + "\n")
        csv.close()

    @staticmethod
    def to_csv_multiple_comparison(parser_list1, parser_list2, filename):
        counter_names = parser_list1.values()[0].counters.keys()
        repository_names = sorted(parser_list1)
        csv = open(filename, "w")
        for repository_name in repository_names:
            pos = repository_name.rfind("/") + 1
            csv.write(";" + repository_name[pos:] + ";;;")
        csv.write("\n")
        for i in range(0, len(parser_list1)):
            csv.write(";origin_avg;origin_std;external_avg;external_std")
        csv.write("\n")
        for counter_name in counter_names:
            csv.write(counter_name + ";")
            for repository_name in repository_names:
                parser1 = parser_list1[repository_name]
                parser2 = parser_list2[repository_name]
                csv.write(str(parser1.counters[counter_name].avg()).replace(".", ",") + ";" +
                          str(parser1.counters[counter_name].std()).replace(".", ",") + ";" +
                          str(parser2.counters[counter_name].avg()).replace(".", ",") + ";" +
                          str(parser2.counters[counter_name].std()).replace(".", ",") + ";")
            csv.write("\n")
        csv.close()
