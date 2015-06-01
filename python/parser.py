class Counter:
    def __init__(self, name, number, description):
        self.name = name
        self.description = description
        try:
            self.number = int(number)
        except ValueError:
            self.number = 0


class Parser:
    def __init__(self, filename=None):
        self.counters = {}
        self.warm_cache = False
        self.repository = ""
        self.num_files = 0
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
                    self.counters[counter.name].number += counter.number
                else:
                    self.counters[counter.name] = counter

    def parse(self, filename):
        self.num_files += 1
        datafile = open(filename, "r")
        for line in datafile:
            self.__parseline(line)
        datafile.close()

    def to_csv(self, filename):
        csv = open(filename, "w")
        csv.write(";" + self.repository)
        for counter in self.counters:
            csv.write(counter.name + ";" + str(counter.number / self.num_files))
        csv.close()

    @staticmethod
    def to_csv_comparison(parser1, parser2, filename):
        csv = open(filename, "w")
        csv.write(";origin;external\n")
        for key in parser1.counters:
            csv.write(parser1.counters[key].name + ";" +
                      str(parser1.counters[key].number / parser1.num_files)
                      + ";" +
                      str(parser2.counters[key].number / parser2.num_files)
                      + "\n")
        csv.close()

    @staticmethod
    def to_csv_multiple_comparison(parser_list1, parser_list2, filename):
        counter_names = parser_list1.values()[0].counters.keys()
        csv = open(filename, "w")
        for repository in parser_list1:
            csv.write(";" + repository + ";")
        csv.write("\n")
        for i in range(0, len(parser_list1)):
            csv.write(";origin;external")
        csv.write("\n")
        for counter_name in counter_names:
            csv.write(counter_name + ";")
            for repository in parser_list1:
                parser1 = parser_list1[repository]
                parser2 = parser_list2[repository]
                csv.write(str(parser1.counters[counter_name].number
                              / parser1.num_files) + ";" +
                          str(parser2.counters[counter_name].number
                              / parser2.num_files) + ";")
            csv.write("\n")
        csv.close()
