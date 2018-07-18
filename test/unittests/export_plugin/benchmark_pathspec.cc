#include <stdint.h>
#include <sys/time.h>


#include <ctime>
#include <cstring>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>

#include "export_plugin/spec_tree.h"

std::string& rtrim(std::string& str, const std::string& chars = "\t\n\v\f\r ")
{
    str.erase(str.find_last_not_of(chars) + 1);
    return str;
}

int main() {
  struct timeval timeStart,
                timeEnd;
  long double diff = 0;
  SpecTree *specs;
  gettimeofday(&timeStart, NULL);
  for (int i = 0; i <= 100; i++) {
    specs = SpecTree::Create(
    "/home/pteuber/Documents/cernvmfs/notes/benchmark-pathspec/cvmfs-atlas.cern.ch-bench3.trace.spec.txt");
  }
  gettimeofday(&timeEnd, NULL);
  diff = ((timeEnd.tv_sec - timeStart.tv_sec) * 1000000 + timeEnd.tv_usec - timeStart.tv_usec)/100;
  std::cout << "Opening spec file took: " << diff << "us" << std::endl;
  diff = 0;
  std::cout << "Hi";
  std::ifstream infile("/home/pteuber/Documents/cernvmfs/notes/benchmark-pathspec/cvmfs-atlas.cern.ch.trace.log");
  std::string line;
  uint32_t steps = 0;
  bool res;
  while (std::getline(infile, line)) {
    const char *path = strdup(line.c_str());
    gettimeofday(&timeStart, NULL);
    res = specs->IsMatching(rtrim(line));
    gettimeofday(&timeEnd, NULL);
    // std::cout << res << std::endl;
    /*if (!res) { 
      std::cout << "ERROR" << path << std::endl;
      delete path;
      return -1;
    }*/
    delete path;
    diff += ((timeEnd.tv_sec - timeStart.tv_sec) * 1000000 + timeEnd.tv_usec - timeStart.tv_usec);
    if (steps % 10000 == 0 && steps > 0) {
      double timePer = diff / steps;
      std::cout << "Total time: " << diff << "us" << std::endl;
      std::cout << "Steps: " << steps << std::endl;
      std::cout << "Average time: " << timePer << "us" << std::endl;
    }
    steps++;
  }
  double timePer = diff / steps;
  std::cout << "Total time: " << diff << "us" << std::endl;
  std::cout << "Steps: " << steps << std::endl;
  std::cout << "Average time: " << timePer << "us" << std::endl;
  delete specs;
  return 0;
}