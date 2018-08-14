/**
 * This file is part of the CernVM File System.
 */
#include <stdint.h>
#include <sys/time.h>


#include <cstring>
#include <ctime>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>

#include "path_filters/dirtab.h"
#include "shrinkwrap/spec_tree.h"

std::string *rtrim(std::string *str,
  const std::string &chars = "\t\n\v\f\r ")
{
    str->erase(str->find_last_not_of(chars) + 1);
    return str;
}

int main(int argc, char **argv) {
  struct timeval timeStart,
                timeEnd;
  if (argc < 1) {
    std::cerr << "ERROR: Call with: benchmark spec_file trace_file";
  }
  long double diff = 0;
  SpecTree *specs;
  // catalog::Dirtab *specs;
  gettimeofday(&timeStart, NULL);
  specs = SpecTree::Create(argv[1]);
  // specs = catalog::Dirtab::Create();
  for (int i = 0; i < 99; i++) {
    delete specs;
    specs = SpecTree::Create(argv[1]);
    // specs = catalog::Dirtab::Create(argv[1]);
  }
  gettimeofday(&timeEnd, NULL);
  diff = ((timeEnd.tv_sec - timeStart.tv_sec)
    * 1000000 + timeEnd.tv_usec - timeStart.tv_usec)/100;
  std::cout << "Opening spec file took: " << diff << "us" << std::endl;
  diff = 0;
  std::cout << "Hi";
  std::ifstream infile(argv[2]);
  std::string line;
  uint32_t steps = 0;
  bool res;
  __asm__ __volatile__("" :: "m" (specs));
  while (std::getline(infile, line)) {
    const char *path = strdup(line.c_str());
    gettimeofday(&timeStart, NULL);
    res = specs->IsMatching(*rtrim(&line));
    gettimeofday(&timeEnd, NULL);
    // std::cout << res << std::endl;
    /*if (!res) { 
      std::cout << "ERROR" << path << std::endl;
      delete path;
      return -1;
    }*/
    __asm__ __volatile__("" :: "m" (res));
    delete path;
    diff += ((timeEnd.tv_sec - timeStart.tv_sec)
      * 1000000 + timeEnd.tv_usec - timeStart.tv_usec);
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
