/**
 * This file is part of the CernVM File System.
 *
 * Fills an internal map of key-value pairs from ASCII files in key=value
 * style.  Parameters can be overwritten.  Used to read configuration from
 * /etc/cvmfs/...
 */

#include "cvmfs_config.h"
#include "options.h"

#include <cstdio>
#include <map>

#include "util.h"

using namespace std;  // NOLINT

namespace options {

struct ConfigValue {
  string value;
  string source;
};

map<string, ConfigValue> *config_ = NULL;


void Init() {
  config_ = new map<string, ConfigValue>();
}


void Fini() {
  delete config_;
}


void ParsePath(const string config_file) {
  FILE *fconfig = fopen(config_file.c_str(), "r");
  if (!fconfig)
    return;

  // Read line by line
  int retval;
  while ((retval = fgetc(fconfig)) != EOF) {
    char c = retval;
    if (c == '\n') {}
    //fgets
  }


  fclose(fconfig);
}


void ClearConfig() {
}


string *GetValue(string *key) {
  return NULL;
}


string *GetSource(string *key) {
  return NULL;
}


vector<string> GetAllKeys() {
  vector<string> result;
  return result;
}

}  // namespace options
