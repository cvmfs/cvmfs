/**
 * This file is part of the CernVM File System.
 */


#include "publish/command.h"

#include <getopt.h>

#include <cassert>
#include <cstdlib>
#include <cstring>

#include "publish/except.h"
#include "util/logging.h"
#include "util/smalloc.h"
#include "util/string.h"

using namespace std;  // NOLINT

namespace publish {

std::string Command::GetExamples() const {
  std::vector<std::string> examples = DoGetExamples();
  std::string result;
  for (unsigned i = 0; i < examples.size(); ++i) {
    result += progname() + " " + GetName() + " " + examples[i] + "\n";
  }
  return result;
}


Command::Options Command::ParseOptions(int argc, char **argv) {
  Options result;
  progname_ = argv[0];
  ParameterList params = GetParams();

  string shortopts;
  struct option *longopts = reinterpret_cast<option *>(
    smalloc((params.size() + 1) * sizeof(struct option)));
  struct option lastopt;
  memset(&lastopt, 0, sizeof(lastopt));
  longopts[params.size()] = lastopt;

  for (unsigned i = 0; i < params.size(); ++i) {
    shortopts.push_back(params[i].short_key);
    if (!params[i].is_switch) shortopts.push_back(':');

    longopts[i].name = strdup(params[i].key.c_str());
    longopts[i].has_arg = params[i].is_switch ? no_argument : required_argument;
    longopts[i].flag = NULL;
    longopts[i].val = params[i].short_key;
  }

  int idx;
  int c;
  opterr = 0;  // Don't print error messages in getopt
  optind = 2;  // Skip the command itself
  while ((c = getopt_long(argc, argv, shortopts.c_str(), longopts, &idx)) != -1)
  {
    bool found = false;
    for (unsigned i = 0; i < params.size(); ++i) {
      if (c == params[i].short_key) {
        Argument argument;
        if (!params[i].is_switch) {
          argument.value_str = optarg;
          argument.value_int = String2Int64(argument.value_str);
        }
        result.Set(params[i], argument);
        found = true;
        break;
      }
    }

    if (!found) {
      assert(c == '?');
      // Easier to duplicate the next few lines than to add an RAII wrapper
      for (unsigned i = 0; i < params.size(); ++i) {
        free(const_cast<char *>(longopts[i].name));
      }
      free(longopts);

      throw EPublish(GetName() + ": unrecognized parameter '" +
                     argv[optind - 1] + "'", EPublish::kFailInvocation);
    }
  }

  for (unsigned i = 0; i < params.size(); ++i) {
    free(const_cast<char *>(longopts[i].name));
  }
  free(longopts);

  for (unsigned i = 0; i < params.size(); ++i) {
    if (!params[i].is_optional && !result.Has(params[i].key)) {
      throw EPublish(
        GetName() + ": missing mandatory parameter '" + params[i].key + "'",
        EPublish::kFailInvocation);
    }
  }

  for (int i = optind; i < argc; ++i) {
    result.AppendPlain(Argument(argv[i]));
  }

  if (result.plain_args().size() < GetMinPlainArgs()) {
    LogCvmfs(kLogCvmfs, kLogStderr, "Usage: %s %s %s",
             progname().c_str(), GetName().c_str(), GetUsage().c_str());
    throw EPublish(GetName() + ": missing argument", EPublish::kFailInvocation);
  }

  return result;
}


//------------------------------------------------------------------------------


CommandList::~CommandList() {
  for (unsigned i = 0; i < commands_.size(); ++i)
    delete commands_[i];
}

Command *CommandList::Find(const std::string &name) {
  Command *result = NULL;
  for (unsigned i = 0; i < commands_.size(); ++i) {
    if (commands_[i]->GetName() == name) {
      result = commands_[i];
      break;
    }
  }
  return result;
}

void CommandList::TakeCommand(Command *command) {
  assert(Find(command->GetName()) == NULL);
  commands_.push_back(command);
}

}  // namespace publish
