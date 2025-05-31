/**
 * This file is part of the CernVM File System.
 */

#include "swissknife_notify.h"

#include "notify/cmd_pub.h"
#include "notify/cmd_sub.h"

namespace {

bool ValidateArgs(const swissknife::ArgumentList &args) {
  const bool publish = args.count('p') > 0;
  const bool subscribe = args.count('s') > 0;

  if (publish == subscribe) {
    LogCvmfs(kLogCvmfs, kLogStdout,
             "Either the \"publish\" (-p) or the \"subscribe\" (-s) option "
             "must be provided");
    return false;
  }

  if (publish && (args.count('r') == 0)) {
    LogCvmfs(kLogCvmfs, kLogStdout,
             "A repository URL (-r) needs to be provided when using the "
             "publish command");
    return false;
  }

  if (subscribe && (args.count('t') == 0)) {
    LogCvmfs(kLogCvmfs, kLogStdout,
             "A subscription topic (-t) needs to be provided when using the "
             "subscribe command");
    return false;
  }

  return true;
}

uint64_t GetMinRevision(const swissknife::ArgumentList &args) {
  if (args.count('m') > 0) {
    return std::atoi(args.find('m')->second->c_str());
  } else {
    return 0;
  }
}

}  // namespace

namespace swissknife {

CommandNotify::~CommandNotify() { }

ParameterList CommandNotify::GetParams() const {
  ParameterList l;
  l.push_back(Parameter::Switch('p', "publish"));
  l.push_back(Parameter::Switch('s', "subscribe"));
  l.push_back(Parameter::Mandatory('u', "notification server URL"));
  l.push_back(
      Parameter::Optional('r', "URL of repository with manifest to publish"));
  l.push_back(Parameter::Optional('t', "subscription topic"));
  l.push_back(Parameter::Optional('m', "minimum revision number of interest"));
  l.push_back(Parameter::Switch(
      'c', "run continuously (don't exit after first notification)"));
  l.push_back(Parameter::Switch('v', "verbose output"));
  return l;
}

int CommandNotify::Main(const ArgumentList &args) {
  if (!ValidateArgs(args)) {
    return 1;
  }

  int ret = 0;
  const bool verbose = args.count('v') > 0;
  const std::string server_url = *args.find('u')->second;
  const bool publish = args.count('p') > 0;
  if (publish) {
    const std::string repository_url = *args.find('r')->second;
    ret = notify::DoPublish(server_url, repository_url, verbose);
  } else {  // subscribe
    const std::string topic = *args.find('t')->second;
    const bool continuous = args.count('c') > 0;
    const uint64_t revision = GetMinRevision(args);
    ret = notify::DoSubscribe(server_url, topic, revision, continuous, verbose);
  }

  return ret;
}

}  // namespace swissknife
