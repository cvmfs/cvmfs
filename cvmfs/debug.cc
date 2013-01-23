#include <iostream>

#include "upload.h"

using namespace upload;

static void SpoolCallback(const SpoolerResult &result) {

  std::cout << result.return_code << std::endl;
}

int main() {

  AbstractSpooler* spooler = AbstractSpooler::Construct("local:/home/rene/Documents/Schweinestall/cvmfs/build/tmp/data/txn:/home/rene/Documents/Schweinestall/cvmfs/build/tmp/");
  assert (spooler);

  spooler->RegisterListener(&SpoolCallback);

  spooler->Process("/home/rene/Documents/Schweinestall/cvmfs/build/cvmfs/debug_spool");

  spooler->WaitForTermination();

  return 0;
}

