/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_UPLOAD_IMPL_H_
#define CVMFS_UPLOAD_IMPL_H_

#include <cassert>

#include "logging.h"
#include "upload_backend.h"

namespace upload {

  template <class PushWorkerT>
  void Spooler::SpawnSpoolerBackend(const SpoolerDefinition &definition) {
    assert (definition.IsValid());

    // spawn spooler backend process
    int pid = fork();
    if (pid < 0) {
      LogCvmfs(kLogSpooler, kLogStderr, "failed to spawn spooler backend");
      assert(pid >= 0); // nothing to do here anymore... good bye
    }

    if (pid > 0)
      return;

    // ---------------------------------------------------------------------------
    // From here on we are in the SpoolerBackend process

    // create a SpoolerBackend object of the requested type
    SpoolerBackend<PushWorkerT> backend(definition.spooler_description);

    // connect the named pipes in the SpoolerBackend
    if (! backend.Connect(definition.paths_out_pipe,
                          definition.digests_in_pipe)) {
      LogCvmfs(kLogSpooler, kLogStderr, "failed to connect spooler backend");
      exit(2);
    }
    LogCvmfs(kLogSpooler, kLogVerboseMsg, "connected spooler backend");

    // do the final initialization of the SpoolerBackend
    if (! backend.Initialize()) {
      LogCvmfs(kLogSpooler, kLogStderr, "failed to initialize spooler backend");
      exit(3);
    }
    LogCvmfs(kLogSpooler, kLogVerboseMsg, "initialized spooler backend");

    // run the SpoolerBackend service
    // returns on a termination signal though the named pipes
    const int retval = backend.Run();
    exit(retval);
  }

}

#endif /* CVMFS_UPLOAD_IMPL_H_ */
