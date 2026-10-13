/**
 * This file is part of the CernVM File System.
 *
 * cvmfs_nfs4_export: serves a local directory read-only over NFSv4.0.  A test
 * vehicle for the NFS server against kernel clients, e.g.
 *
 *   cvmfs_nfs4_export -p 20490 /some/dir
 *   mount -t nfs4 -o vers=4.0,port=20490 127.0.0.1:/ /mnt             (Linux)
 *   mount_nfs -o vers=4,port=20490 localhost:/ /mnt                   (macOS)
 */

#include <pthread.h>
#include <signal.h>
#include <unistd.h>

#include <cstdio>
#include <cstdlib>
#include <string>

#include "nfs4/nfs4_server.h"
#include "nfs4/posix_backend.h"
#include "nfs4/rpc.h"

namespace {

void Usage(const char *progname) {
  fprintf(stderr,
          "Usage: %s [-b address] [-p port] [-w workers] [-r] [-s ms] [-D ms] <dir>\n"
          "  -b  listen address (default 127.0.0.1)\n"
          "  -p  port (default: ephemeral, printed on stdout)\n"
          "  -w  worker threads (default 16)\n"
          "  -r  accept non-loopback clients\n"
          "  -s  delay every read by ms (testing)\n"
          "  -D  answer NFS4ERR_DELAY for requests slower than ms\n",
          progname);
}

}  // anonymous namespace

int main(int argc, char **argv) {
  std::string address = "127.0.0.1";
  unsigned port = 0;
  nfs4::RpcServer::Options rpc_options;
  nfs4::Nfs4Server::Options server_options;
  unsigned read_delay_ms = 0;
  int c;
  while ((c = getopt(argc, argv, "b:p:w:rs:D:h")) != -1) {
    switch (c) {
      case 'b':
        address = optarg;
        break;
      case 'p':
        port = static_cast<unsigned>(atoi(optarg));
        break;
      case 'w':
        rpc_options.workers = static_cast<unsigned>(atoi(optarg));
        break;
      case 'r':
        rpc_options.allow_remote = true;
        break;
      case 's':
        read_delay_ms = static_cast<unsigned>(atoi(optarg));
        break;
      case 'D':
        server_options.delay_after_ms = static_cast<unsigned>(atoi(optarg));
        break;
      default:
        Usage(argv[0]);
        return 1;
    }
  }
  if (optind != argc - 1 || port > 65535) {
    Usage(argv[0]);
    return 1;
  }

  nfs4::PosixBackend *backend = nfs4::PosixBackend::Create(argv[optind]);
  if (backend == NULL) {
    fprintf(stderr, "%s is not a directory\n", argv[optind]);
    return 1;
  }
  backend->set_read_delay_ms(read_delay_ms);
  server_options.delegations = (getenv("CVMFS_NFS_DELEGATIONS") != NULL);
  nfs4::Nfs4Server server(backend, server_options);
  nfs4::RpcServer rpc(&server, rpc_options);
  if (!rpc.Listen(address, static_cast<uint16_t>(port))) {
    fprintf(stderr, "failed to listen on %s:%u\n", address.c_str(), port);
    return 1;
  }

  sigset_t signals;
  sigemptyset(&signals);
  sigaddset(&signals, SIGINT);
  sigaddset(&signals, SIGTERM);
  // Block before starting threads so that they inherit the mask
  pthread_sigmask(SIG_BLOCK, &signals, NULL);
  signal(SIGPIPE, SIG_IGN);

  rpc.Start();
  printf("%u\n", rpc.port());
  fflush(stdout);

  int sig;
  sigwait(&signals, &sig);
  rpc.Stop();
  delete backend;
  return 0;
}
