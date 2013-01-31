/**
 * This file belongs to CERN.
 */

#ifndef CVMFS_QUERYDNS_H_
#define CVMFS_QUERYDNS_H_

#include <ares.h>
#include <arpa/nameser.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <netdb.h>
#include <stdarg.h>
#include <string.h>
#include <ctype.h>
#include <unistd.h>
#include <iostream>
#include "logging.h"
#ifdef CVMFS_NAMESPACE_GUARD
namespace CVMFS_NAMESPACE_GUARD {
#endif

bool QueryDns(const std::string &hostname,
              int type,
              const std::string &dns_server,
              const uint16_t port,
              std::string *result);
#ifdef CVMFS_NAMESPACE_GUARD
}
#endif

#endif //CVMFS_QUERYDNS_H_
