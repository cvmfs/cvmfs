/**
 * This file is part of the CernVM File System
 */

#include "fcgi.h"

#include <cstdio>
#include <cstdlib>
#include <string>

#include "../sanitizer.h"

using namespace std;  // NOLINT

int main(int argc, char **argv) {
  //FILE *f = fopen("/tmp/mycgi.log", "w");
  FastCgi fcgi;
  if (!fcgi.IsFcgi()) {
    printf("not in FastCGI context, starting localhost:9000\n");
    if (!fcgi.MkTcpSocket("127.0.0.1", 9000)) {
      fprintf(stderr, "failed to create tcp socket\n");
      return 1;
    }
  }

  unsigned char *buf;
  unsigned length;
  uint64_t id = 0;
  FastCgi::Event event;
  string request_uri;
  string content;
  sanitizer::UriSanitizer uri_sanitizer;
  while ((event = fcgi.NextEvent(&buf, &length, &id)) != FastCgi::kEventExit) {
    switch (event) {
      case FastCgi::kEventAbortReq:
        fcgi.AbortRequest();
        break;

      case FastCgi::kEventTransportError:
        break;

      case FastCgi::kEventStdin:
        /*fprintf(f, "%s", fcgi.DumpParams().c_str());
        fprintf(f, "LENGTH: %d\n", length);
        fprintf(f, "%s", string((char *)buf, length).c_str());
        fflush(f);*/
        fcgi.GetParam("REQUEST_URI", &request_uri);
        if (!uri_sanitizer.IsValid(request_uri)) {
          fcgi.ReturnBadRequest("Invalid URI");
          break;
        }
        content = "Content-type: text/html\n\n<html>" +
                  request_uri + "</html>\n";
        fcgi.SendData(content, true);
        fcgi.EndRequest(0);
        break;

      default:
        abort();
    }
  }
  return 0;
}
