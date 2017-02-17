#include <unistd.h>

#include <cstdio>
#include <cstring>
#include <string>

#include "letter.h"
#include "mongoose.h"
#include "signature.h"
#include "util/pointer.h"
#include "util/posix.h"
#include "util/string.h"

using namespace std;

struct {
  string fqrn;
  signature::SignatureManager *signature_mgr;
  string cmd;
} user_data;

// This function will be called by mongoose on every new request.
static int begin_request_handler(struct mg_connection *conn) {
  const struct mg_request_info *request_info = mg_get_request_info(conn);
  char content[10000];

  char post_data[10000];
  post_data[0] = '\0';
  int post_data_len;
  post_data_len = mg_read(conn, post_data, sizeof(post_data));
  // TODO: trim final newline
  post_data[post_data_len-1] = '\0';
  letter::Failures retval_ltr;

  //printf("POST DATA %s\n", post_data);

  string message;
  string cert;
  letter::Letter letter(user_data.fqrn, post_data, user_data.signature_mgr);
  //printf("LETTER IS %p\nTEXT %s\n", &letter, letter.text().c_str());
  retval_ltr = letter.Verify(10, &message, &cert);
  string dec;
  Debase64(message, &dec);

  int stdin, stdout, stderr;
  Shell(&stdin, &stdout, &stderr);
  string cmd = user_data.cmd + ";exit \n";
  write(stdin, cmd.data(), cmd.length());
  string out;
  char c;
  mg_printf(conn,
            "HTTP/1.1 200 OK\r\n"
            "Content-Type: text/plain\r\n\r\n");
  while (read(stdout, &c, 1) == 1) {
    if (c == '\n') {
      mg_printf(conn, "%s\n", out.c_str());
      out.clear();
    } else {
      out.push_back(c);
    }
  }

  // Prepare the message we're going to send
  //int content_length = snprintf(content, sizeof(content),
  //                              "Hello from mongoose! type: %s, letter %s "
  //                              "code %d, length %d\n",
  //                              request_info->request_method,
  //                              dec.c_str(),
  //                              retval_ltr,
  //                              post_data_len);



  // Send HTTP reply to the client
  //mg_printf(conn,
  //          "HTTP/1.1 200 OK\r\n"
  //          "Content-Type: text/plain\r\n"
  //          "Content-Length: %d\r\n"        // Always set Content-Length
  //          "\r\n"
  //          "%s",
  //          content_length, content);

  // Returning non-zero tells mongoose that our function has replied to
  // the client, and mongoose should not send client any more data.
  return 1;
}

int main(int argc, char **argv) {
  UniquePtr<signature::SignatureManager>
    signature_mgr(new signature::SignatureManager());
  signature_mgr->Init();

  user_data.fqrn = argv[1];
  if (!signature_mgr->LoadPublicRsaKeys(argv[2]))
    abort();
  user_data.cmd = argv[3];

  struct mg_context *ctx;
  struct mg_callbacks callbacks;

  // List of options. Last element must be NULL.
  const char *options[] = {"num_threads", "2", "listening_ports", "8080", NULL};

  // Prepare callbacks structure. We have only one callback, the rest are NULL.
  memset(&callbacks, 0, sizeof(callbacks));
  callbacks.begin_request = begin_request_handler;

  // Start the web server.
  user_data.signature_mgr = signature_mgr;
  ctx = mg_start(&callbacks, NULL, options);

  // Wait until user hits "enter". Server is running in separate thread.
  // Navigating to http://localhost:8080 will invoke begin_request_handler().
  getchar();

  // Stop the server.
  mg_stop(ctx);

  return 0;
}
