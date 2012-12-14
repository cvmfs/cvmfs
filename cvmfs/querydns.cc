/**
 *
 * This is a function that resolves the TXT result of the given hostname.
 * This function uses c-ares package
 *
 */

#include "querydns.h"

using namespace std; // NOLINT

// Global variables for getting the results of cname and txt.
static string cname;
unsigned char *result_txt;

static void run_ares_mainloop(ares_channel channel)
{ 
  int nfds, count;
  fd_set readers, writers;
  struct timeval tv, *tvp;

  while (1)
  {
    FD_ZERO(&readers);
    FD_ZERO(&writers);
    nfds = ares_fds(channel, &readers, &writers);
    if (nfds == 0)
       break;

    tvp = ares_timeout(channel, NULL, &tv);
    count = select(nfds, &readers, &writers, NULL, tvp);

    ares_process(channel, &readers, &writers);
  }
}

static void callback_txt(void *arg, int status,int timeouts,
                         unsigned char *abuf, int alen)
{
   struct ares_txt_reply *get_txt_result=NULL;

   status = ares_parse_txt_reply(abuf, alen, &get_txt_result);
   if (status != ARES_SUCCESS)
   {
     cout << ares_strerror(status)<<endl;
     result_txt = NULL;
     return;
   }

   result_txt = get_txt_result->txt;
}


static void callback_cname(void *arg, int status,int timeouts,
                           unsigned char *abuf, int alen)
{
   struct hostent *host=NULL;
   
   status = ares_parse_a_reply(abuf, alen, &host,NULL,NULL);
   if (status != ARES_SUCCESS)
   { 
     cout << ares_strerror(status)<<endl;
     cname = "";
     return;
   }
  
   cname = host->h_name;
}

bool QueryDns(string hostname, int type, const string *dns_server,
              const uint16_t port, string *result)
{
  int status;
  int optmask = 0;
  string string_buff;
  ares_channel channel;
  struct ares_options options;
  struct ares_addr_node *set_nameserver;

  set_nameserver = new ares_addr_node[sizeof(ares_addr_node)];

  status = ares_library_init(ARES_LIB_INIT_ALL);
  if (status != ARES_SUCCESS)
  {
     cout << ares_strerror(status);
     return false;
  }

  // Set up the udc port.
  options.udp_port = port;
  optmask |= ARES_OPT_UDP_PORT;

  status = ares_init_options(&channel, &options, optmask); 
  if (status != ARES_SUCCESS)
  {
     cout << ares_strerror(status);
     return false;
  }

  // Set up the name server we'd like to query.
  if ( type == AF_INET)
     inet_pton(type, (*dns_server).c_str(), &set_nameserver->addr.addr4);
  else if ( type == AF_INET6)
     inet_pton(type, (*dns_server).c_str(), &set_nameserver->addr.addr6);
  else
     return false;

  set_nameserver[0].family = type;
  set_nameserver[0].next = NULL;
  status = ares_set_servers(channel, set_nameserver);
  if (status != ARES_SUCCESS)
  {
     cout << ares_strerror(status);
     return false;
  }

  // Query the CNAME for the given hostname.
  ares_query(channel, hostname.c_str(),C_IN, T_CNAME, callback_cname, NULL);
  run_ares_mainloop(channel);
   
  // After get the cname, query the txt result.
  if (cname != "")
  {
     ares_query(channel, cname.c_str(),C_IN, T_TXT, callback_txt, NULL);
     run_ares_mainloop(channel);
  }
  else
     return false;

  // Change the result type and then return it.
  string_buff = string_buff.assign((const char*)result_txt);
  *result = string_buff;

  delete set_nameserver;
  ares_destroy(channel);
  ares_library_cleanup();

  return true;
}

/*
int main(int argc,char* argv[])
{
  string my_string;
  QueryDns("httpproxy.geo.cdn.cernvm.org", AF_INET,"137.138.234.60", 53, &my_string);
  cout <<"Txt Result :"<< my_string << endl;

  return 0;
}
*/
