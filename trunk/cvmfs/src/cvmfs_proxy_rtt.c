/*
 *  cvmfs-proxy-rtt.c
 *  
 */

#include "config.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/select.h>

#include "curl-duplex.h" 
#include "smalloc.h" 


void usage() {
   printf("Returns a set of proxies, ordered by round trip time (RTT)\n");
   printf("Usage: cvmfs_proxy_rtt http://$repository [proxy1 proxy2 ...]\n");
}


static size_t curl_callback(__attribute__((unused)) void *ptr, size_t size, size_t nmemb, void *which)
{
   printf("%s\n", (char *)which);
   
   return size*nmemb;
} 

int main(int argc, char **argv)
{
   if ((argc <= 1) || (strcmp(argv[1], "--help") == 0) || (strcmp(argv[1], "-h") == 0))
   {
      usage();
      return 1;
   }
   if (argc == 2) return 0;
   
   int num = argc-2;
   char *url = argv[1]; 
   char **proxies = &argv[2];
   int i;
   CURL **hcurl;
   CURLM *curl_group;
   
   curl_global_init(CURL_GLOBAL_ALL);
   curl_group = curl_multi_init();
   hcurl = smalloc(num*sizeof(CURL *));
   for (i = 0; i < num; ++i)
   {
      hcurl[i] = curl_easy_init();
      curl_easy_setopt(hcurl[i], CURLOPT_URL, url);
      curl_easy_setopt(hcurl[i], CURLOPT_FAILONERROR, 1);
      curl_easy_setopt(hcurl[i], CURLOPT_WRITEFUNCTION, curl_callback);
      curl_easy_setopt(hcurl[i], CURLOPT_WRITEDATA, proxies[i]);
      curl_easy_setopt(hcurl[i], CURLOPT_PROXY, proxies[i]);
      curl_multi_add_handle(curl_group, hcurl[i]);
   }
   
   
   int still_running;

   while (CURLM_CALL_MULTI_PERFORM == curl_multi_perform(curl_group, &still_running));
   while (still_running) {
      struct timeval timeout;
      int rc; /* select() return code */
      fd_set fdread;
      fd_set fdwrite;
      fd_set fdexcep;
      int maxfd;

      FD_ZERO(&fdread);
      FD_ZERO(&fdwrite);
      FD_ZERO(&fdexcep);

      /* set a suitable timeout to play around with */
      timeout.tv_sec = 1;
      timeout.tv_usec = 0;
 
      /* get file descriptors from the transfers */
      curl_multi_fdset(curl_group, &fdread, &fdwrite, &fdexcep, &maxfd);
 
      /* In a real-world program you OF COURSE check the return code of the
      function calls, *and* you make sure that maxfd is bigger than -1
      so that the call to select() below makes sense! */
 
      rc = select(maxfd+1, &fdread, &fdwrite, &fdexcep, &timeout);

      switch(rc) {
         case -1:
            /* select error */
            break;
         case 0:
            // timeout
         default:
            /* timeout or readable/writable sockets */
            while(CURLM_CALL_MULTI_PERFORM == curl_multi_perform(curl_group, &still_running));
            break;
      }
   }
   
   return 0;
}


