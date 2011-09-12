
#ifndef HTTP_CURL_H
#define HTTP_CURL_H 1

#include "curl-duplex.h"
#include "sha1.h"
#include <stdint.h>

struct mem_url {
   int error_code;
   size_t size;
   size_t pos;
   char *data;
};

int curl_download_path(const char *url, const char *lpath, unsigned char digest[SHA1_DIGEST_LENGTH], int probe_hosts, int compressed); /* Locked */
int curl_download_path_nocache(const char *url, const char *lpath, unsigned char digest[SHA1_DIGEST_LENGTH], int probe_hosts, int compressed); /* Locked */
int curl_download_stream(const char *url, FILE *f, unsigned char digest[SHA1_DIGEST_LENGTH], int probe_hosts, int compressed); /* Locked */
int curl_download_stream_nocache(const char *url, FILE *f, unsigned char digest[SHA1_DIGEST_LENGTH], int probe_hosts, int compressed); /* Locked */
int curl_download_mem(const char *url, struct mem_url *p_mem_url, int probe_hosts, int compressed); /* Locked */
int curl_download_mem_nocache(const char *url, struct mem_url *p_mem_url, int probe_hosts, int compressed); /* Locked */

int curl_download_tee(const char *url, FILE *f, unsigned char digest[SHA1_DIGEST_LENGTH],
                      int (*cb_size)(size_t size_compr, void *user_data), 
                      int (*cb_data_compr)(const void *buf, const size_t buf_size, void *user_data),
                      void *user_data);
                      
void curl_download_parallel(unsigned num, char **url, char **path, unsigned char *digest,
                            int *result); 

int curl_set_host_chain(const char *host_list); /* Locked */
void curl_get_host_info(int *num, int *current, char ***hosts, int **rtt); /* Locked */
void curl_get_proxy_info(int *num, char **current, int *current_lb, char ***proxies, int *num_lb, int **lb_starts); /* Locked */
void curl_switch_proxy_lbgroup(); /* Locked */
void curl_probe_hosts(); /* thread safe, but will override intermediate set host chain after probing */
void curl_switch_host();
void curl_switch_host_locked();
void curl_set_proxy_chain(const char *proxy_list); /* Locked */
void curl_set_timeout(unsigned seconds, unsigned seconds_direct); /* Locked */
void curl_get_timeout(unsigned *seconds, unsigned *seconds_direct); /* Locked */
int64_t curl_get_allbytes(); /* Locked */
int64_t curl_get_alltime(); /* Locked */
void curl_rebalance();

#endif

