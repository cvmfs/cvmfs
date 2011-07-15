/**
 * \file http_curl.c
 * 
 * We use the libcurl to implement our download logic.
 * This module provides functions to download files via HTTP 
 * in different flavours.
 *
 * While downloading, we do decompression and SHA1 calculation.
 * SHA1 is calculated of compressed versions of files.
 *
 * We also implement a proxy chain, organized as ring buffer.
 * This means: as soon as we cannot connect to a proxy, 
 * the ring is probed for another one.
 *
 * The functions are thread-safe, i.e. things are locked by an
 * internal mutex.
 *
 * Developed by Jakob Blomer, 2009 at CERN
 * jakob.blomer@cern.ch
 */

#define _FILE_OFFSET_BITS 64

#include "config.h"
#include "http_curl.h"

#include <stdlib.h>
#include <stdio.h>
#include <stdint.h>
#include <alloca.h>
#include <string.h>
#include <ctype.h>
#include <fcntl.h>
#include "curl-duplex.h"
#include <pthread.h>
#include <sys/time.h>
#include <errno.h>
#include <assert.h>

#include "sha1.h"
#include "compression.h"
#include "debug.h"
#include "smalloc.h"

#define HEADER_ERROR_UNSPECIFIED 1
#define HEADER_ERROR_PROXY 2

/**
 * A pointer to such a structure is passed to libcurl-callbacks.
 */
struct curl_progress {
   int header_error;
   FILE *f;
   sha1_context_t sha1_context;
   int decompress;
   int z_ret;
   z_stream strm;
   unsigned char out[Z_CHUNK];
};
typedef struct curl_progress curl_progress_t;

struct curl_pool_progress {
   FILE *f;
   sha1_context_t sha1_context;
};
typedef struct curl_pool_progress curl_pool_progress_t;

struct curl_tee_progress {
   curl_progress_t progress;
   int (*cb_size)(size_t size_compr, void *user_data);
   int (*cb_data_compr)(const void *buf, const size_t buf_size, void *user_data);
   void *user_data;
};
typedef struct curl_tee_progress curl_tee_progress_t;


static CURL *curl_default = NULL; ///< download handle for a common file.
static CURL *curl_mem = NULL; ///< download handle to put things into memory.
static CURL *curl_tee = NULL; ///< download handle for storing compressed and uncompressed data.
static CURLM *curl_multi = NULL; ///< download handle for multiple parallel downloads
static CURL **curl_pool = NULL; ///< pool of handles for parallel downloads
static unsigned curl_pool_size = 0;
static struct curl_slist *slist = NULL;
static struct curl_slist *slist_nocache = NULL; ///< curl options to circumvent any proxy servers.

static char **curl_host_chain = NULL; ///< list of reverse proxy servers
static int *curl_host_rtt = NULL; ///< created be set_host_chain, filled by probe_hosts.  In time to get .cvmfschecksum ms.  -1 is unprobed, -2 is error
static int curl_host_num = 0;
static int curl_host_current = 0;

static char **curl_proxy_chain = NULL; ///< list of forward proxy lb groups
static char **curl_proxy_lbgroup = NULL; ///< list of forward proxy servers in current LB group, first one: active, last ones: burned
static int curl_proxy_groups = 0;
static int curl_proxy_group_current = 0;
static int curl_proxy_lb_num = 0;
static int curl_proxy_lb_burned = 0;
static int curl_proxy_inuse = 0;
static int curl_proxy_num = 0;

static unsigned curl_timeout = 0; ///< timeout for network operations (not DNS)
static unsigned curl_timeout_direct = 0; ///< timeout for network operations without proxy (not DNS)
uint64_t curl_no_download = 0; ///< Counts number of probing downloads
const uint64_t CURL_PROBE_FREQ = 1000; ///< probe hosts every now and then


static pthread_mutex_t mutex_curl;
static pthread_attr_t pthread_probe_attr;


/* Helper functions */

/**
 * Don't let any fancy stuff kill the URL, except for ':' and '/', 
 * which should keep their meaning.
 */
static char *escape(const char *url) {
   int len = strlen(url);
   char *escaped = scalloc(3*len+1, 1);
   
   int i, pos = 0;
   for (i = 0; i < len; ++i) {
      if (((url[i] >= '0') && (url[i] <= '9')) ||
          ((url[i] >= 'A') && (url[i] <= 'Z')) ||
          ((url[i] >= 'a') && (url[i] <= 'z')) ||
          (url[i] == '/') || (url[i] == ':') || (url[i] == '.') || 
          (url[i] == '+') || (url[i] == '-'))
      {
         escaped[pos++] = url[i];
      } else {
         escaped[pos++] = '%';
         escaped[pos++] = (url[i] / 16) + ((url[i] / 16 <= 9) ? '0' : 'A'-10);
         escaped[pos++] = (url[i] % 16) + ((url[i] % 16 <= 9) ? '0' : 'A'-10); 
      }
   }
   escaped[pos] = 0;
   pmesg(D_CURL, "escaped %s to %s", url, escaped);
   
   return escaped;
}


static int freset(FILE *f) 
{
   if (fflush(f) != 0) /* better fpurge? */
      return 0;
   if (ftruncate(fileno(f), 0) != 0)
      return 0;
   rewind(f);
   return 1;
}


/**
 * Jumps to the next host in the ring of reverse proxy servers.
 */
void curl_switch_host() 
{
   if (curl_host_num > 1) {
      curl_host_current = (curl_host_current+1) % curl_host_num; 
   } else {
      curl_host_current = 0;
   }
}

void curl_switch_host_locked() {
   pthread_mutex_lock(&mutex_curl);
   curl_switch_host();
   pthread_mutex_unlock(&mutex_curl);
}


/**
 * Concatenates hostname and url.  Caller has to free result.
 */
static char *cat_url(const char *url) {
   int len = strlen(curl_host_chain[curl_host_current]) + strlen(url) + 1;
   char *result = smalloc(len);
   snprintf(result, len, "%s%s", curl_host_chain[curl_host_current], url);
   return result;
}



/**
 * Jumps to the next proxy in the ring of forward proxy servers.
 * Selects one randomly from a load-balancing group.
 */
static void curl_switch_proxy() 
{
   int i;
   
   /* Remove LB group if all proxies are burned */
   if (curl_proxy_lbgroup && (curl_proxy_lb_num == curl_proxy_lb_burned))
   {
      for (i = 0; i < curl_proxy_lb_num; ++i)
         free(curl_proxy_lbgroup[i]);
      free(curl_proxy_lbgroup);
      curl_proxy_lbgroup = NULL;
      curl_proxy_lb_num = curl_proxy_lb_burned = 0;
   }
   
   
   /* If there is no LB group, switch to next group in the chain */
   if (!curl_proxy_lbgroup) {
      char *group;
      if (curl_proxy_groups == 0)
      {
         group = "";
         curl_proxy_group_current = 0;
      }
      else if (curl_proxy_groups == 1) {
         group = curl_proxy_chain[0];
         curl_proxy_group_current = 0;
      }
      else {
         group = curl_proxy_chain[++curl_proxy_group_current % curl_proxy_groups];
      }
      
      pmesg(D_CURL, "applying new lb group %s", group);
      
      /* Copy lb-list into array */
      curl_proxy_lb_num = 1;
      const char *scan;
      for (scan = group; *scan != '\0'; scan++) {
         if (*scan == '|')
            curl_proxy_lb_num++;
      }
      
      curl_proxy_lbgroup = smalloc(curl_proxy_lb_num * sizeof(char *));
      scan = group;
      for (i = 0; i < curl_proxy_lb_num; ++i)
      {
         int s = 0;
         for (; (*scan != '|') && (*scan != '\0'); ++scan)
            s++;
         curl_proxy_lbgroup[i] = smalloc(s+1);
         curl_proxy_lbgroup[i][s] = '\0';
         if (s)
            memcpy(curl_proxy_lbgroup[i], scan-s, s);
         pmesg(D_CURL, "set lb member %d to %s", i, curl_proxy_lbgroup[i]);
         scan++;
      }
   }
   
   char *swap_tmp;
   char *proxy;
   
   /* Move active proxy to the back */
   if (curl_proxy_lb_burned) {
      swap_tmp = curl_proxy_lbgroup[0];
      curl_proxy_lbgroup[0] = curl_proxy_lbgroup[curl_proxy_lb_num - curl_proxy_lb_burned];
      curl_proxy_lbgroup[curl_proxy_lb_num - curl_proxy_lb_burned] = swap_tmp;
   }
   curl_proxy_lb_burned++;
   
   /* Select new one */
   int select = 0;
   if ((curl_proxy_lb_num - curl_proxy_lb_burned) > 0) {
      select = random() % (curl_proxy_lb_num - curl_proxy_lb_burned + 1);
      
      /* Move selected proxy to front */
      if (select) {
         swap_tmp = curl_proxy_lbgroup[select];
         curl_proxy_lbgroup[select] = curl_proxy_lbgroup[0];
         curl_proxy_lbgroup[0] = swap_tmp;
      }
   }
   
   proxy = curl_proxy_lbgroup[0];
   
   /* Now switch */
   pmesg(D_CURL, "switching to forward proxy %s (number %d), %d remaining in group", 
            proxy, select, curl_proxy_lb_num - curl_proxy_lb_burned);
   if (strcmp(proxy, "DIRECT") == 0) proxy = "";
   
   unsigned active_timeout;
   if (strcmp(proxy, "") == 0) {
      curl_proxy_inuse = 0;
      active_timeout = curl_timeout_direct;
   } else {
      curl_proxy_inuse = 1;
      active_timeout = curl_timeout;
   }
   
   curl_easy_setopt(curl_default, CURLOPT_PROXY, proxy);
   curl_easy_setopt(curl_mem, CURLOPT_PROXY, proxy);
   curl_easy_setopt(curl_tee, CURLOPT_PROXY, proxy);
   curl_easy_setopt(curl_default, CURLOPT_CONNECTTIMEOUT, active_timeout);
   curl_easy_setopt(curl_default, CURLOPT_LOW_SPEED_TIME, active_timeout);
   curl_easy_setopt(curl_mem, CURLOPT_CONNECTTIMEOUT, active_timeout);
   curl_easy_setopt(curl_mem, CURLOPT_LOW_SPEED_TIME, active_timeout);
   curl_easy_setopt(curl_tee, CURLOPT_CONNECTTIMEOUT, active_timeout);
   curl_easy_setopt(curl_tee, CURLOPT_LOW_SPEED_TIME, active_timeout);
   
   for (i = 0; i < curl_pool_size; ++i) {
      curl_easy_setopt(curl_pool[i], CURLOPT_PROXY, proxy);
      curl_easy_setopt(curl_pool[i], CURLOPT_CONNECTTIMEOUT, active_timeout);
      curl_easy_setopt(curl_pool[i], CURLOPT_LOW_SPEED_TIME, active_timeout);

   }
}

void curl_switch_proxy_lbgroup() {
   pthread_mutex_lock(&mutex_curl);
   curl_proxy_lb_burned = curl_proxy_lb_num;
   curl_switch_proxy();
   pthread_mutex_unlock(&mutex_curl);
}

static void curl_reset_lbgroup() {
   curl_proxy_lb_burned = 1;
}

/* Curl Callbacks */

/*static int curl_debug(CURL *handle, curl_infotype type, char *msg, size_t size, 
                      void *data __attribute__((unused))) 
{
   if (type == CURLINFO_DATA_IN) {
      pmesg(D_CURL, "CURLDEBUG: (data) %u Bytes", size);
   } else {
      char *zero_msg = smalloc(size+1);
      zero_msg[size] = '\0';
      memcpy(zero_msg, msg, size);
      pmesg(D_CURL, "CURLDEBUG: %s", zero_msg);
   }
   return 0;
}*/

/**
 * Checks for http status code errors and sets p_progress->header_error, if needed.
 */
static int check_http_return(void *ptr, size_t num_bytes, int *error_type) {
   char *line = (char *)alloca(num_bytes+1);
   memcpy(line, ptr, num_bytes);
   line[num_bytes] = '\0';
   
   if (strstr(line, "HTTP/1.") == line) {
      char *itr = line+7;
      while (*itr != '\0') {
         if (*itr == ' ') {
            itr++;
            break;
         }
         itr++;
      }
      
      if (*itr == '2') {
         *error_type = 0;
         return 1;
      } else {
         pmesg(D_CURL, "http status code error: %s", line);
         char err_code[4];
         err_code[0] = *itr;
         err_code[3] = '\0';
         if ((*(itr+1) != '\0') && (*(itr+2) != '\0')) {
            err_code[1] = *(itr+1);
            err_code[2] = *(itr+2);
            if ((strcmp(err_code, "407") == 0) || (strcmp(err_code, "502") == 0) ||
                (strcmp(err_code, "504") == 0) || 
                ((strcmp(err_code, "403") == 0) && (curl_proxy_inuse)))
            {
               *error_type = HEADER_ERROR_PROXY;
            } else {
               *error_type = HEADER_ERROR_UNSPECIFIED;
            }
         } else {
            *error_type = HEADER_ERROR_UNSPECIFIED;
         }
         return 0;
      }
   }
   
   return 1;
}


/**
 * libcurl "more data"-callback for common files.
 */
static size_t curl_callback_progress(void *ptr, size_t size, size_t nmemb, void *p_progress) 
{
   curl_progress_t *progress = (curl_progress_t *) p_progress;
   size_t num_bytes = size*nmemb;
   
   if (num_bytes == 0)
      return 0;
      
   sha1_update(&progress->sha1_context, (unsigned char *)ptr, num_bytes);
   
   if (progress->decompress) {
      if ((progress->z_ret = decompress_strm_file(&progress->strm, progress->f, ptr, num_bytes)) < 0)
         return 0;
   } else {
      if (fwrite(ptr, 1, num_bytes, progress->f) != num_bytes)
         return 0;
   }

   return num_bytes;
}


/**
 * libcurl "more data"-callback for parallel downloads.
 */
static size_t curl_callback_pool_progress(void *ptr, size_t size, size_t nmemb, void *p_pool_progress) 
{
   curl_pool_progress_t *pool_progress = (curl_pool_progress_t *) p_pool_progress;
   size_t num_bytes = size*nmemb;
   
   if (num_bytes == 0)
      return 0;
      
   sha1_update(&pool_progress->sha1_context, (unsigned char *)ptr, num_bytes);
   
   if (fwrite(ptr, 1, num_bytes, pool_progress->f) != num_bytes)
      return 0;

   return num_bytes;
}



/**
 * libcurl header callback.  Checks for specific errors.
 */
static size_t curl_callback_header(void *ptr, size_t size, size_t nmemb, void *p_progress) 
{
   size_t num_bytes = size*nmemb;
   
   if (check_http_return(ptr, num_bytes, &(((curl_progress_t *)p_progress)->header_error))) 
      return num_bytes;
   else 
      return 0;
}

/**
 * libcurl header callback for tee.  Checks for specific errors and runs size-callback.
 */
static size_t curl_callback_tee_header(void *ptr, size_t size, size_t nmemb, 
                                       void *p_tee_progress) 
{
   size_t num_bytes = size*nmemb;
   curl_tee_progress_t *tee_progress = (curl_tee_progress_t *)p_tee_progress; 
   
   if (!check_http_return(ptr, num_bytes, &(tee_progress->progress.header_error)))
      return 0;
   
   char *line = (char *)alloca(num_bytes+1);
   memcpy(line, ptr, num_bytes);
   line[num_bytes] = '\0';
    
   unsigned i;
   for (i = 0; i < num_bytes; ++i)
      line[i] = toupper(line[i]);
    
   if (strstr(line, "CONTENT-LENGTH:") == line) {
      char *tmp = (char *)alloca(num_bytes+1);
      unsigned long length = 0;
      sscanf(line, "%s %lu", tmp, &length);
      if (!tee_progress->cb_size(length, tee_progress->user_data))
         return 0;
   }
    
   return num_bytes;
}


/**
 * libcurl "more data"-callback for tee.  Decompresses, updates SHA1, 
 * runs more-data call-back on compressed stream.
 */
static size_t curl_callback_tee_progress(void *ptr, size_t size, size_t nmemb, 
                                         void *p_tee_progress)
{
   curl_tee_progress_t *tee_progress = (curl_tee_progress_t *)p_tee_progress;
   size_t num_bytes = size*nmemb;
   
   if (num_bytes == 0)
      return 0;
      
   sha1_update(&tee_progress->progress.sha1_context, (unsigned char *)ptr, num_bytes);
   
   if ((tee_progress->progress.z_ret = decompress_strm_file(&tee_progress->progress.strm, 
         tee_progress->progress.f, ptr, num_bytes)) < 0)
   {
      return 0;
   }
      
   if (!tee_progress->cb_data_compr(ptr, num_bytes, tee_progress->user_data))
      return 0;

   return num_bytes;
}


/**
 * libcurl "more data"-callback to put into memory.
 */
static size_t curl_callback_mem_progress(void *ptr, size_t size, size_t nmemb, void *p_mem_url) 
{
   //pmesg(D_CURL, "CALLBACK MEM PROGESS");
   struct mem_url *mem_url = (struct mem_url *) p_mem_url;
    
   size_t num_bytes = size*nmemb;
   if (num_bytes == 0)
     return 0;
   if (mem_url->pos + num_bytes > mem_url->size)
     return 0;
       
   memcpy(mem_url->data + mem_url->pos, ptr, num_bytes);
   mem_url->pos += num_bytes; 
   return num_bytes;
}



/**
 * libcurl header callback for download into memory.
 */
static size_t curl_callback_mem_header(void *ptr, size_t size, size_t nmemb, void *p_mem_url) 
{
   //pmesg(D_CURL, "CALLBACK MEM HEADER");
   size_t num_bytes = size*nmemb;
   
   if (!check_http_return(ptr, num_bytes, &(((struct mem_url *)p_mem_url)->error_code)))
      return 0;
   
   char *line = (char *)alloca(num_bytes+1);
   memcpy(line, ptr, num_bytes);
   line[num_bytes] = '\0';
    
   unsigned i;
   for (i = 0; i < num_bytes; ++i)
      line[i] = toupper(line[i]);
    
   if (strstr(line, "CONTENT-LENGTH:") == line) {
      char *tmp = (char *)alloca(num_bytes+1);
      unsigned long length = 0;
      sscanf(line, "%s %lu", tmp, &length);
      struct mem_url *mem_url = (struct mem_url *)p_mem_url;
      if (length > 0)
         mem_url->data = smalloc(length);
      else
         mem_url->data = NULL;
      mem_url->size = length;
   }
    
   return num_bytes;
}


/**
 * Checks a curl error code for a dead proxy server.
 * 
 * \return True, if this is a proxy-dead error code, false otherwise.
 */
static int curl_proxy_error(int curl_error, int header_error)
{
   if (curl_error == CURLE_OK)
      return 0;
   if (curl_error == CURLE_OPERATION_TIMEDOUT)
      return 1;
   return (header_error == HEADER_ERROR_PROXY) ||
          (curl_error == CURLE_COULDNT_RESOLVE_PROXY) || 
          (curl_error == CURLE_COULDNT_RESOLVE_HOST) || 
          (curl_error == CURLE_COULDNT_CONNECT) ||
          (curl_error == -CURLE_COULDNT_RESOLVE_PROXY) || 
          (curl_error == -CURLE_COULDNT_RESOLVE_HOST) || 
          (curl_error == -CURLE_COULDNT_CONNECT);
}


/**
 * Checks a curl error code for a dead host.
 * 
 * \return True, if this is a host-dead error code, false otherwise.
 */

static int curl_host_error(int curl_error)
{
   if (curl_error == CURLE_OK)
      return 0;
   return (curl_error == CURLE_URL_MALFORMAT) ||
          (curl_error == CURLE_COULDNT_RESOLVE_HOST) || 
          (curl_error == CURLE_COULDNT_CONNECT) ||
          (curl_error == CURLE_OPERATION_TIMEDOUT) ||
          (curl_error == -CURLE_URL_MALFORMAT) ||
          (curl_error == -CURLE_COULDNT_RESOLVE_HOST) || 
          (curl_error == -CURLE_COULDNT_CONNECT) ||
          (curl_error == -CURLE_OPERATION_TIMEDOUT);
}



/**
 * Thread wrapper for probing hosts, we don't want to block while probing.
 */
static void *tf_probe_helper(void *data __attribute__((unused))) {
   curl_probe_hosts();
   return NULL;
}


/**
 * Same as curl_probe_hosts, but covered in a separate thread.
 */
static void curl_probe_hosts_async() 
{
   pmesg(D_CURL, "reprobing hosts after %llu downloads", curl_no_download);
   
   pthread_t thread_probe;
   int retval;
   
   retval = pthread_create(&thread_probe, &pthread_probe_attr, tf_probe_helper, NULL);
   assert(retval == 0);
}

static void inc_downloads() {
   /* Only spawn thread when running in daemonized mode, otherwise thread might get killed */
   if (getppid() == 1)
   {
      if (((curl_no_download % CURL_PROBE_FREQ) == 0) && !curl_proxy_inuse)
         curl_probe_hosts_async();
      curl_no_download++;
   }
}



/**
 * Downloads from a url into file pointer.  Calculates SHA1-digest on the fly.
 *
 * \return curl error code or zlib error code.  CURLE_OK on success.
 */
static int curl_download_stream_unprotected(const char *url, FILE *f, unsigned char digest[SHA1_DIGEST_LENGTH], int compressed) 
{
   curl_progress_t progress;
   progress.header_error = 0;
   progress.f = f;
   sha1_init(&progress.sha1_context);
   progress.decompress = compressed;
   if (progress.decompress) {
      if ((progress.z_ret = decompress_strm_init(&progress.strm)) != Z_OK)
         return progress.z_ret;
   }
   
   curl_easy_setopt(curl_default, CURLOPT_WRITEHEADER, (void *)&progress);
   curl_easy_setopt(curl_default, CURLOPT_WRITEDATA, (void *)&progress);
   char *url_escaped = escape(url);
   curl_easy_setopt(curl_default, CURLOPT_URL, url_escaped);
   
   int retries = 0;
   int result = curl_easy_perform(curl_default);
   while (curl_proxy_error(result, progress.header_error) && (retries < curl_proxy_num)) 
   {
      pmesg(D_CURL, "download error %d, switching proxy", result);
      curl_switch_proxy();
      retries++;
      sha1_init(&progress.sha1_context);
      if (progress.decompress) {
         if ((progress.z_ret = decompress_strm_init(&progress.strm)) != Z_OK)
            break;
      }
      if (!freset(f))
         break;
      result = curl_easy_perform(curl_default);
   }
   curl_reset_lbgroup();
   
   free(url_escaped);
   fflush(f);
   if (progress.decompress) {
      decompress_strm_fini(&progress.strm);
      /* Z_STREAM_END */
      if (progress.z_ret != 1)
         return Z_DATA_ERROR;
   }
   sha1_final(digest, &progress.sha1_context);
   return result;
}





/**
 * Downloads stream and probes hosts
 */
static int curl_download_stream_probing(const char *url, FILE *f, unsigned char digest[SHA1_DIGEST_LENGTH], int compressed) {
   int result;
   int attempts = 1;
   char *effective_url = cat_url(url);
   
   inc_downloads();
   
   result = curl_download_stream_unprotected(effective_url, f, digest, compressed);
   //pmesg(D_CURL, "CURL IS %d, %d", result, CURLE_URL_MALFORMAT);
   while ((curl_host_error(result) != 0) && (attempts < curl_host_num)) {
      pmesg(D_CURL, "switching from host %s to host %s", curl_host_chain[curl_host_current], 
            curl_host_chain[(curl_host_current+1) % curl_host_num]);
      curl_switch_host();
      free(effective_url);
      if (!freset(f))
         return result;
      effective_url = cat_url(url);
      result = curl_download_stream_unprotected(effective_url, f, digest, compressed);
      attempts++;
   }
   free(effective_url);
   /* Make a full turn if necessarry */
   if (result != CURLE_OK) curl_switch_host(); 
   return result;
}


/**
 * Protects curl_download_stream_unprotected with internal mutex.
 */
int curl_download_stream(const char *url, FILE *f, unsigned char digest[SHA1_DIGEST_LENGTH], int probe_hosts, int compressed)
{
   pthread_mutex_lock(&mutex_curl);
   
   int result;
   if (probe_hosts) {
      result = curl_download_stream_probing(url, f, digest, compressed);
   } else {
      result = curl_download_stream_unprotected(url, f, digest, compressed);
   }
   
   pthread_mutex_unlock(&mutex_curl);
   return result;
}


/**
 * Like curl_download_stream(), but send no-cache header to circumvent proxy servers.
 */
int curl_download_stream_nocache(const char *url, FILE *f, unsigned char digest[SHA1_DIGEST_LENGTH], int probe_hosts, int compressed)
{
   int result;
   pthread_mutex_lock(&mutex_curl);
   
   curl_easy_setopt(curl_default, CURLOPT_HTTPHEADER, slist_nocache);
   if (probe_hosts) {
      result = curl_download_stream_probing(url, f, digest, compressed);
   } else {
      result = curl_download_stream_unprotected(url, f, digest, compressed);
   }
   curl_easy_setopt(curl_default, CURLOPT_HTTPHEADER, slist);
   
   pthread_mutex_unlock(&mutex_curl);
   return result;
}




/**
 * Downloads from a url into memory.
 *
 * \return curl error code.  CURLE_OK on success.
 */
static int curl_download_mem_unprotected(const char *url, struct mem_url *p_mem_url, int compressed) 
{
   p_mem_url->size = 0;
   p_mem_url->pos = 0;
   p_mem_url->error_code = 0;
   p_mem_url->data = NULL;
   curl_easy_setopt(curl_mem, CURLOPT_WRITEHEADER, (void *)p_mem_url);
   curl_easy_setopt(curl_mem, CURLOPT_WRITEDATA, (void *)p_mem_url);
   char *url_escaped = escape(url);
   curl_easy_setopt(curl_mem, CURLOPT_URL, url_escaped);
   
   int retries = 0;
   int result = curl_easy_perform(curl_mem);
   while (curl_proxy_error(result, p_mem_url->error_code) && (retries < curl_proxy_num)) 
   {
      pmesg(D_CURL, "download error %d, switching proxy", result);
      curl_switch_proxy();
      retries++;
      if (p_mem_url->data) {
         free(p_mem_url->data);
         p_mem_url->size = 0;
         p_mem_url->pos = 0;
         p_mem_url->error_code = 0;
         p_mem_url->data = NULL;
      }
      result = curl_easy_perform(curl_mem);
   }
   curl_reset_lbgroup();
   
   free(url_escaped);
   if ((result != CURLE_OK) && p_mem_url->data)
   {
      free(p_mem_url->data);
      p_mem_url->data = NULL;
      p_mem_url->size = 0;
   }
   
   if (compressed && (result == CURLE_OK)) {
      void *buf;
      size_t size;
      if ((result = decompress_mem(p_mem_url->data, p_mem_url->size, 
                                   &buf, &size)) == 0) 
      {
         free(p_mem_url->data);
         p_mem_url->data = buf;
         p_mem_url->size = size;
      } 
   }
   
   return result;
}



/**
 * Downloads into memory and probes hosts
 */
static int curl_download_mem_probing(const char *url, struct mem_url *p_mem_url, int compressed) {
   int result;
   int attempts = 1;
   char *effective_url = cat_url(url);
   
   inc_downloads();
   
   result = curl_download_mem_unprotected(effective_url, p_mem_url, compressed);
   //pmesg(D_CURL, "CURL IS %d, %d", result, CURLE_URL_MALFORMAT);
   while ((curl_host_error(result) != 0) && (attempts < curl_host_num)) {
      pmesg(D_CURL, "switching from host %s to host %s", curl_host_chain[curl_host_current], 
            curl_host_chain[(curl_host_current+1) % curl_host_num]);
      curl_switch_host();
      free(effective_url);
      effective_url = cat_url(url);
      result = curl_download_mem_unprotected(effective_url, p_mem_url, compressed);
      attempts++;
   }
   free(effective_url);
   /* Make a full turn if necessarry */
   if (result != CURLE_OK) curl_switch_host(); 
   return result;
}


/**
 * Protects curl_download_mem_unprotected with internal mutex.
 */
int curl_download_mem(const char *url, struct mem_url *p_mem_url, int probe_hosts, int compressed)
{
   pthread_mutex_lock(&mutex_curl);
   
   int result;
   if (probe_hosts) {
      result = curl_download_mem_probing(url, p_mem_url, compressed);
   } else {
      result = curl_download_mem_unprotected(url, p_mem_url, compressed);
   }
   
   pthread_mutex_unlock(&mutex_curl);
   return result;
}


/**
 * Like curl_download_mem(), but send no-cache header to circumvent proxy servers.
 */
int curl_download_mem_nocache(const char *url, struct mem_url *p_mem_url, int probe_hosts, int compressed)
{
   int result;
   pthread_mutex_lock(&mutex_curl);
   
   curl_easy_setopt(curl_mem, CURLOPT_HTTPHEADER, slist_nocache);
   if (probe_hosts) {
      result = curl_download_mem_probing(url, p_mem_url, compressed);
   } else {
      result = curl_download_mem_unprotected(url, p_mem_url, compressed);
   }
   curl_easy_setopt(curl_mem, CURLOPT_HTTPHEADER, slist);
   
   pthread_mutex_unlock(&mutex_curl);
   return result;
}



/**
 * Wrapper for curl_download_stream().
 */
int curl_download_path(const char *url, const char *lpath, unsigned char digest[SHA1_DIGEST_LENGTH], int probe_hosts, int compressed) 
{
   pmesg(D_CURL, "download path %s into %s", url, lpath);
   FILE *f = fopen(lpath, "w+");
   if (f) {
      int result = curl_download_stream(url, f, digest, probe_hosts, compressed);
      fclose(f);
      return result;
   } else {
      return -errno;
   }
}


/**
 * Wrapper for curl_download_stream_nocache().
 */
int curl_download_path_nocache(const char *url, const char *lpath, unsigned char digest[SHA1_DIGEST_LENGTH], int probe_hosts, int compressed) 
{
   FILE *f = fopen(lpath, "w+");
   if (f) {
      int result = curl_download_stream_nocache(url, f, digest, probe_hosts, compressed);
      fclose(f);
      return result;
   } else {
      return -errno;
   }
}


/**
 * Similar to curl_download_stream.  Doesn't do host fail-over.  Uses HTTP cache. Allows for
 * processing of the compressed stream via call-backs cb_size and cb_data_compr.
 * The first call-back is executed once, the data call-back may be executed multiple
 * times.
 */
int curl_download_tee(const char *url, FILE *f, unsigned char digest[SHA1_DIGEST_LENGTH],
                      int (*cb_size)(size_t size_compr, void *user_data), 
                      int (*cb_data_compr)(const void *buf, const size_t buf_size, void *user_data),
                      void *user_data)
{
   inc_downloads();
   
   curl_tee_progress_t tee_progress;
   tee_progress.cb_size = cb_size;
   tee_progress.cb_data_compr = cb_data_compr;
   tee_progress.user_data = user_data;
   tee_progress.progress.header_error = 0;
   tee_progress.progress.f = f;
   sha1_init(&tee_progress.progress.sha1_context);
   tee_progress.progress.decompress = 1;
   if ((tee_progress.progress.z_ret = decompress_strm_init(&tee_progress.progress.strm)) != Z_OK)
      return tee_progress.progress.z_ret;
   
   char *url_complete = cat_url(url);
   char *url_escaped = escape(url_complete);
   
   pthread_mutex_lock(&mutex_curl);
   
   curl_easy_setopt(curl_tee, CURLOPT_WRITEHEADER, (void *)&tee_progress);
   curl_easy_setopt(curl_tee, CURLOPT_WRITEDATA, (void *)&tee_progress);
   curl_easy_setopt(curl_tee, CURLOPT_URL, url_escaped);
   
   int retries = 0;
   int result = curl_easy_perform(curl_tee);
   while (curl_proxy_error(result, tee_progress.progress.header_error) && 
          (retries < curl_proxy_num)) 
   {
      pmesg(D_CURL, "download error %d, switching proxy", result);
      curl_switch_proxy();
      retries++;
      sha1_init(&tee_progress.progress.sha1_context);
      if ((tee_progress.progress.z_ret = decompress_strm_init(&tee_progress.progress.strm)) != Z_OK)
         break;
      if (!freset(f))
         break;
      result = curl_easy_perform(curl_tee);
   }
   curl_reset_lbgroup();
   
   pthread_mutex_unlock(&mutex_curl); 
   
   free(url_escaped);
   free(url_complete);
   fflush(f);
   decompress_strm_fini(&tee_progress.progress.strm);
   /* Z_STREAM_END */
   if (tee_progress.progress.z_ret != 1)
      return Z_DATA_ERROR;
   sha1_final(digest, &tee_progress.progress.sha1_context);
   
   return result;
}


/**
 * Gets all the information about hosts. 
 * The arrays hosts and rtt are created and have to be freed by the caller.
 */
void curl_get_host_info(int *num, int *current, char ***hosts, int **rtt) {
   pthread_mutex_lock(&mutex_curl);
   *num = curl_host_num;
   *current = curl_host_current;
   *rtt = smalloc(curl_host_num * sizeof(int));
   *hosts = smalloc(curl_host_num * sizeof(char *));
   memcpy(*rtt, curl_host_rtt, curl_host_num*sizeof(int));
   int i;
   for (i = 0; i < curl_host_num; ++i) 
   {
      int len = strlen(curl_host_chain[i])+1;
      (*hosts)[i] = smalloc(len);
      strncpy((*hosts)[i], curl_host_chain[i], len);
   }
   pthread_mutex_unlock(&mutex_curl);
}


/**
 * Gets all the information about hosts. 
 * current, proxies and lb_starts are allocated and have to be freed by the caller.
 */
void curl_get_proxy_info(int *num, char **current, int *current_lb, char ***proxies, 
                         int *num_lb, int **lb_starts)
{
   pthread_mutex_lock(&mutex_curl);

   *num = curl_proxy_num;
   *num_lb = curl_proxy_groups;
   
   if (*num) {
      *proxies = smalloc(*num * sizeof(char *));
      *lb_starts = smalloc(*num_lb * sizeof(int));
      
      /* Active proxy */
      *current_lb = curl_proxy_group_current % curl_proxy_groups;
      int len, i;
      len = strlen(curl_proxy_lbgroup[0])+1;
      *current = smalloc(len);
      strncpy(*current, curl_proxy_lbgroup[0], len);
      
      int this_proxy = 0;
      for (i = 0; i < *num_lb; ++i)
      {
         (*lb_starts)[i] = this_proxy;
         
         int this_group_num = 1;
         const char *scan;
         for (scan = curl_proxy_chain[i]; *scan != '\0'; scan++) {
            if (*scan == '|')
               this_group_num++;
         }
         
         scan = curl_proxy_chain[i];
         int j;
         for (j = 0; j < this_group_num; ++j)
         {
            int s = 0;
            for (; (*scan != '|') && (*scan != '\0'); ++scan)
               s++;
            (*proxies)[this_proxy] = smalloc(s+1);
            (*proxies)[this_proxy][s] = '\0';
            if (s)
               memcpy((*proxies)[this_proxy], scan-s, s);
            this_proxy++;
            scan++;
         }
      }
   }
   
   pthread_mutex_unlock(&mutex_curl);
}


/**
 * Orders the hostlist according to RTT of downloading .cvmfschecksum.
 * Sets the current host to the best-responsive host.
 * If you change the host list in between, it will be overwritten by
 * this function.
 */
void curl_probe_hosts() {
   int host_num;
   int current;
   char **host_chain;
   int *host_rtt;

   /* Lock and copy the global data structures */
   curl_get_host_info(&host_num, &current, &host_chain, &host_rtt);
   
   /* Stopwatch, two times to fill caches first */
   int i, retries;
   for (retries = 0; retries < 2; ++retries)
   {
      for (i = 0; i < host_num; ++i) 
      {
         char *url = smalloc(strlen(host_chain[i]) + strlen("/.cvmfspublished") + 1);
         strcpy(url, host_chain[i]);
         strcat(url, "/.cvmfspublished");
         
         struct timeval tv_start, tv_end;
         struct mem_url mem_url;
         mem_url.data = NULL;
         gettimeofday(&tv_start, NULL);
         int result = curl_download_mem_nocache(url, &mem_url, 0, 0);
         gettimeofday(&tv_end, NULL);
         if (mem_url.data)
            free(mem_url.data);
         if (result == CURLE_OK) 
         {
            /* Time substraction, from GCC documentation */
            if (tv_end.tv_usec < tv_start.tv_usec) {
               int nsec = (tv_end.tv_usec - tv_start.tv_usec) / 1000000 + 1;
               tv_start.tv_usec -= 1000000 * nsec;
               tv_start.tv_sec += nsec;
            }
            if (tv_end.tv_usec - tv_start.tv_usec > 1000000) {
               int nsec = (tv_end.tv_usec - tv_start.tv_usec) / 1000000;
               tv_start.tv_usec += 1000000 * nsec;
               tv_start.tv_sec -= nsec;
            }
            
            /* Compute the time remaining to wait in ms.
               tv_usec is certainly positive. */
            int elapsed = ((tv_end.tv_sec - tv_start.tv_sec)*1000) + 
                          ((tv_end.tv_usec - tv_start.tv_usec)/1000);
            host_rtt[i] = elapsed;
            pmesg(D_CURL, "probing host %s had %dms rtt", url, elapsed);
         } else {
            pmesg(D_CURL, "error while probing host %s: %d", url, result);
            host_rtt[i] = INT_MAX;
         }
         free(url);
      }
   }
   
   /* Sort entries, insertion sort on both, rtt and hosts */
   for (i = 1; i < host_num; ++i)
   {
      int val_rtt = host_rtt[i];
      char *val_host = host_chain[i];
      int pos;
      for (pos = i-1; (pos >= 0) && (host_rtt[pos] > val_rtt); --pos) 
      {
         host_rtt[pos+1] = host_rtt[pos];
         host_chain[pos+1] = host_chain[pos];
      }
      host_rtt[pos+1] = val_rtt;
      host_chain[pos+1] = val_host;
   }
   for (i = 0; i < host_num; ++i)
   {
      if (host_rtt[i] == INT_MAX) host_rtt[i] = -2;
   }
   
   pthread_mutex_lock(&mutex_curl);
   if (curl_host_chain) {
      for (i = 0; i < curl_host_num; ++i)
         free(curl_host_chain[i]);
      free(curl_host_chain);
      free(curl_host_rtt);
   }
   curl_host_num = host_num;
   curl_host_rtt = host_rtt;
   curl_host_chain = host_chain;
   curl_host_current = 0;
   pthread_mutex_unlock(&mutex_curl);
}


/* Global Settings & Options */


/**
 * Parses a list of ','-separated reverse proxy servers for the host chain.
 * \return Number of hosts
 */
int curl_set_host_chain(const char *host_list) 
{
   int i;
   pthread_mutex_lock(&mutex_curl);
   
   if (!host_list || (host_list[0] == '\0')) {
      pmesg(D_CURL, "I won't set an empty host chain");
      pthread_mutex_unlock(&mutex_curl);
      return 0;
   }
   
   if (curl_host_chain) {
      for (i = 0; i < curl_host_num; ++i)
         free(curl_host_chain[i]);
      free(curl_host_chain);
      free(curl_host_rtt);
   }
   curl_host_chain = NULL;
   curl_host_rtt = NULL;
   curl_host_current = -1;
    
   curl_host_num = 1;
   const char *scan;
   for (scan = host_list; *scan != '\0'; scan++)
      if (((*scan == ',') || (*scan == ';')) && (*(scan+1) != '\0'))
         curl_host_num++;

   curl_host_chain = smalloc(curl_host_num * sizeof(char *));
   curl_host_rtt = smalloc(curl_host_num * sizeof(int));
   scan = host_list;
   for (i = 0; i < curl_host_num; ++i)
   {
      int s = 0;
      for (; (*scan != ',') && (*scan != ';') && (*scan != '\0'); ++scan)
         s++;
      curl_host_chain[i] = smalloc(s+1);
      curl_host_chain[i][s] = '\0';
      if (s)
         memcpy(curl_host_chain[i], scan-s, s);
      pmesg(D_CURL, "set host %d to %s", i, curl_host_chain[i]);
      curl_host_rtt[i] = -1;
      scan++;
   }
   
   curl_switch_host();
   int num = curl_host_num;
   pthread_mutex_unlock(&mutex_curl);
   return num;
}


/**
 * Parses a list of ';'- and '|'-separated proxy servers for the proxy chain.
 */
void curl_set_proxy_chain(const char *proxy_list) 
{
   int i;
   pthread_mutex_lock(&mutex_curl);
   
   if (curl_proxy_chain) {
      for (i = 0; i < curl_proxy_groups; ++i)
         free(curl_proxy_chain[i]);
      free(curl_proxy_chain);
   }
   curl_proxy_chain = NULL;
   curl_proxy_group_current = -1; // turned to 0 by curl_switch_proxy()
   curl_proxy_lb_burned = curl_proxy_lb_num; // trigger group cleanup in curl_switch_proxy()
   
   /* Load balancing top level, check for + separated proxy lists (deprecated!) */
   if (proxy_list) {
      int choice_num = 1;
      const char *scan_lb;
      for (scan_lb = proxy_list; *scan_lb != '\0'; scan_lb++)
         if (*scan_lb == '+')
            choice_num++;

      if (choice_num > 1)
      {
         int choice = random() % choice_num;  
      
         scan_lb = proxy_list;
         int i, s = 0;
         for (i = 0; i <= choice; ++i, ++scan_lb)
         {
            s = 0;
            for (; (*scan_lb != '+') && (*scan_lb != '\0'); ++scan_lb)
               s++;
         }
         char *proxy_lb = alloca(s+1);
         proxy_lb[s] = '\0';
         if (s)
            memcpy(proxy_lb, scan_lb-1-s, s);
         proxy_list = proxy_lb;
         pmesg(D_CURL, "select proxy list from top level load-balance list: %s", proxy_list);
      }
   }
   
   /* Easy way out: no proxy */
   if (!proxy_list || (proxy_list[0] == '\0')) {
      curl_proxy_num = curl_proxy_groups = 0;
      curl_switch_proxy();
      pthread_mutex_unlock(&mutex_curl);
      return;
   }
   
   /* Copy proxy list into array */
   curl_proxy_num = curl_proxy_groups = 1;
   const char *scan;
   for (scan = proxy_list; *scan != '\0'; scan++) {
      if (*scan == ';') {
         curl_proxy_num++;
         curl_proxy_groups++;
      }
      if (*scan == '|') {
         curl_proxy_num++;
      }
   }

   curl_proxy_chain = smalloc(curl_proxy_groups * sizeof(char *));
   scan = proxy_list;
   for (i = 0; i < curl_proxy_groups; ++i)
   {
      int s = 0;
      for (; (*scan != ';') && (*scan != '\0'); ++scan)
         s++;
      curl_proxy_chain[i] = smalloc(s+1);
      curl_proxy_chain[i][s] = '\0';
      if (s)
         memcpy(curl_proxy_chain[i], scan-s, s);
      pmesg(D_CURL, "set proxy group %d to %s", i, curl_proxy_chain[i]);
      scan++;
   }
   
   curl_switch_proxy();
   pthread_mutex_unlock(&mutex_curl);
}

void curl_set_timeout(unsigned seconds, unsigned seconds_direct) 
{
   pmesg(D_CURL, "set timeout to %u seconds", seconds);
   
   pthread_mutex_lock(&mutex_curl);

   curl_timeout = seconds;
   curl_timeout_direct = seconds_direct;
   
   unsigned active_timeout;
   if (curl_proxy_inuse) active_timeout = curl_timeout;
   else active_timeout = curl_timeout_direct;
   
   curl_easy_setopt(curl_default, CURLOPT_CONNECTTIMEOUT, active_timeout);
   curl_easy_setopt(curl_mem, CURLOPT_CONNECTTIMEOUT, active_timeout);
   curl_easy_setopt(curl_tee, CURLOPT_CONNECTTIMEOUT, active_timeout);

   curl_easy_setopt(curl_default, CURLOPT_LOW_SPEED_TIME, active_timeout);
   curl_easy_setopt(curl_mem, CURLOPT_LOW_SPEED_TIME, active_timeout);
   curl_easy_setopt(curl_tee, CURLOPT_LOW_SPEED_TIME, active_timeout);

   /* Timeout when transferspeed below 100B/s for more than 2*curl_timeout seconds */
   curl_easy_setopt(curl_default, CURLOPT_LOW_SPEED_LIMIT, 100);
   curl_easy_setopt(curl_mem, CURLOPT_LOW_SPEED_LIMIT, 100);
   curl_easy_setopt(curl_tee, CURLOPT_LOW_SPEED_LIMIT, 100);
   
   unsigned i;
   for (i = 0; i < curl_pool_size; ++i)
   {
      curl_easy_setopt(curl_pool[i], CURLOPT_CONNECTTIMEOUT, active_timeout);
      curl_easy_setopt(curl_pool[i], CURLOPT_LOW_SPEED_TIME, active_timeout);
      curl_easy_setopt(curl_pool[i], CURLOPT_LOW_SPEED_LIMIT, 100);
   }

   pthread_mutex_unlock(&mutex_curl);
}

void curl_get_timeout(unsigned *seconds, unsigned *seconds_direct)
{
   pthread_mutex_lock(&mutex_curl);
   *seconds = curl_timeout;
   *seconds_direct = curl_timeout_direct;
   pthread_mutex_unlock(&mutex_curl);
}


/**
 * Lock manually!
 */
static void curl_pool_extend(unsigned new_size) {
   if (new_size <= curl_pool_size)
      return;
   
   if (curl_pool_size == 0)
      curl_pool = smalloc(new_size * sizeof(CURL *));
   else
      curl_pool = srealloc(curl_pool, new_size * sizeof(CURL *));
   
   unsigned active_timeout;
   if (curl_proxy_inuse) active_timeout = curl_timeout;
   else active_timeout = curl_timeout_direct;
   
   unsigned i;
   for (i = curl_pool_size; i < new_size; ++i) {
      curl_pool[i] = curl_easy_init();
      curl_easy_setopt(curl_pool[i], CURLOPT_HTTPHEADER, slist);
      curl_easy_setopt(curl_pool[i], CURLOPT_NOSIGNAL, 1);
      curl_easy_setopt(curl_pool[i], CURLOPT_FAILONERROR, 1);
      curl_easy_setopt(curl_pool[i], CURLOPT_IPRESOLVE, CURL_IPRESOLVE_V4);
      curl_easy_setopt(curl_pool[i], CURLOPT_WRITEFUNCTION, curl_callback_pool_progress);
      if (active_timeout > 0) {
         curl_easy_setopt(curl_pool[i], CURLOPT_CONNECTTIMEOUT, active_timeout);
         curl_easy_setopt(curl_pool[i], CURLOPT_LOW_SPEED_TIME, active_timeout);
         curl_easy_setopt(curl_pool[i], CURLOPT_LOW_SPEED_LIMIT, 100);
      }
      if (curl_proxy_inuse)
         curl_easy_setopt(curl_pool[i], CURLOPT_PROXY, curl_proxy_lbgroup[0]);
   }
   
   curl_pool_size = new_size;
}


/**
 * Blocks until a curl multi handle has finished all its downloads.
 */
static void curl_wait_multi(CURLM *handle)
{
   int still_running;

   while (CURLM_CALL_MULTI_PERFORM == curl_multi_perform(handle, &still_running)) {};
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
      int result = curl_multi_fdset(handle, &fdread, &fdwrite, &fdexcep, &maxfd);
      if ((result != CURLE_OK) || (maxfd < 0))
         return;
      
      rc = select(maxfd+1, &fdread, &fdwrite, &fdexcep, &timeout);

      switch(rc) {
         case -1:
            /* select error */
            break;
         case 0:
            // timeout
         default:
            /* timeout or readable/writable sockets */
            while (CURLM_CALL_MULTI_PERFORM == curl_multi_perform(handle, &still_running)) {};
            break;
      }
   }
}



/**
 * Takes arrays of urls and file pointers and downloads them in parallel without 
 * decompressing.
 */
void curl_download_parallel(unsigned num, char **url, char **path, unsigned char *digest,
                            int *result) 
{
   unsigned i;
   FILE **f = alloca(num * sizeof(FILE *));
   curl_pool_progress_t *pool_progress = alloca(num * sizeof(curl_pool_progress_t));
   int *added = alloca(num * sizeof(int));
   char **escape_url = alloca(num * sizeof(char *));

   pthread_mutex_lock(&mutex_curl);

   curl_pool_extend(num);
   for (i = 0; i < num; ++i)
   {
      sha1_init(&pool_progress[i].sha1_context);
      escape_url[i] = escape(url[i]);
      if ((f[i] = fopen(path[i], "w+")) != NULL) 
      {
         pool_progress[i].f = f[i];
         curl_easy_setopt(curl_pool[i], CURLOPT_WRITEDATA, &pool_progress[i]);
         curl_easy_setopt(curl_pool[i], CURLOPT_URL, escape_url[i]);
         curl_multi_add_handle(curl_multi, curl_pool[i]);
         added[i] = 1;
      } else {
         added[i] = 0;
      }
   }

   curl_wait_multi(curl_multi);
   
   for (i = 0; i < num; ++i)
   {
      free(escape_url[i]);
      sha1_final(&digest[i*SHA1_DIGEST_LENGTH], &pool_progress[i].sha1_context);
      if (f[i])
         fclose(f[i]);
      result[i] = -1;
   }

   CURLMsg *curl_result;
   int msg_queued;
   do {
      curl_result = curl_multi_info_read(curl_multi, &msg_queued);
      if (curl_result == NULL)
         break;

      for (i = 0; i < num; ++i)
         if (curl_result->easy_handle == curl_pool[i])
            result[i] = curl_result->data.result;
   } while (1);

   for (i = 0; i < num; ++i) {
      if (added[i])
         curl_multi_remove_handle(curl_multi, curl_pool[i]);
   }

   pthread_mutex_unlock(&mutex_curl);
}


void curl_rebalance() {
   pthread_mutex_lock(&mutex_curl);
   curl_proxy_lb_burned = 0;
   curl_switch_proxy();
   pthread_mutex_unlock(&mutex_curl);
}


/**
 * Initialization on load of module.
 */
static void __attribute__((constructor)) initialize (void) 
{
   curl_global_init(CURL_GLOBAL_ALL);
   
   int retval = pthread_mutex_init(&mutex_curl, NULL);
   assert(retval==0);
   
   char *cernvm_id;
   if (getenv("CERNVM_UUID") != NULL)
   {
      int len_custom_header = strlen("X-CVMFS2: ") + strlen(VERSION) + 1 + strlen(getenv("CERNVM_UUID")) + 1;
      cernvm_id = alloca(len_custom_header);
      snprintf(cernvm_id, len_custom_header, "X-CVMFS2: %s %s", VERSION, getenv("CERNVM_UUID"));
   } else {
      int len_custom_header = strlen("X-CVMFS2: ") + strlen(VERSION) + 1 + strlen("anonymous") + 1;
      cernvm_id = alloca(len_custom_header);
      snprintf(cernvm_id, len_custom_header, "X-CVMFS2: %s anonymous", VERSION);
   }
   
   slist = curl_slist_append(slist, "Connection: Keep-Alive");
   slist = curl_slist_append(slist, "Pragma:");
   slist = curl_slist_append(slist, cernvm_id);
   slist_nocache = curl_slist_append(slist_nocache, "Pragma: no-cache");
   slist_nocache = curl_slist_append(slist_nocache, "Cache-Control: no-cache");
   slist_nocache = curl_slist_append(slist_nocache, cernvm_id);

   curl_default = curl_easy_init();
   curl_easy_setopt(curl_default, CURLOPT_HTTPHEADER, slist);
   curl_easy_setopt(curl_default, CURLOPT_NOSIGNAL, 1);
   //curl_easy_setopt(curl_default, CURLOPT_FAILONERROR, 1);
   curl_easy_setopt(curl_default, CURLOPT_IPRESOLVE, CURL_IPRESOLVE_V4);
   curl_easy_setopt(curl_default, CURLOPT_WRITEFUNCTION, curl_callback_progress);
   curl_easy_setopt(curl_default, CURLOPT_HEADERFUNCTION, curl_callback_header);
   
   curl_mem = curl_easy_init();
   curl_easy_setopt(curl_mem, CURLOPT_HTTPHEADER, slist);
   curl_easy_setopt(curl_mem, CURLOPT_NOSIGNAL, 1);
   //curl_easy_setopt(curl_default, CURLOPT_FAILONERROR, 1);
   curl_easy_setopt(curl_mem, CURLOPT_IPRESOLVE, CURL_IPRESOLVE_V4);
   curl_easy_setopt(curl_mem, CURLOPT_WRITEFUNCTION, curl_callback_mem_progress);
   curl_easy_setopt(curl_mem, CURLOPT_HEADERFUNCTION, curl_callback_mem_header);
   
   curl_tee = curl_easy_init();
   curl_easy_setopt(curl_tee, CURLOPT_HTTPHEADER, slist);
   curl_easy_setopt(curl_tee, CURLOPT_NOSIGNAL, 1);
   //curl_easy_setopt(curl_tee, CURLOPT_FAILONERROR, 1);
   curl_easy_setopt(curl_tee, CURLOPT_IPRESOLVE, CURL_IPRESOLVE_V4);
   curl_easy_setopt(curl_tee, CURLOPT_WRITEFUNCTION, curl_callback_tee_progress);
   curl_easy_setopt(curl_tee, CURLOPT_HEADERFUNCTION, curl_callback_tee_header);
   
   /*curl_easy_setopt(curl_default, CURLOPT_VERBOSE, 1);
   curl_easy_setopt(curl_default, CURLOPT_DEBUGFUNCTION, curl_debug);
   curl_easy_setopt(curl_mem, CURLOPT_VERBOSE, 1);
   curl_easy_setopt(curl_mem, CURLOPT_DEBUGFUNCTION, curl_debug);*/

   curl_multi = curl_multi_init();
   curl_pool_extend(4);
   
   curl_set_host_chain("http://localhost");
   
   retval = pthread_attr_init(&pthread_probe_attr);
   assert(retval == 0);
   retval = pthread_attr_setdetachstate(&pthread_probe_attr, PTHREAD_CREATE_DETACHED);
   assert(retval == 0);
   
   struct timeval tv_now;
   gettimeofday(&tv_now, NULL);
   srandom(tv_now.tv_usec);
}
