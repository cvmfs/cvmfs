#include <sys/xattr.h>
#include <string.h>
#include <stdlib.h>
#include <errno.h>
#include <stdio.h>
#include <unistd.h>
#include <pthread.h>
#include "curl/curl.h"
#include <assert.h>
#include "sharding.h"

#ifdef __cplusplus
extern "C" {
#endif 

#define CHUNK_SIZE (1024*1024*24)  // 24MB to match CVMFS chunk size

static unsigned int hash_sdbm(const char *key);
static char** shard_path_shard( struct shard_t *ptr, const char* path,  const int add_bias, size_t offs) ;

static void __set_online( struct shard_t *ptr, int idx );
static void __set_offline( struct shard_t *ptr, int idx );
static int __get_status( struct shard_t *ptr, int idx );
static void* __checker( void* );
static size_t __sink(char *ptr, size_t size, size_t nmemb, void *userdata) { return size * nmemb; }

static unsigned int hash_key(const char *url)
{
        unsigned int a = hash_sdbm( url );
        /* This function is one of Bob Jenkins' full avalanche hashing
         * functions, which when provides quite a good distribution for little
         * input variations. The result is quite suited to fit over a 32-bit
         * space with enough variations so that a randomly picked number falls
         * equally before any server position.
         * Check http://burtleburtle.net/bob/hash/integer.html for more info.
         */
        a = (a+0x7ed55d16) + (a<<12);
        a = (a^0xc761c23c) ^ (a>>19);
        a = (a+0x165667b1) + (a<<5);
        a = (a+0xd3a2646c) ^ (a<<9);
        a = (a+0xfd7046c5) + (a<<3);
        a = (a^0xb55a4f09) ^ (a>>16);

        /* ensure values are better spread all around the tree by multiplying
         * by a large prime close to 3/4 of the tree.
         */
        return a * 3221225473U;
}

struct __score_t {
 unsigned int score;
 int idx;
} __score_t;

static int _score_cmp( const void*a, const void*b ) {
  struct __score_t *p1 = (struct __score_t*)a;
  struct __score_t *p2 = (struct __score_t*)b;
  if(p1->score < p2->score) return -1;
  if(p1->score > p2->score) return +1;
  return 0;
}

static char** shard_path_shard( struct shard_t *ptr, const char* path,  const int add_bias, size_t offs) {
  char **pref = (char**) malloc(sizeof(char*) * ptr->N );
  struct __score_t *score = (struct __score_t*) alloca( sizeof(struct __score_t*) * ptr->N );
  char buf[65536];
  unsigned int i;
  if(NULL==pref) { return NULL; }
  if(NULL==score) { free(pref); return NULL; }


  long unsigned b = offs / CHUNK_SIZE; 

  // the hash is taken from a key which include the proxy_url, the object uri, the chunk index offset into the file and, optionally, the pid as a randomising factor

  for(i=0; i<ptr->N; i++ ) {
    int online  = __get_status( ptr, i );

    // if the host is offline, put it to the back of the list    
    // in the situation where all proxies are offline, the behaviour will
    // thus be to try round-robbining over the proxy list
    // otherwise hash it 
    if ( online ) { 
      if(add_bias) {
        snprintf( buf,  sizeof(buf) , "%s:%s:%lu:%lu", ptr->proxy_url[i], path, b, (unsigned long) getpid() );
      } else {
        snprintf( buf,  sizeof(buf) , "%s:%s:%lu", ptr->proxy_url[i], path, b );
      }
      buf[sizeof(buf)-1] = 0; //snprint 
      score[i].score = hash_key(buf);
      score[i].idx   = i;
    } else {
     score[i].idx =i;
     score[i].score = UINT_MAX;
    }

  }
  
  qsort( score, ptr->N, sizeof(struct __score_t), _score_cmp );
  for(i=0;i<ptr->N; i++) {
    pref[i] = ptr->proxy_url[ score[i].idx ];
  }  
  return pref;
}

static unsigned int hash_sdbm(const char *key)
{
        unsigned int len = strlen(key);
        unsigned int hash = 0;
        int c;

        while (len--) {
                c = *key++;
                hash = c + (hash << 6) + (hash << 16) - hash;
        }
        return hash;
}



/* Exported functions */

const enum shard_type shard_policy( const char*path ) {
  if( strstr( path, ".cvmfs" )) return SHARD_TYPE_RANDOM;

  return SHARD_TYPE_SHARDED;
}


void shard_set_logging_function( struct shard_t *ptr, void (*func)(const char*, int, int )) {
  ptr->log_proxy_state = func;
}

char** shard_path( struct shard_t *ptr, const char* path, size_t offs ) {
  enum shard_type  method = shard_policy( path );
  return shard_path_with_policy( ptr, path, offs, method );
}
char** shard_path_with_policy( struct shard_t *ptr, const char* path, size_t offs, enum shard_type method ) {
  switch( method ) {
    case SHARD_TYPE_RANDOM:
      return shard_path_shard( ptr, path, 1, offs );
    case SHARD_TYPE_SHARDED:
      return shard_path_shard( ptr, path, 0, offs );
    default:
      return NULL;
  }
}

struct shard_t* shard_init( enum shard_config_type t) {
   struct shard_t* retval = (struct shard_t*) malloc(sizeof(struct shard_t));
   if(!retval) { return NULL; }
   retval->N = 0;
   retval->N_online = 0;
   retval->proxy_url = NULL;

   retval->status = NULL;

   retval->healthcheck_running = 0;
   retval->healthcheck_stop    = 0;
   retval->log_proxy_state = NULL;

   pthread_rwlock_init(&retval->rwlock, NULL);

   if( SHARD_CONFIG_AUTO == t ) { 
   //get the proxy list from the archive extended attribute
   char attr[65537]; // extended attributes are at most 64K;
   char *saveptr=NULL;
   char *next   =NULL;

   int len;
   len = getxattr("/physical/cvmfs/production.archive.gcp.jump/archive", "user.proxy_list", attr, sizeof(attr) );

   if(len<0) { return NULL; }
   saveptr = attr;
   while( (next = strtok_r( saveptr, "\n", &saveptr ))!=NULL ) {
     if(strlen(next)>0) {
       retval = shard_add_proxy( retval, next );
       if(!retval) { return retval; }
     }
   }
   }
   return retval;
}

int shard_healthcheck_start( struct shard_t *ptr ) {
   if (! ptr->healthcheck_running ) {
     pthread_create( &(ptr->checker), NULL, &__checker, (void*) ptr );
     ptr->healthcheck_running = 1;
     return 0;
   }
   return 1;
}

int shard_healthcheck_stop( struct shard_t *ptr ) {
   if( ptr->healthcheck_running ) {
     ptr->healthcheck_stop = 1;
     pthread_join(  ptr->checker, NULL );
     ptr->healthcheck_stop = 0;
     ptr->healthcheck_running = 0;
     return 0;
   }
   return 1;
}


struct shard_t * shard_add_proxy(struct shard_t *ptr,  const char *proxy ) {
  if(ptr==NULL || proxy==NULL ) { return ptr; }

// don't add if the healthcheck thread is running !
// since this isn't a situation we care about, dont deal with it, just avoid failure
  if(ptr->healthcheck_running) { return ptr; }

  ptr->N++;
  ptr->N_online++;
  ptr->proxy_url = (char**)realloc( ptr->proxy_url, sizeof(char*) * ptr->N );
  if(!ptr->proxy_url) { free(ptr); return NULL; }
  ptr->proxy_url[ptr->N-1] = strdup(proxy);
  
  ptr->status = (int*)realloc( ptr->status, sizeof(int) * ptr->N );
  ptr->status[ ptr->N-1 ] = 1;
  if(!ptr->status) { free(ptr->proxy_url); free(ptr); return NULL; }

#if DEBUG
  fprintf(stderr, "shard_add_proxy: Added '%s\n", proxy );
#endif 

  return ptr;
}

static void __set_online( struct shard_t *ptr, int idx ) {
  pthread_rwlock_wrlock( &(ptr->rwlock) );
  if ( ptr->status[idx]==0 ) {
    ptr->status[idx]=1;
    ptr->N_online++;
  }

  assert(ptr->N_online>=0 && ptr->N_online <= ptr->N );

#if DEBUG
  fprintf(stderr, "Setting proxy %s ONline. %d proxies online\n", ptr->proxy_url[idx], ptr->N_online );
#endif

  if(ptr->log_proxy_state) {
    ptr->log_proxy_state( ptr->proxy_url[idx], 1, ptr->N_online );
  }
  pthread_rwlock_unlock( &(ptr->rwlock) );
}

static void __set_offline( struct shard_t *ptr, int idx ) {
  pthread_rwlock_wrlock( &(ptr->rwlock) );
  if (ptr->status[idx]==1 ) {
    ptr->status[idx]=0;
    ptr->N_online--;
  }

  assert(ptr->N_online>=0 && ptr->N_online <= ptr->N );

#if DEBUG
  fprintf(stderr, "Setting proxy %s OFFline. %d proxies online\n", ptr->proxy_url[idx], ptr->N_online );
#endif

  if(ptr->log_proxy_state) {
    ptr->log_proxy_state( ptr->proxy_url[idx], 0, ptr->N_online );
  }

  pthread_rwlock_unlock( &(ptr->rwlock) );

}

static int __get_status( struct shard_t *ptr, int idx ) {
  pthread_rwlock_rdlock( &(ptr->rwlock) );
  int i= ptr->status[idx];
  pthread_rwlock_unlock( &(ptr->rwlock) );
  return i;
}

static void* __checker( void *d ) {
 struct shard_t *ptr = (struct shard_t*) d;

 char buf[65536];

 CURL* curl = curl_easy_init();
 CURLcode res;
 curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1);
 curl_easy_setopt (curl, CURLOPT_FAILONERROR, 1L);
 curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, &__sink );
 curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT,  1L );

 int all_check_interval = 6; // poll all proxies at this interval  * sleep_interval
 int sleep_interval = 10; // seconds between cycles
 int iter=0;                                 
 while(!ptr->healthcheck_stop) {
    unsigned int i=0;
    for(i=0; (i<ptr->N) && (!ptr->healthcheck_stop); i++ ) {
      if( ! __get_status( ptr, i ) ) { /// only check proxies which are marked as offline
        snprintf( buf, sizeof(buf)-1, "%s/healthz", ptr->proxy_url[i] );
        buf[sizeof(buf)-1]=0;
        curl_easy_setopt(curl, CURLOPT_URL, buf );
        long responseCode = 999;
        res = curl_easy_perform(curl);
        if( CURLE_OK == res ) { curl_easy_getinfo (curl, CURLINFO_RESPONSE_CODE, &responseCode);

        } 
        if( (200==responseCode) ) { 
          __set_online( ptr, i ); 
        }     
      }
    }
    for(int j=0; j<sleep_interval && !ptr->healthcheck_stop; j++) {
      sleep(1);
    }

    if(!iter) {
        // check all online proxies, offline any that are not returning 200 on their /healthz endpoint
        // this will catch dms that have been set to be administratively down, but which are
        // still serving traffic
        //
      for(i=0; (i<ptr->N) && (!ptr->healthcheck_stop); i++ ) {
        if( __get_status( ptr, i ) ) { /// only check proxies which are marked as online
          snprintf( buf, sizeof(buf)-1, "%s/healthz", ptr->proxy_url[i] );
          buf[sizeof(buf)-1]=0;
          curl_easy_setopt(curl, CURLOPT_URL, buf );
          long responseCode = 999;
          res = curl_easy_perform(curl);
          if( CURLE_OK == res ) { curl_easy_getinfo (curl, CURLINFO_RESPONSE_CODE, &responseCode);

          } 
          if( (200!=responseCode) ) { 
            __set_offline( ptr, i ); 
          }     

        }
      }
    }
		// increment after above check, so that that check runs on first iteration
    iter = (iter+1)% all_check_interval;
 }

 curl_easy_cleanup(curl);

 ptr->healthcheck_running = 0;

 return NULL;
}



void shard_free( struct shard_t *ptr ){
  unsigned int i;
  for(i=0; i<ptr->N; i++) {
   free( ptr->proxy_url[i] );
  } 
  free(ptr->proxy_url);
  free(ptr->status);
  free(ptr);
}


int shard_set_proxy_offline( struct shard_t *ptr, const char *proxy ) {
  unsigned int i;
  for( i=0; i < ptr->N; i++ ) {
    if (! strcmp( proxy, ptr->proxy_url[i] ) ) {
       __set_offline( ptr, i );
       return 0;
    }
  }
  return 1;
}

int shard_get_number_of_proxies_online( struct shard_t *ptr ) {
  pthread_rwlock_rdlock( &(ptr->rwlock) );
  int i= ptr->N_online;
  pthread_rwlock_unlock( &(ptr->rwlock) );
  return i;
}

#ifdef _TEST_
int main(int argc, char**argv) {
  struct shard_t *ptr = shard_init( SHARD_CONFIG_AUTO );
  char ** pref;
  unsigned int i=0;
 char buf[65000];

if(ptr) {
 shard_healthcheck_start(ptr);
printf("HEALTHCHECK THREAD STARTED\n");

  while(strlen(gets(buf))) {
  	pref=shard_path( ptr,  buf, 0  );
 	for(i=0; i< ptr->N; i++ ) {
	    printf("%s\n", pref[i] );
	 }
	 free(pref);
 }
 shard_healthcheck_stop(ptr);
}
 exit(0);
 shard_free(ptr);
 return 0;

}
#endif


#ifdef __cplusplus
}
#endif 
