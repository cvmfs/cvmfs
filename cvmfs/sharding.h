#ifndef __JUMP_SHARDING_H__
#define __JUMP_SHARDING_H__ 1

#include <sys/xattr.h>
#include <string.h>
#include <stdlib.h>
#include <errno.h>
#include <stdio.h>
#include <unistd.h>
#include <pthread.h>
#include "curl/curl.h"

#ifdef __cplusplus
extern "C" {
#endif

extern struct shard_t {
 char   **proxy_url;
 unsigned int N;
 int    *status;
 unsigned int  N_online;
 int healthcheck_running;
 int healthcheck_stop;
 pthread_rwlock_t  rwlock;
 pthread_t         checker;
 void(*log_proxy_state)(const char*, int, int);
} shard_t;

extern enum shard_type {
 SHARD_TYPE_RANDOM,
 SHARD_TYPE_SHARDED

} shard_type;

extern enum shard_config_type {
 SHARD_CONFIG_AUTO,
 SHARD_CONFIG_NONE
} shard_config_type;

/* Initialize a sharding instance */
/* SHARD_CONFIG_AUTO: get proxies from xattr of /physical/cvmfs/production.archive.gcp.jump/archive */
/* SHARD_CONFIG_NONE: proxies need to be added explicitly with shard_add_proxy */

struct shard_t* shard_init( enum shard_config_type );


struct shard_t * shard_add_proxy(struct shard_t *ptr, const char *proxy );

struct shard_t * shard_remove_proxy(struct shard_t *ptr, const char *proxy );

/* Destroy a sharding instance */
void shard_free( struct shard_t *ptr );

/* Return the sharding policy for a specified path.  */
const enum shard_type shard_policy( const char*path ) ;

/* Return a list of shards for a specified path, ordered by decreasing priority. Policy is automatically determined */
char** shard_path( struct shard_t *ptr, const char* path, size_t offset ) ;

/* As shard_path, but allow explicit selection of policy */
char** shard_path_with_policy( struct shard_t *ptr, const char* path, size_t offset, enum shard_type method ) ;

/* Mark a proxy as offline. This will cause the marked proxy to be polled and automatically returned to service */
int shard_set_proxy_offline( struct shard_t *ptr, const char *proxy );


/* Return the number of proxues that are online */
int shard_get_number_of_proxies_online( struct shard_t *ptr ); 

/* register a callback for when a proxy comes back online */
void shard_set_logging_function( struct shard_t *ptr, void (*func)(const char*, int ,int )) ;
int shard_healthcheck_start( struct shard_t *ptr );
int shard_healthcheck_stop( struct shard_t *ptr );
#ifdef __cplusplus
}
#endif

#endif
