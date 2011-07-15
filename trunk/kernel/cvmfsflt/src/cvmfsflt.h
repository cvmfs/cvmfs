#ifndef _CVMFSFLT_H
#define _CVMFSFLT_H 1

#include <linux/module.h>	/* Needed by all modules */
#include <linux/kernel.h>	/* Needed for KERN_INFO */
#include <linux/init.h>		/* Needed for the macros */
#include <linux/fs.h>
#include <asm/uaccess.h>	/* for put_user */
#include <linux/slab.h>    /* for kmalloc/kfree */ 

#include "redirfs.h"


#define CVMFSFLT_VERSION "0.1"
#define DEVICE_NAME "cvmfs"   /* Dev appears as /proc/devices/cvmfs */
#define RBUF_SIZE (PAGE_SIZE/sizeof(void *))   /* number of entries in ring buffer */


/* Module-global variables */
extern redirfs_filter cvmfsflt;
extern atomic_t cvmfsflt_lockdown;
extern atomic_t cvmfsflt_nowops; /* number of write operations in progress */
extern atomic_t cvmfsflt_noll; /* number of loglines in the ring buffer */
extern struct cvmfsflt_logline *ll_current;
extern struct cvmfsflt_logline * rbuf[RBUF_SIZE];   /* ring buffer with pointers to log lines */
extern int rbuf_head, rbuf_tail; /* Current positions in ring buffer */
extern struct semaphore rbuf_full, rbuf_empty;   /* semaphores for producer-consumer synchronization */


/* Data structures */
struct cvmfsflt_logline {
   /* Required by redirfs, we loop a logline through the pre-post-callstack. */
   struct redirfs_data rfs_data;

   struct semaphore locked; /* unlocked in post call, when return value is known */
   int go_ahead; /* once the line is unlocked, it is unlocked forever */
   int pos; /* for reading in chunks */

   /*
    * 'R' regular file
    * 'L' symbolic link
    * 'D' directory
    * 'U' unknown
    */
   char type;
   
   /*
    * 'C' create
    * 'D' delete
    * 'T' touch
    * 'A' change attributes
    * 'I' move in (rename)
    * 'O' move out (rename)
    */
   char op;
   
   /*
    * 'S' success
    * 'F' failed
    */
   char ret;
   
   size_t plen; /* path1 and path2 are followed by each other in memory */
   char *path1;
   char *path2; /* for rename */
};


struct cvmfsflt_logline * cvmfsflt_ll_alloc(
   const char type, const char op, const char *path1, const char *path2);
void cvmfsflt_ll_free(struct cvmfsflt_logline *); 
int cvmfsflt_ll_insert(struct cvmfsflt_logline *logline, const int may_block);


int cvmfsflt_sys_init(void);
void cvmfsflt_sys_cleanup(void);

int cvmfsflt_dev_init(void);
void cvmfsflt_dev_cleanup(void);

int cvmfsflt_data_init(void);
void cvmfsflt_data_cleanup(void);

int cvmfsflt_rfs_init(void);
void cvmfsflt_rfs_cleanup(void);

#endif
