#include "cvmfsflt.h"

/* Ring buffer */
struct cvmfsflt_logline * rbuf[RBUF_SIZE];   /* ring buffer with pointers to log lines */
int rbuf_head, rbuf_tail; /* Current positions in ring buffer */
struct semaphore rbuf_full, rbuf_empty;   /* semaphores for producer-consumer synchronization */
struct cvmfsflt_logline *ll_current = NULL;
static spinlock_t rbuf_wlock = SPIN_LOCK_UNLOCKED;
atomic_t cvmfsflt_noll = ATOMIC_INIT(0);  /* number of loglines in the ring buffer */


/*
 * Allocates space for a log line.
 * Strings path1 and path2 are copied (if non-NULL).
 */
struct cvmfsflt_logline * cvmfsflt_ll_alloc(
   const char type, const char op, const char *path1, const char *path2)
{
   char *ll_path1 = NULL, *ll_path2 = NULL;
   struct cvmfsflt_logline *logline = NULL;
   
   /* Copy path strings */
   size_t path_len = 2; /* combined path length, 2 for the terminating characters */
   size_t path_split = 1; /* points to the index of the start of the second path */
   if (path1) {
      size_t l = strlen(path1);
      path_len += l;
      path_split += l; 
   }
   if (path2)
      path_len += strlen(path2);

   /* Allocate memory for strings and cpoy */
   ll_path1 = kmalloc(path_len, GFP_KERNEL);
   if (!ll_path1)
      return ERR_PTR(-ENOMEM);
      
   ll_path2 = ll_path1+path_split;
   ll_path1[path_split-1] = '\0';
   ll_path1[path_len-1] = '\0';
   if (path1)
      memcpy(ll_path1, path1, strlen(path1));
   if (path2)
      memcpy(ll_path2, path2, strlen(path2));
   
   /* Alloc memory for logline struct */
   logline = kmalloc(sizeof(struct cvmfsflt_logline), GFP_KERNEL);
   if (!logline) {
      kfree(ll_path1);
      return ERR_PTR(-ENOMEM);
   }
   
   /* Initialize the logline */
   logline->path1 = ll_path1;
   logline->path2 = ll_path2;
   logline->type = type;
   logline->op = op;
   logline->pos = 0;
   logline->plen = path_len;
   logline->go_ahead = 0;
   sema_init(&logline->locked, 0); /* line is locked, no return value yet */
   
   return logline;
}



/*
 * Frees a logline if not NULL.
 * Doesn't care about the state of the sempahore!
 */
void cvmfsflt_ll_free(struct cvmfsflt_logline *logline) {
   if (logline) {
      kfree(logline->path1); /* frees path1 and path2 */
      kfree(logline);
   }
}


/*
 * Creates a logline and inserts it into ring buffer.
 * Returns 0 on success or a negative error value.
 */
int cvmfsflt_ll_insert(struct cvmfsflt_logline *logline, const int may_block) 
{
   /* Insert into ringbuffer */
   if (down_trylock(&rbuf_empty)) {
      /* non-blocking? */
      if (!may_block)
         return -EAGAIN;
      
      if (down_interruptible(&rbuf_empty))
         return -EINTR;
   }
         
   /* Make sure the line is read before we unload */
   try_module_get(THIS_MODULE);
   
   /* Remember that we have to read out this line */
   atomic_inc(&cvmfsflt_noll);
   
   spin_lock(&rbuf_wlock);
   rbuf[rbuf_head] = logline;
   rbuf_head = (rbuf_head+1) % RBUF_SIZE;
   spin_unlock(&rbuf_wlock);
   
   up(&rbuf_full);
   
   return 0;
}



int cvmfsflt_data_init(void)
{
   /* Initalize ring buffer semaphores */
   rbuf_head = rbuf_tail = 0;
   sema_init(&rbuf_full, 0);
   sema_init(&rbuf_empty, RBUF_SIZE);
   ll_current = NULL;
   return 0;
}

void cvmfsflt_data_cleanup(void)
{
}
