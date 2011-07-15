#include "cvmfsflt.h"

static int major;   /* Major number assigned to the device driver */
static atomic_t device_open = ATOMIC_INIT(0);   /* Indicates if the device is open, only one reader allowed */
static atomic_t in_devread = ATOMIC_INIT(0);    /* Indicates if there is an ongoing read operation */


/* 
 * Called when a process tries to open the device file, like
 * "cat /dev/cvmfs"
 */
static int cvmfsdev_open(struct inode *inode, struct file *file)
{
   if (file->f_mode & FMODE_WRITE)
      return -EPERM;
      
   if (file->f_flags & O_NONBLOCK)
      return -EINVAL;
   
   if (atomic_add_return(1, &device_open) > 1) {
      atomic_dec(&device_open);
      return -EBUSY;
   }

   /* we allow to read from the char device after return, 
      so don't unload the module until release called */
	try_module_get(THIS_MODULE);

	return 0;
}


/* 
 * Called when a process closes the device file.
 */
static int cvmfsdev_release(struct inode *inode, struct file *file)
{
   atomic_dec(&device_open);

	/* 
	 * Decrement the module usage count. 
	 */
	module_put(THIS_MODULE);

	return 0;
}


/* 
 * Called when a process, which already opened the dev file, attempts to
 * read from it.
 * We make sure we have only one reader in open/release.
 */
static ssize_t cvmfsdev_read(struct file *file,	/* see include/linux/fs.h   */
			   char *buffer,	/* buffer to fill with data */
			   size_t length,	/* length of the buffer     */
			   loff_t *offset /* "position in file", unused*/)
{
   /*
	 * Number of bytes actually written to the buffer 
	 */
   int bytes_read = 0;
   
   /*
    * Only allow one reading thread at a time.
    */
   if (atomic_add_return(1, &in_devread) > 1) {
      atomic_dec(&in_devread);
      return -EBUSY;
   }
   
	while (length) {
      if (!ll_current) {
         /* No logline in read buffer, get next one from ring buffer */
         if (down_trylock(&rbuf_full)) {
            /* If we have something, we give it back to the user.
               Otherwise we block. */
            if (bytes_read)
               break;
            
            if (down_interruptible(&rbuf_full)) {
               atomic_dec(&in_devread);
               return -EINTR;
            }
         }
         
         ll_current = rbuf[rbuf_tail];
         rbuf_tail = (rbuf_tail+1) % RBUF_SIZE;
         
         up(&rbuf_empty);
      }
      
      /* This logline might still be waiting for the post-call */
      if (!ll_current->go_ahead) {
         if (down_trylock(&ll_current->locked)) {
            /* If we have something, we give it back to the user.
               Otherwise we block. */
            if (bytes_read)
               break;
            
            if (down_interruptible(&ll_current->locked)) {
               atomic_dec(&in_devread);
               return -EINTR;
            }
         }
         ll_current->go_ahead = 1;
      }
      
      /* Here we go, write it out to user memory */
      switch (ll_current->pos) {
         case 0: 
            put_user(ll_current->type, buffer); 
            ll_current->pos++;
            break;
         case 1: 
            put_user(ll_current->op, buffer); 
            ll_current->pos++;
            break;
         case 2: 
            put_user(ll_current->ret, buffer); 
            ll_current->pos++;
            break;
         default:
            if (ll_current->pos < ll_current->plen + 3) {
               put_user(ll_current->path1[ll_current->pos - 3], buffer);
               ll_current->pos++;
            } else {
               /* That's it, send newline and free the logline */
               char nl = '\n';
               put_user(nl, buffer);
               cvmfsflt_ll_free(ll_current);
               ll_current = NULL;

               /* Adjust counters */
               module_put(THIS_MODULE);
               atomic_dec(&cvmfsflt_noll);
            }
      }
      
      /* One byte more sent, adjust counters */
      bytes_read++;
      buffer++;
      length--;
   }
   
   atomic_dec(&in_devread);
   return bytes_read;
}



/* Call-back functions for character device /dev/cvmfs */
static struct file_operations fops = {
	.read = cvmfsdev_read,
	.open = cvmfsdev_open,
	.release = cvmfsdev_release
};

int cvmfsflt_dev_init(void) 
{
   major = register_chrdev(0, DEVICE_NAME, &fops);
   if (major < 0) {
      return major;
   }
   
   printk(KERN_INFO "cvmfsflt: CernVM-FS logging char device has major number %d.\n", major);
   return 0;
}

void cvmfsflt_dev_cleanup(void)
{
   unregister_chrdev(major, DEVICE_NAME);
}
