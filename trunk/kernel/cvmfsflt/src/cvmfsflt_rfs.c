#include "cvmfsflt.h"

atomic_t cvmfsflt_lockdown = ATOMIC_INIT(0);   /* In case we process the journal we want to make the tree immutable.
                                                  Exported by sysfs */
atomic_t cvmfsflt_nowops = ATOMIC_INIT(0);   /* number of write operations that are in progress */

redirfs_filter cvmfsflt;

/*
 * NOP, freeing is done by the character device on read.
 */
static void cvmfsflt_rfsdata_free(struct redirfs_data *unused)
{
}


/*
 * Gets the filename from a dentry.  If the dentry is not in filter path,
 * we return the inode.
 * May return NULL, i.e. -ENOMEM.
 */
static char *cvmfsflt_get_path(struct dentry *dentry)
{
   int ret;
   char *filename = NULL;
   redirfs_root root;
   redirfs_path *paths_flt = NULL;
   struct redirfs_path_info *path_flt_info = NULL;
   
   root = redirfs_get_root_dentry(cvmfsflt, dentry);
	if (!root) {
		/* Might happen if path is outside filter root.
         Best thing we can do here is return the inode */
      unsigned long ino;
      
      if (!dentry->d_inode) {
         /* No inode either, fall back to 0 (dirty) */
         ino = 0;
      } else {
         ino = dentry->d_inode->i_ino; 
      }
      
      filename = kzalloc(sizeof(char) * PAGE_SIZE, GFP_KERNEL);
      if (!filename) {
         printk(KERN_WARNING "cvmfsflt: filename = kzalloc failed\n");
         return NULL;
      }
      
      snprintf(filename, PAGE_SIZE, "%lu", ino);
      
      goto fail_root;
	}
   
   paths_flt = redirfs_get_paths_root(cvmfsflt, root);
   if (IS_ERR(paths_flt) || !paths_flt) {
      printk(KERN_WARNING "cvmfsflt: redirfs_get_paths failed\n");
      goto fail_paths_flt;
   }
   
   path_flt_info = redirfs_get_path_info(cvmfsflt, paths_flt[0]);
   if (IS_ERR(path_flt_info)) {
      printk(KERN_WARNING "cvmfsflt: redirfs_get_path_info failed\n");
      goto fail_path_info;
   }
   
   filename = kzalloc(sizeof(char) * PAGE_SIZE, GFP_KERNEL);
	if (!filename) {
      printk(KERN_WARNING "cvmfsflt: filename = kzalloc failed\n");
		goto fail_filename_alloc;
   }

	ret = redirfs_get_filename(path_flt_info->mnt, dentry, filename, PAGE_SIZE);
   if (ret) {
		printk(KERN_ERR "cvmfsflt: rfs_get_filename failed\n");
      kfree(filename);
		filename = NULL;
	}
   
fail_filename_alloc:
   redirfs_put_path_info(path_flt_info);
fail_path_info:
   redirfs_put_paths(paths_flt);
fail_paths_flt:
   redirfs_put_root(root);
fail_root:
   return filename;
}


static char cvmfsflt_get_filetype(struct dentry *dentry) 
{
   umode_t i_mode;
      
   if (!dentry || !dentry->d_inode)
      return 'U';
      
   i_mode = dentry->d_inode->i_mode;
   if (S_ISREG(i_mode))
      return 'R';
   else if (S_ISLNK(i_mode))
      return 'L';
   else if (S_ISDIR(i_mode))   
      return 'D';

   return 'U';
}


/*
 * Create logline and insert into ring buffer.
 */
static enum redirfs_rv cvmfsflt_precall(
   redirfs_context context, struct redirfs_args *args) 
{
   int ret;
   struct redirfs_data *rfs_data;
   struct cvmfsflt_logline *logline;
   char cvmfsflt_type, cvmfsflt_op;
   char *path1 = NULL, *path2 = NULL;
   int may_block = 1;
   int rename_in = 0;
   
   if (args->type.id == REDIRFS_OP_END) {
      args->type.id = REDIRFS_DIR_IOP_RENAME;
      rename_in = 1;
   }
   
   atomic_inc(&cvmfsflt_nowops);
   
   /* are we in lockdown mode? */
   if (atomic_read(&cvmfsflt_lockdown)) {
      atomic_dec(&cvmfsflt_nowops);
      args->rv.rv_int = -EBUSY;
      return REDIRFS_STOP;
   }
   
   /* Fill the logline depending on the call */
   switch (args->type.id) {
      case REDIRFS_REG_FOP_OPEN:
         path1 = cvmfsflt_get_path(args->args.f_open.file->f_dentry);
         if (!path1) { 
            args->rv.rv_int = -ENOMEM;
            goto fail_getpath;
         }
         may_block = !(args->args.f_open.file->f_flags & O_NONBLOCK);
         cvmfsflt_type = 'R';
         cvmfsflt_op = 'T';
         break;
         
      case REDIRFS_REG_IOP_SETATTR:
         path1 = cvmfsflt_get_path(args->args.i_setattr.dentry);
         if (!path1) { 
            args->rv.rv_int = -ENOMEM;
            goto fail_getpath;
         }
         cvmfsflt_type = 'R';
         cvmfsflt_op = 'A';
         break;
         
      case REDIRFS_DIR_IOP_CREATE:
         path1 = cvmfsflt_get_path(args->args.i_create.dentry);
         if (!path1) { 
            args->rv.rv_int = -ENOMEM;
            goto fail_getpath;
         }
         cvmfsflt_type = 'R';
         cvmfsflt_op = 'C';
         break;
         
      case REDIRFS_DIR_IOP_LINK:
         path1 = cvmfsflt_get_path(args->args.i_link.dentry);
         if (!path1) { 
            args->rv.rv_int = -ENOMEM;
            goto fail_getpath;
         }
         cvmfsflt_type = cvmfsflt_get_filetype(args->args.i_link.old_dentry);
         cvmfsflt_op = 'C';
         break;
         
      case REDIRFS_DIR_IOP_UNLINK:
         path1 = cvmfsflt_get_path(args->args.i_unlink.dentry);
         if (!path1) { 
            args->rv.rv_int = -ENOMEM;
            goto fail_getpath;
         }
         cvmfsflt_type = cvmfsflt_get_filetype(args->args.i_unlink.dentry);
         cvmfsflt_op = 'D';
         break;
         
      case REDIRFS_DIR_IOP_SYMLINK:
         path1 = cvmfsflt_get_path(args->args.i_symlink.dentry);
         if (!path1) { 
            args->rv.rv_int = -ENOMEM;
            goto fail_getpath;
         }
         cvmfsflt_type = 'L';
         cvmfsflt_op = 'C';
         break;
         
      case REDIRFS_DIR_IOP_MKDIR:
         path1 = cvmfsflt_get_path(args->args.i_mkdir.dentry);
         if (!path1) { 
            args->rv.rv_int = -ENOMEM;
            goto fail_getpath;
         }
         cvmfsflt_type = 'D';
         cvmfsflt_op = 'C';
         break;
         
     case REDIRFS_DIR_IOP_RMDIR:    
         path1 = cvmfsflt_get_path(args->args.i_rmdir.dentry);
         if (!path1) { 
            args->rv.rv_int = -ENOMEM;
            goto fail_getpath;
         }
         cvmfsflt_type = 'D';
         cvmfsflt_op = 'D';
         break;
         
      case REDIRFS_DIR_IOP_RENAME:
         path1 = cvmfsflt_get_path(args->args.i_rename.old_dentry);
         path2 = cvmfsflt_get_path(args->args.i_rename.new_dentry);
         if (!path1 || !path2) { 
            args->rv.rv_int = -ENOMEM;
            goto fail_getpath;
         }
         cvmfsflt_type = cvmfsflt_get_filetype(args->args.i_unlink.dentry);
         cvmfsflt_op = rename_in ? 'I' : 'O';
         break;
         
      case REDIRFS_DIR_IOP_SETATTR:
         path1 = cvmfsflt_get_path(args->args.i_setattr.dentry);
         if (!path1) { 
            args->rv.rv_int = -ENOMEM;
            goto fail_getpath;
         }
         cvmfsflt_type = 'D';
         cvmfsflt_op = 'A';
         break;
         
      case REDIRFS_LNK_IOP_SETATTR:
         path1 = cvmfsflt_get_path(args->args.i_setattr.dentry);
         if (!path1) { 
            args->rv.rv_int = -ENOMEM;
            goto fail_getpath;
         }
         cvmfsflt_type = 'L';
         cvmfsflt_op = 'A';
         break;
      
      default:
         cvmfsflt_type = '?';
         cvmfsflt_op = '?';
   }
   
   /* Allocate logline */
   logline = cvmfsflt_ll_alloc(cvmfsflt_type, cvmfsflt_op, path1, path2);
   if (IS_ERR(logline)) {
      args->rv.rv_int = PTR_ERR(logline);
      goto fail_llalloc;
   }
   if (path1) {
      kfree(path1);
      path1 = NULL;
   }
   if (path2) {
      kfree(path2);
      path2 = NULL;
   }
   
   /* Attach to context */
   ret = redirfs_init_data(&logline->rfs_data, cvmfsflt, cvmfsflt_rfsdata_free, NULL);
   if (ret) {
      args->rv.rv_int = ret;
      goto fail_ctxinit;
   }

   rfs_data = redirfs_attach_data_context(cvmfsflt, context, &logline->rfs_data);
   if (!rfs_data) {
      args->rv.rv_int = PTR_ERR(rfs_data);
      goto fail_ctxattach;
   }
   redirfs_put_data(rfs_data);
   
   /* insert into ringbuffer */
   ret = cvmfsflt_ll_insert(logline, may_block);
   if (ret) {
      args->rv.rv_int = ret;
      goto fail_llinsert;
   }
   
   return REDIRFS_CONTINUE;

fail_llinsert:
   rfs_data = redirfs_detach_data_context(cvmfsflt, context);
   redirfs_put_data(rfs_data);
   redirfs_put_data(rfs_data); /* again for init */
fail_ctxattach:
fail_ctxinit:
   cvmfsflt_ll_free(logline);
fail_llalloc:
fail_getpath:
   if (path1)
      kfree(path1);
   if (path2)
      kfree(path2);
   atomic_dec(&cvmfsflt_nowops);
   return REDIRFS_STOP;
}


/*
 * Wrapper for open call, catch only if in write mode.
 */
static enum redirfs_rv cvmfsflt_preopen(
   redirfs_context context, struct redirfs_args *args) 
{
   if (!(args->args.f_open.file->f_mode & FMODE_WRITE))
      return REDIRFS_CONTINUE;
   
   return cvmfsflt_precall(context, args);
}


/*
 * Wrapper for rename-in call. Small hack: we adjust the
 * op type but set it back in cvmfs_precall.
 * This is to distinguish from a rename-out call.
 */
static enum redirfs_rv cvmfsflt_pre_rename_in(
   redirfs_context context, struct redirfs_args *args)
{
   args->type.id = REDIRFS_OP_END;
   
   return cvmfsflt_precall(context, args);
}


/*
 * After release of a file opened in write mode, decrease the
 * nowops counter.
 */
static enum redirfs_rv cvmfsflt_postrelease(
   redirfs_context context, struct redirfs_args *args) 
{
   if (!(args->args.f_release.file->f_mode & FMODE_WRITE))
      return REDIRFS_CONTINUE;
   
   atomic_dec(&cvmfsflt_nowops);
   
   return REDIRFS_CONTINUE;
}



/*
 * Complete logline with return value.
 */
static enum redirfs_rv cvmfsflt_postcall(
   redirfs_context context, struct redirfs_args *args) 
{
   struct redirfs_data *rfs_data;
   struct cvmfsflt_logline *logline;
   
   rfs_data = redirfs_detach_data_context(cvmfsflt, context);
   if (!rfs_data)
      return REDIRFS_CONTINUE;
      
   /* For file open we decrease the nowops counter on release */
   if (args->type.id != REDIRFS_REG_FOP_OPEN)
      atomic_dec(&cvmfsflt_nowops);
   
   logline = container_of(rfs_data, struct cvmfsflt_logline, rfs_data);
   redirfs_put_data(rfs_data);
   /* again for init data */
   redirfs_put_data(rfs_data);
   
   if (args->rv.rv_int < 0)
      logline->ret = 'F';
   else
      logline->ret = 'S';

   /* Now we allow to read it out. */
   up(&logline->locked);
   
   return REDIRFS_CONTINUE;
}


static struct redirfs_op_info cvmfsflt_op_info[] = {
   {REDIRFS_REG_IOP_SETATTR, cvmfsflt_precall, cvmfsflt_postcall},
   {REDIRFS_REG_FOP_OPEN, cvmfsflt_preopen, cvmfsflt_postcall},
   {REDIRFS_REG_FOP_RELEASE, NULL, cvmfsflt_postrelease},
   
   {REDIRFS_DIR_IOP_CREATE, cvmfsflt_precall, cvmfsflt_postcall},
   {REDIRFS_DIR_IOP_LINK, cvmfsflt_precall, cvmfsflt_postcall},
   {REDIRFS_DIR_IOP_UNLINK, cvmfsflt_precall, cvmfsflt_postcall},
   {REDIRFS_DIR_IOP_SYMLINK, cvmfsflt_precall, cvmfsflt_postcall},
   {REDIRFS_DIR_IOP_MKDIR, cvmfsflt_precall, cvmfsflt_postcall},
   {REDIRFS_DIR_IOP_RMDIR, cvmfsflt_precall, cvmfsflt_postcall},
   {REDIRFS_DIR_IOP_RENAME, cvmfsflt_precall, cvmfsflt_postcall},
   {REDIRFS_DIR_IOP_SETATTR, cvmfsflt_precall, cvmfsflt_postcall},
   
   /* following three are not called on ext3 */
   {REDIRFS_LNK_IOP_SETATTR, cvmfsflt_precall, cvmfsflt_postcall},
   {REDIRFS_LNK_FOP_OPEN, cvmfsflt_preopen, cvmfsflt_postcall},
   {REDIRFS_LNK_FOP_RELEASE, NULL, cvmfsflt_postrelease},
   
   {REDIRFS_OP_END, NULL, NULL}
};


static struct redirfs_filter_operations cvmfsflt_ops = {
   .pre_rename = cvmfsflt_pre_rename_in,
	.post_rename = cvmfsflt_postcall
};

static struct redirfs_filter_info cvmfsflt_info = {
	.owner = THIS_MODULE,
	.name = "cvmfsflt",
	.priority = 600000007,
	.active = 1,
   .ops = &cvmfsflt_ops
};


int cvmfsflt_rfs_init(void)
{
   int ret;
   
   cvmfsflt = redirfs_register_filter(&cvmfsflt_info);
	if (IS_ERR(cvmfsflt))
      return PTR_ERR(cvmfsflt); 
   
   ret = redirfs_set_operations(cvmfsflt, cvmfsflt_op_info);
   if (ret) {
      int err;
      err = redirfs_unregister_filter(cvmfsflt);
      if (err)
         printk(KERN_ERR "cvmfsflt: unregister filter failed (%d)\n", err);
      else
         redirfs_delete_filter(cvmfsflt);
		return ret;
	}
   
   return 0;
}

void cvmfsflt_rfs_cleanup(void)
{
   /* Filter is already unregistered, otherwise module ref count is grater 0 */
   redirfs_delete_filter(cvmfsflt);
}
