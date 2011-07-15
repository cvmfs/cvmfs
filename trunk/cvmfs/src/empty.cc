/**
 * \file cvmfs.cc
 * \namespace cvmfs
 *
 * CernVM-FS is a FUSE module which implements an HTTP read-only filesystem.
 * The original idea is based on GROW-FS.
 *
 * CernVM-FS shows a remote HTTP directory as local file system.  The client
 * sees all available files.  On first access, a file is downloaded and
 * cached locally.  All downloaded pieces are verified with SHA1.
 *
 * To do so, a directory hive has to be transformed into a CVMFS2
 * "repository".  This can be done by the CernVM-FS server tools. 
 *
 * This preparation of directories is transparent to web servers and
 * web proxies.  They just serve static content, i.e. arbitrary files.
 * Any HTTP server should do the job.  We use Apache + Squid.  Serving
 * files from the memory of a web proxy brings significant performance
 * improvement.
 *  
 *
 * Developed by Jakob Blomer 2009 at CERN
 * jakob.blomer@cern.ch
 */
 
#define FUSE_USE_VERSION 25
#define _FILE_OFFSET_BITS 64

#include <fuse.h>
#include <fuse/fuse_opt.h>

#include <string>
#include <iostream>
#include <sstream>
#include <fstream>
#include <vector>
#include <map>
#include <cstdlib>
#include <cstring>
#include <csignal>
#include <ctime>
#include <cassert>
#include <cstdio>

#include <dirent.h>
#include <errno.h>
#include <stddef.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/statfs.h>
#include <sys/wait.h>
#include <sys/errno.h>
#include <sys/mount.h>
#include <stdint.h>
#include <unistd.h> 
#include <fcntl.h> 
#include <sys/time.h>
#include <sys/resource.h>
#include <pthread.h>
#include <openssl/crypto.h>
#include <utime.h>



using namespace std;


namespace cvmfs {
 
   static int cvmfs_getattr(const char *c_path, struct stat *info) {
      const string path = "/tmp/empty" + string(c_path);
      if (stat(path.c_str(), info) != 0)
         return -errno;
      
      return 0;
   }


   /**
    * Reads a symlink from the catalog.  Environment variables are expanded.
    */
   static int cvmfs_readlink(const char *c_path, char *buf, size_t size) {
      const string path = "/tmp/empty" + string(c_path);

      if (readlink(path.c_str(), buf, size) != 0)
         return -errno;
      
      return 0;
   }
   

   /**
    * The ls-callback. If we hit a nested catalog, we load it.
    */
   static int cvmfs_readdir(const char *c_path, 
                            void *buf, 
                            fuse_fill_dir_t filler, 
                            off_t offset __attribute__((unused)),
                            struct fuse_file_info *fi __attribute__((unused)))
   {
      const string path = "/tmp/empty" + string(c_path);
      
      DIR *dp = opendir(path.c_str());
      if (dp) {
         struct dirent64 *d;
         while ((d = readdir64(dp)) != NULL) {
            const string name = d->d_name;
            struct stat info;
            if (lstat((path + "/" + d->d_name).c_str(), &info) == 0) {
               filler(buf, d->d_name, &info, 0);
            } else {
               closedir(dp);
               return -errno;
            }
         }
         closedir(dp);
      } else {
         return -errno;
      }
      
      return 0;
      
   }
   

   /**
    * Open a file from cache.  If necessary, file is downloaded first.
    * Also catalog reload magic can happen, if file download fails.
    *
    * \return Read-only file descriptor in fi->fh
    */
   static int cvmfs_open(const char *c_path, struct fuse_file_info *fi)
   {
      const string path = "/tmp/empty" + string(c_path);
      int fd;
      
      if ((fd = open(path.c_str(), fi->flags)) >= 0) {
         fi->fh = fd;
         return 0;
      }
      
      return -errno;
   }


   /**
    * Redirected to pread into cache.
    */
   static int cvmfs_read(const char *path __attribute__((unused)), 
                         char *buf, 
                         size_t size, 
                         off_t offset, 
                         struct fuse_file_info *fi)
   {     
      const int64_t fd = fi->fh;
      return pread(fd, buf, size, offset);
   }
   
   
   /**
    * \return -EROFS
    */
   static int cvmfs_write(const char *path __attribute__((unused)),
                          const char *buf, 
                          size_t size, 
                          off_t offset, 
                          struct fuse_file_info *fi)
   {
      const int64_t fd = fi->fh;
      return pwrite(fd, buf, size, offset);
   }


   /**
    * File close operation. Redirected into cache.
    */
   static int cvmfs_release(const char *path __attribute__((unused)), struct fuse_file_info *fi)
   {
      const int64_t fd = fi->fh;
      if (close(fd) != 0)
         return -errno;
      
      return 0;
   }
   

   /**
    * Not implemented.
    */
#if FUSE_USE_VERSION==22
   static int cvmfs_statfs(const char *path __attribute__((unused)), struct statfs *info)
#else
   static int cvmfs_statfs(const char *path __attribute__((unused)), struct statvfs *info)
#endif
   {
      /* We will return 0 which will cause the fs 
         to be ignored in "df" for example */
      memset(info, 0, sizeof(*info));
      return 0;
   }
   

   /**
    * \return -EROFS
    */
   static int cvmfs_chmod(const char *c_path, mode_t mode)
   {
      const string path = "/tmp/empty" + string(c_path);
      
      if (chmod(path.c_str(), mode) != 0)
         return -errno;
      
      return 0;
   }
   
   
   /**
    * \return -EROFS
    */
   static int cvmfs_mkdir(const char *c_path, mode_t mode)
   {
      const string path = "/tmp/empty" + string(c_path);
      
      if (mkdir(path.c_str(), mode) != 0)
         return -errno;
      
      return 0;

   }
   
   
   /**
    * \return -EROFS
    */
   static int cvmfs_unlink(const char *c_path) {
      const string path = "/tmp/empty" + string(c_path);
      
      if (unlink(path.c_str()) != 0)
         return -errno;
      
      return 0;

   }
   
   
   /**
    * \return -EROFS
    */
   static int cvmfs_rmdir(const char *c_path) {
      const string path = "/tmp/empty" + string(c_path);
      
      if (rmdir(path.c_str()) != 0)
         return -errno;
      
      return 0;
   }
   
   
   /**
    * \return -EROFS
    */
   static int cvmfs_symlink(const char *c_from, const char *c_to)
   {
      const string from = "/tmp/empty" + string(c_from);
      const string to = "/tmp/empty" + string(c_to);
         
      if (symlink(from.c_str(), to.c_str()) != 0)
         return -errno;
      
      return 0;
   }
   
   
   /**
    * \return -EROFS
    */
   static int cvmfs_rename(const char *c_from, const char *c_to)
   {
      const string from = "/tmp/empty" + string(c_from);
      const string to = "/tmp/empty" + string(c_to);
      
      if (rename(from.c_str(), to.c_str()) != 0)
         return -errno;
      
      return 0;
   }
   
   
   /**
    * \return -EROFS
    */
   static int cvmfs_link(const char *c_from, const char *c_to)
   {
      const string from = "/tmp/empty" + string(c_from);
      const string to = "/tmp/empty" + string(c_to);
      
      if (link(from.c_str(), to.c_str()) != 0)
         return -errno;
      
      return 0;
   }
   
   
   /**
    * \return -EROFS
    */
   static int cvmfs_chown(const char *c_path,
                          uid_t uid, gid_t gid)
   {
      const string path = "/tmp/empty" + string(c_path);

      if (chown(path.c_str(), uid, gid) != 0)
         return -errno;
      
      return 0;
   }
   
   
   /**
    * \return -EROFS
    */
   static int cvmfs_truncate(const char *c_path, off_t size)
   {
      const string path = "/tmp/empty" + string(c_path);
      
      if (truncate(path.c_str(), size) != 0)
         return -errno;
      
      return 0;
   }
   
   
   /**
    * \return -EROFS
    */
   static int cvmfs_utime(const char *c_path, struct utimbuf *buf)
   {
      const string path = "/tmp/empty" + string(c_path);
      
      if (utime(path.c_str(), buf) != 0)
         return -errno;
      
      return 0;

   }
   
   
   /**
    * \return -EROFS
    */
   static int cvmfs_mknod(const char *c_path, mode_t mode, dev_t rdev)
   {
      const string path = "/tmp/empty" + string(c_path);

      if (mknod(path.c_str(), mode, rdev) != 0)
         return -errno;
      
      return 0;
   }
   

   
   /** 
    * Puts the callback functions in one single structure
    */
   static void set_cvmfs_ops(struct fuse_operations *cvmfs_operations) {
      /* Implemented */
      cvmfs_operations->getattr	= cvmfs_getattr;
      cvmfs_operations->readlink	= cvmfs_readlink;
      cvmfs_operations->readdir	= cvmfs_readdir;
      cvmfs_operations->open	   = cvmfs_open;
      cvmfs_operations->read	   = cvmfs_read;
      cvmfs_operations->release 	= cvmfs_release;
      cvmfs_operations->chmod    = cvmfs_chmod;
      
      /* Return EROFS (read-only filesystem) */
      cvmfs_operations->mkdir    = cvmfs_mkdir;
      cvmfs_operations->unlink   = cvmfs_unlink;
      cvmfs_operations->rmdir    = cvmfs_rmdir;
      cvmfs_operations->symlink	= cvmfs_symlink;
      cvmfs_operations->rename   = cvmfs_rename;
      cvmfs_operations->chown    = cvmfs_chown;
      cvmfs_operations->link	   = cvmfs_link;
      cvmfs_operations->truncate	= cvmfs_truncate;
      cvmfs_operations->utime    = cvmfs_utime;
      cvmfs_operations->write    = cvmfs_write;
      cvmfs_operations->mknod    = cvmfs_mknod;
      cvmfs_operations->statfs   = cvmfs_statfs;
      
      /* Init/Fini */
      cvmfs_operations->init     = NULL;
      cvmfs_operations->destroy  = NULL;
   }
   
} /* namespace cvmfs */



using namespace cvmfs;




/**
 * Boot the beast up!
 */
int main(int argc, char *argv[])
{
   static struct fuse_operations cvmfs_operations;

   /* Set fuse callbacks, remove url from arguments */
   cout << "Mounting redirector" << endl;
   set_cvmfs_ops(&cvmfs_operations);
   return fuse_main(argc, argv, &cvmfs_operations);
}
