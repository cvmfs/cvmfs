#include "cvmfsflt.h"

static ssize_t cvmfsflt_lockdown_show(redirfs_filter filter,
   struct redirfs_filter_attribute *attr, char *buf)
{
	int val;
   
   val = atomic_read(&cvmfsflt_lockdown);
   if (val)
      buf[0] = '1';
   else
      buf[0] = '0';
   
   return 1;
}

static ssize_t cvmfsflt_lockdown_store(redirfs_filter filter,
   struct redirfs_filter_attribute *attr, const char *buf, size_t count)
{
   if (count < 1)
      return -EINVAL;
      
   switch (buf[0]) {
      case '0': 
         atomic_set(&cvmfsflt_lockdown, 0);
         return 1;
      case '1':
         atomic_set(&cvmfsflt_lockdown, 1);
         return 1;
      default:
         return -EINVAL;
   }
}


static ssize_t cvmfsflt_nowops_show(redirfs_filter filter,
   struct redirfs_filter_attribute *attr, char *buf)
{
   return snprintf(buf, PAGE_SIZE, "%d", atomic_read(&cvmfsflt_nowops));
}

static ssize_t cvmfsflt_nowops_store(redirfs_filter filter,
   struct redirfs_filter_attribute *attr, const char *buf, size_t count)
{
   return -EPERM;
}


static ssize_t cvmfsflt_noll_show(redirfs_filter filter,
   struct redirfs_filter_attribute *attr, char *buf)
{
   return snprintf(buf, PAGE_SIZE, "%d", atomic_read(&cvmfsflt_noll));
}

static ssize_t cvmfsflt_noll_store(redirfs_filter filter,
   struct redirfs_filter_attribute *attr, const char *buf, size_t count)
{
   return -EPERM;
}


static struct redirfs_filter_attribute cvmfsflt_lockdown_attr = 
   REDIRFS_FILTER_ATTRIBUTE(lockdown, 0644, cvmfsflt_lockdown_show,
                            cvmfsflt_lockdown_store);

static struct redirfs_filter_attribute cvmfsflt_nowops_attr = 
   REDIRFS_FILTER_ATTRIBUTE(nowops, 0444, cvmfsflt_nowops_show,
                            cvmfsflt_nowops_store);

static struct redirfs_filter_attribute cvmfsflt_noll_attr = 
   REDIRFS_FILTER_ATTRIBUTE(noll, 0444, cvmfsflt_noll_show,
                            cvmfsflt_noll_store);                            

int cvmfsflt_sys_init(void) 
{
   int ret;
   
   ret = redirfs_create_attribute(cvmfsflt, &cvmfsflt_lockdown_attr);
   if (ret)
      goto fail_lockdown;
      
   ret = redirfs_create_attribute(cvmfsflt, &cvmfsflt_nowops_attr);
   if (ret)
      goto fail_nowops;
      
   ret = redirfs_create_attribute(cvmfsflt, &cvmfsflt_noll_attr);
   if (ret)
      goto fail_noll;
   
   return 0;
   
fail_noll:
   redirfs_remove_attribute(cvmfsflt, &cvmfsflt_nowops_attr);
fail_nowops:
   redirfs_remove_attribute(cvmfsflt, &cvmfsflt_lockdown_attr);
fail_lockdown:
   return ret;
}


void cvmfsflt_sys_cleanup(void) 
{
   redirfs_remove_attribute(cvmfsflt, &cvmfsflt_noll_attr);
   redirfs_remove_attribute(cvmfsflt, &cvmfsflt_nowops_attr);
   redirfs_remove_attribute(cvmfsflt, &cvmfsflt_lockdown_attr);
}
