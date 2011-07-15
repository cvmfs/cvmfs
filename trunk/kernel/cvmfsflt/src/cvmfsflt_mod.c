#include "cvmfsflt.h"

int __init cvmfsflt_init(void)
{
	int ret;
   
   ret = cvmfsflt_data_init();
   if (ret) {
      printk(KERN_ALERT "cvmfsflt: initializing data structures failed (%d)\n", ret);
      return ret;
   }
   
   ret = cvmfsflt_dev_init();
   if (ret) {
      printk(KERN_ALERT "cvmfsflt: registering char device failed (%d)\n", ret);
      goto fail_dev;
   }
   
   ret = cvmfsflt_rfs_init();
   if (ret) {
      printk(KERN_ALERT "cvmfsflt: initializing rfs filter failed (%d)\n", ret);
      goto fail_rfs;
   }
   
   ret = cvmfsflt_sys_init();
   if (ret) {
      printk(KERN_ALERT "cvmfsflt: registering sysfs attribute failed (%d)\n", ret);
      goto fail_sys;
   }
   
   printk(KERN_INFO "cvmfsflt: CernVM-FS redirfs filter initialized\n");
   
   return 0;

fail_sys:
   redirfs_unregister_filter(cvmfsflt);
   cvmfsflt_rfs_cleanup();
fail_rfs:
   cvmfsflt_dev_cleanup();
fail_dev:
   cvmfsflt_data_cleanup();
   return ret;
}


/*
 * Module unload.  Kernel checks if ref counter is zero.
 */
void __exit cvmfsflt_exit(void)
{
   cvmfsflt_sys_cleanup();
   cvmfsflt_rfs_cleanup();
   cvmfsflt_dev_cleanup();
   cvmfsflt_data_cleanup();
   
   printk(KERN_INFO "cvmfsflt: unloading\n");
}

module_init(cvmfsflt_init);
module_exit(cvmfsflt_exit);

MODULE_LICENSE("Dual BSD/GPL");
MODULE_AUTHOR("Jakob Blomer <jakob.blomer@cern.ch>");
MODULE_DESCRIPTION("A filter for redirfs that logs changes to a directory tree.");
MODULE_SUPPORTED_DEVICE("cvmfs");   /* This module uses /dev/cvmfs. */
