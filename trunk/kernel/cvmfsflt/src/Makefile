obj-m += cvmfsflt.o
cvmfsflt-objs :=  cvmfsflt_sysfs.o cvmfsflt_dev.o cvmfsflt_data.o cvmfsflt_mod.o cvmfsflt_rfs.o

all:
	make -C /lib/modules/$(shell uname -r)/build M=$(PWD) modules

clean:
	make -C /lib/modules/$(shell uname -r)/build M=$(PWD) clean
