bin_PROGRAMS = 
if BUILD_SERVER
bin_PROGRAMS += cvmfsd_ctrl
install-exec-hook:
	chmod u+s $(DESTDIR)$(bindir)/cvmfsd_ctrl
endif

cvmfsd_ctrl_SOURCES = cvmfsd_ctrl.c

cvmfsd_ctrl_CFLAGS = $(CVMFS_CFLAGS)
cvmfsd_ctrl_LDFLAGS = $(CVMFS_LIBS)
