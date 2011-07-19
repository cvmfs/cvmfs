all:
	$(MAKE) -C /lib/modules/$(shell uname -r)/build M=$(shell pwd)/src modules
install:
	$(MAKE) -C /lib/modules/$(shell uname -r)/build M=$(shell pwd)/src modules_install
clean:
	$(MAKE) -C /lib/modules/$(shell uname -r)/build M=$(shell pwd)/src clean
	rm -f src/Module.markers src/modules.order src/redirfs.h
dist-hook:
	rm -f $(distdir)/kernel/cvmfsflt/src/taint.sh $(distdir)/kernel/cvmfsflt/src/untaint.sh
check:
mostlyclean: clean
maintainer-clean: clean
dvi:
pdf:
ps:
info:
html:
tags:
ctags:
distdir:
install-exec:
install-data:
install-dvi:
install-html:
install-info:
install-ps:
install-pdf:
installdirs:
installcheck:
uninstall:

