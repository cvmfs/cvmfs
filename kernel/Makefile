all:
	$(MAKE) -C redirfs
	[ redirfs/src/Module.symvers -nt cvmfsflt/src/Module.symvers ] && cp redirfs/src/Module.symvers cvmfsflt/src/ || true
	[ redirfs/src/redirfs.h -nt cvmfsflt/src/redirfs.h ] && cp redirfs/src/redirfs.h cvmfsflt/src/ || true
	$(MAKE) -C cvmfsflt
install:
	$(MAKE) -C redirfs install
	$(MAKE) -C cvmfsflt install
	depmod -a
clean:
	$(MAKE) -C redirfs clean
	$(MAKE) -C cvmfsflt clean
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
