ifndef THISDIR
$(error "You must pass in a value for THISDIR (normally current dir name from LOCALTOP).")
endif
ifndef SCRAM_ADMIN_DIR
SCRAM_ADMIN_DIR:= .SCRAM/$(SCRAM_ARCH)
endif
ifndef SCRAM_TOOLS_DIR
SCRAM_TOOLS_DIR:= $(SCRAM_ADMIN_DIR)/timestamps
endif
SCRAM_MAKEFILE := $(firstword $(MAKEFILE_LIST))
ToolTimeStamps := $(wildcard $(SCRAM_TOOLS_DIR)/*)
DIRCACHE_MKDIR := $(SCRAM_INTwork)/MakeData
PUB_DIRCACHE_MKDIR := $(SCRAM_ADMIN_DIR)/MakeData
TOOLS_MKDIR    := $(PUB_DIRCACHE_MKDIR)/Tools
$(SCRAM_INTwork)/Makefile: $(TOOLS_MKDIR).mk
	@echo -include $(PUB_DIRCACHE_MKDIR)/variables.mk           > $@.tmp &&\
	echo -include $(SCRAM_CONFIGDIR)/SCRAM/GMake/Makefile       >> $@.tmp &&\
	echo -include $(SCRAM_CONFIGDIR)/SCRAM/GMake/Makefile.rules >> $@.tmp  &&\
	mv $@.tmp $@
	
$(TOOLS_MKDIR).mk: $(ToolTimeStamps)
	@mkdir -p $(TOOLS_MKDIR);\
	if [ ! -f $@ ] ; then \
	  $(SCRAM_CONFIGDIR)/SCRAM/updateToolMK.pl $(SCRAM_ARCH) > $@.tmp ;\
	else \
	  $(SCRAM_CONFIGDIR)/SCRAM/updateToolMK.pl $(SCRAM_ARCH) $(notdir $?) > $@.tmp ;\
	fi
	@cat $(TOOLS_MKDIR)/*.mk >> $@.tmp
	@mv $@.tmp $@

$(SCRAM_TOOLS_DIR)/%: $(SCRAM_CONFIGDIR)/toolbox/$(SCRAM_ARCH)/tools/selected/%.xml
	@for tool in $*; do $(SCRAM) setup $* && touch $@; done

$(SCRAM_CONFIGDIR)/% $(PUB_DIRCACHE_MKDIR)/% $(DIRCACHE_MKDIR)/%:
