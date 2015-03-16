package SCRAM_ExtraBuildRule;
require 5.004;
use Exporter;
@ISA=qw(Exporter);

sub new()
{
  my $class=shift;
  my $self={};
  $self->{template}=shift;
  bless($self, $class);
  return $self;  
}

sub isPublic ()
   {
   my $self=shift;
   my $class = shift;
   if ($class eq "LIBRARY") {return 1;}
   return 0;
   }

sub Project()
{
  my $self=shift;
  my $common=$self->{template};
  my $fh=$common->filehandle();
  $common->symlinkPythonDirectory(1);
  $common->autoGenerateClassesH(1);
  #$self->addPluginSupport(plugin-type,plugin-flag,plugin-refresh-cmd,dir-regexp-for-default-plugins,plugin-store-variable,plugin-cache-file,plugin-name-exp,no-copy-shared-lib)
  $common->addPluginSupport("edm","EDM_PLUGIN","edmPluginRefresh",'\/plugins$',"SCRAMSTORENAME_LIB",".edmplugincache",'$name="${name}.edmplugin"',"yes");
  $common->addPluginSupport("rivet","RIVET_PLUGIN","RivetPluginRefres",'\/plugins$',"SCRAMSTORENAME_LIB",".rivetcache",'$name="Rivet${name}.\$(SHAREDSUFFIX)"',"yes");
  $common->setProjectDefaultPluginType ("edm");
  $common->setLCGCapabilitiesPluginType ("edm");
  $common->addSymLinks("src python 2 python '' -/LCG/");
  $common->addSymLinks("src/LCG include/LCG 1 . ''");
  print $fh "EDM_WRITE_CONFIG:=edmWriteConfigs\n";
  print $fh "EDM_CHECK_CLASS_VERSION:=\$(SCRAM_SOURCEDIR)/FWCore/Utilities/scripts/edmCheckClassVersion\n";
  print $fh "COMPILE_PYTHON_SCRIPTS:=yes\n";
  print $fh "CPPDEFINES+=-DPROJECT_NAME='\"\$(SCRAM_PROJECTNAME)\"' -DPROJECT_VERSION='\"\$(SCRAM_PROJECTVERSION)\"'\n";
######################################################################
# Dependencies: run ignominy analysis for release documentation
  print $fh ".PHONY: dependencies\n",
            "dependencies:\n",
            "\t\@cd \$(LOCALTOP); \\\n",
            "\tmkdir -p \$(LOCALTOP)/doc/deps/\$(SCRAM_ARCH); \\\n",
            "\tcd \$(LOCALTOP)/doc/deps/\$(SCRAM_ARCH); \\\n",
            "\tignominy -f -i -A -g all \$(LOCALTOP)\n";
######################################################################
# Documentation targets. Note- must be lower case otherwise conflict with rules
# for dirs which have the same name:
  print $fh ".PHONY: userguide referencemanual doc doxygen\n",
            "doc: referencemanual\n",
            "\t\@echo \"Documentation/release notes built for \$(SCRAM_PROJECTNAME) v\$(SCRAM_PROJECTVERSION)\"\n",
            "userguide:\n",
            "\t\@if [ -f \$(LOCALTOP)/src/Documentation/UserGuide/scripts/makedoc ]; then \\\n",
            "\t  doctop=\$(LOCALTOP); \\\n",
            "\telse \\\n",
            "\t  doctop=\$(RELEASETOP); \\\n",
            "\tfi; \\\n",
            "\tcd \$\$doctop/src; \\\n",
            "\tDocumentation/UserGuide/scripts/makedoc \$(LOCALTOP)/src \$(LOCALTOP)/doc/UserGuide \$(RELEASETOP)/src\n",
            "referencemanual:\n",
            "\t\@cd \$(LOCALTOP)/src/Documentation/ReferenceManualScripts/config; \\\n",
            "\tsed -e 's|\@PROJ_NAME@|\$(SCRAM_PROJECTNAME)|g' \\\n",
            "\t-e 's|\@PROJ_VERS@|\$(SCRAM_PROJECTVERSION)|g' \\\n",
            "\t-e 's|\@CMSSW_BASE@|\$(LOCALTOP)|g' \\\n",
            "\t-e 's|\@INC_PATH@|\$(LOCALTOP)/src|g' \\\n",
            "\tdoxyfile.conf.in > doxyfile.conf; \\\n",
            "\tcd \$(LOCALTOP); \\\n",
            "\tls -d src/*/*/doc/*.doc | sed 's|\(.*\).doc|mv \"&\" \"\\1.dox\"|' | /bin/sh; \\\n",
            "\tif [ `expr substr \$(SCRAM_PROJECTVERSION) 1 1` = \"2\" ]; then \\\n",
            "\t  ./config/fixdocs.sh \$(SCRAM_PROJECTNAME)\"_\"\$(SCRAM_PROJECTVERSION); \\\n",
            "\telse \\\n",
            "\t  ./config/fixdocs.sh \$(SCRAM_PROJECTVERSION); \\\n",
            "\tfi; \\\n",
            "\tls -d src/*/*/doc/*.doy |  sed 's/\(.*\).doy/sed \"s|\@PROJ_VERS@|\$(SCRAM_PROJECTVERSION)|g\" \"&\" > \"\\1.doc\"/' | /bin/sh; \\\n",
            "\trm -rf src/*/*/doc/*.doy; \\\n",
            "\tcd \$(LOCALTOP)/src/Documentation/ReferenceManualScripts/config; \\\n",
            "\tdoxygen doxyfile.conf; \\\n",
            "\tcd \$(LOCALTOP); \\\n",
            "\tls -d src/*/*/doc/*.dox | sed 's|\(.*\).dox|mv \"&\" \"\\1.doc\"|' | /bin/sh;\n",
            "doxygen:\n",
            "\t\@rm -rf \$(LOCALTOP)/\$(WORKINGDIR)/doxygen &&\\\n",
            "\tmkdir -p \$(LOCALTOP)/\$(WORKINGDIR)/doxygen &&\\\n",
            "\tscriptdir=\$(LOCALTOP)/\$(SCRAM_SOURCEDIR)/Documentation/ReferenceManualScripts/doxygen/utils &&\\\n",
            "\t[ -d \$\$scriptdir ] || scriptdir=\$(RELEASETOP)/\$(SCRAM_SOURCEDIR)/Documentation/ReferenceManualScripts/doxygen/utils &&\\\n",
            "\tcd \$\$scriptdir/doxygen &&\\\n",
            "\tcp -t \$(LOCALTOP)/\$(WORKINGDIR)/doxygen cfgfile footer.html header.html doxygen.css DoxygenLayout.xml doxygen ../../script_launcher.sh &&\\\n",
            "\tcd \$(LOCALTOP)/\$(WORKINGDIR)/doxygen &&\\\n",
            "\tchmod +rwx doxygen script_launcher.sh &&\\\n",
            "\tsed -e 's|\@CMSSW_BASE@|\$(LOCALTOP)|g' cfgfile > cfgfile.conf &&\\\n",
            "\t./doxygen cfgfile.conf &&\\\n",
            "\t./script_launcher.sh \$(SCRAM_PROJECTVERSION) \$\$scriptdir \$(LOCALTOP) &&\\\n",
            "\techo \"Reference Manual is generated.\"\n";
######################################################################
  print $fh ".PHONY: gindices\n",
            "gindices:\n",
            "\t\@cd \$(LOCALTOP)/src; \\\n",
            "\trm -rf  \$(LOCALTOP)/src/.glimpse_*; mkdir  \$(LOCALTOP)/src/.glimpse_full; \\\n",
            "\tls -d \$(LOCALTOP)/src/*/*/*  | glimpseindex -F -H \$(LOCALTOP)/src/.glimpse_full; \\\n",
	    "\tchmod 0644 .glimpse_full/.glimpse_*; \\\n",
	    "\tmv .glimpse_full/.glimpse_filenames .; \\\n",
            "\tfor  x in `ls -A1 .glimpse_full` ; do \\\n",
            "\t  ln -s .glimpse_full/\$\$x \$\$x; \\\n",
            "\tdone; \\\n",	    
            "\tsed -i -e 's|\$(LOCALTOP)/src/||g' .glimpse_filenames; \\\n",
	    "\tcd .glimpse_full; ln -s ../.glimpse_filenames .\n";
######################################################################
  print $fh ".PHONY: productmap\n",
            "productmap:\n",
            "\t\@cd \$(LOCALTOP); \\\n",
            "\tmkdir -p src; rm -f src/ReleaseProducts.list; echo \">> Generating Product Map in src/ReleaseProducts.list.\";\\\n",
            "\t(RelProducts.pl \$(LOCALTOP) > \$(LOCALTOP)/src/ReleaseProducts.list || exit 0)\n";
######################################################################
  print $fh ".PHONY: depscheck\n",
            "depscheck:\n",
            "\t\@ReleaseDepsChecks.pl --detail\n";
  return 1;
}

sub Extra_template()
{
  my $self=shift;
  my $common=$self->{template};
  $common->pushstash();$common->moc_template();$common->popstash();
  $common->plugin_template();
  $common->pushstash();$common->lexyacc_template();$common->popstash();
  $common->pushstash();$common->codegen_template();$common->popstash();
  $common->pushstash();$common->dict_template();   $common->popstash();
  return 1;
}

1;
