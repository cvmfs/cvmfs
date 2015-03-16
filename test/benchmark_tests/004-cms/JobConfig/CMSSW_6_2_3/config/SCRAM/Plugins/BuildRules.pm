package SCRAM::Plugins::BuildRules;
use vars qw( @ISA );
use Exporter;
use File::Basename;
use Cache::CacheUtilities;
use BuildSystem::Template::Plugins::PluginCore;
use BuildSystem::TemplateStash;
@ISA=qw(Exporter);

sub loadInit_ ()
{
  my $self=shift;
  foreach my $var ("SCRAM_PROJECTNAME", "LOCALTOP", "SCRAM_ARCH", "SCRAM_INTwork", "SCRAM_CONFIGDIR", "THISDIR")
  {
    my $val="";
    if(exists $ENV{$var}){$val=$ENV{$var};}
    if($val=~/^\s*$/){die "Environment variable \"$var\" does not exist.";}
  }
  foreach my $ext ("f","f77","F","F77")
  {$self->{cache}{SourceExtensions}{fortran}{$ext}=1;}
  foreach my $ext ("cc","cpp","cxx","C")
  {$self->{cache}{SourceExtensions}{cxx}{$ext}=1;}
  foreach my $ext ("c")
  {$self->{cache}{SourceExtensions}{c}{$ext}=1;}
  $ENV{LOCALTOP}=&fixPath($ENV{LOCALTOP});
  if ($ENV{SCRAM_VERSION}=~/^V[2-9]/){$self->{dbext}="db.gz";}
  else{$self->{dbext}="db";}
}

sub new()
{
  my $class = shift;
  my $self = {};
  bless($self, $class);
  $self->loadInit_();
  $self->{swap_prod_mkfile}=0;
  return $self;
}

sub process()
{
  my $self=shift;
  my $name=shift;
  my $data=shift;
  $self->{FH}=shift;
  $self->{context}=BuildSystem::TemplateStash->new();
  $self->{context}->stash($data);
  $self->pushstash();
  $self->{core}=BuildSystem::Template::Plugins::PluginCore->new($self->{context});
  $self->{swap_prod_mkfile}=0;
  my $ret=$self->runTemplate("${name}_template");
  $self->popstash();
  if ($self->{swap_prod_mkfile})
  {
    my $oldfile=$data->{MAKEFILE};
    my $newfile=$oldfile;
    my $arch=$ENV{SCRAM_ARCH};
    $newfile=~s/\/\.SCRAM\/$arch\/MakeData\/DirCache\//\/tmp\/$arch\/MakeData\/DirCache\//;
    if ($newfile ne $oldfile)
    {
      close($data->{MAKEFILEFH});
      use File::Copy;
      use FileHandle;
      move($oldfile,$newfile);
      $data->{MAKEFILEFH} = FileHandle->new();
      $data->{MAKEFILEFH}->open(">>$newfile");
      $self->{FH}=$data->{MAKEFILEFH};
    }
  }
  return $ret;
}

sub filehandle()
{
  my $self=shift;;
  return $self->{FH};
}

sub core()
{
  my $self=shift;
  return $self->{core};
}

sub error()
{
  my $self=shift;
  return "$@\n";
}

sub swapMakefile ()
{
  my $self=shift;
  if ($self->get('class') eq 'LIBRARY'){$self->{swap_prod_mkfile}=1;}
}

##################### Stash Interface ###############
sub get(){my $self=shift;return $self->{context}->get(@_);}

sub set(){my $self=shift;return $self->{context}->set(@_);}

sub stash(){my $self=shift;return $self->{context};}

sub pushstash(){my $self=shift;$self->{context}->pushstash();}

sub popstash(){my $self=shift;$self->{context}->popstash();}

#################################################

sub hasData ()
{
  my $self=shift;
  my $data=shift;
  my $key=shift;
  my $r=ref($data);
  if($r eq "ARRAY"){foreach my $d (@$data){if($d eq $key){return 1;}}}
  elsif($r eq "HASH"){foreach my $d (keys %$data){if($d eq $key){return 1;}}}
  return 0;
}

sub getTool ()
{
  my $self=shift;
  my $tool=shift;
  if((exists $self->{cache}{toolcache}) && (exists $self->{cache}{toolcache}{SETUP}{$tool}))
  {return $self->{cache}{toolcache}{SETUP}{$tool};}
  return {};
}

sub isDependentOnTool ()
{
  my $self=shift;
  my $tool=shift;
  my $bdata=$self->{core}->data("USE");
  if((defined $bdata) && (ref($bdata) eq "ARRAY"))
  {
    foreach my $t (@$bdata)
    {
      my $tx=lc($t);
      foreach my $t1 (@$tool){if($tx eq $t1){return 1;}}
    }
  }
  return 0;
}

sub isToolAvailable ()
{
  my $self=shift;
  my $tool=lc(shift) || return 0;
  if((exists $self->{cache}{toolcache}) && (exists $self->{cache}{toolcache}{SETUP}{$tool})){return 1;}
  return 0;
}

sub toolDeps ()
{
  my $self=shift;
  my @tools=();
  my $bdata=$self->{context}->stash()->get('branch')->branchdata();
  if((defined $bdata) && (ref($bdata) eq "BuildSystem::DataCollector"))
  {foreach my $t (@{$bdata->{BUILD_ORDER}}){push @tools,$t;}}
  return @tools;
}

sub getEnv ()
{
  my $self=shift;
  my $var=shift;
  if(exists $ENV{$var}){return $ENV{$var};}
  return "";
}

sub processTemplate ()
{return &doTheTemplateProcessing(shift,"process",@_);}

sub includeTemplate ()
{return &doTheTemplateProcessing(shift,"include",@_);}

sub doTheTemplateProcessing ()
{
  my $self=shift;
  my $type=shift || "process";
  my $name=shift || return;
  if (defined $self->{cache}{ProjectPlugin})
  {
    my $plugin=$self->{cache}{ProjectPlugin};
    if($type eq "include"){$self->stash()->pushstash();}
    eval {$ret=$plugin->$name($self,@_);};
    if($type eq "include"){$self->stash()->popstash();}
  }
  return $ret;
}

sub getProjectPlugin ()
{
  my $self=shift;
  return $self->{cache}{ProjectPlugin} || undef;
}

sub getProjectPluginTemplate ()
{
  my $self=shift;
  my $ret=undef;
  if (defined $self->{cache}{ProjectPlugin})
  {
    if (exists $self->{cache}{ProjectPlugin}->{template})
    {$ret=$self->{cache}{ProjectPlugin}->{template};}
  }
  return $ret;
}

sub unsupportedProductType ()
{
  my $self=shift;
  my $stash=$self->{context}->stash();
  my $path = $stash->get("path");
  my $type = $stash->get("type");
  print STDERR "WARNING: Product type \"$type\" not supported yet from \"$path\".\n";
  if($path=~/\/src$/)
  {print STDERR "WARNING: You are only suppose to build a single library from \"$path\".\n";}
  return;
}

sub getSubDirIfEnabled ()
{
  my $self=shift;
  my $flag=$self->{core}->flags("ADD_SUBDIR");
  if(($flag=~/^yes$/i) || ($flag == 1))
  {
    my $path=$self->{context}->stash()->get('path');
    my $subdir=[];
    $subdir=&readDir($path,1,-1);
    return join(" ",sort(@$subdir));
  }
  return;
}
##############################################################
sub fixData ()
{
  my $self=shift;
  my $data=shift;
  my $type=shift;
  my $bf=shift;
  my $section=shift;
  my $ndata=[];
  if (ref($data) ne "ARRAY") {return "";}
  if (scalar(@$data)==0){return "";}
  if(defined $section){$section="export";}
  else{$section="non-export";}
  my $udata={};
  if ($type eq "INCLUDE")
  {
    my $ldir=dirname($bf);
    my $ltop=$self->{cache}{LocalTop};
    foreach my $d (@$data)
    {
      my $x=$d;
      $x=~s/^\s*//;$x=~s/\s*$//;
      if($x=~/^[^\/\$]/){$x="${ltop}/${ldir}/${x}";}
      $x=&fixPath($x);
      if(!exists $udata->{$x}){$udata->{$x}=1;push @$ndata,$x;}
      else{print STDERR "***WARNING: Multiple usage of \"$d\". Please cleanup \"include\" in \"$section\" section of \"$bf\".\n";}
    }
  }
  elsif($type eq "USE")
  {
    foreach my $u (@$data)
    {
      my $x=$u;
      $x=~s/^\s*//;$x=~s/\s*$//;
      my $lx=lc($x);
      if (exists $self->{cache}{InvalidUses}{$lx}){print STDERR "***WARNING: Invalid direct dependency on tool '$lx'. Please cleanup \"$bf\".\n";}
      if(!$self->isToolAvailable($lx)){$lx=$x;}
      if(!exists $udata->{$lx}){$udata->{$lx}=1;push @$ndata,$lx;}
      else{print STDERR "***WARNING: Multiple usage of \"$lx\". Please cleanup \"use\" in \"$section\" section of \"$bf\".\n";}
    }
  }
  elsif($type eq "LIB")
  {
    foreach my $l (@$data)
    {
      my $x=$l;
      $x=~s/^\s*//;$x=~s/\s*$//;
      if($x eq "1"){$x=$self->{context}->stash()->get('safename');}
      if(!exists $udata->{$x}){$udata->{$x}=1;push @$ndata,$x;}
      else{print STDERR "***WARNING: Multiple usage of \"$l\". Please cleanup \"lib\" in \"$section\" section of \"$bf\".\n";}
    }
  }
  if(scalar(@$ndata)==0){return "";}
  return $ndata;
}

##############################################################
sub allProductDirs ()
{
  my $self=shift;
  return keys %{$self->{cache}{ProductTypes}};
}

sub addProductDirMap ()
{
  my $self=shift;
  my $type=lc(shift) || return;
  my $reg=shift || return;
  my $dir=shift || return;
  my $index=shift;
  if(!defined $index){$index=100;}
  $self->{cache}{ProductTypes}{$type}{DirMap}{$index}{$reg}=$dir;
  return;
}

sub resetProductDirMap ()
{
  my $self=shift;
  my $type=lc(shift) || return;
  delete $self->{cache}{ProductTypes}{$type}{DirMap};
  return;
}

sub getProductStore ()
{
  my $self=shift;
  my $stash=$self->{context}->stash();
  my $type = shift || $stash->get('type');
  my $path = shift || $stash->get('path');
  if(exists $self->{cache}{ProductTypes}{$type}{DirMap})
  {
    foreach my $ind (sort {$a <=> $b} keys %{$self->{cache}{ProductTypes}{$type}{DirMap}})
    {
      foreach my $reg (keys %{$self->{cache}{ProductTypes}{$type}{DirMap}{$ind}})
      {if($path=~/$reg/){return $self->{cache}{ProductTypes}{$type}{DirMap}{$ind}{$reg};}}
    }
  }
  else{print STDERR "****ERROR: Product store \"$type\" not available. Please fix the build template loaded for \"$path\".\n";}
  return "";
}

sub setRootReflex ()
{
  my $self=shift;
  my $tool=shift || return;
  $self->{cache}{RootRflx}=lc($tool);
  return;
}

sub getRootReflex ()
{
  my $self=shift;
  return $self->{cache}{RootRflx};
}

##############################################################
sub addSymLinks()
{
  my ($self,$dir)=@_;
  if ($dir ne ""){$self->{cache}{SymLinks}{$dir}=1;}
}

sub removeSymLinks()
{
  my ($self,$dir)=@_;
  delete $self->{cache}{SymLinks}{$dir};
}

sub getSymLinks()
{
  my $self=shift;
  my @dirs=();
  if (exists $self->{cache}{SymLinks}){@dirs=keys %{$self->{cache}{SymLinks}};}
  return @dirs;
}

sub createSymLinks()
{
  my $self=shift;
  my $fh=$self->filehandle();
  print $fh "CONFIGDEPS += \$(COMMON_WORKINGDIR)/cache/project_links\n",
            "\$(COMMON_WORKINGDIR)/cache/project_links: FORCE_TARGET\n",
            "\t\@echo '>> Creating project symlinks';\\\n",
            "\t[ -d \$(\@D) ] ||  \$(CMD_mkdir) -p \$(\@D) &&\\\n";
  my @dirs=$self->getSymLinks();
  if (scalar(@dirs)==0){return;}
  foreach my $cmd (@dirs)
  {print $fh "\t",$self->{cache}{ProjectConfig},"/SCRAM/createSymLinks.pl $cmd &&\\\n";}
  print $fh "\tif [ ! -f \$@ ] ; then touch \$@; fi\n\n";
  return 1;
}
##############################################################
sub setLCGCapabilitiesPluginType ()
{
  my $self=shift;
  my $type=lc(shift) || $self->{cache}{DefaultPluginType};
  if($type && (!exists $self->{cache}{SupportedPlugins}{$type}))
  {
    print STDERR "****ERROR: LCG Capabilities Plugin type \"$type\" not supported.\n";
    print STDERR "           Currently available plugins are:",join(",",sort keys %{$self->{cache}{SupportedPlugins}}),".\n";
  }
  else{$self->{cache}{LCGCapabilitiesPlugin}=$type;}
  return;
}

sub getLCGCapabilitiesPluginType ()
{
  my $self=shift;
  my $type=$self->{cache}{LCGCapabilitiesPlugin} || $self->{cache}{DefaultPluginType};
  return $type;
}

sub addPluginSupport ()
{
  my $self=shift;
  my $type=lc(shift) || return;
  my $flag=uc(shift) || return;
  my $refresh=shift  || return;
  my $reg=shift      || "";
  my $dir=shift      || "SCRAMSTORENAME_MODULE";
  my $cache=shift    || ".cache";
  my $name=shift     || '$name="${name}.reg"';
  my $ncopylib=shift || "";
  my $err=0;
  foreach my $t (keys %{$self->{cache}{SupportedPlugins}})
  {
    if($t eq $type){next;}
    my $c=$self->{cache}{SupportedPlugins}{$t}{Cache};
    my $r=$self->{cache}{SupportedPlugins}{$t}{Refresh};
    if($r eq $refresh){print STDERR "****ERROR: Can not have two plugins type (\"$t\" and \"$type\") using the same plugin refresh command \"$r\"\n.";$err=1;}
    if("$c" eq "$cache"){print STDERR "****ERROR: Can not have two plugins type (\"$t\" and \"$type\") using the same plugin cache file \"$c\"\n.";$err=1;}
  }
  if(!$err)
  {
    $self->{cache}{SupportedPlugins}{$type}{Refresh}=$refresh;
    $self->{cache}{SupportedPlugins}{$type}{Flag}=[];
    foreach my $f (split /:/,$flag){push @{$self->{cache}{SupportedPlugins}{$type}{Flag}}, $f;}
    $self->{cache}{SupportedPlugins}{$type}{Cache}=$cache;
    $self->{cache}{SupportedPlugins}{$type}{DefaultDirName}=$reg;
    $self->{cache}{SupportedPlugins}{$type}{Dir}=$dir;
    $self->{cache}{SupportedPlugins}{$type}{Name}=$name;
    $self->{cache}{SupportedPlugins}{$type}{NoSharedLibCopy}=$ncopylib;
    $self->{cache}{SupportedPlugins}{$type}{DirMap}={};
  }
  return;
}

sub addPluginDirMap ()
{
  my $self=shift;
  my $type=lc(shift) || return;
  my $reg=shift || return;
  my $dir=shift || return;
  my $index=shift;
  if(!defined $index){$index=100;}
  if(!exists $self->{cache}{SupportedPlugins}{$type})
  {print STDERR "****ERROR: Not a valid plugin type \"$type\". Available plugin types are:",join(", ", keys %{$self->{cache}{SupportedPlugins}}),"\n";}
  $self->{cache}{SupportedPlugins}{$type}{DirMap}{$index}{$reg}=$dir;
  return;
}

sub removePluginSupport ()
{
  my $self=shift;
  my $type=lc(shift) || return;
  delete $self->{cache}{SupportedPlugins}{$type};
  if ($self->{cache}{LCGCapabilitiesPlugin} eq $type){$self->{cache}{LCGCapabilitiesPlugin}="";}
  if ($self->{cache}{DefaultPluginType} eq $type){$self->{cache}{DefaultPluginType}="";}
  return;
}

sub getPluginProductDirs ()
{
  my $self=shift;
  my %dirs=();
  my $type=lc(shift) || return keys(%dirs);
  if(exists $self->{cache}{SupportedPlugins}{$type})
  {
    $dirs{$self->{cache}{SupportedPlugins}{$type}{Dir}}=1;
    foreach my $ind (keys %{$self->{cache}{SupportedPlugins}{$type}{DirMap}})
    {foreach my $x (keys %{$self->{cache}{SupportedPlugins}{$type}{DirMap}{$ind}}){$dirs{$self->{cache}{SupportedPlugins}{$type}{DirMap}{$ind}{$x}}=1;}}
  }
  return keys(%dirs);
}

sub getPluginData ()
{
  my $self=shift;
  my $key=shift || return "";
  my $type=lc(shift) || $self->getDefaultPluginType ();
  my $val="";
  if($type && (exists $self->{cache}{SupportedPlugins}{$type}) && exists $self->{cache}{SupportedPlugins}{$type}{$key}){$val=$self->{cache}{SupportedPlugins}{$type}{$key};}
  return $val;
}

sub getPluginTypes ()
{
  my $self=shift;
  return keys %{$self->{cache}{SupportedPlugins}};
}

sub setProjectDefaultPluginType ()
{
  my $self=shift;
  my $type=lc(shift) || $self->{cache}{DefaultPluginType};
  if($type && (!exists $self->{cache}{SupportedPlugins}{$type}))
  {
    print STDERR "****ERROR: Invalid plugin type \"$type\". Currently supported plugins are:",join(",",sort keys %{$self->{cache}{SupportedPlugins}}),".\n";
    return;
  }
  $self->{cache}{DefaultPluginType}=$type;
  return;
}

sub setDefaultPluginType ()
{
  my $self=shift;
  my $type=lc(shift) || $self->{cache}{DefaultPluginType};
  if($type && (!exists $self->{cache}{SupportedPlugins}{$type}))
  {
    my $core=$self->{core};
    my @bf=keys %{$core->bfdeps()};
    print STDERR "****ERROR: Invalid plugin type \"$type\". Currently supported plugins are:",join(",",sort keys %{$self->{cache}{SupportedPlugins}}),".\n";
    print STDERR "           Please fix the \"$bf[@bf-1]\" file first. For now no plugin will be generated for this product.\n";
    $type="";
  }
  $self->{context}->stash()->set('plugin_type',$type);
  return;
}

sub getDefaultPluginType ()
{
  my $self=shift;
  my $type="";
  if(exists $self->{cache}{DefaultPluginType}){$type=$self->{cache}{DefaultPluginType};}
  return $type;
}

sub checkPluginFlag ()
{
  my $self=shift;
  my $stash=$self->{context}->stash();
  my $core=$self->{core};
  my $path = $stash->get('path');
  my $libname=$stash->get('safename');
  my @bf=keys %{$core->bfdeps()};
  my $flags=$core->allflags();
  my $err=0;
  my $plugintype=$stash->get('plugin_type');
  my $plugin=0;
  if ($plugintype ne "")
  {
    $plugin=1;
    $plugintype=lc($plugintype);
    if(!exists $self->{cache}{SupportedPlugins}{$plugintype})
    {
      $err=1;
      print STDERR "****ERROR: Plugin type \"$plugintype\" not supported. Currently available plugins are:",join(",",sort keys %{$self->{cache}{SupportedPlugins}}),".\n";
      $plugintype="";
    }
  }
  else
  {
    my %xflags=();
    foreach my $ptype (keys %{$self->{cache}{SupportedPlugins}})
    {
      foreach my $pflag (@{$self->{cache}{SupportedPlugins}{$ptype}{Flag}})
      {
        if(exists $flags->{$pflag})
        {
          $xflags{$pflag}=1;
	  $plugin=$flags->{$pflag}[0];
	  $plugintype=$ptype;
	  if($plugin!~/^[01]$/)
	  {
            print STDERR "****ERROR: Only allowed values for \"$pflag\" flag are \"0\" OR \"1\". Please fix this for \"$libname\" library in \"$bf[@bf-1]\" file.\n";
            $err=1;
	  }
	}
      }
    }
    if(scalar(keys %xflags)>1)
    {
      print STDERR "****ERROR: More than one plugin flags\n";
      foreach my $f (keys %xflags){print STDERR "             $f\n";}
      print STDERR "           are set for \"$libname\" library in \"$bf[@bf-1]\" file.\n";
      print STDERR "           You only need to provide one flag. Please fix this first otherwise plugin will not be registered.\n";
      $err=1;
    }
    if($plugintype eq "")
    {
      foreach my $t (keys %{$self->{cache}{SupportedPlugins}})
      {
        my $exp=$self->{cache}{SupportedPlugins}{$t}{DefaultDirName};
        if($path=~/$exp/)
        {
          if(exists $flags->{DEFAULT_PLUGIN})
          {
  	    $self->setDefaultPluginType($flags->{DEFAULT_PLUGIN});
	    $plugintype=$stash->get('plugin_type');
	    if($plugintype eq ""){$err=1;}
	  }
	  else{$plugintype=$self->{cache}{DefaultPluginType};}
	  $plugin=1;
	  last;
        }
      }
    }
    if ($plugintype eq "")
    {
      if(exists $flags->{DEFAULT_PLUGIN})
      {
        $self->setDefaultPluginType($flags->{DEFAULT_PLUGIN});
	$plugintype=$stash->get('plugin_type');
	if($plugintype eq ""){$err=1;}
      }
    }
  }
  my $pnf = $stash->get('plugin_name_force');
  my $pn = $stash->get('plugin_name');
  if(($plugintype eq "") && ($pn ne "")){$plugintype=$self->{cache}{DefaultPluginType};$plugin=1;}
  
  if($plugin == 1){if($pn eq ""){$pn=$libname;}}
  if(($pn ne "") && ($pnf eq "") && ($libname ne $pn))
  {
    print STDERR "****ERROR: Plugin name should be same as the library name. Please fix the \"$bf[@bf-1]\" file and replace \"$pn\" with \"$libname\"\n";
    print STDERR "           Please fix the above error otherwise library \"$libname\" will not be registered as plugin.\n";
    $err=1;
  }
  if($err)
  {
    if(!$self->isReleaseArea()){exit 1;}
    else{$stash->set('plugin_name', $pn);return;}
  }
  
  $stash->set('plugin_name', $pn);
  if($pn ne "")
  {
    my $pd = $self->{cache}{SupportedPlugins}{$plugintype}{Dir};
    my $f=0;
    foreach my $ind (sort {$a <=> $b} keys %{$self->{cache}{SupportedPlugins}{$plugintype}{DirMap}})
    {
      foreach my $reg (keys %{$self->{cache}{SupportedPlugins}{$plugintype}{DirMap}{$ind}})
      {if($path=~/$reg/){$pd=$self->{cache}{SupportedPlugins}{$plugintype}{DirMap}{$ind}{$reg}; $f=1;last;}}
      if($f){last;}
    }
    $stash->set('plugin_type', $plugintype);
    $stash->set('plugin_dir',$pd);
    my $nexp=$self->{cache}{SupportedPlugins}{$plugintype}{Name};
    my $name=$pn;
    eval $nexp;
    $stash->set('plugin_product', $name);
    $stash->set("no_shared_lib_copy",$self->{cache}{SupportedPlugins}{$plugintype}{NoSharedLibCopy});
  }
  return;
}
######################################################
sub dumpCompilersFlags()
{
  my ($self,$keys)=@_;
  #Compiler tools variables initilize
  my %allFlagsHash=();
  foreach my $flag (@{$self->{cache}{DefaultCompilerFlags}}){$allFlagsHash{$flag}=1;}
  foreach my $toolname ($self->getCompilerTypes())
  {
    my $compilers = $self->getCompilers($toolname);
    foreach my $compiler (keys %$compilers)
    {
      foreach my $flag (keys %{$self->getTool($compiler)->{FLAGS}})
      {
        if ($flag=~/^(SCRAM_.+|REM_.+|SHAREDSUFFIX|CCCOMPILER)$/){next;}
        $allFlagsHash{$flag}=1;
      }
    }
  }
  my $allFlags="";
  foreach my $flag (sort keys %allFlagsHash)
  {
    $allFlags.="$flag ";
    foreach my $type ("","REM_")
    {
      foreach my $var ("","BIN_","TEST_","EDM_","CAPABILITIES_","LCGDICT_","ROOTDICT_","DEV_", "RELEASE_"){push @$keys,"${type}${var}${flag}:=";}
    }
  }
  push @$keys,"ALL_COMPILER_FLAGS := $allFlags";
}

sub addAllVariables ()
{
  my $self=shift;
  my @keys=();
  my %skipTools=();
  push @keys, "CXXSRC_FILES_SUFFIXES     := ".join(" ",$self->getSourceExtensions("cxx"));
  push @keys, "CSRC_FILES_SUFFIXES       := ".join(" ",$self->getSourceExtensions("c"));
  push @keys, "FORTRANSRC_FILES_SUFFIXES := ".join(" ",$self->getSourceExtensions("fortran"));
  push @keys, "SRC_FILES_SUFFIXES        := \$(CXXSRC_FILES_SUFFIXES) \$(CSRC_FILES_SUFFIXES) \$(FORTRANSRC_FILES_SUFFIXES)";
  push @keys, "SCRAM_ADMIN_DIR           := .SCRAM/\$(SCRAM_ARCH)";
  push @keys, "SCRAM_TOOLS_DIR           := \$(SCRAM_ADMIN_DIR)/timestamps";
  $self->dumpCompilersFlags(\@keys);
  my $f77deps=$self->getCompiler("F77");
  $self->{cache}{InvalidUses}={};
  foreach my $f ($self->getCompilerTypes())
  {
    my $compilers = $self->getCompilers($f);
    my $c=$self->getCompiler($f);
    if ($c ne $f77deps){$self->{cache}{InvalidUses}{$c}=1;}
    foreach $c (keys %$compilers){if ($c ne $f77deps){$self->{cache}{InvalidUses}{$c}=1;}}
  }
  if ($self->isMultipleCompilerSupport())
  {
    push @keys, "SCRAM_MULTIPLE_COMPILERS := yes";
    push @keys, "SCRAM_DEFAULT_COMPILER    := ".$self->getCompiler("");
    push @keys, "SCRAM_COMPILER            := \$(SCRAM_DEFAULT_COMPILER)";
    push @keys, "ifdef COMPILER";
    push @keys, "SCRAM_COMPILER            := \$(COMPILER)";
    push @keys, "endif";
    foreach my $f ($self->getCompilerTypes()){push @keys, "${f}_TYPE_COMPILER := ".$self->getCompiler($f);}
    push @keys,"ifndef SCRAM_IGNORE_MISSING_COMPILERS";
    foreach my $f ($self->getCompilerTypes())
    {
      my $c=$self->getCompiler($f);
      push @keys, "\$(if \$(wildcard \$(SCRAM_TOOLS_DIR)/\$(SCRAM_COMPILER)-\$(${f}_TYPE_COMPILER)),,\$(info ****WARNING: You have selected \$(SCRAM_COMPILER) as compiler but there is no \$(SCRAM_COMPILER)-\$(${f}_TYPE_COMPILER) tool setup. Default compiler \$(SCRAM_DEFAULT_COMPILER)-\$(${f}_TYPE_COMPILER) will be used to comple $f files))";
    }
    push @keys,"endif";
    if((exists $self->{cache}{toolcache}) && (exists $self->{cache}{toolcache}{SETUP}))
    {
      my $defCompiler = $self->getCompiler("");
      foreach my $f ($self->getCompilerTypes())
      {
        my $c="${defCompiler}-".$self->getCompiler($f);
        if (exists $self->{cache}{toolcache}{SETUP}{$c}){$self->addVariables($c,\@keys,0,1);}
        $skipTools{$c}=1;
      }
    }
    $self->addVirtualCompilers(\@keys);
  }
  my $selfbase="$ENV{SCRAM_PROJECTNAME}_BASE";
  $self->shouldAddToolVariables($selfbase);
  push @keys,"$ENV{SCRAM_PROJECTNAME}_BASE:=$ENV{LOCALTOP}";
  $self->addVariables("self",\@keys,1);
  $skipTools{self}=1;
  foreach my $t (keys %{$self->{cache}{toolcache}{SETUP}}){if (!exists $skipTools{$t}){$self->addVariables($t,\@keys,0);}}
  return @keys;
}

sub addVariables ()
{
  my $self=shift;
  my $t=shift;
  my $keys=shift;
  my $force=shift || 0;
  my $skipCompilerCheck=shift || 0;
  my $type="global";
  my $basevar=uc($t)."_BASE"; $basevar=~s/-/_/g;
  if(exists $self->{cache}{toolcache}{SETUP}{$t}{VARIABLES})
  {
    if(exists $self->{cache}{toolcache}{SETUP}{$t}{$basevar})
    {
      push @$keys,"$basevar:=".$self->{cache}{toolcache}{SETUP}{$t}{$basevar};
      $self->{cache}{ToolVariables}{$type}{$basevar}=1;
    }
    if (($self->isMultipleCompilerSupport()) && (!$skipCompilerCheck) && ($self->{cache}{toolcache}{SETUP}{$t}{SCRAM_COMPILER}))
    {
      $type=$t;
      my $ctool=$t; $ctool=~s/\-[^-]+$//;
      push @$keys,"ifeq (\$(strip \$(SCRAM_COMPILER)),${ctool})";
    }
    foreach my $v (@{$self->{cache}{toolcache}{SETUP}{$t}{VARIABLES}})
    {
      if ($v eq $basevar){next;}
      if(exists $self->{cache}{toolcache}{SETUP}{$t}{$v})
      {
        if(($force) || ($self->shouldAddToolVariables($v, $type)))
        {
          my $val=$self->{cache}{toolcache}{SETUP}{$t}{$v};
          if ($v=~/^BUILDENV_(.*)$/){$val="export $1:=$val";}
          else{$val="$v:=$val";}
          push @$keys,$val;
        }
      }
    }
    if ($type eq $t){push @$keys,"endif";} 
  }
}

sub shouldAddToolVariables()
{
  my ($self,$var,$type)=@_;
  if (!$type){$type="global";}
  if(exists $self->{cache}{ToolVariables}{$type}{$var}){return 0;}
  $self->{cache}{ToolVariables}{$type}{$var}=1;  
  return 1;
}

sub shouldAddMakeData ()
{
  my $self=shift;
  my $stash=$self->{context}->stash();
  if(exists $stash->{nomake_data}){return 0;}
  return 1;
}

sub shouldRunMoc ()
{
  my $self=shift;
  my $hasmoc=0;
  if($self->isDependentOnTool(["qt"]))
  {
    my $stash=$self->{context}->stash();
    my $src=$stash->get('path');
    my $inc=$src;
    $inc=~s/\/src$/\/interface/;
    $stash->set(mocsrc => "$src");
    $stash->set(mocinc => "$inc");
    my $mocfiles="";
    foreach my $dir ($src, $inc)
    {
      my $dref;
      if(opendir($dref, $dir))
      {
         foreach my $file (readdir($dref))
	 {
	   if($file=~/^\./){next;}
	   if(-d "${dir}/${file}"){next;}
	   if($file=~/.+?\.(h|cc|cpp|cxx|C)$/)
	   {
	     my $fref;
	     if (open($fref,"${dir}/${file}"))
	     {
	       my $line;
	       while($line=<$fref>)
	       {
	         chomp $line;
	         if($line=~/Q_OBJECT/){$mocfiles.=" ${file}";$hasmoc=1;last;}
	       }
	       close($fref);
	     }
	   }
	 }
	 closedir($dref);
      }
    }
    if ($hasmoc)
    {
      $stash->set(mocfiles => "$mocfiles");
      $stash->set(mocbase => "QT_BASE");
    }
  }
  return $hasmoc;
}

sub isLibSymLoadChecking ()
{
  my $self=shift;
  my $flag=$self->{core}->flags("NO_LIB_CHECKING");
  if(($flag!~/^yes$/i) && ($flag ne "1")){$flag="";}
  else{$flag="no";}
  return $flag;
}

sub getLocalBuildFile ()
{
  my $self=shift;
  my $path=$self->{context}->stash()->get('path');
  if (exists $self->{cache}{BuildFileMap}{$path}){return $self->{cache}{BuildFileMap}{$path};}
  my $bn=$self->{cache}{BuildFile};
  my $bf="${path}/${bn}.xml";
  if(!-f $bf)
  {
    $bf="${path}/${bn}";
    if (!-f $bf)
    {
      my $pub = $self->{core}->publictype();
      if ($pub)
      {
        $path=dirname($path);
        $bf="${path}/${bn}.xml";
	if (!-f $bf){$bf="${path}/${bn}";}
      }
    }
  }
  if(!-f $bf){$bf="";}
  else
  {
    if ($bf!~/\.xml$/)
    {
      if(!exists $self->{WrongBF}{$bf})
      {
        $self->{WrongBF}{$bf}=1;
	if (exists $self->{FH}){my $fh=$self->{FH}; print $fh "NON_XML_BUILDFILE += $bf\n";}
	else{print STDERR "**** ERROR: non-xml BuildFile are obsolete now. Please convert $bf to xml format using \"scram build -c\" command.\n";}
      }
    }
    $bf=~s/\.xml$//;
  }
  $self->{cache}{BuildFileMap}{$path}=$bf;
  return $bf;
}

sub setBuildFileName ()
{
  my $self=shift;
  my $file=shift || return;
  $self->{cache}{BuildFile}=$file;
  return ;
}

sub getBuildFileName ()
{
  my $self=shift;
  return $self->{cache}{BuildFile};
}

sub isMultipleCompilerSupport(){my ($self)=@_; return $self->{cache}{MultipleCompilerSupport};}

sub setCompiler ()
{
  my $self=shift;
  my $type=uc(shift);
  my $compiler=lc(shift);
  $self->{cache}{"${type}Compiler"}=$compiler;
  return ;
}

sub getCompiler ()
{
  my $self=shift;
  my $type=uc(shift);
  if (exists $self->{cache}{"${type}Compiler"}){return  $self->{cache}{"${type}Compiler"};}
  return "";
}

sub getCompilers ()
{
  my $self=shift;
  my $type=uc(shift);
  if (exists $self->{cache}{Compilers}{$type}){return $self->{cache}{Compilers}{$type};}
  my $defCompiler=$self->getCompiler($type);
  if ($self->isMultipleCompilerSupport())
  {
    if((exists $self->{cache}{toolcache}) && (exists $self->{cache}{toolcache}{SETUP}))
    {
      foreach my $t (keys %{$self->{cache}{toolcache}{SETUP}})
      {
        if (exists $self->{cache}{toolcache}{SETUP}{$t}{SCRAM_COMPILER})
        {
	  if ($t=~/^(.+)-${defCompiler}$/){$self->{cache}{Compilers}{$type}{$t}=$1;}
        }
      }
    }
  }
  else{$self->{cache}{Compilers}{$type}{$defCompiler}="";}
  return $self->{cache}{Compilers}{$type};
}

sub getCompilerTypes(){my ($self)=@_;return @{$self->{cache}{CompilerTypes}};}

sub addVirtualCompilers()
{
  my ($self,$keys)=@_;
  foreach my $f ($self->getCompilerTypes())
  {
    my $c=$self->getCompiler($f);
    push @$keys,"ALL_TOOLS  += $c";
    push @$keys,"${c}_EX_USE    := \$(if \$(strip \$(wildcard \$(LOCALTOP)/\$(SCRAM_TOOLS_DIR)/\$(SCRAM_COMPILER)-$c)),\$(SCRAM_COMPILER)-$c,\$(SCRAM_DEFAULT_COMPILER)-$c)";
  }
}

sub isReleaseArea ()
{
  my $self=shift;
  return $self->{cache}{ReleaseArea};
}

sub hasPythonscripts ()
{
  my $self=shift;
  my $stash=$self->{context}->stash();
  my $path=&fixPath($stash->get('path'));
  my $pythonprod={};
  my $flags=$self->{core}->allflags();
  if(exists $flags->{PYTHONPRODUCT})
  {
    my $flags1=$flags->{PYTHONPRODUCT};
    my $bfile=$self->getLocalBuildFile();
    my $xfiles=[];
    my $xdirs=[];
    foreach my $p (@$flags1)
    {
      my @files=split /,/,$p;
      my $count=scalar(@files);
      if($count==1){push @files,"";$count++;}
      if($count==0){print STDERR "ERROR: Invalid use of \"PYTHONPRODUCT\" flag in \"$bfile\" file. Please correct it.\n";}
      else
      {
        my $des=$self->{cache}{PythonProductStore}."/".$files[$count-1];
	$des=~s/\/+$//;
	pop @files;
	my $list="";
	foreach my $fs (@files)
	{
	  foreach my $f (split /\s+/,$fs)
	  {
	    $f=&fixPath("${path}/${f}");
	    if(!-f $f){print STDERR "ERROR: No such file \"$f\" for \"PYTHONPRODUCT\" flag in \"$bfile\" file. Please correct it.\n";}
	    else{$pythonprod->{$f}=1;push @$xfiles,$f;push @$xdirs,$des;}
	  }
	}
      }
    }
    $stash->set("xpythonfiles",$xfiles);
    $stash->set("xpythondirs",$xdirs);
  }
  my $scripts = 0;
  if($self->{cache}{SymLinkPython} == 0)
  {
    foreach my $f (@{&readDir($path,2,-1)})
    {
      if(exists $pythonprod->{$f}){next;}
      if($f=~/\.py$/)
      {$scripts = 1;last;}
    }
  }
  else{$scripts=1;}
  $stash->set(hasscripts => $scripts);
  return $scripts;
}

sub symlinkPythonDirectory ()
{
  my $self=shift;
  $self->{cache}{SymLinkPython}=shift;
  return;
}

sub isSymlinkPythonDirectory ()
{
  my $self=shift;
  return $self->{cache}{SymLinkPython};
}

sub isRuleCheckerEnabled ()
{
  my $self=shift;
  my $res=0;
  if ((exists $ENV{CMS_RULECHECKER_ENABLED}) && ($ENV{CMS_RULECHECKER_ENABLED}=~/^(yes|1)$/i))
  {
    my $path=$self->{context}->stash()->get('path');
    if($path=~/\/src$/){$res=1;}
  }
  return $res;
}

sub isCodeGen ()
{
  my $self=shift;
  my $res=0;
  my $path=$self->{context}->stash()->get('path');
  foreach my $f (@{&readDir($path,2,1)})
  {if($f=~/\/.+?\.desc\.xml$/){$res=1;last;}}
  return $res;
}

sub setLibPath ()
{
  my $self=shift;
  my $stash=$self->{context}->stash();
  my $path = $stash->get('path');
  if($path=~/src\/.+\/src$/){$stash->set('libpath', 1);}
  else{$stash->set('libpath', 0);}
  return;
}
sub searchLexYacc ()
{
  my $self=shift;
  my $stash=$self->{context}->stash();
  my $lex="";
  my $parse="";
  my $path = $stash->get('path');
  foreach my $f (@{&readDir($path,2,1)})
  {
    if($f=~/\/.+?lex\.l$/){$lex.=" $f";}
    elsif($f=~/\/.+?parse\.y$/){$parse.=" $f";}
  }
  $stash->set(lexyacc => $lex);
  $stash->set(parseyacc => $parse);
  if($lex || $parse){return 1;}
  return 0;
}

sub autoGenerateClassesH ()
{
  my $self=shift;
  $self->{cache}{AutoGenerateClassesHeader}=shift;
}

sub isAutoGenerateClassesH ()
{
  my $self=shift;
  return $self->{cache}{AutoGenerateClassesHeader};
}

sub searchLCGRootDict ()
{
  my $self=shift;
  my $stash=$self->{context}->stash();
  my $core=$self->{core};
  my $stubdir="";
  my $lcgheader=[];
  my $lcgxml=[];
  my $rootmap=0;
  my $genreflex_args="\$(GENREFLEX_ARGS)";
  my $rootdict="";
  my $path=$stash->get('path');
  my $dir=$path;
  my $top=$ENV{LOCALTOP};
  my @files=split /\s+/,$core->productfiles();
  my $flag=0;
  if(scalar(@files)>0)
  {
    my $firstfile=$files[0];
    if($firstfile=~/^(.+?)\/[^\/]+$/){$stubdir=$1;$dir.="/$stubdir";}
  }
  my $hfile=$core->flags("LCG_DICT_HEADER");
  my $xfile=$core->flags("LCG_DICT_XML");
  my %xmldef=();
  if($hfile=~/^\s*$/)
  {
    if($stubdir ne ""){$hfile="${stubdir}/classes.h";}
    else{$hfile="classes.h";}
  }
  if($xfile=~/^\s*$/)
  {
    if($stubdir ne ""){$xfile="${stubdir}/classes_def.xml";}
    else{$xfile="classes_def.xml";}
  }
  my @h=();
  my @x=();
  foreach my $f (split /\s+/,$xfile){push @x,"${path}/${f}";}
  foreach my $f (split /\s+/,$hfile){push @h,"${path}/${f}";}
  my $xc=scalar(@x);
  if ((scalar(@h) == $xc) && ($xc>0))
  {
    for(my $i=0;$i<$xc;$i++)
    {
      if (-f $x[$i])
      {
        if (-f $h[$i]){$xmldef{$x[$i]}=$h[$i];}
	elsif($self->isAutoGenerateClassesH()){$xmldef{$x[$i]}="\$(WORKINGDIR)/classes/".$x[$i].".h";}
	else{$xmldef{$x[$i]}="";}
      }
    }
  }
  @h=(); @x=();
  foreach my $f (keys %xmldef)
  {
    push @x,$f;
    $f=$xmldef{$f};
    if ($f){push @h,$f;}
  }
  $rootmap = $core->flags("ROOTMAP");
  if($rootmap=~/^\s*(yes|1)\s*$/i){$rootmap=1;}
  else{$rootmap=0;}
  $xc=scalar(@x);
  if ((scalar(@h) == $xc) && ($xc>0))
  {
    for(my $i=0;$i<$xc;$i++)
    {
      push @$lcgheader,$h[$i];
      push @$lcgxml,$x[$i];
    }
    my $tmp = $core->flags("GENREFLEX_ARGS");
    if($tmp=~/^\s*\-\-\s*$/){$genreflex_args="";}
    elsif($tmp!~/^\s*$/){$genreflex_args=$tmp;}
    $tmp = $core->flags("GENREFLEX_FAILES_ON_WARNS");
    if($tmp!~/^\s*(no|0)\s*$/i){$genreflex_args.=" --fail_on_warnings";}
    my $plugin=$stash->get('plugin_name');
    my $libname=$stash->get('safename');
    if(($plugin ne "") && ($plugin eq $libname))
    {
      my @bf=keys %{$stash->get('core.bfdeps()')};
      print STDERR "****ERROR: One should not set EDM_PLUGIN flag for a library which is also going to generate LCG dictionaries.\n";
      print STDERR "           Please take appropriate action to fix this by either removing the\n";
      print STDERR "           EDM_PLUGIN flag from the \"$bf[@bf-1]\" file for library \"$libname\"\n";
      print STDERR "           OR LCG DICT header/xml files for this edm plugin library.\n";
      if((exists $ENV{RELEASETOP}) && ($ENV{RELEASETOP} ne "")){exit 1;}
    }
  }
  elsif($xc>0){print STDERR "****WARNING: Not going to generate LCG DICT from \"$path\" because NO. of .h (\"$hfile\") does not match NO. of .xml (\"$xfile\") files.\n";}
  my $dref;
  my $bn=$self->{cache}{BuildFile};
  opendir($dref, $dir) || die "ERROR: Can not open \"$dir\" directory. \"${path}/${bn}\" is refering for files in this directory.";
  foreach my $file (readdir($dref))
  {
    if($file=~/.*?LinkDef\.h$/)
    {
      if($stubdir ne ""){$file="${stubdir}/${file}";}
      $rootdict.=" $file";
    }
  }
  closedir($dref);
  $stash->set('classes_def_xml', $lcgxml);
  $stash->set('classes_h', $lcgheader);
  $stash->set('rootmap', $rootmap);
  $stash->set('genreflex_args', $genreflex_args);
  $stash->set('rootdictfile', $rootdict);
  return;
}

sub fixProductName ()
{
  my $self=shift;
  my $name=shift;
  if($name=~/^.+?\/([^\/]+)$/)
  {print STDERR "WARNING: Product name should not have \"/\" in itSetting $name=>$1\n";$name=$1;}
  return $name;
}

sub getGenReflexPath ()
{
  my $self=shift;
  my $genrflx="";
  foreach my $t ("ROOTRFLX","ROOTCORE")
  {
    if(exists $self->{cache}{ToolVariables}{global}{"${t}_BASE"})
    {$genrflx="\$(${t}_BASE)/root/bin/genreflex";last;}
  }
  return $genrflx;
}

sub getRootCintPath ()
{
  my $self=shift;
  my $cint="";
  foreach my $t ("ROOTCORE", "ROOTRFLX")
  {
    if(exists $self->{cache}{ToolVariables}{global}{"${t}_BASE"})
    {$cint="\$(${t}_BASE)/bin/rootcint";last;}
  }
  return $cint;
}

sub shouldSkipForDoc ()
{
  my $self=shift;
  my $name=$self->{core}->name();
  if($name=~/^(domain|doc)$/){return 1;}
  return 0;
}

#############################################3
# Source Extenstions
sub setPythonProductStore ()
{
  my $self=shift;
  my $val=shift || return;
  $self->{cache}{PythonProductStore}=$val;
  return;
}

sub setValidSourceExtensions ()
{
  my $self=shift;
  my $stash = $self->{context}->stash();
  my $class = $stash->get('class');
  my %exts=();
  my @exttypes=$self->getSourceExtensionsTypes();
  my %unknown=();
  foreach my $t (@exttypes){$exts{$t}=[];}
  if ($class eq "LIBRARY")
  {
    foreach my $t (@exttypes)
    {
      foreach my $e ($self->getSourceExtensions($t))
      {push @{$exts{$t}},$e;}
    }
  }
  elsif($class eq "PYTHON")
  {
    foreach my $e ($self->getSourceExtensions("cxx"))
    {push @{$exts{cxx}},$e;}
  }
  foreach my $t (@exttypes)
  {
    my $tn="${t}Extensions";
    $stash->set($tn,$exts{$t});
  }
  my $un=[];
  foreach my $e (keys %unknown){push @{$un},$e;}
  $stash->set("unknownExtensions",$un);
  return;
}

sub addSourceExtensionsType()
{
  my $self=shift;
  my $type=lc(shift) || return;
  if(!exists $self->{cache}{SourceExtensions}{$type})
  {$self->{cache}{SourceExtensions}{$type}={};}
  return;
}

sub removeSourceExtensionsType()
{
  my $self=shift;
  my $type=lc(shift) || return;
  delete $self->{cache}{SourceExtensions}{$type};
  return;
}

sub getSourceExtensionsTypes()
{
  my $self=shift;
  return keys %{$self->{cache}{SourceExtensions}};
}

sub addSourceExtensions ()
{
  my $self=shift;
  my $type=lc(shift) || return;
  foreach my $e (@_)
  {$self->{cache}{SourceExtensions}{$type}{$e}=1;}
  return;
}

sub removeSourceExtensions ()
{
  my $self=shift;
  my $type=lc(shift) || return;
  if(exists $self->{cache}{SourceExtensions}{$type})
  {
    foreach my $e (@_)
    {delete $self->{cache}{SourceExtensions}{$type}{$e};}
  }
  return;
}

sub getSourceExtensions ()
{
  my $self=shift;
  my @ext=();
  my $type=lc(shift) || return @ext;
  if(exists $self->{cache}{SourceExtensions}{$type})
  {@ext=keys %{$self->{cache}{SourceExtensions}{$type}};}
  return @ext;
}

sub getSourceExtensionsStr ()
{return join(" ",&getSourceExtensions(@_));}
#########################################
sub depsOnlyBuildFile
{
  my $self=shift;
  my $stash=$self->{context}->stash();
  my $treeitem=$stash->get("branch");
  my $sname=$stash->get("safepath");
  my $cache=$treeitem->branchdata();
  if(defined $cache)
  {
    my $core=$self->{core};
    my $src=$ENV{SCRAM_SOURCEDIR};
    my $path=$stash->get("path");
    my $pack=$path; $pack=~s/^$src\///;
    my $localbf = $self->getLocalBuildFile();
    my $fname=".SCRAM/$ENV{SCRAM_ARCH}/MakeData/DirCache/${sname}.mk";
    my $fref;
    open($fref,">$fname") || die "Can not open file for writing: $fname";
    print $fref "ifeq (\$(strip \$($pack)),)\n";
    print $fref "$sname := self/${pack}\n";
    print $fref "$pack  := $sname\n";
    print $fref "${sname}_BuildFile    := \$(WORKINGDIR)/cache/bf/${localbf}\n";
    my $ex=$self->{core}->data("EXPORT");
    foreach my $data ("INCLUDE", "USE")
    {
      my %udata=();
      foreach my $d (split ' ',$self->getCacheData($data)){$udata{$d}=1;}
      my $dataval=$self->fixData($core->value($data),$data,$localbf) || [];
      foreach my $d (@$dataval){$udata{$d}=1;}
      if (defined $ex)
      {
	$dataval=$self->fixData($self->{core}->value($data,$ex),$data,$localbf,1) || [];
	foreach my $d (@$dataval){$udata{$d}=1;}
      }
      $dataval=join(" ",keys %udata);
      if ($dataval!~/^\s*$/){print $fref "${sname}_LOC_${data} := $dataval\n";}
    }
    print $fref "${sname}_EX_USE   := \$(foreach d,\$(${sname}_LOC_USE),\$(if \$(\$(d)_EX_FLAGS_NO_RECURSIVE_EXPORT),,\$d))\n";
    print $fref "ALL_EXTERNAL_PRODS += ${sname}\n";
    print $fref "${sname}_INIT_FUNC += \$\$(eval \$\$(call EmptyPackage,$sname,$path))\nendif\n\n";
    close($fref);
    $treeitem->{MKDIR}{"$ENV{LOCALTOP}/.SCRAM/$ENV{SCRAM_ARCH}/MakeData/DirCache"}=1;
  }
  else
  {
    my $fname=".SCRAM/$ENV{SCRAM_ARCH}/MakeData/DirCache/${sname}.mk";
    if (-f $fname)
    {
      unlink $fname;
      $treeitem->{MKDIR}{"$ENV{LOCALTOP}/.SCRAM/$ENV{SCRAM_ARCH}/MakeData/DirCache"}=1;
    }
  }
  return;
}

#########################################
# Util functions
sub fixPath ()
{
  my $dir=shift;
  my @parts=();
  my $p="/";
  if($dir!~/^\//){$p="";}
  foreach my $part (split /\//, $dir)
  {
    if($part eq ".."){pop @parts;}
    elsif(($part ne "") && ($part ne ".")){push @parts, $part;}
  }
  return "$p".join("/",@parts);
}

sub findActualPath ()
{
  my $file=shift;
  if(-l $file)
  {
    my $dir=dirname($file);
    $file=readlink($file);
    if($file!~/^\//){$file="${dir}/${file}";}
    return &findActualPath($file);
  }
  return $file;
}

sub readDir ()
{
  my $dir=shift;
  my $type=shift;
  my $depth=shift;
  my $data=shift || undef;
  if(!defined $type){$type=0;}
  if(!defined $depth){$type=1;}
  my $first=0;
  if(!defined $data){$data=[];$first=1;}
  my $dref;
  opendir($dref,$dir) || die "Can not open directory $dir for reading.";
  foreach my $f (readdir($dref))
  {
    if(($f eq ".") || ($f eq "..")){next;}
    $f="${dir}/${f}";
    if((-d "$f") && ($depth != 1))
    {&readDir("$f",$type,$depth-1,$data);}
    if($type == 0){push @$data,$f;}
    elsif(($type == 1) && (-d "$f")){push @$data,$f;}
    elsif(($type == 2) && (-f "$f")){push @$data,$f;}
  }
  closedir($dref);
  if($first){return $data;}
}

sub runToolFunction ()
{
  my $self=shift;
  my $func=shift || return "";
  my $tool=shift || "self";
  if($tool eq "self"){$tool=$ENV{SCRAM_PROJECTNAME};}
  $tool=lc($tool);
  $func.="_${tool}";
  if(exists &$func){return $self->$func(@_);}
  return "";
}

sub runTemplate ()
{
  my $self=shift;
  my $func=shift || return "";
  my $ret=0;
  if (defined $self->{cache}{ProjectPlugin})
  {
    eval {$ret=$self->{cache}{ProjectPlugin}->$func(@_);};
    if(!$@){return $ret;}
  }
  eval {$ret=$self->$func(@_);};
  if ($@){print STDERR "****ERROR:Unable to run $func. Template/Function not found.\n";}
  return $ret;
}

#############################################
# generating library safe name for a package
#############################################
sub safename_coral ()  {return &safename_CMSProjects(shift,"safename_SubsystemPackageBased",shift);}
sub safename_cmssw ()  {return &safename_CMSProjects(shift,"safename_SubsystemPackageBased",shift);}
sub safename_default (){return &safename_CMSProjects(shift,"safename_SubsystemPackageBased",shift);}

sub safename_CMSProjects ()
{
  my $self=shift;
  my $func=shift;
  my $dir=shift;
  my $class=$self->{context}->stash()->get('class');
  my $val = "";
  if (($class eq "LIBRARY") || ($class eq "PYTHON"))
  {
    my $src=$ENV{SCRAM_SOURCEDIR};
    my $rel=quotemeta($ENV{LOCALTOP});
    $dir=dirname($dir);
    $dir=~s/^${rel}\/${src}\/(.+)$/$1/;
    $val=&$func($dir);
    if($class eq "PYTHON"){$val="Py$val";}
  }
  return $val;
}

sub safename_PackageBased ()
{
  my $dir=shift;
  if($dir=~/^([^\/]+)\/([^\/]+)$/){return "${2}";}
  return "";
}

sub safename_SubsystemPackageBased ()
{
  my $dir=shift;
  my $name="";
  if($dir=~/^([^\/]+)\/([^\/]+)$/)
  {
    if ($1 eq "LCG"){$name="lcg_${2}";}
    else{$name="${1}${2}";}
  }
  return $name;
}
########################################
sub addCacheData ()
{
  my $self=shift;
  my $name=shift || return;
  my $value=shift || "";
  $self->{cache}{CacheData}{$name}=$value;
  return;
}

sub getCacheData ()
{
  my $self=shift;
  my $name=shift || return "";
  if(exists $self->{cache}{CacheData}{$name}){return $self->{cache}{CacheData}{$name};}
  return "";
}

#####################################
sub updateEnvVarMK
{
  my $self=shift;
  my $ltop=$self->{cache}{LocalTop};
  my $mkfile="${ltop}/.SCRAM/$ENV{SCRAM_ARCH}/MakeData/variables.mk";
  my $fref;
  open($fref,">$mkfile") || die "Can not open file for writing: $mkfile";
  print $fref "############## All Tools Variables ################\n";
  foreach my $var ($self->addAllVariables())
  {print $fref "$var\n";}
  my $env=$self->{context}->stash()->get('environment');
  print $fref "############## All SCRAM ENV variables ################\n";
  foreach my $key (keys %$env)
  {
    if($key=~/^(SCRAMV1_.+|SCRAM)$/){next;}
    if($key=~/^SCRAM_(BUILDVERBOSE|NOPLUGINREFRESH|NOSYMCHECK|NOLOADCHECK|TOOL_HOME|VERSION|LOOKUPDB|SYMLINKS)$/){next;}
    if(!$self->shouldAddToolVariables($key)){next;}
    print $fref "$key:=".$env->{$key}."\n";
  }
  my $core=
  my $stores=$self->{core}->allscramstores();
  if(ref($stores) eq "HASH")
  {
    print $fref "################ ALL SCRAM Stores #######################\n";
    print $fref "ALL_PRODUCT_STORES:=\n";
    foreach my $store (keys %{$stores})
    {
      print $fref "$store:=".$stores->{$store}."\n";
      print $fref "ALL_PRODUCT_STORES+=\$($store)\n";
    }
  }
  close($fref);
  return;
}

######################################
# Template initialization for different levels
sub initTemplate_PROJECT ()
{
  my $self=shift;
  my $ltop=$ENV{LOCALTOP};
  my $odir=$ltop;
  my $dbext=$self->{dbext};
  if(-f ".SCRAM/$ENV{SCRAM_ARCH}/ToolCache.${dbext}")
  {
    $self->{cache}{toolcache}=&Cache::CacheUtilities::read(".SCRAM/$ENV{SCRAM_ARCH}/ToolCache.${dbext}");
    my $odir1=$self->{cache}{toolcache}{topdir};
    if($odir1 ne ""){$odir=$odir1;}
  }
  my $stash=$self->{context}->stash();
  my $defcompiler="";
  if ((exists $self->{cache}{toolcache}{SETUP}{self}{FLAGS}) && (exists $self->{cache}{toolcache}{SETUP}{self}{FLAGS}{DEFAULT_COMPILER}))
  {$defcompiler=$self->{cache}{toolcache}{SETUP}{self}{FLAGS}{DEFAULT_COMPILER}[0];}
  if ($defcompiler eq ""){$defcompiler="gcc";}
  $self->{cache}{Compiler}=$defcompiler;
  $self->{cache}{CompilerTypes}=[];
  $self->{cache}{DefaultCompilerFlags}=[];
  foreach my $flag ("CXXFLAGS","CFLAGS","FFLAGS","CPPDEFINES","LDFLAGS","CPPFLAGS"){push @{$self->{cache}{DefaultCompilerFlags}},$flag;}
  foreach  my $type ("CXX","C","F77")
  {
    push @{$self->{cache}{CompilerTypes}},$type;
    $self->{cache}{"${type}Compiler"}=lc($type)."compiler";
  }
  $self->{cache}{MultipleCompilerSupport}=!$self->isToolAvailable($self->{cache}{CXXCompiler}) || 0;
  $self->{cache}{SymLinkPython}=0;
  $self->{cache}{ProjectName}=$ENV{SCRAM_PROJECTNAME};
  $self->{cache}{LocalTop}=$ltop;
  $self->{cache}{ProjectConfig}="$ENV{SCRAM_CONFIGDIR}";
  $self->{cache}{AutoGenerateClassesHeader}=0;
  $self->{cache}{BuildProducts}="BuildProducts";
  $self->initTemplate_common2all();
  $stash->set('ProjectLOCALTOP',$ltop);
  $stash->set('ProjectOldPath',$odir);
  my $env=$stash->get('environment');
  $env->{SCRAM_INIT_LOCALTOP}=$odir;
  my $bdir="${ltop}/$ENV{SCRAM_INTwork}/cache";
  system("mkdir -p ${bdir}/prod ${bdir}/bf ${bdir}/log");
  if((exists $ENV{RELEASETOP}) && ($ENV{RELEASETOP} ne "")){$stash->set('releasearea',0);$self->{cache}{ReleaseArea}=0;}
  else{$stash->set('releasearea',1);$self->{cache}{ReleaseArea}=1;}
  if(!-d "${ltop}/external/$ENV{SCRAM_ARCH}")
  {
    system("${ltop}/$ENV{SCRAM_CONFIGDIR}/SCRAM/linkexternal.pl --arch $ENV{SCRAM_ARCH}");
    system("mkdir -p ${ltop}/external/$ENV{SCRAM_ARCH}");
  }
  $self->{cache}{LCGProjectLibPrefix}="lcg_";
  $self->setRootReflex ("rootrflx");
  if ((exists $ENV{SCRAM_BUILDFILE}) && ($ENV{SCRAM_BUILDFILE} ne ""))
  {$self->{cache}{BuildFile}=$ENV{SCRAM_BUILDFILE};}
  else{$self->{cache}{BuildFile}="BuildFile";}
  my $plugin="SCRAM_ExtraBuildRule";
  eval "require $plugin";
  if(!$@){$self->{cache}{ProjectPlugin}=$plugin->new($self);}
  else{$self->{cache}{ProjectPlugin}=undef;}
  return;
}

sub initTemplate_LIBRARY ()
{
  my $self=shift;
  $self->initTemplate_common2all();
  my $stash=$self->{context}->stash(); 
  my $path=$stash->get('path');
  my $sname=$self->runToolFunction("safename","self", "$ENV{LOCALTOP}/${path}");
  if($sname eq "")
  {
    $self->processTemplate("Safename_generator");
    $sname=$stash->get('safename');
    if($sname eq ""){$sname=$self->runToolFunction("safename","default", "$ENV{LOCALTOP}/${path}");}
  }
  if($sname ne ""){$stash->set("safename", $sname);}
  else
  {
    print STDERR "*** ERROR: Unable to generate library safename for package \"$path\" of project $ENV{SCRAM_PROJECTNAME}\n";
    print STDERR "    Please send email to hn-cms-sw-develtools\@cern.ch\n";
    exit 1;
  }
  return;
}

sub initTemplate_PYTHON ()
{return &initTemplate_LIBRARY(shift);}
 
sub initTemplate_common2all ()
{
  my $self=shift;
  my $stash=$self->{context}->stash();
  $stash->set("ProjectName",$self->{cache}{ProjectName});
  $stash->set("ProjectConfig",$self->{cache}{ProjectConfig});
  return;
}

##########################################################
sub SubSystem_template()
{
  my $self=shift;
  $self->initTemplate_common2all();
  my $fh=$self->{FH};
  print $fh "ALL_SUBSYSTEMS+=\$(patsubst src/%,%,",$self->get("path"),")\n";
  print $fh "subdirs_",$self->get("safepath")," = ",$self->core()->safesubdirs(),"\n";
  return 1;
}

sub Package_template()
{
  my $self=shift;
  $self->initTemplate_common2all();
  my $fh=$self->{FH};
  my $path=$self->get("path");
  if($self->get("suffix") eq "")
  {
    $self->depsOnlyBuildFile();
    my $safepath=$self->get("safepath");
    print $fh "ALL_PACKAGES += \$(patsubst src/%,%,$path)\n";
    print $fh "subdirs_${safepath} := ",$self->core()->safesubdirs(),"\n";
  }
  else{print $fh "all_empty_packages += \$(patsubst src/%,%,$path)\n";}
  return 1;  
}

sub Documentation_template()
{return &SubSystem_template(shift);}

sub Project_template()
{
  my $self=shift;
  $self->initTemplate_PROJECT ();
  my $core=$self->core();
  my $path=$self->get("path");
  my $safepath=$self->get("safepath");
  my $fh=$self->{FH};
  $self->setPythonProductStore('$(SCRAMSTORENAME_PYTHON)');
  #$self->addPluginSupport(plugin-type,plugin-flag,plugin-refresh-cmd,dir-regexp-for-default-plugins,plugin-store-variable,plugin-cache-file,plugin-name-exp,no-copy-shared-lib)
  #$self->addProductDirMap (prod-type,regexp-prod-src-path,prod-store,search-index (default is 100, samller index means those regexp will be matched first)
  foreach my $type ("lib","bin","test","python","logs","include")
  {$self->addProductDirMap ($type,'.+',"SCRAMSTORENAME_".uc($type));}
  $self->addProductDirMap ("scripts",'.+',"SCRAMSTORENAME_BIN");
  $self->updateEnvVarMK();
  $self->object2ProductsMap();
  
  if ($self->isBigLibs()){print $fh "BIGLIBS := yes\n";}
  else{print $fh "BIGLIBS := \n";}

  # LIB/INCLUDE/USE from toplevel BuildFile
  my $proj=lc($self->{cache}{ProjectName});
  if (!$self->isToolAvailable($proj)){$proj="";}
  foreach my $var ("LIB","INCLUDE","USE")
  {
    print $fh "$var :=\n";
    my $val=$self->fixData($core->value($var),$var,"");
    if(($var eq "USE") && ($self->hasData($val,"self")==0))
    {
      if ($val eq ""){$val = ["self"];}
      else{unshift @$val,"self";}
    }
    if ($val ne "")
    {
      my $vals=join(" ",@$val);
      if ($var eq "USE"){$vals="$vals $proj";}
      print $fh "$var += $vals\n";
      $self->addCacheData($var,$vals);
    }
  }
  foreach my $tn ($self->getCompilerTypes())
  {
    my $c = $self->getCompiler($tn);
    print $fh "\$(foreach f,\$(ALL_COMPILER_FLAGS),\$(eval \$f += \$(${c}_EX_FLAGS_\$f_ALL)))\n";
    print $fh "\$(foreach f,\$(ALL_COMPILER_FLAGS),\$(eval REM_\$f += \$(${c}_EX_FLAGS_REM_\$f_ALL)))\n";
  }
  my $rflx=$self->getRootReflex();
  if ($rflx ne "")
  {
    print $fh "LCGDICT_DEPS := $rflx\n";
    my $tool = $self->getTool($rflx);
    foreach my $flag (keys %{$tool->{FLAGS}})
    {
      if ($flag=~/^GENREFLEX_/){print $fh "$flag := \$(${rflx}_EX_FLAGS_${flag})\n";}
    }
  }
  
  # All flags from top level BuildFile
  my $flags=$core->allflags();
  foreach my $flag (keys %$flags)
  {
    my $val=join(" ",@{$flags->{$flag}});
    if ($flag=~/^HOOK_.+$/){print $fh "$flag:=$val\n";}
    else{print $fh "$flag+=$val\n";}
  }

  # Makefile section of toplevel BuildFile
  my $mk=$core->value("MAKEFILE");
  if($mk ne ""){foreach my $line (@$mk){print $fh "$line\n";}}
  print $fh "\n\n";
  
  print $fh "ifeq (\$(strip \$(GENREFLEX)),)\n",
            "GENREFLEX:=",$self->getGenReflexPath(),"\n",
            "endif\n",
            "ifeq (\$(strip \$(GENREFLEX_CPPFLAGS)),)\n",
            "GENREFLEX_CPPFLAGS:=-DCMS_DICT_IMPL -D_REENTRANT -DGNU_SOURCE\n",
            "endif\n",
            "ifeq (\$(strip \$(GENREFLEX_ARGS)),)\n",
            "GENREFLEX_ARGS:=--deep\n",
            "endif\n",
            "ifeq (\$(strip \$(ROOTCINT)),)\n",
            "ROOTCINT:=",$self->getRootCintPath(),"\n",
            "endif\n",
	    "\n",
            "LIBDIR+=\$(self_EX_LIBDIR)\n",
            "ifdef RELEASETOP\n",
            "ifeq (\$(strip \$(wildcard \$(RELEASETOP)/\$(PUB_DIRCACHE_MKDIR)/DirCache.mk)),)\n",
            "\$(error Release area has been removed/modified as \$(RELEASETOP)/\$(PUB_DIRCACHE_MKDIR)/DirCache.mk is missing.)\n",
            "endif\n",
            "endif\n",
            "LIBTYPE:= ",$core->data("LIBTYPE"),"\n",
	    "\n",
            "subdirs_${safepath}+=\$(filter-out Documentation, ",$core->safesubdirs(),")\n\n";

  $self->processTemplate("Project");
  $self->createSymLinks();
  
  my %pdirs=();
  foreach my $ptype ($self->getPluginTypes())
  {foreach my $dir ($self->getPluginProductDirs($ptype)){$pdirs{$dir}=1;}}
  foreach my $ptype ($self->getPluginTypes ())
  {
    my $refreshcmd=$self->getPluginData("Refresh", $ptype);
    my $cachefile= $self->getPluginData("Cache",   $ptype);
    print $fh "PLUGIN_REFRESH_CMDS += ${refreshcmd}\n",
              "define do_${refreshcmd}\n",
              "  echo \"\@\@\@\@ Refreshing Plugins:${refreshcmd}\" &&\\\n",
              "${refreshcmd} \$(1)\n",
              "endef\n";
    foreach my $dir ($self->getPluginProductDirs($ptype))
    {
      print $fh "\$($dir)/${cachefile}: \$(SCRAM_INTwork)/cache/${ptype}_${refreshcmd} \$(SCRAM_INTwork)/cache/prod/${refreshcmd}\n",
                "\t\@if [ -f \$< ] ; then \\\n",
                "\t  if [ -f \$\@ ] ; then \\\n",
		"\t    if [ -s foo ] ; then \\\n",
		"\t      touch -t 198001010100 \$\@ ;\\\n",
		"\t    else \\\n",
		"\t      rm -f \$\@ ; \\\n",
		"\t    fi;\\\n",
		"\t  fi;\\\n",
                "\t  \$(call do_${refreshcmd},\$(\@D)) &&\\\n",
                "\t  touch \$\@ ;\\\n",
                "\tfi\n",
                "${refreshcmd}_cache := \$($dir)/${cachefile}\n";
    }
    
    print $fh "\$(SCRAM_INTwork)/cache/${ptype}_${refreshcmd}: \n",
              "\t\@:\n";
  }
  print $fh "###############################################################################\n\n";
  $self->processTemplate("Common_rules");
  return 1;
}

sub plugin_template ()
{
  my ($self,$cap)=@_;
  $self->checkPluginFlag();
  if ($self->get("plugin_name") ne "")
  {
    my $safename = $self->get("safename");
    my $ptype = $self->get("plugin_type");
    my $pname = $self->get("plugin_name");
    my $fh=$self->{FH};
    if ($self->isBigLibs())
    {
      my $class = $self->get("class");
      print $fh "${pname}_prodtype := ${class}${ptype}${cap}\n";
    }
    else{
      print $fh "${safename}_PRE_INIT_FUNC += \$\$(eval \$\$(call ${ptype}Plugin,$pname,$safename,\$(",$self->get("plugin_dir"),"),",$self->get("path"),"))\n";
    }
    if ($safename eq $pname){$self->swapMakefile();}
  }
  return;
}

sub rootmap ()
{
  my $self=shift;
  my $rootmap=shift || 0;
  if ($rootmap=~/^\s*(1|yes)\s*$/i)
  {
    my $sname=$self->get("safename");
    my $fh=$self->{FH};
    print $fh "${sname}_PRE_INIT_FUNC += \$\$(eval \$\$(call RootMap,$sname,\$(",$self->getProductStore("lib"),")))\n";
  }
}

sub dict_template()
{
  my $self=shift;
  $self->searchLCGRootDict();
  my $x=$self->get("classes_h");
  if(scalar(@$x)>0)
  {
    $self->pushstash();
    $self->lcgdict_template();
    $self->popstash();
  }
  else{$self->rootmap($self->get("rootmap"));}
  if ($self->get("rootdictfile") ne "")
  {
    $self->pushstash();
    $self->rootdict_template();
    $self->popstash();
  }
  return 1;
}

sub rootdict_template()
{
  my $self=shift;
  my $safename=$self->get("safename");
  my $path=$self->get("path");
  my $rootdictfile=$self->get("rootdictfile");
  my $fh=$self->{FH};
  print $fh "${safename}_PRE_INIT_FUNC += \$\$(eval \$\$(call RootDict,${safename},${path},${rootdictfile}))\n";
}

sub lcgdict_template()
{
  my $self=shift;
  my $safename=$self->get("safename");
  my $class=$self->get("class");
  my $fh=$self->{FH};
  my $ptype=$self->getLCGCapabilitiesPluginType();
  my $capabilities="";
  if ($ptype ne "")
  {
    $self->set("plugin_name","${safename}Capabilities");
    $self->pushstash();
    $self->set("plugin_name_force",1);
    $self->set("plugin_type",$self->getLCGCapabilitiesPluginType());
    $self->plugin_template("cap");
    $self->popstash();
    $capabilities="Capabilities";
    if ($self->isBigLibs())
    {
      my $path=$self->get("path");
      my $bprod = $self->getObjectProducts("${path}/${safename}");
      print $fh "${bprod}_bigobjs += ${safename}${capabilities}\n";
    }
  }
  print $fh "${safename}_PRE_INIT_FUNC += \$\$(eval \$\$(call LCGDict,${safename},",$self->get("rootmap"),",",
	    join(" ",@{$self->get("classes_h")}),",",join(" ",@{$self->get("classes_def_xml")}),",",
	    "\$(",$self->getProductStore("lib"),"),",$self->get("genreflex_args"),",$capabilities))\n";
}

sub library_template ()
{
  my $self=shift;
  if($self->get("suffix") ne ""){return 1;}
  $self->initTemplate_LIBRARY ();
  my $core=$self->core();
  my $types=$core->buildproducts();
  if($types)
  {
    foreach my $type (keys %$types)
    {
      $self->set("type",$type);
      $self->unsupportedProductType();
    }
  }
  my $path=$self->get("path"); my $safepath=$self->get("safepath");my $safename=$self->get("safename");
  my $parent=$self->get("parent");my $class=$self->get("class");
  my $fh=$self->{FH};
  if ($self->getObjectProducts("${path}/${safename}") eq ""){return 1;}
  $core->branchdata()->name($safename);
  print $fh "ifeq (\$(strip \$($parent)),)\n",
            "ALL_COMMONRULES += $safepath\n",
            "${safepath}_parent := $parent\n",
            "${safepath}_INIT_FUNC := \$\$(eval \$\$(call CommonProductRules,${safepath},${path},$class))\n",
            "${safename} := self/${parent}\n",
            "${parent} := ${safename}\n",
            "${safename}_files := \$(patsubst ${path}/%,%,\$(wildcard \$(foreach dir,${path} ",$self->getSubDirIfEnabled(),",\$(foreach ext,\$(SRC_FILES_SUFFIXES),\$(dir)/*.\$(ext)))))\n";
  if ($parent=~/^LCG\/(.+)$/){print $fh "$1 := ${safename}\n";}
  $self->library_template_generic();
  print $fh "endif\n";
  return 1;
}

sub dumpBuildFileLOC ()
{
  my ($self,$core,$fh,$localbf,$safename,$path,$no_export,$lib,$bigprod)=@_;
  my $locuse = "";
  my $class = $self->get("class");
  if ($self->isBigLibs()){print $fh "${safename}_CLASS         := ${class}.",$self->get("type")||"lib","\n";}
  if ($localbf ne "")
  {
    print $fh "${safename}_BuildFile    := \$(WORKINGDIR)/cache/bf/${localbf}\n";
    foreach my $xpre ("","REM_")
    {
      foreach my $xflag (@{$self->{cache}{DefaultCompilerFlags}})
      {
        my $flag="${xpre}${xflag}";
        my $v=$core->flags($flag);
        if($v ne ""){print $fh "${safename}_LOC_FLAGS_${flag}   := $v\n";}
      }
    }
    foreach my $data ("INCLUDE")
    {
      my $dataval=$self->fixData($core->value($data),$data,$localbf);
      if($dataval ne ""){print $fh "${safename}_LOC_${data}   := ",join(" ",@$dataval),"\n";}
    }
    if ($lib){foreach my $d (@{$core->flagsdata("NO_EXPORT")}){foreach my $x (split(" ",$d)){$no_export->{$x}=0;}}}
    foreach my $data ("LIB")
    {
      my $dataval=$self->fixData($core->value($data),$data,$localbf);
      if($dataval ne ""){print $fh "${safename}_LOC_${data}   := ",join(" ",@$dataval),"\n";}
    }
    my $dataval=$self->fixData($core->value("USE"),"USE",$localbf);
    if($dataval ne "")
    {
      $locuse = join(" ",@$dataval);
      if ($lib){foreach my $d (@$dataval){if (exists $no_export{$d}){$no_export->{$d}=1;}}}
    }
    if ($lib)
    {
      my $flag=$self->isLibSymLoadChecking ();
      if ($flag ne ""){print $fh "${safename}_libcheck     := $flag\n";}
    }
    my $flag=$core->flags("SKIP_FILES");
    if($flag ne ""){print $fh "${safename}_SKIP_FILES   := $flag\n";}
  }
  if (($self->isBigLibs()) && (!$bigprod))
  {
    my $bprod = $self->getObjectProducts("${path}/${safename}");
    print $fh "${safename}_bigprod := $bprod\n";
    if ($lib)
    {
      print $fh "${bprod}_bigobjs += $safename\n";
    }
  }
  print $fh "${safename}_LOC_USE := ",$self->getCacheData("USE")," $locuse\n";
}

sub dumpBuildFileData ()
{
  my ($self,$lib)=@_;
  my $fh=$self->{FH};
  my $core=$self->core();
  my $safename=$self->get("safename");
  my $localbf = $self->getLocalBuildFile();
  my %no_export=();
  my $path=$self->get("path");
  $self->dumpBuildFileLOC ($core,$fh,$localbf,$safename,$path,\%no_export,$lib,undef);
  if ($lib){$self->processTemplate("Extra_template");}
  if (($lib) && ($localbf ne ""))
  {
    my $ex=$core->data("EXPORT");
    if(($core->publictype() == 1) && ($ex ne ""))
    {
      if ($self->get("plugin_type") eq "")
      {
        foreach my $data ("INCLUDE","LIB")
        {
	  if (($self->isBigLibs()) && ($data eq "LIB")) {next;}
          my $dataval=$self->fixData($core->value($data,$ex),$data,$localbf,1);
          if($dataval ne "")
          {
            my $xdata="";
            if ($data eq "INCLUDE"){$xdata=join(" ",@$dataval);}
            else
            {
              foreach my $l (@$dataval)
              {
                if ($l eq $safename){$xdata=$l;}
                else{print STDERR "***ERROR: Exporting library \"$l\" from $localbf is wrong. Please remove this lib from export section of this BuildFile.\n";}
              }
            }
            print $fh "${safename}_EX_${data}   := $xdata\n";
          }
        }
        my $noexpstr="";
        foreach my $d (keys %no_export)
        {
          if ($no_export{$d}==1){$noexpstr.=" $d";}
          else
          {
            print STDERR "****WARNING: $d is not defined as direct dependency in $localbf.\n",
                         "****WARNING: Please remove $d from the NO_EXPORT flag in $localbf\n";
          }
        }
	my $exptools="\$(${safename}_LOC_USE)";
        if ($noexpstr ne ""){$exptools="\$(filter-out $noexpstr,$exptools)";}
	print $fh "${safename}_EX_USE   := \$(foreach d,$exptools,\$(if \$(\$(d)_EX_FLAGS_NO_RECURSIVE_EXPORT),,\$d))\n";
      }
      else{print STDERR "****WARNING: No need to export library once you have declared your library as plugin. Please cleanup $localbf by removing the <export></export> section.\n",}
    }
  }
  if ($localbf ne "")
  {
    my $mk=$core->data("MAKEFILE");
    if($mk){foreach my $line (@$mk){print $fh "$line\n";}}
  }
  $self->setValidSourceExtensions();
  print $fh "${safename}_PACKAGE := self/${path}\nALL_PRODS += $safename\n";
  my $safepath=$self->get("safepath");
  my $store1= $self->getProductStore("scripts");
  my $store2= $self->getProductStore("logs");
  my $ins_script=$core->flags("INSTALL_SCRIPTS");
  my $class=$self->get("class");
  if ($lib)
  {
    my $store3= $self->getProductStore("lib");
    print $fh "${safename}_INIT_FUNC        += \$\$(eval \$\$(call Library,$safename,$path,$safepath,\$($store1),$ins_script,\$($store3),\$($store2)))\n";
    if($self->isBigLibs() && ($self->get("plugin_type") eq "")){print $fh "${safename}_prodtype := $class\n";}
  }
  elsif ($class ne "PYTHON")
  {
    my $type=$self->get("type");
    my $store3= $self->getProductStore($type);
    print $fh "${safename}_INIT_FUNC        += \$\$(eval \$\$(call Binary,${safename},${path},${safepath},\$(${store1}),${ins_script},\$(${store3}),$type,\$(${store2})))\n";
  }
  else
  {
    print $fh "${safename}_INIT_FUNC        += \$\$(eval \$\$(call PythonProduct,${safename},${path},${safepath},",$self->hasPythonscripts(),",",$self->isSymlinkPythonDirectory(),",",
	      "\$(",$self->getProductStore("python"),"),\$(",$self->getProductStore("lib"),"),",join(" ",@{$self->get("xpythonfiles")}),",",join(" ",@{$self->get("xpythondirs")}),"))\n";
  }
}

sub library_template_generic () {&dumpBuildFileData(shift,1);}

sub binary_template_generic() {&dumpBuildFileData(shift);}

sub binary_template ()
{
  my ($self,$autoPlugin)=@_;
  if($self->get("suffix") ne ""){return 1;}
  $self->initTemplate_common2all();
  my $core=$self->core();
  my $safepath=$self->get("safepath"); my $path=$self->get("path");
  my $fh=$self->{FH};
  my $class=$self->get("class");
  my $types=$core->buildproducts();
  my $localbf = $self->getLocalBuildFile();
  if($types)
  {
    foreach my $ptype (keys %$types)
    {
      if ($ptype eq "LIBRARY")
      {
        foreach my $prod (keys %{$types->{$ptype}})
	{
	  my $safename=$self->fixProductName($prod);
	  $self->set("safename",$safename);
          if ($self->getObjectProducts("${path}/${safename}") eq ""){next;}
	  $core->thisproductdata($safename,$ptype);
	  print $fh "ifeq (\$(strip \$($safename)),)\n";
	  if (defined $autoPlugin)
	  {
            print $fh "${safename}_files := \$(patsubst ${path}/%,%,\$(wildcard \$(foreach dir,${path} ",$self->getSubDirIfEnabled(),",\$(foreach ext,\$(SRC_FILES_SUFFIXES),\$(dir)/*.\$(ext)))))\n";
	  }
	  else
	  {
            my $prodfiles = $core->productfiles();
	    print $fh "${safename}_files := \$(patsubst ${path}/%,%,\$(foreach file,${prodfiles},\$(eval xfile:=\$(wildcard ${path}/\$(file)))\$(if \$(xfile),\$(xfile),\$(warning No such file exists: ${path}/\$(file). Please fix ${localbf}.))))\n";
          }
	  print $fh "$safename := self/${path}\n";
          $self->set("type",$types->{$ptype}{$prod}{TYPE});
	  $self->pushstash();$self->library_template_generic();$self->popstash();
	  print $fh "else\n",
	            "\$(eval \$(call MultipleWarningMsg,$safename,$path))\n",
                    "endif\n";
        }
      }
      elsif($ptype eq "BIN")
      {
        foreach my $prod (keys %{$types->{$ptype}})
	{
	  my $safename=$self->fixProductName($prod);
	  $self->set("safename",$safename);
          if ($self->getObjectProducts("${path}/${safename}") eq ""){next;}
	  $core->thisproductdata($safename,$ptype);
	  my $prodfiles = $core->productfiles();
	  if ($prodfiles ne "")
	  {
	    print $fh "ifeq (\$(strip \$($safename)),)\n",
	              "${safename}_files := \$(patsubst ${path}/%,%,\$(foreach file,${prodfiles},\$(eval xfile:=\$(wildcard ${path}/\$(file)))\$(if \$(xfile),\$(xfile),\$(warning No such file exists: ${path}/\$(file). Please fix ${localbf}.))))\n",
                      "$safename := self/${path}\n";
            if ($class eq "TEST")
	    {
	      my $fval = $core->flags("NO_TESTRUN");
	      if ($fval=~/^(yes|1)$/i)
	      {
	        print $fh "${safename}_TEST_RUNNER_CMD := echo\n";
		print $fh "${safename}_NO_TESTRUN := yes\n";
	      }
	      else
	      {
		$fval = $core->flags("TEST_RUNNER_CMD");
	        if ($fval ne ""){print $fh "${safename}_TEST_RUNNER_CMD :=  $fval\n";}
	        else{print $fh "${safename}_TEST_RUNNER_CMD :=  $safename ",$core->flags("TEST_RUNNER_ARGS"),"\n";}
	      }
	      $fval = $core->flags("PRE_TEST");
	      if ($fval ne ""){print $fh "${safename}_PRE_TEST := $fval\n";}
	      $self->set("type","test");
	    }
	    else{$self->set("type",$types->{$ptype}{$prod}{TYPE});}
	    $self->pushstash();$self->binary_template_generic();$self->popstash();
	    print $fh "else\n",
	            "\$(eval \$(call MultipleWarningMsg,$safename,$path))\n",
                    "endif\n";
          }
	}
      }
      else{$self->set("type",lc($ptype));$common->unsupportedProductType ();}
    }
  }
  my $parent=$self->get("parent");
  print $fh "ALL_COMMONRULES += $safepath\n",
            "${safepath}_parent := $parent\n",
            "${safepath}_INIT_FUNC += \$\$(eval \$\$(call CommonProductRules,$safepath,$path,$class))\n";
  return 1;
}

sub src2store_copy()
{
  my $self=shift;
  if($self->get("suffix") ne ""){return 1;}
  $self->initTemplate_common2all();
  my $filter=shift;
  my $store=shift;
  my $fh=$self->{FH};
  my $core=$self->core();
  my $safepath=$self->get("safepath");
  my $path=$self->get("path");
  print $fh "${safepath}_files := \$(filter-out \\#% %\\#,\$(notdir \$(wildcard \$(foreach dir,\$(LOCALTOP)/${path},\$(dir)/${filter}))))\n";
  my $flag=$core->flags("SKIP_FILES");
  if($flag ne ""){print $fh "${safepath}_SKIP_FILES := $flag\n";}
  print $fh "\$(eval \$(call Src2StoreCopy,${safepath},${path},${store},${filter}))\n";
}

sub scripts_template ()
{
  my $self=shift;
  $self->src2store_copy('*',"\$(".$self->getProductStore("scripts").")");
  return 1;
}

sub moc_template ()
{
  my $self=shift;
  if ($self->shouldRunMoc())
  {
    my $fh=$self->{FH};
    my $safename=$self->get("safename");
    print $fh "${safename}_PRE_INIT_FUNC += \$\$(eval \$\$(call AddMOC,${safename},",$self->get("mocfiles"),",",$self->get("mocinc"),",",$self->get("mocsrc"),",",$self->get("mocbase"),"))\n";
  }
}

sub codegen_template ()
{
  my $self=shift;
  my $flag=$self->core()->flags("CODEGENPATH");
  if (($flag ne "") && ($self->isCodeGen()))
  {
    my $fh=$self->{FH};
    print $fh "\$(eval \$(call CodeGen,",$self->get("safename"),",",$self->get("path"),",$flag))\n";
  }
  return 1;
}

sub donothing_template()
{
  my $self=shift;
  if ($self->get("suffix") ne ""){return 1;}
  $self->initTemplate_common2all ();
  my $fh=$self->{FH}; my $safepath=$self->get("safepath");
  print $fh ".PHONY : all_${safepath} ${safepath}\n",
            "${safepath} all_${safepath}:\n";
  return 1;
}

sub lexyacc_template ()
{
  my $self=shift;
  if ($self->searchLexYacc())
  {
    my $fh=$self->{FH};
    my $safename=$self->get("safename");
    print $fh "${safename}_PRE_INIT_FUNC += \$\$(eval \$\$(call LexYACC,$safename,",$self->get("path"),",",$self->get("lexyacc"),",",$self->get("parseyacc"),"))\n";
  }
}

sub plugins_template()
{
  my $self=shift;
  my $core=$self->{core};
  my $autoPlugin=undef;
  my $skip=$core->flags("SKIP_FILES");
  if (($skip eq "*") || ($skip eq "%")){return 1;}
  if (($self->getLocalBuildFile() ne "") && (!$core->hasbuildproducts()))
  {
    my $flags = $core->allflags();
    foreach my $ptype (keys %{$self->{cache}{SupportedPlugins}})
    {
      foreach my $pflag (@{$self->{cache}{SupportedPlugins}{$ptype}{Flag}})
      {
        if((exists $flags->{$pflag}) && ($flags->{$pflag}[0] eq "0")){return 1;}
      }
    }
    my $name=$core->parent()."Auto"; $name=~s/\/+//g;
    $core->addbuildproduct($name," ","lib","LIBRARY");
    $autoPlugin=1;
  }
  return $self->binary_template($autoPlugin);
}

sub python_template()
{
  my $self=shift;
  if ($self->get("suffix") ne ""){return 1;}
  $self->initTemplate_PYTHON ();
  
  my $core=$self->core();
  my $types=$core->buildproducts();
  if($types)
  {
    foreach my $type (keys %$types)
    {
      $self->set("type",$type);
      $self->unsupportedProductType();
    }
  }
  my $path=$self->get("path"); my $safepath=$self->get("safepath");my $safename=$self->get("safename");my $class=$self->get("class");
  my $fh=$self->{FH};
  $core->branchdata()->name($safename);
  print $fh "ifeq (\$(strip \$(${safename})),)\n",
            "$safename := self/${path}\n",
            "${safepath}_parent := $parent\n",
            "ALL_PYTHON_DIRS += \$(patsubst src/%,%,$path)\n",
            "${safename}_files := \$(patsubst ${path}/%,%,\$(wildcard \$(foreach dir,${path} ",$self->getSubDirIfEnabled(),",\$(foreach ext,\$(SRC_FILES_SUFFIXES),\$(dir)/*.\$(ext)))))\n";
  $self->dumpBuildFileData();
  print $fh "else\n",
            "\$(eval \$(call MultipleWarningMsg,$safename,$path))\n",
            "endif\n",
            "ALL_COMMONRULES += $safepath\n",
            "${safepath}_INIT_FUNC += \$\$(eval \$\$(call CommonProductRules,$safepath,$path,$class))\n";
  return 1;
}

sub test_template() {&binary_template(shift);}

sub BigProduct_template()
{
  my $self=shift;
  if($self->get("suffix") ne ""){return 1;}
  my $fh=$self->{FH};
  my $path=$self->get("path");
  my $safename = basename($path);
  $self->set("safename",$safename);
  my $safepath=$self->get("safepath");
  my $class=$self->get("class");
  my $core=$self->core();
  $core->branchdata()->name($safename);
  print $fh "ifeq (\$(strip \$($safename)),)\n$safename:=$safename\n";
  my $localbf = $self->getLocalBuildFile();
  my %no_export=();
  $self->dumpBuildFileLOC ($core,$fh,$localbf,$safename,$path,\%no_export,"true","true");
  my $noexpstr="";
  foreach my $d (keys %no_export)
  {
    if ($no_export{$d}==1){$noexpstr.=" $d";}
    else
    {
      print STDERR "****WARNING: $d is not defined as direct dependency in $localbf.\n",
                   "****WARNING: Please remove $d from the NO_EXPORT flag in $localbf\n";
    }
  }
  my $exptools="\$(${safename}_LOC_USE)";
  if ($noexpstr ne ""){$exptools="\$(filter-out $noexpstr,$exptools)";}
  print $fh "${safename}_EX_USE     := \$(foreach d,$exptools,\$(if \$(\$(d)_LOC_FLAGS_NO_RECURSIVE_EXPORT),,\$d))\n";
  print $fh "${safename}_pkgs       := \$(",$self->{OBJ2PROD}{$safename}{BASE},")/${path}/pkgs.txt\n";
  my $mk=$core->data("MAKEFILE");
  if($mk){foreach my $line (@$mk){print $fh "$line\n";}}
  $self->processTemplate("Product");
  print $fh "ALL_BIG_PRODS += $safename\n";
  print $fh "${safename}_INIT_FUNC += \$\$(eval \$\$(call BigProduct,$safename,$path,$safepath))\n";
  print $fh "endif\n",
}

sub object2ProductsMap()
{
  my $self=shift;
  my $dbfile=".SCRAM/$ENV{SCRAM_ARCH}/ObjectCache.".$self->{dbext};
  my $dirty=0;
  if (-f $dbfile){$self->{OBJ2PROD}=&Cache::CacheUtilities::read($dbfile);}
  my %prods=();
  foreach my $base ("LOCALTOP","RELEASETOP")
  {
    if ((exists $ENV{$base}) && ($ENV{$base} ne ""))
    {
      my $basedir=$ENV{$base};
      if (-e "${basedir}/$ENV{SCRAM_SOURCEDIR}/".$self->{cache}{BuildProducts})
      {
	foreach my $file (glob("${basedir}/$ENV{SCRAM_SOURCEDIR}/".$self->{cache}{BuildProducts}."/*/pkgs.txt"))
	{
	  my $prod = $file;
	  $prod=~/\/([^\/]+)\/pkgs\.txt$/o; $prod=$1;
	  if (exists $prods{$prod}){next;}
	  $prods{$prod}=1;
	  my $mtime=0;
	  if ($self->{OBJ2PROD}{$prod}{BASE} eq $base)
	  {
	    $mtime=(stat($file))[9];
	    if ($mtime == $self->{OBJ2PROD}{$prod}{MTIME}){next;}
	  }
	  else{$mtime=(stat($file))[9];}
	  $self->{OBJ2PROD}{$prod}{MTIME}=$mtime;
	  $self->{OBJ2PROD}{$prod}{BASE}=$base;
	  $self->{OBJ2PROD}{$prod}{OBJS}{DROP}=[];
	  $self->{OBJ2PROD}{$prod}{OBJS}{ADD}=[];
          my $ref;
          open($ref,$file) || die "ERROR: Can not open file to read: $file\n";
          while(my $line=<$ref>)
          {
            chomp $line;
            foreach my $item (split /\s+/,$line)
            {
              if($item=~/^!(.+)/o){push @{$self->{OBJ2PROD}{$prod}{OBJS}{DROP}},$1;}
	      else{push @{$self->{OBJ2PROD}{$prod}{OBJS}{ADD}},$item;}
            }
          }
          close($ref);
	  $dirty=1;
	}
      }
    }
  }
  if ($dirty){&Cache::CacheUtilities::write($self->{OBJ2PROD},$dbfile);}
}

sub isBigLibs()
{
  my $self=shift;
  return exists $self->{OBJ2PROD};
}

sub getObjectProducts()
{
  my ($self,$obj)=@_;
  if (!$self->isBigLibs()){return $obj;}
  $obj=~s/^$ENV{SCRAM_SOURCEDIR}\///o;
  my $prod="";
  if (exists $self->{cache}{OBJ2PROD}{$obj}){$prod=$self->{cache}{OBJ2PROD}{$obj};}
  else
  {
    $prod=$self->searchObjectProducts($obj);
    $self->{cache}{OBJ2PROD}{$obj}=$prod;
  }
  return $prod;
}

sub searchObjectProducts()
{
  my ($self,$obj)=@_;
  my $mprod="";
  foreach my $prod (keys %{$self->{OBJ2PROD}})
  {
    foreach my $preg (@{$self->{OBJ2PROD}{$prod}{OBJS}{ADD}})
    {
      if ($obj eq $preg){return $prod;}
      if ($mprod)       {next;}
      if ($obj=~/^$preg$/)
      {
        my $ok=1;
	foreach my $nreg (@{$self->{OBJ2PROD}{$prod}{OBJS}{DROP}}){if ($obj=~/^$nreg$/){$ok=0; last;}}
	if ($ok){$mprod=$prod;}
      }
    }
  }
  return $mprod;
}
1;
