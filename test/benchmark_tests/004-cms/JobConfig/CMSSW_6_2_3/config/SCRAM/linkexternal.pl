#!/usr/bin/env perl
BEGIN{unshift @INC,$ENV{SCRAM_TOOL_HOME};}
use File::Basename;
use Cwd;
use Getopt::Long;
use Cache::CacheUtilities;
$|=1;
my $SCRAM_CMD="$ENV{SCRAM_TOOL_HOME}/../bin/scram";
my %cache=();
$cache{validlinks}{LIBDIR}="lib";
$cache{validlinks}{PATH}="bin";
$cache{validlinks}{PYTHONPATH}="python";
$cache{validlinks}{CMSSW_SEARCH_PATH}="data";
$cache{validlinks}{SHAREDIR}="share";


$cache{defaultlinks}{LIBDIR}=1;

$cache{ignorefiles}{LIBDIR}{"python.+"}="d";
$cache{ignorefiles}{LIBDIR}{"modules"}="d";
$cache{ignorefiles}{LIBDIR}{"pkgconfig"}="d";
$cache{ignorefiles}{LIBDIR}{"archive"}="d";
$cache{ignorefiles}{LIBDIR}{'__.SYMDEF\s+SORTED'}="f";
$cache{ignorefiles}{PYTHONPATH}{"CVS"}="d";
$cache{ignorefiles}{CMSSW_SEARCH_PATH}{"etc"}="d";
$cache{ignorefiles}{CMSSW_SEARCH_PATH}{".package-checksum"}="f";

$cache{runtime_map}{LIBDIR}=["LD_LIBRARY_PATH", "DYLD_FALLBACK_LIBRARY_PATH"];

if(&GetOptions(
               "--update=s",\@update,
	       "--pre=s",\@pre,
	       "--post=s",\@post,
	       "--arch=s",\$arch,
	       "--all",\$all,
	       "--help",\$help,
              ) eq ""){print "Wrong arguments.\n"; &usage(1);}

if(defined $help){&usage(0);}
if(defined $all){$all=1;} else{$all=0;}

if((!defined $arch) || ($arch=~/^\s*$/)){$arch=`$SCRAM_CMD arch`; chomp $arch;}
$ENV{SCRAM_ARCH}=$arch;

my $curdir   = cwd();
my $localtop = &fixPath(&scramReleaseTop($curdir));
if (!-d "${localtop}/.SCRAM/${arch}"){die "$curdir: Not a SCRAM-Based area. Missing .SCRAM directory.";}
chdir($localtop);
my $scramver=&scramVersion($localtop);
my $cacheext="db";
my $admindir="";
if ($scramver eq ""){die "ERROR: Something worng, could not find SCRAM VERSION.\n";}
if($scramver=~/^V[2-9]/){$cacheext="db.gz";$admindir=$arch;}

my %projdata=&readProjectData($localtop,$admindir);
if (!exists $projdata{SCRAM_PROJECTNAME}){die "ERROR: Can not find project name.\n";}

if (($all==0) && (!exists $projdata{RELEASETOP})){$all=1;}

if(!-f "${localtop}/.SCRAM/${arch}/ToolCache.${cacheext}"){system("$SCRAM_CMD b -r echo_CXX 2>&1 >/dev/null");}
$cache{toolcache}=&Cache::CacheUtilities::read("${localtop}/.SCRAM/${arch}/ToolCache.${cacheext}");

$cache{skipTools}{self}=1;
$cache{skipTools}{lc($projdata{SCRAM_PROJECTNAME})}=1;
if (exists $cache{toolcache}{SETUP}{self}{FLAGS})
{
  my $flags=$cache{toolcache}{SETUP}{self}{FLAGS};
  if (exists $flags->{EXTERNAL_SYMLINK})
  {
    $cache{defaultlinks}={};
    foreach my $x (@{$flags->{EXTERNAL_SYMLINK}})
    {
      my $ux=uc($x);
      if (exists $cache{validlinks}{$ux}){$cache{defaultlinks}{$ux}=1;}
    }
  }
  if (exists $flags->{SKIP_TOOLS_SYMLINK})
  {
    foreach my $t (@{$flags->{SKIP_TOOLS_SYMLINK}}){$cache{skipTools}{lc($t)}=1;}
  }
  foreach my $f (keys %{$flags})
  {
    if ($f=~/^SYMLINK_DEPTH_(.+)$/)
    {
      if (exists $cache{validlinks}{$1})
      {
        my $dep=$flags->{$f}[0];
	if ($dep<1){$dep=1;}
	$cache{sym_depth}{$1}=$dep;
      }
    }
  }
}

if(scalar(keys %{$cache{defaultlinks}})==0){exit 0;}

#### Ordered list of removed tools
my %tmphash=();
$cache{updatetools}=[];
foreach my $t (@update)
{
  my $t=lc($t);
  if(!exists $tmphash{$t}){$tmphash{$t}=1;push @{$cache{updatetools}},$t;}
}

#### Ordered list of tools to be set first
%tmphash=();
$cache{pretools}=[];
foreach my $t (@pre)
{
  my $t=lc($t);
  if(!exists $tmphash{$t}){$tmphash{$t}=1;push @{$cache{pretools}},$t;}
}

#### Ordered list of tools to be set last
$cache{posttools}=[];
$cache{posttools_uniq}={};
foreach my $t (@post)
{
  my $t=lc($t);
  if(!exists $cache{posttools_uniq}{$t}){$cache{posttools_uniq}{$t}=1;push @{$cache{posttools}},$t;}
}

push @{$cache{extradir}}, "";
for(my $i=0;$i<20;$i++)
{
  if($i<10){push @{$cache{extradir}},"0$i";}
  else{push @{$cache{extradir}},"$i";}
}

#### Read previous link info
my $externals="external/${arch}";
my $linksDB="${externals}/links.DB";
&readLinkDB ();

if(exists $cache{toolcache}{SETUP})
{
  %tmphash=();
  foreach my $t (keys %{$cache{toolcache}{SETUP}})
  {
    if (exists $cache{skipTools}{$t})
    {
      &removeLinks($t);
      delete $cache{BASES}{$t};
      next;
    }
    my %nbases=();
    my $tc=$cache{toolcache}{SETUP}{$t};
    foreach my $x (keys %{$cache{defaultlinks}})
    {
      if (exists $tc->{$x})
      {
	my $ref=ref($tc->{$x});
	my $dirs=[];
	if ($ref eq "ARRAY"){$dirs=$tc->{$x};}
	elsif($ref eq ""){$dirs=[$tc->{$x}];}
	foreach my $dir (@$dirs)
        {
	  $nbases{$dir}=1;
	  $cache{"${x}_BASES"}{$t}{$dir}=1;
        }
      }
      if (exists $tc->{RUNTIME})
      {
        my $xp = $cache{runtime_map}{$x} || [$x];
        foreach my $y (@$xp)
        {
          if(exists $tc->{RUNTIME}{"PATH:${y}"})
          {
            foreach my $dir (@{$tc->{RUNTIME}{"PATH:${y}"}})
            {
              $nbases{$dir}=1;
              $cache{"${x}_BASES"}{$t}{$dir}=1;
            }
          }
        }
      }
    }
    foreach my $dir (keys %nbases)
    {
      if (!exists $cache{BASES}{$t}{$dir}){$tmphash{$t}=1;}
      $cache{BASES}{$t}{$dir}=2;
    }
    foreach my $dir (keys %{$cache{BASES}{$t}})
    {
      if ($cache{BASES}{$t}{$dir} == 2){$cache{BASES}{$t}{$dir}=1;}
      else
      {
        $tmphash{$t}=1;
	delete $cache{BASES}{$t}{$dir};
      }
    }
  }
  if(scalar(keys %tmphash)>0)
  {
    foreach my $t (@{$cache{updatetools}}){$tmphash{$t}=1;}
    $cache{updatetools}=[];
    foreach my $t (keys %tmphash){push @{$cache{updatetools}},$t;}
  }
}

#### Remove all the links for tools passed via command-line arguments
foreach my $t (@{$cache{updatetools}}){&removeLinks($t);}

##### Ordered list of all tools
&getOrderedTools ();

$cache{DBLINK}={};
foreach my $tooltype ("pretools", "alltools" , "posttools")
{
  if(exists $cache{$tooltype})
  {
    foreach my $t (@{$cache{$tooltype}})
    {
      if (exists $cache{skipTools}{$t}){next;}
      if(($tooltype eq "alltools") && (exists $cache{posttools_uniq}{$t})){next;}
      if($all || (-f "${localtop}/.SCRAM/${admindir}/InstalledTools/$t"))
      {if(!exists $cache{donetools}{$t}){$cache{donetools}{$t}=1;&updateLinks($t);}}
    }
  }
}

if(-d $externals)
{
  my $ref;
  open($ref, ">$linksDB") || die "Can not open file \"$linksDB\" for writing.";
  if(exists $cache{DBLINK})
  {foreach my $x1 (sort keys %{$cache{DBLINK}}){foreach my $x2 (sort keys %{$cache{DBLINK}{$x1}}){$x2=~s/^$externals\///;print $ref "L:$x1:$x2\n";}}}
  if(exists $cache{BASES})
  {foreach my $x1 (sort keys %{$cache{BASES}}){foreach my $x2 (sort keys %{$cache{BASES}{$x1}}){print $ref "B:$x1:$x2\n";}}}
  close($ref);

  foreach my $type (sort keys %{$cache{validlinks}})
  {
    $type=$cache{validlinks}{$type};
    foreach my $s (@{$cache{extradir}})
    {
      my $ldir="${externals}/${type}${s}";
      if(-d $ldir){if(!exists $cache{dirused}{$ldir}){system("rm -fr $ldir");}}
      else{last;}
    }
  }
  if(exists $cache{PREDBLINKR})
  {foreach my $lfile (keys %{$cache{PREDBLINKR}}){if(-l $lfile){system("rm -f $lfile");}}}
}
exit 0;

sub readLinkDB ()
{
  if(!exists $cache{PREDBLINK})
  {
    if(-f "$linksDB")
    {
      my $ref;
      open($ref, "${externals}/links.DB") || die "Can not open file \"${externals}/links.db\" for reading.";
      while(my $line=<$ref>)
      {
        chomp $line;
	if($line=~/^L:([^:]+?):(.+)$/){$cache{PREDBLINK}{$1}{"${externals}/${2}"}=1;$cache{PREDBLINKR}{"${externals}/${2}"}{$1}=1;}
	elsif($line=~/^B:([^:]+?):(.+)$/){$cache{BASES}{$1}{$2}=1;}
      }
      close($ref);
    }
    else{$cache{PREDBLINK}={};$cache{PREDBLINKR}={};}
  }
}

sub removeLinks ()
{
  my $tool=shift || return;
  if(exists $cache{PREDBLINK}{$tool})
  {
    foreach my $file (keys %{$cache{PREDBLINK}{$tool}})
    {
      if(-l $file)
      {
	if (scalar(keys %{$cache{PREDBLINKR}{$file}})==1){system("rm -f $file");}
	delete $cache{PREDBLINKR}{$file}{$tool};
      }
    }
    if(exists $cache{toolcache}{SETUP}{$tool}{LIB})
    {
      foreach my $l (@{$cache{toolcache}{SETUP}{$tool}{LIB}})
      {
	my $lf="${localtop}/tmp/${arch}/cache/prod/lib${l}";
	if(-f "$lf"){if(open(LIBFILE,">$lf")){close(LIBFILE);}}
      }
    }
    delete $cache{PREDBLINK}{$tool};
  }
}

sub updateLinks ()
{
  my $t=shift;
  foreach my $type (sort keys %{$cache{defaultlinks}})
  {    
    my $dep=$cache{sym_depth}{$type} || 1;
    foreach my $dir (keys %{$cache{"${type}_BASES"}{$t}}){&processBase($t,$dir,$type,$dep,"");}
  }
}

sub processBase()
{
  my ($t,$dir,$type,$dep,$rpath)=@_;
  if(-d $dir)
  {
    $dir=&fixPath($dir);
    my $d;
    opendir($d, $dir) || die "Can not open directory \"$dir\" for reading.";
    my @files=readdir($d);
    closedir($d);
    foreach my $f (@files)
    {
      if($f=~/^\.+$/){next;}
      my $ff="${dir}/${f}";
      if (&isIgnoreLink($type,$ff,$f)){next;}
      if (($dep>1) && (-d $ff)){&processBase($t,$ff,$type,$dep-1,"${rpath}${f}/");}
      else{&createLink ($t,$ff,$type,"${rpath}${f}");}
    }
  }
}

sub isIgnoreLink()
{
  my ($type,$srcfile,$file)=@_;
  if(exists $cache{ignorefiles}{$type})
  {
    foreach my $reg (keys %{$cache{ignorefiles}{$type}})
    {
      my $ftype=$cache{ignorefiles}{$type}{$reg};
      if($file=~/^${reg}$/)
      {
        if((-d $srcfile) && ($ftype=~/^[da]$/i)){return 1;}
        elsif((-f $srcfile) && ($ftype=~/^[fa]$/i)){return 1;}
        elsif($ftype=~/^a$/i){return 1;}
      }
    }
  }
  return 0;
}

sub createLink ()
{
  my ($tool,$srcfile,$type,$file)=@_;
  my $lfile="";
  if(exists $cache{links}{$type}{$srcfile})
  {
    $lfile=$cache{links}{$type}{$srcfile};
    $cache{DBLINK}{$tool}{$lfile}=1;
    $cache{DBLINKR}{$lfile}=1;
    return;
  }
  $type = $cache{validlinks}{$type};
  my $ldir="";
  foreach my $s (@{$cache{extradir}})
  {
    $ldir="${externals}/${type}${s}";
    $lfile="${ldir}/${file}";
    my $xdir=dirname($lfile);
    if(!-d "$xdir"){system("mkdir -p $xdir");}
    if(!-l "$lfile"){system("cd $xdir;ln -s $srcfile .");last;}
    elsif(readlink("$lfile") eq "$srcfile"){last;}
    elsif(!exists $cache{DBLINKR}{$lfile}){system("rm -f $lfile; cd $xdir;ln -s $srcfile .");last;}
  }
  $cache{dirused}{$ldir}=1;
  $cache{links}{$type}{$srcfile}=$lfile;
  $cache{DBLINK}{$tool}{$lfile}=1;
  $cache{DBLINKR}{$lfile}=1;
  if(exists $cache{PREDBLINKR}{$lfile}){delete $cache{PREDBLINKR}{$lfile};}
}

sub getOrderedTools ()
{
  $cache{alltools}=[];
  if ($scramver=~/^V[2-9]/){&getOrderedToolsV2(@_);}
  else{&getOrderedToolsV1(@_);}
}

sub getOrderedToolsV2 ()
{
  use BuildSystem::ToolManager;
  my @compilers=();
  my %tmphash=();
  foreach my $t (reverse @{$cache{toolcache}->toolsdata()})
  {
    my $tn=$t->toolname();
    if ($t->scram_compiler()) {push @compilers,$tn;next;}
    if(($tn=~/^\s*$/) || (!exists $cache{toolcache}{SETUP}{$tn}) || ($tn eq "self")){next;}
    if(!exists $tmphash{$tn}){$tmphash{$tn}=1;push @{$cache{alltools}},$tn;}
  }
  foreach my $tn (@compilers)
  {
    if(($tn=~/^\s*$/) || (!exists $cache{toolcache}{SETUP}{$tn})){next;}
    if(!exists $tmphash{$tn}){$tmphash{$tn}=1;push @{$cache{alltools}},$tn;}
  }
}

sub getOrderedToolsV1 ()
{
  my @orderedtools=();
  foreach my $t (keys %{$cache{toolcache}{SELECTED}})
  {
    my $index=$cache{toolcache}{SELECTED}{$t};
    if($index>=0)
    {
      if(!defined $orderedtools[$index]){$orderedtools[$index]=[];}
      push @{$orderedtools[$index]},$t;
    }
  }
  my %tmphash=();
  for(my $i=@orderedtools-1;$i>=0;$i--)
  {
    if(!defined $orderedtools[$i]){next;}
    foreach my $t (@{$orderedtools[$i]})
    {
      if((!defined $t) || ($t=~/^\s*$/) || (!exists $cache{toolcache}{SETUP}{$t}) || ($t eq "self")){next;}
      if(!exists $tmphash{$t}){$tmphash{$t}=1;push @{$cache{alltools}},$t;}
    }
  }
}

sub usage ()
{
  print "Usage: $0    [--update <tool>  [--update <tool>  [...]]]\n";
  print "             [--pre    <tool>  [--pre    <tool>  [...]]]\n";
  print "             [--post   <tool>  [--post   <tool>  [...]]]\n";
  print "             [--all] [--arch <arch>] [--help]\n\n";
  print "--update <tool>  Name of tool(s) for which you want to\n";
  print "                 update the links.\n";
  print "--pre    <tool>  Name of tool(s) for which you want to\n";
  print "                 create links before any other tool.\n";
  print "--post   <tool>  Name of tool(s) for which you want to\n";
  print "                 create links at the end\n";
  print "--all            By default links for tools setup in\n";
  print "                 your project area will be added. Adding\n";
  print "                 this option will force to create links for\n";
  print "                 all the tools available in your project.\n";
  print "--arch   <arch>  SCRAM_ARCH value. Default is obtained\n";
  print "                 by running \"$SCRAM_CMD arch\" command.\n";
  print "--help           Print this help message.\n";
  exit shift || 0;
}

#############################################################
sub fixPath ()
{
  my $dir=shift || return "";
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

sub scramReleaseTop()
{return &checkWhileSubdirFound(shift,".SCRAM");}

sub checkWhileSubdirFound()
{
  my $dir=shift;
  my $subdir=shift;
  while((!-d "${dir}/${subdir}") && ($dir ne "/")){$dir=dirname($dir);}
  if(-d "${dir}/${subdir}"){return $dir;}
  return "";
}

sub scramVersion ()
{
  my $dir=shift;
  my $ver="";
  if (-f "${dir}/config/scram_version")
  {
    my $ref;
    if(open($ref,"${dir}/config/scram_version"))
    {
      $ver=<$ref>; chomp $ver;
      close($ref);
    }
  }
  return $ver;
}

sub readProjectData()
{
  my ($dir,$arch)=@_;
  my @files=("${dir}/.SCRAM/Environment");
  if ($arch ne ""){push @files,"${dir}/.SCRAM/${arch}/Environment";}
  my %data=();
  my $ref;
  foreach my $file (@files)
  {
    if(open($ref,$file))
    {
      while(my $l=<$ref>)
      {
        chomp $l;
        my ($k,$v)=split("=",$l,2);
        $data{$k}=$v;
      }
      close($ref);
    }
  }
  return %data;
}
