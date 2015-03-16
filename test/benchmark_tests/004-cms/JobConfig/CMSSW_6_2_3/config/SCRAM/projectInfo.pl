#!/usr/bin/env perl
BEGIN{unshift @INC,$ENV{SCRAM_TOOL_HOME};}
use File::Basename;
use Cwd;
use Cache::CacheUtilities;
$|=1;

my $localtop = shift || &usage();
my $cmd      = uc(shift) || &usage();
if ($cmd!~/^(USES|USED_BY|ORIGIN)$/){&usage();}
my $pack     = shift || &usage();
my $arch     = shift || $ENV{SCRAM_ARCH} || &usage();
$ENV{SCRAM_ARCH}=$arch;

my $envfile="${localtop}/.SCRAM/${arch}/Environment";
if (!-f $envfile){$envfile="${localtop}/.SCRAM/Environment";}
my $reltop   = `grep RELEASETOP= $envfile | sed 's|RELEASETOP=||'`; chomp $reltop;
my $cacheext="db";
if(&scramVersion($localtop)=~/^V[2-9]/){$cacheext="db.gz";}
my $cfile="${localtop}/.SCRAM/${arch}/ToolsDepsCache.${cacheext}";
my $cache=undef;
my $tools={};
if (-f $cfile)
{
  $cache=&Cache::CacheUtilities::read($cfile);
  my @s=stat($cfile);
  foreach my $file (keys %{$cache->{FILES}})
  {
    if (-f $file)
    {
      my @s1=stat($file);
      if ($s[9] > $s1[9]){next;}
    }
    $cache=undef;
    last;
  }
}
if (!defined $cache)
{
  $cache=&updateExternals();
  if ($reltop ne "")
  {
    if (open(DEPFILE,">$cfile"))
    {
      close(DEPFILE);
      &Cache::CacheUtilities::write($cache,$cfile);
    }
  }
}

my $func="process_$cmd";
&$func($cache,$pack);

sub updateExternals ()
{
  my $data={};
  my $tfile="${localtop}/.SCRAM/${arch}/ToolCache.${cacheext}";
  $tools=&Cache::CacheUtilities::read($tfile);
  $data->{FILES}{$tfile}=1;
  foreach my $t (keys %{$tools->{SETUP}})
  {
    my $file="${localtop}/.SCRAM/${arch}/timestamps/${t}";
    if (!-f $file){die "No such file: $file\n"}
    $data->{FILES}{$file}=1;
    $data->{DATA}{$t}{USES}={};
    $data->{DATA}{$t}{TYPE}="tool";
    my $tc=$tools->{SETUP}{$t};
    if (exists $tc->{USE}){foreach my $d (@{$tc->{USE}}){$data->{DATA}{$t}{USES}{&FixToolName($d)}=1;}}
    if (exists $tc->{LIB}){foreach my $l (@{$tc->{LIB}}){$data->{PROD}{$l}{ORIGIN}{$t}="tool";}}
  }
  if (-f "${localtop}/.SCRAM/${arch}/MakeData/Tools/SCRAMBased/order")
  {
    foreach my $t (`sort -r ${localtop}/.SCRAM/${arch}/MakeData/Tools/SCRAMBased/order`)
    {
      chomp $t;
      $t=~s/^\d+://;
      my $base="";
      if ($t eq "self"){$base=$reltop;}
      else{$base=uc($t)."_BASE";$base=$tools->{SETUP}{$t}{$base};}
      &updateSCRAMTool($t,$base,$data);
    }
  }
  &updateSCRAMTool("self",$localtop,$data);
  &updateDeps($data);
  delete $data->{DATA};
  $tools=undef;
  return $data;
}

sub updateDeps ()
{
  my $data=shift;
  my $pack=shift;
  if (!defined $pack)
  {
    foreach my $d (keys %{$data->{DATA}}){&updateDeps($data,$d);}
    return;
  }
  if(exists $data->{DEPS}{$pack}){return;}
  $data->{DEPS}{$pack}{USES}={};
  $data->{DEPS}{$pack}{USED_BY}={};
  $data->{DEPS}{$pack}{TYPE}=$data->{DATA}{$pack}{TYPE};
  foreach my $u (keys %{$data->{DATA}{$pack}{USES}})
  {
    if (exists $data->{DATA}{$u}){&updateDeps($data,$u);}
    $data->{DEPS}{$pack}{USES}{$u}=1;
    $data->{DEPS}{$u}{USED_BY}{$pack}=1;
    foreach my $d (keys %{$data->{DEPS}{$u}{USES}}){$data->{DEPS}{$pack}{USES}{$d}=1;$data->{DEPS}{$d}{USED_BY}{$pack}=1;}
  }
}

sub updateSCRAMTool ()
{
  my $tool=shift;
  my $base=shift;
  my $data=shift;
  my $file="${base}/.SCRAM/${arch}/ProjectCache.${cacheext}";
  my $c=&Cache::CacheUtilities::read($file);
  $data->{FILES}{$file}=1;
  foreach my $dir (keys %{$c->{BUILDTREE}})
  {
    my $dc=$c->{BUILDTREE}{$dir};
    if ((exists $dc->{RAWDATA}) && ($dc->{RAWDATA}{DEPENDENCIES}))
    {
      my $class=$dc->{CLASS};
      my $prod=undef;
      if($class=~/^(LIBRARY|CLASSLIB|SEAL_PLATFORM)$/){$dir=$dc->{PARENT};$prod=$dc->{NAME};}
      $data->{DATA}{$dir}{USES}={};
      $data->{DATA}{$dir}{TYPE}=$tool;
      $dc=$dc->{RAWDATA}{DEPENDENCIES};
      foreach my $d (keys %{$dc}){$data->{DATA}{$dir}{USES}{&FixToolName($d)}=1;}
      if (defined $prod){$data->{PROD}{$prod}{ORIGIN}{$dir}=$tool;}
      else
      {
        $dc=$c->{BUILDTREE}{$dir}{RAWDATA};
        if (exists $dc->{content}{BUILDPRODUCTS})
        {
          $dc=$dc->{content}{BUILDPRODUCTS};
          foreach my $type ("LIBRARY", "BIN")
          {
            if(exists $dc->{$type})
            {
              foreach my $prod (keys %{$dc->{$type}})
              {$data->{PROD}{$prod}{ORIGIN}{$dir}=$tool;}
            }
          }
        }
      }
    }
  }
}

sub FixToolName ()
{
  my $t=shift;
  my $lct=lc($t);
  if(exists $tools->{SETUP}{$lct}){return $lct;}
  return $t;
}

sub process_USES ()
{
  my $data=shift;
  my $pack=shift;
  my @packs=();
  my $str = "${pack}_USES = ";
  if (exists $data->{DEPS}{$pack}{USES})
  {
    foreach my $d (keys %{$data->{DEPS}{$pack}{USES}}){push @packs,$data->{DEPS}{$d}{TYPE}."/${d}";}
    $str.=join(" ",sort  @packs);
  }
  print "$str\n";
}

sub process_ORIGIN ()
{
  my $data=shift;
  my $prod=shift;
  my $str="${prod}_ORIGIN = ";
  if (exists $data->{PROD}{$prod}{ORIGIN})
  {
    foreach my $dir (keys %{$data->{PROD}{$prod}{ORIGIN}})
    {
      my $tool=$data->{PROD}{$prod}{ORIGIN}{$dir};
      $str.="$tool/$dir ";
    }
  }
  print "$str\n";
}

sub process_USED_BY ()
{
  my $data=shift;
  my $pack=shift;
  my @packs=();
  my $str="${pack}_USED_BY = ";
  if (exists $data->{DEPS}{$pack}{USED_BY})
  {
    foreach my $d (keys %{$data->{DEPS}{$pack}{USED_BY}}){push @packs,$data->{DEPS}{$d}{TYPE}."/${d}";}
    $str.=join(" ",sort  @packs);
  }
  print "$str\n";
}

sub usage(){die "Usage: $0 <localtop> <USES|USED_BY|ORIGIN> <tool|package> [<arch>]\n";}

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
