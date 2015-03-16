#!/usr/bin/env perl
use File::Basename;

my $src=shift;            #source directory to search directories in
my $des=shift;            #destination directory to create symlinks in
my $depth=shift;          #how deep in source directory we search
my $subdir=shift || "";   #sub-directory to search. There is a special cases
                          # . mean find directory with same name as parent e.g. in LCG project we have PackageA/PackageA
my $linkdir=shift || "";  #name of symlink to create
my $srcfilter="";
my $srcnfilter="";
while(my $arg=shift)      #filter to match in src path. A - infront of filter means ignore source those src patchs
{
  $arg=~s/^\s*//; $arg=~s/\s*$//;
  if ($arg=~/^-(.+)$/){$srcnfilter.="${1}\|";}
  elsif($arg=~/^(\+|)(.+)$/){$srcfilter.="${2}\|";}
}
$srcfilter=~s/\|$//; $srcnfilter=~s/\|$//; 
if (-d $src)
{
  foreach my $dir (`find $src -maxdepth $depth -mindepth $depth -name "*" -type d`)
  {
    chomp $dir;
    if ($dir=~/^\./){next;}
    if (($srcnfilter ne "") && ($dir=~/$srcnfilter/)){next;}
    if (($srcfilter ne "") && ($dir!~/$srcfilter/)){next;}
    my $rpath=$dir; $rpath=~s/$src\/*//;
    my $sdir=&getSubDir($dir,$subdir);
    my $ldir=&getSubDir($dir,$linkdir);
    if (-d "${dir}${sdir}")
    {
      my $slink="${des}/${rpath}${ldir}";
      my $slinkdir=dirname($slink);
      $ldir=$slinkdir;
      $ldir=~s/[a-zA-Z0-9-_]+/../g;
      if (!-l $slink)
      {
        system(" [ -d $slinkdir ] || mkdir -p $slinkdir; ln -s ${ldir}/${dir}${sdir} $slink");
        print "  ${dir}${sdir} -> $slink\n";
      }
    }
  }
}

if (-d $des)
{
  my %rm=(); my %ok=();
  foreach my $d (`find $des -name "*" -type l`)
  {
    chomp $d;
    my $d1=$d;
    $d1=~s/\/[^\/]+$//;
    if (!-e $d){unlink $d; $rm{$d1}=1;}
    else{$ok{$d1}=1;}
  }
  foreach my $k (keys %ok){delete $rm{$k};}
  my $del=join(" ",keys %rm);
  if ($del!~/^\s*$/){system("rm -rf $del");}
}

sub getSubDir()
{
  my ($dir,$sdir)=@_;
  if ($sdir eq ""){return "";}
  if ($sdir eq "."){$sdir="/".basename($dir);}
  else{$sdir="/${sdir}";}
  return $sdir;
}
