#!/usr/bin/env perl
BEGIN{unshift @INC,$ENV{SCRAM_TOOL_HOME};}
use File::Basename;
use Cwd;
use Cache::CacheUtilities;
$|=1;

my $arch     = shift     || $ENV{SCRAM_ARCH} || die "Usage: $0 <arch> [<tool> [<tool> [...]]]";
my $curdir   = cwd();
my $localtop = &fixPath(&scramReleaseTop($curdir));
$ENV{SCRAM_ARCH}=$arch;
if (!-d "${localtop}/.SCRAM/${arch}"){die "$curdir: Not a SCRAM-Based area. Missing .SCRAM directory.";}
my $envfile="${localtop}/.SCRAM/${arch}/Environment";
if (!-f $envfile){$envfile="${localtop}/.SCRAM/Environment";}
my $reltop   = `grep RELEASETOP= $envfile | sed 's|RELEASETOP=||'`; chomp $reltop;
$reltop      = &fixPath($reltop);
my $proj_name = lc($ENV{SCRAM_PROJECTNAME});
my $cacheext="db";
if(&scramVersion($localtop)=~/^V[2-9]/){$cacheext="db.gz";}

my %tools=();
while(my $t=shift){$tools{lc($t)}=1;}
my $tcache=&Cache::CacheUtilities::read("${localtop}/.SCRAM/${arch}/ToolCache.${cacheext}");

if(scalar(keys %tools)==0){foreach my $t (keys %{$tcache->{SETUP}}){$tools{$t}=1;}}
my @toolvar=("INCLUDE","LIB");

my $skline=0;
my %mkprocess=();
$mkprocess{skiplines}[$skline++] = qr/^.+_(files|XDEPS|SKIP_FILES|libcheck|parent)\s+[:+]=/o;
$mkprocess{skiplines}[$skline++] = qr/^.+_LOC_((?!USE).+)\s+[:+]=/o;
$mkprocess{skiplines}[$skline++] = qr/^.+_EX_((?!LIB).+)\s+[:+]=/o;
$mkprocess{skiplines}[$skline++] = qr/^(ALL_COMMONRULES|NON_XML_BUILDFILE)\s+\+=/;
$mkprocess{skiplines}[$skline++] = qr/^.+_PACKAGE\s+:=\s+self\//;
$mkprocess{skiplines}[$skline++] = qr/\$\(call\s+/;
$mkprocess{skipcount}=$skline; $skline = 0;

$mkprocess{editlines}[$skline]{reg}     = qr/^(.+)_LOC_USE\s*:=\s*(.+)$/;
$mkprocess{editlines}[$skline]{cont}    = 1;
$mkprocess{editlines}[$skline++]{value} = '$line="${1}_EX_USE := \$(foreach d, ${2},\$(if \$(\$(d)_EX_FLAGS_NO_RECURSIVE_EXPORT),,\$d))"';
$mkprocess{editlines}[$skline]{reg}     = qr/^\s*ALL_PRODS(\s+\+=.+)$/;
$mkprocess{editlines}[$skline++]{value} = '$line="ALL_EXTERNAL_PRODS${1}"';
$mkprocess{editlines}[$skline]{reg}     = qr/^(.+)\s+self(\s*.*)$/;
$mkprocess{editlines}[$skline++]{value} = '$line="${1} ${tool}${2}"';
$mkprocess{editlines}[$skline]{reg}     = qr/^(.+)\s+self\/(.+)$/;
$mkprocess{editlines}[$skline++]{value} = '$line="${1} ${tool}/${2}"';
$mkprocess{editlines}[$skline]{reg}     = qr/^(.+_BuildFile\s+:=\s+)(.+\/cache\/bf\/([^\s]+))\s*$/;
$mkprocess{editlines}[$skline++]{value} = '$line="${1}\$($basevar)/.SCRAM/\$(SCRAM_ARCH)/MakeData/DirCache.mk"';
$mkprocess{editcount}=$skline;

my $tooldir=".SCRAM/${arch}/MakeData/Tools";
my $stooldir="${tooldir}/SCRAMBased";
if(!-d $stooldir){system("mkdir -p $stooldir; touch ${stooldir}/order");}
foreach my $t (keys %tools)
{
  if(!exists $tcache->{SETUP}{$t})
  {
    if(-f "${tooldir}/${t}.mk")
    {system("rm -f ${tooldir}/${t}.mk");}
    if (-d "${stooldir}/${t}")
    {system("rm -rf ${stooldir}/${t} ${stooldir}/${t}.mk");}
    next;
  }
  my $c=$tcache->{SETUP}{$t};
  my $sproj=$c->{SCRAM_PROJECT} || 0;
  my @tvars=@toolvar;
  if(($t eq "self") || ($t eq $proj_name)){push @tvars,"LIBDIR";}
  elsif(&isSymlinkSkipped($tcache,$t)){push @tvars,"LIBDIR";}
  open(TFILE,">${tooldir}/${t}.mk") || die "Can not open file for writing: ${tooldir}/${t}.mk\n";
  print TFILE "ALL_TOOLS      += $t\n";
  if ($sproj) {print TFILE "ALL_SCRAM_PROJECTS += $t\n";}
  foreach my $f (@tvars)
  {
    if(exists $c->{$f})
    {
      my $x=join(" ",@{$c->{$f}});
      if($x!~/^\s*$/)
      {
        print TFILE "${t}_EX_$f := $x\n";
        if (($t eq "self") && ($f eq "LIBDIR")){print TFILE "${t}_EX_$f += \$(${proj_name}_EX_$f)\n";}
      }
    }
  }
  if(exists $c->{USE})
  {
    my %au=();
    my $x="";
    foreach my $u (@{$c->{USE}})
    {
      $u=lc($u);
      if(!exists $au{$u}){$x.=" $u";}
    }
    if($x!~/^\s*$/){print TFILE "${t}_EX_USE :=$x\n";}
  }
  if(exists $c->{FLAGS})
  {
    foreach my $k (keys %{$c->{FLAGS}})
    {
      my $join=" ";
      if($k eq "CPPDEFINES"){$join=" -D";}
      my $x=join($join,@{$c->{FLAGS}{$k}});
      if($x!~/^\s*$/){print TFILE "${t}_EX_FLAGS_${k}  :=$join$x\n";}
    }
  }
  my $sproj=$c->{SCRAM_PROJECT} || 0;
  if ($sproj){$sproj=100000-(2000*&getScramProjectOrder($c,$t));}
  if($t eq "self"){print TFILE "${t}_ORDER := 20000\n";}
  elsif($sproj)   {print TFILE "${t}_ORDER := $sproj\n";}
  print TFILE "\n";
  close(TFILE);
  if($sproj || (($t eq "self") && ($reltop ne "")))
  {
    my $base="";
    if($t eq "self"){$base=$reltop;$sproj=20000;}
    else{$base=uc($t)."_BASE";$base=~s/-/_/g;$base=$c->{$base};}
    system("grep -v '.*:$t\$' ${stooldir}/order > ${stooldir}/order.new");
    system("mv   ${stooldir}/order.new ${stooldir}/order");
    if(($base ne "") && (-d $base))
    {
      my $infile="${base}/.SCRAM/${arch}/MakeData/DirCache.mk";
      my $outfile="${stooldir}/${t}.mk";
      if(-f $infile)
      {
	$mkprocess{base}=$base;
	$mkprocess{tool}=$t;
	&mkprocessfile($infile,"${outfile}.tmp",\%mkprocess);
	system("mv ${outfile}.tmp $outfile; echo $sproj:$t >> ${stooldir}/order");
      }
    }
  }
}

my $ref;
open($ref,">${stooldir}/all.mk") || die "Can not open for writing: ${stooldir}/all.mk";
foreach my $line (`sort -r ${stooldir}/order | sed 's|.*:||'`)
{
  chomp $line;
  print $ref "include ${stooldir}/${line}.mk\n";
}
close($ref);

if (($reltop ne "") && (exists $tools{self}))
{
  my $pcache="${reltop}/.SCRAM/${arch}/ProjectCache.${cacheext}";
  if (-f $pcache)
  {
    my $ref;
    open($ref,">${stooldir}/self_libs_def.mk") || die "Can not open for writing: ${stooldir}/self_libs_def.mk";
    my $pc=&Cache::CacheUtilities::read($pcache);
    foreach my $dir (keys %{$pc->{BUILDTREE}})
    {      
      if (exists $pc->{BUILDTREE}{$dir}{RAWDATA})
      {
        my $c=$pc->{BUILDTREE}{$dir}{RAWDATA};
	if ($pc->{BUILDTREE}{$dir}{CLASS} eq "LIBRARY"){print $ref $pc->{BUILDTREE}{$dir}{NAME}."_PACKAGE := self/$ENV{SCRAM_SOURCEDIR}/$dir\n";}
	elsif ((exists $c->{content}) && (exists $c->{content}{BUILDPRODUCTS}))
	{
	  $c = $pc->{BUILDTREE}{$dir}{RAWDATA}{content}{BUILDPRODUCTS};
	  foreach my $t (keys %{$c})
	  {
	    foreach my $n (keys %{$c->{$t}}){print $ref "${n}_PACKAGE := self/$ENV{SCRAM_SOURCEDIR}/$dir\n";}
	  }
	}
      }
    }
    close($ref);
  }
}

##############################################################
sub isSymlinkSkipped()
{
  my ($cache,$tool)=@_;
  my $tools=$cache->{SETUP}{self}{FLAGS}{SKIP_TOOLS_SYMLINK} || [];
  foreach my $t (@$tools){if ($t eq $tool){return 1;}}
  return 0;
}

sub mkprocessfile ()
{
  my $infile=shift;
  my $outfile=shift;
  my $data=shift;
  my $tool=$data->{tool};
  my $base=$data->{base};
  my $basevar=uc($tool)."_BASE";
  if ($tool eq "self"){$basevar="RELEASETOP";}
  my $iref; my $oref;
  open($iref,$infile) || die "Can not open file for reading: $infile";
  open($oref,">$outfile") || die "Can not open file for writing: $outfile";
  my $line;
  my $scount=$data->{skipcount};
  my $ecount=$data->{editcount};
  while($line=<$iref>)
  {
    chomp $line;
    my $skip=0;
    for(my $i=0;$i<$scount;$i++)
    {
      my $skipline=$data->{skiplines}[$i];
      if($line=~$skipline){$skip=1;last;}
    }
    if(!$skip)
    {
      for(my $i=0;$i<$ecount;$i++)
      {
        my $k=$data->{editlines}[$i]{reg};
	if ($line=~$k)
	{
	  my $v=$data->{editlines}[$i]{value};
	  eval $v;
	  if (!exists $data->{editlines}[$i]{cont}){last;}
	}
      }
      print $oref "$line\n";
    }
  }
  close($iref);
  close($oref);
}

sub getScramProjectOrder ()
{
  my $c=shift;
  my $tool=lc(shift);
  my $cache=shift || {};
  if(exists $cache->{$tool}){return $cache->{$tool};}
  my $bvar=uc($tool)."_BASE";
  my $order=1;
  if(exists $c->{$bvar})
  {
    my $tcfile=$c->{$bvar}."/.SCRAM/${arch}/ToolCache.${cacheext}";
    if(!-f $tcfile){die "No such file: $tcfile";}
    my $tc=&Cache::CacheUtilities::read($tcfile);
    foreach my $t (keys %{$tc->{SETUP}})
    {
      my $c=$tc->{SETUP}{$t};
      my $sp=$c->{SCRAM_PROJECT} || 0;
      if($sp)
      {
        my $o=&getScramProjectOrder($c,$t,$cache);
	if($o>=$order){$order=$o+1;}
      }
    }
  }
  $cache->{$tool}=$order;
  return $order;
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
