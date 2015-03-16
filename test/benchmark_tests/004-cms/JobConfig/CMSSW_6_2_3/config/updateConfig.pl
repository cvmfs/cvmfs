#!/usr/bin/env perl
use File::Basename;
use Getopt::Long;

if(&GetOptions(
               "--project=s",\$project,
               "--version=s",\$version,
               "--scram=s",\$scram,
               "--toolbox=s",\$toolbox,
	       "--config=s",\$config,
	       "--keys=s",\%keys,
	       "--arch=s",\$arch,
               "--help",\$help,
              ) eq ""){print "ERROR: Wrong arguments.\n"; &usage_msg(1);}


if(defined $help){&usage_msg(0);}
if((!defined $project) || ($project=~/^\s*$/)){die "Missing or empty project name.";}
else{$project=uc($project);}
if((!defined $version) || ($version=~/^\s*$/)){die "Missing or empty project version.";}
if((!defined $scram) || ($scram=~/^\s*$/)){die "Missing or empty scram version.";}
if((!defined $toolbox) || ($toolbox=~/^\s*$/)){die "Missing or empty SCRAM tool box path.";}
my $tooldir="configurations";
if($scram=~/^V[2-9]/){$tooldir="tools";}
if(!-d "${toolbox}/${tooldir}"){die "Wrong toolbox directory. Missing directory ${toolbox}/${tooldir}.";}

my $dir="";
if((!defined $config) || ($config=~/^\s*$/))
{
  $dir=dirname($0);
  if($dir!~/^\//){use Cwd;$dir=getcwd()."/${dir}";}
  $dir=&fixPath($dir);
  if($dir=~/^(.+)\/config$/){$config=$1;}
  else{die "Missing config directory path which needs to be updated.";}
}
$dir="${config}/config";

if ((!defined $arch) || ($arch eq ""))
{
  if (!exists $ENV{SCRAM_ARCH}){$arch=`scram arch`; chomp $arch;}
  else{$arch=$ENV{SCRAM_ARCH};}
}
$ENV{SCRAM_ARCH}=$arch;

my %cache=();
foreach my $f ("bootsrc","BuildFile","Self","SCRAM_ExtraBuildRule","boot"){$cache{SCRAMFILES}{$f}=1;}
$cache{KEYS}{PROJECT_NAME}=$project;
$cache{KEYS}{PROJECT_VERSION}=$version;
$cache{KEYS}{PROJECT_TOOL_CONF}=$toolbox;
$cache{KEYS}{PROJECT_CONFIG_BASE}=$config;
$cache{KEYS}{SCRAM_VERSION}=$scram;

foreach my $k (keys %keys){$cache{KEYS}{$k}=$keys{$k};}

my $regexp="";
foreach my $k (keys %{$cache{KEYS}})
{
  my $v=$cache{KEYS}{$k};
  $regexp.="s|\@$k\@|$v|g;";
}
foreach my $k (keys %{$cache{EXKEYS}})
{
  my $xk=$k;
  foreach my $a (keys %{$cache{EXKEYS}{$k}})
  {
    if($arch=~/^$a/)
    {
      $xk=$cache{EXKEYS}{$k}{$a};
      last;
    }
  }
  $regexp.="s|\@$k\@|$xk|g;";
}

opendir(DIR,$dir) || die "Can not open directory for reading: $dir";
foreach my $file (readdir(DIR))
{
  if($file=~/^CVS$/){next;}
  if($file=~/^\./){next;}
  my $fpath="${dir}/${file}";
  if((!-e  $fpath) || (-d $fpath) || (-l $fpath)){next;}
  if($file=~/^${project}_(.+)$/){system("mv $fpath ${dir}/${1}");}
}
closedir(DIR);
foreach my $type (keys %{$cache{SCRAMFILES}}){system("touch ${dir}/XXX_${type}; rm -f ${dir}/*_${type}*");}

system("find $dir -name \"*\" -type f | xargs sed -i.backup$$ -e '".$regexp."'");
system("find $dir -name \"*.backup$$\" -type f | xargs rm -f");
system("rm -rf ${dir}/site;  echo $scram > ${dir}/scram_version");

if ($replaceArch)
{
  my $thisscript=basename($0);
  $regexp="";
  foreach my $k (keys %{$cache{REPLACE}{$replaceArch}})
  {
    my $v=$cache{REPLACE}{$replaceArch};
    $regexp.="s|\@$k\@|$v|g;";
  }
  foreach my $f (`find $dir -name "*" -type f`)
  {
    chomp $f;
    if ($f=~/\/${thisscript}$/){next;}
    system("sed -i -e '".$regexp."' $f");
  }
}

sub usage_msg()
{
  my $code=shift || 0;
  print "$0 --project <name> --version <version> --scram <scram version>\n",
        "   --toolbox <toolbox> [--config <dir>] [--arch <arch>] [--help]\n\n",
        "  This script will copy all <name>_<files> files into <files>\n",
        "  and replace project names, version, scram verion, toolbox path",
	"  and extra keys/values provided via the command line. e.g.\n",
	"  $0 -p CMSSW -v CMSSW_4_5_6 -s V1_2_0 -t /path/cmssw-tool-conf/CMS170 --keys MYSTRING1=MYVALUE1 --keys MYSTRING2=MYVALUE2\n",
	"  will release\n",
	"    \@PROJECT_NAME\@=CMSSW\n",
	"    \@PROJECT_VERSION\@=CMSSW_4_5_6\n",
	"    \@SCRAM_VERSION\@=V1_2_0\n",
	"    \@PROJECT_TOOL_CONF\@=/path/cmssw-tool-conf/CMS170\n",
	"    \@MYSTRING1\@=MYVALUE1\n",
	"    \@MYSTRING2\@=MYVALUE2\n\n";
  exit $code;
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
