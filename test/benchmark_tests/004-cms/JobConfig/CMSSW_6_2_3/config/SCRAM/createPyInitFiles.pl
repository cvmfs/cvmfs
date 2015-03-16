#!/usr/bin/env perl
use File::Basename;

my $err=0;
if (-d "python")
{
  foreach my $d (`find python -name "*" -type d`)
  {
    chomp $d;
    if ($d=~/^python$/){next;}
    my $ref;
    if (!open($ref,">${d}/__init__.py"))
    {
      my $err=1;
      print STDERR "Can not open file for writing: ${d}/__init__.py\n";
      next;
    }
    if ($ENV{RELEASETOP} eq "")
    {
      print $ref "import os\n";
      print $ref "localrt=os.getenv('LOCALRT', None)\n";
      print $ref "if localrt != None:\n";
      print $ref "  __path__.insert(0,localrt+'/${d}')\n";
    }
    else
    {
      print $ref "__path__.append('$ENV{RELEASETOP}/${d}')\n";
    }
    close($ref);
  }
}

exit $err;
