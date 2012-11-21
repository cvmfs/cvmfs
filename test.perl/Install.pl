#!/usr/bin/perl

use strict;
use warnings;
use LWP::Simple;
use Getopt::Long;
use File::Copy;
use File::Copy::Recursive qw(dircopy);
use File::Find;
use Term::ANSIColor;
use FindBin qw($RealBin);

$| = 1;

my $prefix = '/opt';
my $manpath = '/usr/local/man';
my $bindir = '/usr/local/bin';
my $bin_name = 'cvmfs-test';
my $force = undef;

my $ret = GetOptions ("bindir=s" => \$bindir,
					   "manpath=s" => \$manpath,
					   "prefix=s" => \$prefix,
					   "bin-name=s" => \$bin_name,
					   "force" => \$force );


# This functions accept an absolute path and will recursive
# remove all files and directories. Is the equivalent of
# 'rm -r' in any Linux system.
sub recursive_rm {
	my $path = shift;
	my $remove = sub {
		if (!-l and -d) {
			rmdir($File::Find::name)
		}
		else {
			unlink($File::Find::name);
		}
	};
	if (-e $path) {
		finddepth ( { wanted => $remove }, $path );
	}
}

unless(defined($force)) {
	if (-e "$prefix/cvmfs-test/.installed") {
		print "\ncvmfs-test seems to be already installed.\n";
		print "You can force a reinstallation with --force option.\n\n";
		exit 0;
	}
}

my $user_id = `id -u`;
chomp($user_id);

if ($user_id ne '0') {
	print 'To complete the installation process you need to be able to use sudo on your system. Are you? (N/y)';
	my $sudoers = <STDIN>;
	unless ($sudoers eq "y\n" or $sudoers eq "Y\n") { exit 0 }
}

my $zmq_retrieve = 'http://download.zeromq.org/zeromq-2.2.0.tar.gz';
my $zmq_source = './zeromq-2.2.0.tar.gz';
my $zmq_source_dir = './zeromq-2.2.0';

my $arch = `arch`;
chomp($arch);
my $libsuffix = '';

if ($arch eq 'x86_64') { $libsuffix = '64' }

print 'Downloading ZeroMQ source tarball... ';
getstore($zmq_retrieve, $zmq_source);
print "Done.\n";

if (-f $zmq_source) {
	print 'Extracting ZeroMQ source... ';
	system("tar -xzf $zmq_source");
	print "Done.\n";

	chdir $zmq_source_dir;

	system("./configure --prefix=/usr --libdir=/usr/lib" . $libsuffix);
	system('make');
	system('sudo make install');

	chdir '..';
	
	recursive_rm("$RealBin/$zmq_source_dir");
	unlink($zmq_source);
}
else {
	print color 'red';
	print "Something went wrong while trying to download $zmq_retrieve.\n";
	print "Check your internet connection and retry.\n";
	print color 'reset';
	exit 0;
}


print 'Installing cpanminus... ';
system('curl --cacert cacert.pem -L http://cpanmin.us | perl - --self-upgrade');
print "Done.\n";

print 'Creating symlink to cpanminus in /usr/bin... ';
system('ln -s /usr/local/bin/cpanm /usr/bin/cpanm');
print "Done.\n";

system("which cpanm > /dev/null 2>&1");
if ( $? == 0 ) {
	print 'Installing ZeroMQ perl modules... ';
	system('sudo cpanm ZeroMQ');
	print "Done.\n";
	
	print 'Installing IO::Interface perl module... ';
	system('sudo cpanm IO::Interface');
	print "Done.\n";

	print 'Upgrading Socket.pm version... ';
	system('sudo cpanm Socket');
	print "Done.\n";
}
else {
	print color 'red';
	print "Something went wrong while trying to download cpanm.\n";
	print "Check your internet connection and retry.\n";
	print color 'reset';
	exit 0;
}


unlink("$manpath/man1/$bin_name.1");
copy("$RealBin/man/cvmfs-test.1", "$manpath/man1/$bin_name.1");

dircopy($RealBin, "$prefix/cvmfs-test");
system("sudo chown -R cvmfs-test:cvmfs-test $prefix/cvmfs-test");

unlink("$bindir/$bin_name");
symlink "$prefix/cvmfs-test/cvmfs-testshell.pl", "$bindir/$bin_name";

system("$bin_name --setup");

open(my $create_sentinel, '>', "$prefix/cvmfs-test/.installed");
close($create_sentinel);

exit 0;
