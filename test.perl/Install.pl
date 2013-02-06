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
my $manpath = '/usr/man';
my $bindir = '/usr/bin';
my $bin_name = 'cvmfs-test';
my $cpanm_bin = '/usr/bin/cpanm';
my $force = undef;

my $user_id = `id -u`;
chomp($user_id);

# This function check if the user is running Install.pl with root priviledges
sub is_root {
	if ($user_id == 0) {
		return 1;
	}
	else {
		return 0;
	}
}

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

# This function will be called with --uninstall command line options
sub uninstall {
	if (is_root()) {
		if (-f "$prefix/cvmfs-test/.installed") {
			print "This process will not remove cpanm or any perl module installed during the installation.\n";
			print "This is because it can\'t know if they were installed witch cvmfs-test or independently.\n";
			
			print "Removing $bindir/$bin_name... ";
			unlink("$bindir/$bin_name");
			print "Done.\n";
			
			print "Removing $manpath/man1/$bin_name.1... ";
			unlink("$manpath/man1/$bin_name.1");
			print "Done.\n";
			
			print 'Removing everything else... ';
			recursive_rm("$prefix/cvmfs-test");
			print "Done.\n";
			
			print "Uninstall complete.\n";
		}
		else {
			print "cvmfs-test doesn't seem to be installed in $prefix. Unable to uninstall.\n";
		}
	}
	else {
		print "You need to be root to uninstall cvmfs-test.\n";
	}
	exit 0;
}

my $ret = GetOptions ("bindir=s" => \$bindir,
					  "manpath=s" => \$manpath,
					  "prefix=s" => \$prefix,
					  "force" => \$force,
					  "uninstall" => sub { uninstall() } );

unless(defined($force)) {
	if (-e "$prefix/cvmfs-test/.installed") {
		print "\ncvmfs-test seems to be already installed.\n";
		print "You can force a reinstallation with --force option.\n\n";
		exit 0;
	}
}

unless (is_root()) {
	print "You need to be root to install cvmfs-test.\n";
	exit 0;
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

	print "Compiling $zmq_source... ";
	system('./configure --prefix=/usr --libdir=/usr/lib' . $libsuffix . " > configure.log 2>&1");
	system("make > make.log 2>&1");
	system('sudo make install');
	
	if (!-e '/usr/lib' . $libsuffix . '/libzmq.so') {
		print color 'red';
		print "Something went wrong while trying to compile ZeroMQ.\n";
		print "Have a look at $zmq_source_dir/configure.log and $zmq_source_dir/make.log.\n";
		print color 'reset';
		exit 0;
	}
	print "Done.\n";

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
system("curl --cacert cacert.pem -o $cpanm_bin -L http://cpanmin.us");
system("chmod 555 $cpanm_bin");
print "Done.\n";

my $cpanm_inpath = `which cpanm 2> /dev/null`;
chomp($cpanm_inpath);
if ( $cpanm_inpath ne "" ) {
	print 'Installing ZeroMQ perl modules... ';
	system('sudo cpanm ZeroMQ');
	print "Done.\n";
	
	print 'Installing IO::Interface perl module... ';
	system('sudo cpanm IO::Interface');
	print "Done.\n";
	
	print 'Installing Term::ReadLine::Gnu perl module... ';
	system('sudo cpanm Term::ReadLine::Gnu');
	print "Done.\n";

	print 'Upgrading Socket.pm version... ';
	system('sudo cpanm Socket');
	print "Done.\n";
}
else {
	print color 'red';
	print "Something went wrong while trying to run cpanm.\n";
	print color 'reset';
	exit 0;
}

unlink("$manpath/man1/$bin_name.1");
copy("$RealBin/man/cvmfs-test.1", "$manpath/man1/$bin_name.1");

dircopy($RealBin, "$prefix/cvmfs-test");

unlink("$bindir/$bin_name");
symlink "$prefix/cvmfs-test/cvmfs-testshell.pl", "$bindir/$bin_name";

system("$bin_name --setup");

open(my $create_sentinel, '>', "$prefix/cvmfs-test/.installed");
close($create_sentinel);

system("sudo chown -R cvmfs-test:cvmfs-test $prefix/cvmfs-test");

# Chowning again the wrapper to root
my $chowned = chown 0, 0, "$prefix/cvmfs-test/cvmfs-testdwrapper";
unless ($chowned == 1) {
	print "Error while changing cvmfs-testdwrapper owner and group to root.\n";
	print "It must belong to root in order for the shell to works. Do it manually.\n";
}

# Adding againg setuid bit to the wrapper
my $chmoded = chmod 04555, "$prefix/cvmfs-test/cvmfs-testdwrapper";
unless ($chmoded == 1) {
	print "Error while setting setuid bit to cvmfs-testdwrapper. Add it manually.\n";
}

exit 0;
