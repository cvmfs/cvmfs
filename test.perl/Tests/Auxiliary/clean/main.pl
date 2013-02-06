use strict;
use warnings;
use ZeroMQ qw/:all/;
use FindBin qw($RealBin);
use Getopt::Long;
use Tests::Common qw(recursive_rm multiple_rm open_test_socket close_test_socket);

# This will be the name for the socket. This socket isn't supposed to receive any message.
# But the server will still try to send them to it. When the server will realize, that this socket
# isn't listening, the server will discard them.
my $testname = 'CLEAN';

# Variables to store commandline options switch
my $deep = undef;

# Getopt variable to avoid warning, this script doesn't really need them
my $shell_path = undef;

my $ret = GetOptions ( "deep" => \$deep,
					   "shell-path=s" => \$shell_path );

# Print statement are self explanatory.

print 'Erasing RSA keys... ';
system("sudo rm -f /tmp/cvmfs_test.key /tmp/cvmfs_test.csr /tmp/cvmfs_test.crt /tmp/whitelist.test.* /tmp/cvmfs_master.key /tmp/cvmfs_master.pub > /dev/null 2>&1");
print "Done.\n";

print 'Erasing configuration files in /etc/cvmfs/config.d... ';
system("sudo rm -f /etc/cvmfs/config.d/127.0.0.1.conf /etc/cvmfs/config.d/mytestrepo.cern.ch > /dev/null 2>&1");
print "Done.\n";

print 'Erasing /tmp/cvmfs.faulty... ';
unlink('/tmp/cvmfs.faulty');
print "Done.\n";

print 'Erasing /tmp/server directory... ';
recursive_rm('/tmp/server');
print "Done.\n";

print 'Erasing /tmp/cvmfs_backup directory ';
recursive_rm('/tmp/cvmfs_backup');
print "Done.\n";

print 'Restarting services... ';
system("sudo Tests/Common/restarting_services.sh >> /dev/null 2>&1");
print "Done.\n";

# Cleaning operations to be executed only if --deep option was passed
if (defined($deep)) {
	print 'Erasing any /etc/resolv.conf backup... ';
	multiple_rm('/tmp', 'resolv_');
	print "Done.\n";
	
	print 'Erasing any unlinked FIFO... ';
	multiple_rm('/tmp', 'test_fifo');
	print "Done.\n";
	
}

# Opening the socket to launch 'killall' command.
my ($socket, $ctxt) = open_test_socket($testname);

print 'Killing all existing processes... ';
$socket->send('killall');
print "Done.\n";

# Closing the socket, we no longer need it.
close_test_socket($socket, $ctxt);

exit 0;
