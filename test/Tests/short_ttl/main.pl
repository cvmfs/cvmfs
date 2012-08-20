use strict;
use warnings;
use ZeroMQ qw/:all/;
use Functions::FIFOHandle qw(print_to_fifo);
use Tests::Common qw(get_daemon_output killing_services check_repo setup_environment restart_cvmfs_services find_files recursive_mkdir open_test_socket close_test_socket set_stdout_stderr open_shellout_socket);
use File::Copy;
use File::Find;
use Getopt::Long;
use FindBin qw($RealBin);

# Folders where to extract the repo, document root for httpd and other useful folder
my $tmp_repo = '/tmp/server/repo/';
my $repo_pub = $tmp_repo . 'pub';

# Variables for GetOpt
my $outputfile = '/var/log/cvmfs-test/short_ttl.out';
my $errorfile = '/var/log/cvmfs-test/short_ttl.err';
my $outputfifo = '/tmp/returncode.fifo';
my $no_clean = undef;
my $no_remount = undef;

# Socket path and socket name. Socket name is set to let the server to select
# the socket where to send its response.
my $socket_protocol = 'ipc://';
my $socket_path = '/tmp/server.ipc';
my $testname = 'SHORT_TTL';

# Name for the cvmfs repository
my $repo_name = '127.0.0.1';

# Variables used to record tests result. Set to 0 by default, will be changed
# to 1 if it will be able to mount the repo.
my ($mount_successful, $mount_cache, $ttl_cache, $ttl_normal, $remount_successful) = (0, 0, 0, 0, 0);

# Array to store PID of services. Every service will be killed after every test.
my @pids;

# Retrieving command line options
my $ret = GetOptions ( "stdout=s" => \$outputfile,
					   "stderr=s" => \$errorfile,
					   "no-clean" => \$no_clean,
					   "no-remount" => \$no_remount,
					   "fifo=s" => \$outputfifo );
					   
# Forking the process so the daemon can come back in listening mode.
my $pid = fork();

# This will be ran only by the forked process. Everything here will be logged in a file and
# will not be sent back to the daemon.
if (defined ($pid) and $pid == 0) {
	set_stdout_stderr($outputfile, $errorfile);
	
	# Opening the socket to communicate with the server and setting is identity.
	my ($socket, $ctxt) = open_test_socket($testname);
	
	# Opening the socket to send the output to the shell
	my ($shell_socket, $shell_ctxt) = open_shellout_socket();
	
	# Cleaning the environment if --no-clean is undef.
	# See 'Tests/clean/main.pl' if you want to know what this command does.
	if (!defined($no_clean)) {
		print "Cleaning the environment:\n";
		$socket->send('clean');
		get_daemon_output($socket);
		sleep 5;
	}
	else {
		print "\nSkipping cleaning.\n";
	}
	
	# Common test setup
	setup_environment($tmp_repo, $repo_name);
	
	# Configuring cvmfs for the first two tests.
	print 'Configuring cvmfs... ';
	system("sudo $RealBin/config_cvmfs.sh");
	print "Done.\n";
	
	# Retrieving cvmfs version for short_ttl time
	print 'Checking cvmfs version to calculate short_ttl settings... ';
	my $cvmfs_version = `cvmfs2 --version`;
	chomp($cvmfs_version);
	my $expected_ttl;
	if ($cvmfs_version =~ m/2\.0/) {
		$expected_ttl = 4;
	}
	else {
		$expected_ttl = 3;
	}
	print "Done.\n";
	
	# For this testcase I'm not going to kill https for each test as long as I need it
	# with the same configuration for all tests. Restarting cvmfs will be enough.
	print "Starting services for mount_successfull test...\n";
	$socket->send("httpd --root $repo_pub --index-of --all --port 8080");
	@pids = get_daemon_output($socket, @pids);
	sleep 5;
	print "Services started.\n";
	
	print '-'x30 . 'MOUNT_SUCCESSFUL' . '-'x30 . "\n";
	# For this first test, we should be able to mount the repo. So, if possible, setting its variable
	# to 1.
	if (check_repo("/cvmfs/$repo_name")){
	    $mount_successful = 1;
	}
	
	if ($mount_successful == 1) {
	    $shell_socket->send("Able to mount the repo with active server... OK.\n");
	}
	else {
	    $shell_socket->send("Unable to mount the repo with active server... WRONG.\n");
	}
	
	print '-'x30 . 'TTL_NORMAL' . '-'x30 . "\n";
	print 'Checking ttl for live connection... ';
	$ttl_normal = `attr -g expires /cvmfs/127.0.0.1 | grep -v expires`;
	chomp($ttl_normal);
	print "Done.\n";
	
	if ($ttl_normal <= 60 and $ttl_normal >= 55) {
		$shell_socket->send("TTL for live connection was $ttl_normal... OK.\n");
	}
	else {
		$shell_socket->send("TTL for live connection was $ttl_normal... WRONG.\n");
	}
	
	print 'Unmounting cvmfs repo... ';
	system('sudo umount cvmfs2');
	print "Done.\n";
	
	print '-'x30 . 'MOUNT_FROM_CACHE' . '-'x30 . "\n";	
	@pids = killing_services($socket, @pids);
	
	if (check_repo("/cvmfs/$repo_name")){
	    $mount_cache = 1;
	}
	
	if ($mount_cache == 1) {
		$shell_socket->send("Able to mount the repo from cache... OK.\n");
	}
	else {
		$shell_socket->send("Unable to mount the repo from cache... WRONG.\n");
	}
	
	print '-'x30 . 'TTL_CACHE' . '-'x30 . "\n";
	print 'Checking ttl for cached repository... ';
	$ttl_cache = `attr -g expires /cvmfs/127.0.0.1 | grep -v expires`;
	chomp($ttl_cache);
	print "Done.\n";
	
	if ($ttl_cache <= $expected_ttl and $ttl_cache >= 1) {
		$shell_socket->send("TTL for cached connection was $ttl_cache... OK.\n");
	}
	else {
		$shell_socket->send("TTL for cached connection was $ttl_cache... WRONG.\n");
	}
	
	unless(defined($no_remount)) {
		print '-'x30 . 'REMOUNT_SUCCESSFUL' . '-'x30 . "\n";
		print "Restarting httpd...\n";
		$socket->send("httpd --root $repo_pub --index-of --all --port 8080");
		@pids = get_daemon_output($socket, @pids);
		print "Done.\n";
		
		my $offset = $ttl_cache + ($expected_ttl - $ttl_cache) + 1;
		print "Sleeping $offset minutes to check if remount is done.\n";
		my $slept = sleep ($offset * 60);
		print "Slept for $slept seconds.\n";
		
		check_repo("/cvmfs/$repo_name");
		
		print 'Checking if live remount is done... ';
		$remount_successful = `attr -g expires /cvmfs/127.0.0.1 | grep -v expires`;
		chomp($remount_successful);
		print "Done.\n";
		
		if ($remount_successful <= 60 and $remount_successful >= 55) {
			$shell_socket->send("TTL after $offset minutes was $remount_successful... OK.\n");
		}
		else {
			$shell_socket->send("TTL after $offset minutes was $remount_successful... WRONG.\n");
		}
	}
	else {
		$shell_socket->send("Remount test skipped.\n");
	}
	
	close_test_socket($socket, $ctxt);
	
	$shell_socket->send("END\n");
	close_test_socket($shell_socket, $shell_ctxt);
}

# This will be ran by the main script.
# These lines will be sent back to the daemon and the damon will send them to the shell.
if (defined ($pid) and $pid != 0) {
	print "$testname test started.\n";
	print "You can read its output in $outputfile.\n";
	print "Errors are stored in $errorfile.\n";
	print "PROCESSING:$testname\n";
	# This is the line that makes the shell waiting for test output.
	# Change whatever you want, but don't change this line or the shell will ignore exit status.
	print "READ_RETURN_CODE:$outputfifo\n";
}

exit 0;
