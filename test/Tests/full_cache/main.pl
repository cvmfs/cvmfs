use strict;
use warnings;
use ZeroMQ qw/:all/;
use Tests::Common qw(get_daemon_output killing_services check_repo setup_environment restart_cvmfs_services set_stdout_stderr open_test_socket close_test_socket open_shellout_socket);
use Getopt::Long;
use FindBin qw($RealBin);

# Folders where to extract the repo and document root for httpd
my $tmp_repo = '/tmp/server/repo/';
my $repo_pub = $tmp_repo . 'pub';

# Variables for GetOpt
my $outputfile = '/var/log/cvmfs-test/full_cache.out';
my $errorfile = '/var/log/cvmfs-test/full_cache.err';
my $setup = undef;
my $no_clean = undef;
my $do_all = undef;

# Socket name is set to let the server to select
# the socket where to send its response.
my $testname = 'FULL_CACHE';

# Name for the cvmfs repository
my $repo_name = '127.0.0.1';

# Variables used to record tests result. Set to 0 by default, will be changed
# to 1 if it will be able to mount the repo.
my ($mount_successful, $transferred_kbytes) = (0, 0);

# Array to store PID of services. Every service will be killed after every test.
my @pids;

# Retrieving command line options
my $ret = GetOptions ( "stdout=s" => \$outputfile,
					   "stderr=s" => \$errorfile,
					   "no-clean" => \$no_clean,
					   "setup" => \$setup,
					   "do-all" => \$do_all );


# If setup option was invoked, compile zpipe and exit.
if (defined($setup)) {
	print 'Compiling zpipe... ';
	system("gcc -o Tests/Common/zpipe.run Tests/Common/zpipe.c -lz");
	print "Done.\n";
	print "Setup complete. You're now able to run the test.\n";
	exit 0;
}
					   
# This test need zpipe to be compiled. If it's not compiled yet, exiting and asking for
# setup.
unless (-e "Tests/Common/zpipe.run") {
	print "zpipe has to be compiled in order to run this test.\n";
	print "Run 'repo_signature --setup' to compile it.\n";
	if (defined($do_all)) {
		print "RUN_SETUP\n";
	}
	exit 0;
}

# Forking the process so the daemon can come back in listening mode.
my $pid = fork();

# This will be ran only by the forked process. Everything here will be logged in a file and
# will not be sent back to the daemon.
if (defined ($pid) and $pid == 0) {
	# Setting STDOUT and STDERR to file in log folder.
	set_stdout_stderr($outputfile, $errorfile);

	# Opening the socket to communicate with the server and setting is identity.
	my ($socket, $ctxt) = open_test_socket($testname);
	
	# Opening the socket to send the output to the shell
	my ($shell_socket, $shell_ctxt) = open_shellout_socket();

	# Cleaning the environment if --no-clean is undef.
	# See 'Tests/clean/main.pl' if you want to know what this command does.
	if (!defined($no_clean)) {
		print "\nCleaning the environment:\n";
		$socket->send("clean");
		get_daemon_output($socket);
		sleep 5;
	}
	else {
		print "\nSkipping cleaning.\n";
	}
	
	# Common environment setup	
	setup_environment($tmp_repo, $repo_name);
	
	# Configuring cvmfs for sequent test
	print 'Configuring cvmfs... ';
	system("sudo $RealBin/config_cvmfs.sh");
	print "Done.\n";
	
	# Creating a very large file to be served for --deliver-crap httpd option
	print 'Creating faulty file... ';
	open (my $faultyfh, '>', '/tmp/cvmfs.faulty_temp');
	while( -s '/tmp/cvmfs.faulty_temp' < 1024*1024*100 ) {
		my $random = rand(1000000);
		print $faultyfh $random;
	}
	close($faultyfh);
	# Compressing the file with zpipe
	while (-s '/tmp/cvmfs.faulty' < 1024*1024*100 ) {
		system('cat /tmp/cvmfs.faulty_temp | Tests/Common/zpipe.run >> /tmp/cvmfs.faulty');
	}
	# Erasing temp file
	my $cvmfsfaulty_size = -s '/tmp/cvmfs.faulty';
	my $cvmfsfaulty_ksize = int($cvmfsfaulty_size / 1024);
	unlink('/tmp/cvmfs.faulty_temp');
	print "Done.\n";
	
	print '-'x30 . 'FULL_CACHE' . '-'x30 . "\n";
	print "Starting services for full_cache test...\n";
	$socket->send("httpd --root $repo_pub --port 8080 --deliver-crap");
	@pids = get_daemon_output($socket, @pids);
	sleep 5;
	$socket->send("webproxy --port 3128 --backend http://$repo_name:8080 --record-transfer");
	@pids = get_daemon_output($socket, @pids);
	sleep 5;	
	print "Services started.\n";
	
	if (check_repo("/cvmfs/$repo_name")){
	    $mount_successful = 1;
	}
	
	if ($mount_successful == 1) {
		$shell_socket->send("Able to mount the repo with very large garbage zlib... WRONG.\n");
	}
	else {
		$shell_socket->send("Unable to mount the repo with very large garbage zlib... OK.\n");
	}
	
	# Checking how much bytes were received
	open (my $transferfh, '<', '/tmp/transferred_data');
	my $transferred_bytes = $transferfh->getline;
	$transferred_kbytes = int($transferred_bytes / 1024);
	close($transferfh);
	unlink('/tmp/transferred_data');
	
	sleep 2;
	if ($transferred_bytes < $cvmfsfaulty_size / 50) {
		$shell_socket->send("Only $transferred_kbytes KB on $cvmfsfaulty_ksize KB were received... OK.\n");
	}
	else {
		$shell_socket->send("$transferred_kbytes KB were received... WRONG.\n");
	}
	
	@pids = killing_services($socket, @pids);

	restart_cvmfs_services();
	
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
	print "READ_RETURN_CODE\n";
}

exit 0;
