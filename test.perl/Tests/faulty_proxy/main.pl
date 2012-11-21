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
my $outputfile = '/var/log/cvmfs-test/faulty_proxy.out';
my $errorfile = '/var/log/cvmfs-test/faulty_proxy.err';
my $no_clean = undef;
my $do_all = undef;
my $shell_path = '127.0.0.1:6651';

# Socket path and socket name. Socket name is set to let the server to select
# the socket where to send its response.
my $testname = 'FAULTY_PROXY';

# Name for the cvmfs repository
my $repo_name = '127.0.0.1';

# Variables used to record tests result. Set to 0 by default, will be changed
# to 1 if it will be able to mount the repo.
my ($proxy_crap, $server_timeout, $mount_successful) = (0, 0, 0);

# Array to store PID of services. Every service will be killed after every test.
my @pids;

# Retrieving command line options
my $ret = GetOptions ( "stdout=s" => \$outputfile,
					   "stderr=s" => \$errorfile,
					   "no-clean" => \$no_clean,
					   "do-all" => \$do_all,
					   "shell-path=s" => \$shell_path );


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
	my ($shell_socket, $shell_ctxt) = open_shellout_socket('tcp://', $shell_path);

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
	
	# Creating file to be served for --deliver-crap proxy option
	print 'Creating faulty file... ';
	open (my $faultyfh, '>', '/tmp/cvmfs.faulty') or die "Couldn't create /tmp/cvmfs.faulty: $!\n";
	print $faultyfh 'A'x1024x10;
	print "Done.\n";
	
	print '-'x30 . 'MOUNT SUCCESSFUL' . '-'x30 . "\n";
	print "Starting services for mount_successfull test...\n";
	$socket->send("httpd --root $repo_pub --index-of --all --port 8080");
	@pids = get_daemon_output($socket, @pids);
	sleep 5;
	$socket->send("webproxy --port 3128 --backend http://$repo_name:8080");
	@pids = get_daemon_output($socket, @pids);
	sleep 5;	
	print "Services started.\n";

	# For this first test, we should be able to mount the repo. So, if possible, setting its variable
	# to 1.
	if (check_repo("/cvmfs/$repo_name")){
	    $mount_successful = 1;
	}
	
	if ($mount_successful == 1) {
	    $shell_socket->send("Able to mount the repo with right configuration... OK.\n");
	}
	else {
	    $shell_socket->send("Unable to mount the repo with right configuration... WRONG.\n");
	}

	@pids = killing_services($socket, @pids);

	restart_cvmfs_services();

	print '-'x30 . 'PROXY CRAP' . '-'x30 . "\n";
	print "Starting services for proxy_crap test...\n";
	$socket->send("httpd --root $repo_pub --index-of --all --port 8080");
	@pids = get_daemon_output($socket, @pids);
	sleep 5;
	$socket->send("webproxy --port 3128 --deliver-crap --fail all");
	@pids = get_daemon_output($socket, @pids);
	sleep 5;
	print "Done.\n";

	# For this test, we shouldn't be able to mount the repo. If possibile, setting its variable
	# to 1.
	if (check_repo('/cvmfs/$repo_name')){
	    $proxy_crap = 1;
	}
	
	if ($proxy_crap == 1) {
	    $shell_socket->send("Able to mount the repo with faulty proxy configuration... WRONG.\n");
	}
	else {
	    $shell_socket->send("Unable to mount the repo with faulty proxy configuration... OK.\n");
	}

	@pids = killing_services($socket, @pids);

	restart_cvmfs_services();
	
	print '-'x30 . 'SERVER TIMEOUT' . '-'x30 . "\n";
	print "Starting services for server_timeout test...\n";
	$socket->send("httpd --root $repo_pub --index-of --all --port 8080 --timeout");
	@pids = get_daemon_output($socket, @pids);
	sleep 5;
	$socket->send("webproxy --port 3128 --backend http://$repo_name:8080");
	@pids = get_daemon_output($socket, @pids);
	sleep 5;
	print "All services started.\n";

	# For this test, we shouldn't be able to mount the repo. If possibile, setting its variable
	# to 1.
	if (check_repo('/cvmfs/$repo_name')){
	    $server_timeout = 1;
	}
	
	if ($server_timeout == 1) {
	    $shell_socket->send("Able to mount repo with server timeout configuration... WRONG.\n");
	}
	else {
	    $shell_socket->send("Unable to mount the repo with server timeout configuration... OK.\n");
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
