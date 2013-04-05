use strict;
use warnings;
use ZeroMQ qw/:all/;
use Tests::Common qw (get_daemon_output killing_services check_repo setup_environment restart_cvmfs_services check_mount_timeout set_stdout_stderr open_test_socket close_test_socket open_shellout_socket);
use Getopt::Long;
use FindBin qw($RealBin);

# Folders where to extract the repo and document root for httpd
my $tmp_repo = '/tmp/server/repo/';
my $repo_pub = $tmp_repo . 'pub';

# Variables for GetOpt
my $outputfile = '/var/log/cvmfs-test/mount_repo.out';
my $errorfile = '/var/log/cvmfs-test/mount_repo.err';
my $no_clean = undef;
my $shell_path = '127.0.0.1:6651';

# Socket name is set to let the server to select
# the socket where to send its response.
my $testname = 'MOUNT_REPO';

# Repository name
my $repo_name = '127.0.0.1';

# Variables used to record tests result. Set to 0 by default, will be changed
# to 1 for mount_successful if the test succed and to seconds needed for timeout
# for the other two tests.
my ($mount_successful) = (0);

# Variable use for debug purpose;
my $break_point = undef;

# Retrieving command line options
my $ret = GetOptions ( "stdout=s" => \$outputfile,
					   "stderr=s" => \$errorfile,
					   "no-clean" => \$no_clean,
					   "shell-path=s" => \$shell_path,
					   "breakpoint|bp=i" => \$break_point );

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
	
	# Common test setup
	setup_environment($tmp_repo, $repo_name);
	
		# Configuring cvmfs for the first two tests.
	print 'Configuring cvmfs... ';
	system("sudo $RealBin/config_cvmfs.sh");
	print "Done.\n";

	print '-'x30 . 'MOUNT_SUCCESSFUL' . '-'x30 . "\n";
	print "Starting services for mount_successfull test...\n";
	$socket->send("httpd --root $repo_pub --index-of --all --port 8080");
	get_daemon_output($socket);
	sleep 5;
	$socket->send('webproxy --port 3128 --backend http://127.0.0.1:8080');
	get_daemon_output($socket);
	sleep 5;
	print "Done.\n";
	
	# Exiting if break_point is set to 1
	if ($break_point == 1) {
			close_test_socket($socket, $ctxt);
			
			$shell_socket->send("Exiting at breakpoint $break_point. Good debug.\n");
			$shell_socket->send("END\n");
			close_test_socket($shell_socket, $shell_ctxt);
			exit 0;
	}
	
	# For this first test, we should be able to mount the repo. So, if possibile, setting its variable
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
