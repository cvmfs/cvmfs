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
my $outputfile = '/var/log/cvmfs-test/dns_timeout.out';
my $errorfile = '/var/log/cvmfs-test/dns_timeout.err';
my $no_clean = undef;
my $outputfifo = '/tmp/returncode.fifo';

# Socket name is set to let the server to select
# the socket where to send its response.
my $testname = 'DNS_TIMEOUT';

# Repository name
my $repo_name = 'mytestrepo.cern.ch';

# Variables used to record tests result. Set to 0 by default, will be changed
# to 1 for mount_successful if the test succed and to seconds needed for timeout
# for the other two tests.
my ($mount_successful, $server_timeout, $proxy_timeout) = (0, 0, 0);

# Array to store PID of services. Every service will be killed after every test.
my @pids;

# Retrieving command line options
my $ret = GetOptions ( "stdout=s" => \$outputfile,
					   "stderr=s" => \$errorfile,
					   "no-clean" => \$no_clean,
					   "fifo=s" => \$outputfifo );


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
	
	# Common test setup
	setup_environment($tmp_repo, $repo_name);

	# Creating /etc/resolv.conf backup
	print 'Creating resolv.conf backup... ';
	my $resolv_temp = `mktemp /tmp/resolv_XXXXXX` || die "Couldn't backup /etc/resolv.conf: $!\n.";
	chomp($resolv_temp);
	system("sudo cat /etc/resolv.conf > $resolv_temp");
	print "Done.\n";

	# Overwriting /etc/resolv.conf with an empty file to avoid that the system complains about
	# dns answer arriving from a different host than the one expected.
	print 'Overwriting resolv.conf... ';
	system('sudo bash -c "echo \"\" > /etc/resolv.conf"');
	print "Done.\n";
	
	# Saving iptables rules
	print 'Creating iptables rules backup... ';
	system('sudo Tests/Common/iptables_rules.sh backup');
	print "Done.\n";
	
	# Adding iptables rules to redirect any dns request to non-standard port 5300
	print 'Adding iptables rules... ';
	system('sudo Tests/Common/iptables_rules.sh forward domain 5300');
	print "Done.\n";

	# Configuring cvmfs for the first two tests.
	print 'Configuring cvmfs... ';
	system("sudo $RealBin/config_cvmfs.sh");
	print "Done.\n";

	print '-'x30 . 'MOUNT_SUCCESSFUL' . '-'x30 . "\n";
	print "Starting services for mount_successfull test...\n";
	$socket->send("httpd --root $repo_pub --index-of --all --port 8080");
	@pids = get_daemon_output($socket, @pids);
	sleep 5;
	$socket->send('webproxy --port 3128 --backend http://mytestrepo.cern.ch:8080');
	@pids = get_daemon_output($socket, @pids);
	sleep 5;
	$socket->send('named --port 5300 --add mytestrepo.cern.ch=127.0.0.1');
	@pids = get_daemon_output($socket, @pids);
	sleep 5;
	print "Done.\n";

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

	@pids = killing_services($socket, @pids);

	restart_cvmfs_services();

	print '-'x30 . 'PROXY_TIMEOUT' . '-'x30 . "\n";
	print "Starting services for proxy_timeout test...\n";
	$socket->send("httpd --root $repo_pub --index-of --all --port 8080");
	@pids = get_daemon_output($socket, @pids);
	sleep 5;
	$socket->send('webproxy --port 3128 --backend http://mytestrepo.cern.ch:8080');
	@pids = get_daemon_output($socket, @pids);
	sleep 5;
	$socket->send('named --port 5300 --add mytestrepo.cern.ch=127.0.0.1 --timeout');
	@pids = get_daemon_output($socket, @pids);
	sleep 5;
	print "Done.\n";

	# For this test, we shouldn't be able to mount the repo. If possibile, setting its variable
	# to 1.
	$proxy_timeout = check_mount_timeout("/cvmfs/$repo_name", 10);
	
	if ($proxy_timeout <= 20 and $proxy_timeout >= 18) {
	    $shell_socket->send("Proxy timeout took $proxy_timeout seconds to fail... OK.\n");
	}
	else {
	    $shell_socket->send("Proxy timeout took $proxy_timeout seconds to fail... WRONG.\n");
	}

	@pids = killing_services($socket, @pids);

	restart_cvmfs_services();
	
	print '-'x30 . 'SERVER_TIMEOUT' . '-'x30 . "\n";
	
	# Reconfigurin cvmfs to not use any proxy to test timeout setting with direct connection
	print 'Configuring cvmfs without proxy... ';
	system("sudo $RealBin/config_cvmfs_noproxy.sh");
	print "Done.\n";

	print "Starting services for server_timeout test...\n";
	$socket->send("httpd --root $repo_pub --index-of --all --port 8080");
	@pids = get_daemon_output($socket, @pids);
	sleep 5;
	$socket->send('named --port 5300 --add mytestrepo.cern.ch=127.0.0.1 --timeout');
	@pids = get_daemon_output($socket, @pids);
	sleep 5;
	print "All services started.\n";

	# For this test, we shouldn't be able to mount the repo. If possibile, setting its variable
	# to 1.
	$server_timeout = check_mount_timeout("/cvmfs/$repo_name", 5);
	
	if ($server_timeout <= 10 and $server_timeout >=8) {
	    $shell_socket->send("Server timeout took $server_timeout seconds to fail... OK.\n");
	}
	else {
	    $shell_socket->send("Server timeout took $server_timeout seconds to fail... WRONG.\n");
	}	

	@pids = killing_services($socket, @pids);

	restart_cvmfs_services();
	
	# Restoring resolv.conf
	print 'Restoring resolv.conf backup... ';
	system("sudo cp $resolv_temp /etc/resolv.conf");
	print "Done.\n";
	
	# Restarting iptables, it will load previously saved rules
	print 'Restoring iptables rules... ';
	system('sudo Tests/Common/iptables_rules.sh restore');
	print "Done.\n";
	
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
