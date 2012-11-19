use strict;
use warnings;
use ZeroMQ qw/:all/;
use Tests::Common qw(get_daemon_output killing_services check_repo setup_environment restart_cvmfs_services find_files recursive_mkdir set_stdout_stderr open_test_socket close_test_socket open_shellout_socket);
use File::Copy;
use File::Find;
use Getopt::Long;
use FindBin qw($RealBin);

# Folders where to extract the repo, document root for httpd and other useful folder
my $tmp_repo = '/tmp/server/repo/';
my $repo_pub = $tmp_repo . 'pub';
my $repo_data = $repo_pub . '/data';
my $datachunk_backup = '/tmp/cvmfs_backup/datachunk';

# Variables for GetOpt
my $outputfile = '/var/log/cvmfs-test/repo_signature.out';
my $errorfile = '/var/log/cvmfs-test/repo_signature.err';
my $no_clean = undef;
my $setup = undef;
my $do_all = undef;
my $shell_path = '127.0.0.1:6651';

# Test name used for socket identity and some output lines
my $testname = 'REPO_SIGNATURE';

# Name for the cvmfs repository
my $repo_name = '127.0.0.1';

# Variables used to record tests result. Set to 0 by default, will be changed
# to 1 if it will be able to mount the repo.
my ($mount_successful, $broken_signature, $garbage_datachunk, $garbage_zlib) = (0, 0, 0, 0);

# Array to store PID of services. Every service will be killed after every test.
my @pids;

# Retrieving command line options
my $ret = GetOptions ( "stdout=s" => \$outputfile,
					   "stderr=s" => \$errorfile,
					   "no-clean" => \$no_clean,
					   "setup" => \$setup,
					   "do-all" => \$do_all,
					   "shell-path=s" => \$shell_path );

# If setup option was invoked, compile zpipe and exit.
if (defined($setup)) {
	print 'Compiling zpipe... ';
	system("gcc -o Tests/Common/zpipe.run Tests/Common/zpipe.c -lz");
	print "Done.\n";
	print "Setup complete. You're now able to run the test.\n";
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
	my ($shell_socket, $shell_ctxt) = open_shellout_socket('tcp://', $shell_path);
	
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
	    $shell_socket->send("Able to mount the repo with right configuration... OK.\n");
	}
	else {
	    $shell_socket->send("Unable to mount the repo with right configuration... WRONG.\n");
	}

	restart_cvmfs_services();
	
	print '-'x30 . 'BROKEN_SIGNATURE' . '-'x30 . "\n";
	my $published = $repo_pub . '/catalogs/.cvmfspublished';
	print "Creating $published backup... ";
	copy($published, "$published.bak");
	print "Done.\n";
	print "Creating a corrupted $published... ";
	open(my $cvmfs_pub_fh, '>>', $published);
	print $cvmfs_pub_fh '0';
	close $cvmfs_pub_fh;
	print "Done.\n";
	
	# For this second test, we should not be able to mount the repo. If possible, setting its variable
	# to 1.
	if (check_repo("/cvmfs/$repo_name")){
	    $broken_signature = 1;
	}
	
	if ($broken_signature == 1) {
	    $shell_socket->send("Able to mount the repo with garbage .cvmfspublished... WRONG.\n");
	}
	else {
	    $shell_socket->send("Unable to mount the repo with garbage .cvmfspublished... OK.\n");
	}
	
	print "Restoring $published... ";
	unlink($published);
	copy("$published.bak", $published);
	print "Done.\n";

	restart_cvmfs_services();
	
	print '-'x30 . 'GARBAGE_DATACHUNK' . '-'x30 ."\n";
	print 'Retrieving files... ';
	my @file_list = find_files($repo_data);
	print 'Creating backup directory for data chunk... ';
	recursive_mkdir($datachunk_backup);
	print "Done.\n";
	print 'Appending plain text to all files... ';
	foreach (@file_list) {
		(my $filename = $_) =~ s/\/.*\/(.*)$/$1/;
		copy($_, "$datachunk_backup/$filename");		
		open (my $data_fh, '>>', $_);
		print $data_fh 'garbage';
		close $data_fh;
	}
	print "Done.\n";
	
	# For this third test, we should not be able to mount the repo. If possible, setting its variable
	# to 1.
	if (check_repo("/cvmfs/$repo_name")){
	    $garbage_datachunk = 1;
	}
	
	if ($garbage_datachunk == 1) {
	    $shell_socket->send("Able to mount the repo with garbage datachunk... WRONG.\n");
	}
	else {
	    $shell_socket->send("Unable to mount the repo with garbage datachunk... OK.\n");
	}
	
	print 'Restoring data chunk backup... ';
	foreach (@file_list) {
		(my $filename = $_) =~ s/\/.*\/(.*)$/$1/;
		copy("$datachunk_backup/$filename", $_);
	}
	print "Done.\n";
	
	restart_cvmfs_services();
	
	print '-'x30 . 'GARBAGE_ZLIB' . '-'x30 ."\n";
	# We don't need to retrieve again the file_list as long as it is the same of previous test
	print 'Appending zlib compressed content to all files... ';
	foreach (@file_list) {
		(my $filename = $_) =~ s/\/.*\/(.*)$/$1/;		
		system("sh -c \"echo garbage | Tests/Common/zpipe.run >> $_\"");
	}
	print "Done.\n";
	
	# For this third test, we should not be able to mount the repo. If possible, setting its variable
	# to 1.
	if (check_repo("/cvmfs/$repo_name")){
	    $garbage_zlib = 1;
	}	
	
	if ($garbage_zlib == 1) {
	    $shell_socket->send("Able to mount the repo with garbage zlib... WRONG.\n");
	}
	else {
	    $shell_socket->send("Unable to mount the repo with garbage zlib... OK.\n");
	}
	
	print 'Restoring data chunk backup... ';
	foreach (@file_list) {
		(my $filename = $_) =~ s/\/.*\/(.*)$/$1/;
		copy("$datachunk_backup/$filename", $_);
	}
	print "Done.\n";
	
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
