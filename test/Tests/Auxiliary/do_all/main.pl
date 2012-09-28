use strict;
use warnings;
use Tests::Common qw(set_stdout_stderr open_test_socket close_test_socket open_shellout_socket);
use ZeroMQ qw/:all/;
use File::Find;
use Getopt::Long;

# Variables for command line options
my $outputfile = '/var/log/cvmfs-test/do_all.out';
my $errorfile = '/var/log/cvmfs-test/do_all.err';
my $no_clean = undef;

# Retrieving command line options
my $ret = GetOptions ( "stdout=s" => \$outputfile,
					   "stderr=s" => \$errorfile,
					   "no-clean" => \$no_clean );
					   
# Test name used for socket identity
my $testname = 'DO_ALL';

sub check_process {
	my $process_name = shift;
	my $running = `ps -fu cvmfs-test | grep -i $process_name | grep -v grep`;
	print $running . "\n";
	return $running;
}

# Function to get daemon output and call the function to send test output to the shell.
# I'm not going to use the get_daemon_output from Tests::Common because
# this behaviour is specific to this test.
sub get_daemon_output {
	# Retrieving socket to use
	my $socket = shift;
	my $shell_socket = shift;
	
	my ($data, $reply) = ('', '');
	
	while ($data ne "END\n") {
		$reply = $socket->recv();
		$data = $reply->data;
		
		if ($data =~ m/PROCESSING/) {
			my $process_name = (split /:/,$data)[-1];
			chomp($process_name);
			$shell_socket->send($data);
			while(check_process($process_name)) {
				sleep 3;
			}
		}
		elsif ($data =~ m/RUN_SETUP/) {
			return "RUN_SETUP";
		}
		
		print $data if $data ne "END\n" and $data !~ m/READ_RETURN_CODE/;
	}
}

# Forking the process
my $pid = fork();

if (defined ($pid) and $pid == 0) {
	# Setting STDOUT and STDERR to file in log folder.
	set_stdout_stderr($outputfile, $errorfile);
	
	# Opening the socket to communicate with the daemon
	my ($socket, $ctxt) = open_test_socket($testname);
	
	# Opening the socket to send the output to the shell
	my ($shell_socket, $shell_ctxt) = open_shellout_socket();
	
	# Array to store every main.pl files
	my @main_pl;
	
	# This functions will select tests main.pl file
	my $select = sub {
		if ($File::Find::name =~ m/.*\/main.pl$/ and $File::Find::name !~ m/Auxiliary/) {
		print "Found: $File::Find::name\n";
		push @main_pl, $File::Find::name;
		}
	};
	find( { wanted => $select }, 'Tests/' );
	
	# Sending a command to the daemon for each main.pl file found to start different test
	foreach (@main_pl) {
		my $command = (split /\//, $_)[-2];
		$socket->send("$command --do-all");
		my $ret_value = get_daemon_output($socket, $shell_socket);
		if ($ret_value eq "RUN_SETUP") {
			$socket->send("$command --do-all --setup");
		}
	}
	
	$shell_socket->send("All tests processed.\n");
	$shell_socket->send("END_ALL\n");
	
	close_test_socket($socket, $ctxt);
	close_test_socket($shell_socket, $shell_ctxt);
}

# This will be ran by the main script.
# These lines will be sent back to the daemon and the damon will send them to the shell.
if (defined ($pid) and $pid != 0) {
	print "$testname will run all tests. It will take a long time\n";
	print "You can read its output in $outputfile.\n";
	print "Errors are stored in $errorfile.\n";
	print "PROCESSING:$testname\n";
	# This is the line that makes the shell waiting for test output.
	# Change whatever you want, but don't change this line or the shell will ignore exit status.
	print "READ_RETURN_CODE\n";
}

exit 0;
