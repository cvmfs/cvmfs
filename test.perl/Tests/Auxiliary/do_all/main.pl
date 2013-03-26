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
my %options;
my $shell_path = '127.0.0.1:6651';

# Retrieving command line options
my $ret = GetOptions ( "stdout=s" => \$outputfile,
					   "stderr=s" => \$errorfile,
					   "no-clean" => \$no_clean,
					   "option=s" => \%options,
					   "shell-path=s" => \$shell_path );
					   
# Test name used for socket identity
my $testname = 'DO_ALL';

sub check_process {
	my $process_name = shift;
	my $running = `ps -fu cvmfs-test | grep -i $process_name | grep -i perl | grep -v do_all | grep -v grep`;
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
	
	# This variable will be used to check if a re-run with --setup is required
	my $rerun = 0;
	
	while ($data ne "END\n") {
		$reply = $socket->recv();
		$data = $reply->data;
		
		if ($data =~ m/PROCESSING/) {
			print $data;
			my $process_name = (split /:/,$data)[-1];
			chomp($process_name);
			$shell_socket->send($data);
			while(check_process($process_name)) {
				sleep 3;
			}
		}
		elsif ($data =~ m/RUN_SETUP/) {
			$rerun= 1;
		}
		
		if ($data ne "END\n" and $data !~ m/READ_RETURN_CODE/ and $data !~ m/RUN_SETUP/ and $data !~ m/PROCESSING/) {
			print $data;
		}
	}
	
	return $rerun;
}

# Forking the process
my $pid = fork();

if (defined ($pid) and $pid == 0) {
	# Setting STDOUT and STDERR to file in log folder.
	set_stdout_stderr($outputfile, $errorfile);
	
	# Opening the socket to communicate with the daemon
	my ($socket, $ctxt) = open_test_socket($testname);
	
	# Opening the socket to send the output to the shell
	my ($shell_socket, $shell_ctxt) = open_shellout_socket('tcp://', $shell_path);
	
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
		
		print "Sending command: $command.\n";
		
		if (exists $options{$command}) {
			$socket->send("$command --do-all $options{$command}");
		}
		else {
			$socket->send("$command --do-all");
		}
		my $rerun = get_daemon_output($socket, $shell_socket);
		if ($rerun) {			
			if (exists $options{$command}) {
				$socket->send("$command --setup --do-all $options{$command}");
			}
			else {
				$socket->send("$command --setup --do-all");
			}
			get_daemon_output($socket, $shell_socket);
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
