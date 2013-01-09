package Functions::Shell;

#######################################
# Here will be store all the functions that will be used to change shell behaviour
# and environment
#######################################

use strict;
use warnings;
use threads;
use threads::shared;
use Functions::Help qw(help);
use Proc::Daemon;
use Fcntl ':mode';
use Getopt::Long;
use IO::Interface::Simple;
use Functions::Setup qw(setup fixperm);
use Functions::ShellSocket qw(connect_shell_socket send_shell_msg receive_shell_msg close_shell_socket term_shell_ctxt bind_shell_socket);
use Term::ANSIColor;
use Time::HiRes qw(sleep);
use Functions::Virtualization qw(start_distributed);

# Next lines are needed to export subroutines to the main package
use base 'Exporter';
use vars qw/ @EXPORT_OK /;
@EXPORT_OK = qw(check_daemon check_command start_daemon get_daemon_output exit_shell);

# The next line is here to help me find the directory of the script
# if you have a better method, let me know.
use FindBin qw($RealBin);

# This variable will be set to 1 if the shell is speaking to a daemon on a remote machine
my $remote = 0;

# This function will check whether the daemon is running.
sub check_daemon {
	# Retrieving daemon path from main package
	my $daemon_path = $main::daemon_path;
	
	# Checking with ps if the daemon is running locally
	my $running = `ps -ef | grep cvmfs-testdwrapper | grep -v grep | grep -v defunct`;
	
	# If the daemon is local, ps is enough, otherwise the shell will try to ping the daemon
	unless ($remote) {
		return ($running);
	}
	else {
		return ping_daemon($daemon_path);
	}
}

# This function will be used to ping a remote daemon and check if it's awake
sub ping_daemon {
	my $daemon_path = shift;
	my $seconds = shift;
	
	# Assigning $seconds a default value of 5
	if (!defined($seconds)) {
		$seconds = 5;
	}
	
	# This variable will be used to register if a pong is received and will be shared with
	# the inner thread
	my $pong = 0;
	share($pong);
	
	# This variable will be incremented by the while in the inner sub and will be shared
	# with the inner thread
	my $count = 0;
	share($count);
	
	# This sub will be ran in a thread and will ping the daemon hoping for a response
	my $actual_pinging = sub {
		my ($ping_socket, $ping_ctxt) = connect_shell_socket($daemon_path, undef, 'SHELL_PING');
		while (($count < $seconds) and !$pong) {
			send_shell_msg($ping_socket, "PING");
			my $msg = receive_shell_msg($ping_socket, "ZMQ_NOBLOCK");
			if (defined($msg)) {
				$pong = 1;
			}
			$count++;
			sleep 0.5;
		}
		close_shell_socket($ping_socket);
		term_shell_ctxt($ping_ctxt);
	};
	
	# Starting the pinging thread
	my $pinging_threads = threads->create($actual_pinging);
	$pinging_threads->detach();
	
	# Waiting for pong to be received
	until ($pong or $count >= 5) {
		sleep 0.5;
	}
	
	return $pong;
}

sub check_process {
	my $process_name = shift;
	my $running = `ps -fu cvmfs-test | grep -i $process_name | grep -v grep`;
	return $running;
}

# This function is used to check if the command typed has to be ran by the shell
# or by the daemon. If the command is bundled in the shell, it launches the
# corresponding function.
sub check_command {
	my $socket = shift;
	my $ctxt = shift;
	my $daemon_path = shift;
	my $command = shift;
	
	# Variables to memorize if the command was found and executed
	my $executed = 0;
	
	# Switching the value of $command
	for ($command){
		if ($_ eq 'exit' or $_ eq 'quit' or $_ eq 'q') { exit_shell($socket, $ctxt) }
		elsif ($_ eq 'status' or $_ eq 'ping') { print_status(); $executed = 1 }
		elsif ($_ =~ m/^start\s*.*/ ) { ($socket, $ctxt) = start_daemon($daemon_path, undef, undef, $command); $executed = 1 }
		elsif ($_ =~ m/^help\s*.*/ or $_ =~ m/^h\s.*/) { help($command), $executed = 1 }
		elsif ($_ eq 'setup' ) { setup(); $executed = 1 }
		elsif ($_ eq 'fixperm') { fixperm(); $executed = 1 }
		elsif ($_ =~ m/^restart\s*.*/ ) { ($socket, $ctxt) = restart_daemon($socket, $ctxt, $daemon_path, $command); $executed = 1 }
		elsif ($_ =~ m/^wait-daemon\s*.*/ ) { ($socket, $ctxt) = wait_daemon($socket, $ctxt); $executed = 1 }
	}
	
	# If the daemon is not running and no command was executed, print on screen a message
	# saying that the command was not found
	if(!check_daemon() and !$executed){
		print "Command not found. Type 'help' for a list of available commands.\n";
	}
	
	# Returning the value of $executed to check if the command was found and executed
	return ($executed, $socket, $ctxt);
}

sub wait_daemon {
	my $socket = shift;
	my $ctxt = shift;
	
	# Closing old socket and context
	if (defined($socket) and defined($ctxt)) {
		close_shell_socket($socket);
		term_shell_ctxt($ctxt);
	}
	
	# Opening the socket to wait for the daemon to send its ip
	my ($shell_socket, $shell_ctxt) = bind_shell_socket();
	
	# Wait for the daemon message
	print "Waiting for the daemon...\n";
	my $answer = receive_shell_msg($shell_socket);
	print "Received a connection from $answer.\n";
	
	# Splitting the message and assigning it to variables
	my @ip_port = split /:/, $answer;
	my ($daemon_ip, $daemon_port) = ($ip_port[0], $ip_port[1]);
	my $daemon_path = "$daemon_ip:$daemon_port";
	
	# Closing the socket. The same socket will be used to receive tests output.
	$shell_socket = close_shell_socket($shell_socket);
	$shell_ctxt = term_shell_ctxt($shell_ctxt);
	
	# Opening the socket to communicate with the server
	($socket, $ctxt) = connect_shell_socket($daemon_path);
	
	# Setting $remote = 1 if the connection was successfull
	if ($socket) {
		$remote = 1;
	}
	
	# Returning new socket and context
	return ($socket, $ctxt);
}

# This function will print the current status of the daemon
sub print_status {
	if(check_daemon()){
		unless ($remote) {
			print "Daemon is running.\n";
		}
		else {
			print "Daemon is running on $main::daemon_path.\n";
		}
	}
	else {
		unless ($remote) {
			print "The daemon is not running. Type 'start' to run it.\n";
		}
		else {
			print "No daemon could be reached at $main::daemon_path. Type 'start' to run it locally.\n";
		}
	}
}

# This functions will check that file permission are set
sub check_permission {
	my ($user, $suid, $owner, $log_owner, $sudoers);
	# Checking if the user exists in the system
	$user = `cat /etc/passwd | grep cvmfs-test`;
	
	# Checking if the user own the daemon file
	if (-e "$RealBin/cvmfs-testdwrapper"){
		my $uid = (stat("$RealBin/cvmfs-testdwrapper"))[4];
		$owner = (getpwuid($uid))[0];
	}
	else {
		$owner = 0;
	}
	
	# Checking if the file has the setuid bit
	if (-e "$RealBin/cvmfs-testdwrapper") {
		my $mode = (stat("$RealBin/cvmfs-testdwrapper"))[2];
		$suid = $mode & S_ISUID;
	}
	else {
		$suid = 0;
	}
	
	# Checking if the log directory exists and the owner is cvmfs-test
	if ( -e '/var/log/cvmfs-test' ) {
		my $log_uid = (stat('/var/log/cvmfs-test'))[4];
		$log_owner = (getpwuid($log_uid))[0];
	}
	else {
		$log_owner = 0;
	}
	
	# Return true only if all conditions are true
	if ($user and $owner eq "root" and $suid and $log_owner eq 'cvmfs-test'){
		return 1;
	}
	else {
		return 0;
	}
}

# This function will print a loading animation while waiting for test output
sub loading_animation {
	# This function will immediatily return if the daemon is running remotely since I didn't
	# find a way to check if the process is running remotely
	if ($remote) {
		return;
	}
	
	my $process_name = shift;
	$process_name = lc($process_name);
	
	my @char = qw( | / - \ );
	my $i = 0;
	
	# Disabling STDOUT buffer
	STDOUT->autoflush;
	
	# Making cursos invisible
	system('tput civis');
	
	while (check_process($process_name)) {
		print $char[$i % 4] . "\b";
		sleep 0.2;
		$i++;
	}
	
	# Making cursor visible again
	system('tput cnorm');
}

# This function will be use to get test output from the socket.
sub get_test_output {
	# Opening the socket to retrieve output
	my ($shell_socket, $shell_ctxt) = bind_shell_socket();
	
	#Be careful: this is blocking. Be sure to not send READ_RETURN_CODE signal to the shell
	# if you are not going to send something through the socket. The shell will hang.
	my $return_line = '';
	while ($return_line !~ m/END/ or check_process('do_all')) {
		$return_line = receive_shell_msg($shell_socket);
		
		# Coloring the output in green or red
		if ($return_line =~ m/OK.$/) {

			print color 'green';
			print $return_line;
			print color 'reset';
		}
		elsif($return_line =~m/WRONG.$/) {
			print color 'red';
			print $return_line;
			print color 'reset';
		}
		else {
			print $return_line unless $return_line =~ m/END/;
		}
		

		# Waiting two seconds if END_ALL received
		if ($return_line eq "END_ALL\n") {
			sleep 5;
		}
	}
	
	# Closing the socket and the context
	$shell_socket = close_shell_socket($shell_socket);
	$shell_ctxt = term_shell_ctxt($shell_ctxt);
}

# This function will call a loop to wait for a complex output from the daemon
sub get_daemon_output {
	my $socket = shift;
	my $ctxt = shift;
	my $reply = '';
	while ($reply ne "END\n") {
		$reply = receive_shell_msg($socket);
		# Switch on the value of $reply to catch any special sinagl from the daemon.
		for ($reply) {
			# This variable will be used to record if the shell has got any special signal.
			# Most of special signal will not be printed as output part.
			my $processed = 0;
			# This case if the daemon has stopped itself
			if ($_ =~ m/DAEMON_STOPPED/) { $socket = close_shell_socket($socket); $ctxt = term_shell_ctxt($ctxt); $remote = 0 if $remote; $processed = 1 }
			elsif ($_ =~ m/PROCESSING/) {
				my $process_name = (split /:/, $_)[-1];
				chomp($process_name);
				print "Processing $process_name...\n";
				my $loading_threads = threads->create(\&loading_animation, $process_name);
				$loading_threads->detach();
				$processed = 2;
			}
			# This case if the daemon tell the shell to wait for PID to term.
			elsif ($_ =~ m/READ_RETURN_CODE/) {
				get_test_output();
				$processed = 2;
			}
			# Other cases with special signal that are useless for the shell.
			# SAVE_PID, by now, is useless for the shell, only tests have the need to
			# remember wich services that have started.
			elsif ($_ =~ m/SAVE_PID/) {
				$processed = 2;
			}
			# Setting $reply to END to terminate to wait output.
			if ($processed == 1) {
				$reply = "END\n";
				sleep 3;
			}
			# Setting $reply to NO_PRINT if we don't need to print the signal as output part.
			elsif ($processed == 2) {
				$reply = "NO_PRINT";
			}
		}
		print $reply if $reply ne "END\n" and $reply ne "NO_PRINT";
	}
}

# This function will start the daemon if it's not already running
sub start_daemon {
	my $daemon_path = shift;
	my $shell_path = shift;
	my $iface = shift;
	if (defined (@_) and scalar(@_) > 0) {
		# Retrieving arguments
		my $line = shift;
		
		# Splitting $line in an array depending on blank...
		my @words = split /[[:blank:]]/, $line;
		# Everything but the first word, if exist, are options.
		my @options = splice(@words, 1);
	
		# Setting ARGV to @options for GetOptions works properly
		@ARGV = @options;
	}
	
	# Setting default values for options
	my $daemon_output = '/var/log/cvmfs-test/daemon.output';
	my $daemon_error = '/var/log/cvmfs-test/daemon.error';
	my $distributed = undef;
	
	# Options that works only with --distributed but are here to avoid warnings
	my $force = undef;
	my $shell_iface = undef;
	
	# I need this line as get options will destroy @ARGV
	my @backup_argv = @ARGV;
	
	# Parsing options
	my $ret = GetOptions (  "stdout=s" => \$daemon_output,
						    "stderr=s" => \$daemon_error,
						    "distributed" => \$distributed,
						    "force" => \$force,
						    "shell-iface=s" => \$shell_iface );
						   
	# If a distributed environment is requested, pass everything to start_distributed()
	if (defined($distributed)){
		my $error = Functions::Virtualization::start_distributed(@backup_argv);
		if ($error) {
			my ($socket, $ctxt) = wait_daemon();
			return ($socket, $ctxt);
		}
		return;
	}
	
	# If we are here, we don't have any other command line processing to do, so here we
	# join @ARGV content to pass it as a string to the daemon.
	my $daemon_options = "";
	if (defined($shell_path) and defined($iface)) {
		$daemon_options = '--shell-path ' . $shell_path . ' --iface ' . $iface;
	}
		
	if (!check_daemon()){
		if(check_permission()){									  
			my ($daempid, $daemin, $daemout, $daemerr);
			print 'Starting daemon... ';
			my $daemonpid = Proc::Daemon::Init( { 
													work_dir => $RealBin,
													pid_file => '/tmp/daemon.pid',
													child_STDOUT => $daemon_output,
													child_STDERR => $daemon_error,
													exec_command => "./cvmfs-testdwrapper ./cvmfs-testd.pl $daemon_options",
												} );
			# Sleep and wait for the daemon to start or fail
			sleep 1;

			# Checking if the daemon were started
			if (check_daemon()) {
				print "Done.\n";
			}
			else {
				print "Failed.\nHave a look to $daemon_error.\n";
			}
			
			# Opening the socket to communicate with the server
			print "Opening the socket... ";
			my ($socket, $ctxt) = connect_shell_socket($daemon_path);
			if ($socket) {
				print "Done.\n";
				return ($socket, $ctxt);
			}
			else {
				print "Failed.\n";
			}
		}
		else {
			print "Wrong permission on important files. Did you run 'setup'?\n";
		}
	}
	else {
		print "Daemon is already running. Cannot run another instance.\n";
	}
}

# This function will stop and restart the daemon
sub restart_daemon {
	my $socket = shift;
	my $ctxt = shift;
	my $daemon_path = shift;
	# Retrieving options to pass to start_daemon
	my $line = shift;
	
	if (check_daemon()) {
		send_shell_msg($socket, 'stop');
		get_daemon_output($socket, $ctxt);
		sleep 1;
		($socket, $ctxt) = start_daemon($daemon_path, $line);
		return ($socket, $ctxt);
	}
	else {
		print "Daemon is not running. Type 'start' to run it.\n"
	}
}

# This functions will close the shell after closing socket and terminate ZeroMQ context
sub exit_shell {
	my $socket = shift;
	my $ctxt = shift;
	# The shell will pass a true value when it's called in non interactive mode
	# so it will force the closure of the daemon.
	my $force = shift;
	if (check_daemon() and !$force) {
		print "The daemon's still running, would you like to stop it before exiting? [Y/n] ";
		my $stop_it = STDIN->getline;
		unless ($stop_it eq "n\n" or $stop_it eq "N\n") {
			send_shell_msg($socket, 'stop');
			get_daemon_output($socket, $ctxt);
		}
		else {
			print 'Closing the socket... ';
			$socket = close_shell_socket($socket);
			$ctxt = term_shell_ctxt($ctxt);
			print "Done.\n";
		}
	}
	else {
		if (defined($force) and $force == 1) {
			send_shell_msg($socket, 'stop');
			get_daemon_output($socket, $ctxt);
		}
		if (defined($socket)) {
			print 'Closing the socket... ';
			$socket = close_shell_socket($socket);
			$ctxt = term_shell_ctxt($ctxt);
			print "Done.\n";
		}
	}
	
	exit 0;
}

1;
