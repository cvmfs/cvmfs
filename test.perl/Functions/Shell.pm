package Functions::Shell;

#######################################
# Here will be store all the functions that will be used to change shell behaviour
# and environment
#######################################

use strict;
use warnings;
use threads;
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
use Functions::Active qw (show_active check_daemon);

# Next lines are needed to export subroutines to the main package
use base 'Exporter';
use vars qw/ @EXPORT_OK /;
@EXPORT_OK = qw(check_command start_daemon get_daemon_output exit_shell set_remote);

# The next line is here to help me find the directory of the script
# if you have a better method, let me know.
use FindBin qw($RealBin);

# This variable will be set to 1 if the shell is speaking to a daemon on a remote machine
my $remote = 0;

# Getter and setter for $remote
sub get_remote {
	return $remote;
}

sub set_remote {
	my $val_to_set = shift;
	
	if ($val_to_set == 1 or $val_to_set == 0) {
		$remote = $val_to_set;
	}
	else {
		print "ERROR: Uknown \$remote value: $remote.\n";
	}
}

# This function is called to check if a process is running
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
		elsif ($_ =~ m/^status/ or $_ =~ m/^ping/) { print_status($command); $executed = 1 }
		elsif ($_ =~ m/^start\s*.*/ ) { ($socket, $ctxt) = start_daemon($daemon_path, undef, undef, $command); $executed = 1 }
		elsif ($_ =~ m/^help\s*.*/ or $_ =~ m/^h\s.*/) { help($command), $executed = 1 }
		elsif ($_ eq 'setup' ) { setup(); $executed = 1 }
		elsif ($_ eq 'fixperm') { fixperm(); $executed = 1 }
		elsif ($_ =~ m/^restart\s*.*/ ) { ($socket, $ctxt) = restart_daemon($socket, $ctxt, $daemon_path, $command); $executed = 1 }
		elsif ($_ =~ m/^wait-daemon\s*.*/ ) { ($socket, $ctxt) = wait_daemon($socket, $ctxt); $executed = 1 }
		elsif ($_ =~ m/^connect-to\s*.*/ ) { ($socket, $ctxt) = connect_to($daemon_path, $command, $socket, $ctxt); $executed = 1 }
	}
	
	# If the daemon is not running and no command was executed, print on screen a message
	# saying that the command was not found
	if(!check_daemon() and !$executed){
		print "Command not found. Type 'help' for a list of available commands.\n";
	}
	
	# Returning the value of $executed to check if the command was found and executed
	return ($executed, $socket, $ctxt);
}

# This function is used to passive wait a daemon to connect to the shell
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
		set_remote(1);
	}
	
	# Returning new socket and context
	return ($socket, $ctxt);
}

# This function is used to actively connect to a remote daemon
sub connect_to {
	my $daemon_path = shift;
	my $command = shift;
	my $socket = shift;
	my $ctxt = shift;
	
	# Checking in $command if the user supplied an alternative IP to daemon_path
	my ($ip) = $command =~ m/(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})/;
	my ($port) = $command =~ m/:(\d{3,4})/;
	
	# Setting port to default
	unless ($port) {
		$port = 6650;
	}
	
	# Resetting $daemon_path if user has supplied alternative ip and port.
	if ($ip) {
		$daemon_path = "$ip:$port";
		
		# Setting $main::connect_to for other function to access this value
		$main::connect_to = $daemon_path;
	}
	
	if ($daemon_path !~ m/127\.0\.0\.1/ and $daemon_path !~ m/localhost/) {
		set_remote(1);
	}
	else {
		set_remote(0);
	}
	
	if (check_daemon($daemon_path)) {
		print "Daemon found on $daemon_path.\n";
		
		# Closing old socket
		print 'Closing old socket... ';
		$socket = close_shell_socket();
		$ctxt = term_shell_ctxt();
		if ($socket or $ctxt) {
			print "ERROR.\n";
		}
		else {
			print "Done.\n";
		}
		
		# Opening new socket
		print 'Opening new socket... ';
		my ($newsocket, $newctxt) = connect_shell_socket($daemon_path);
		print "Done.\n";
		
		return ($newsocket, $newctxt);
	}
	else {
		set_remote(0);
		print "Daemon could not be reached on $daemon_path.\n";
		print "To start a server locally use 'start'.\n" if $daemon_path =~ m/127\.0\.0\.1/ or $daemon_path =~ m/localhost/;
		return ($socket, $ctxt);
	}
}

# This function will print the current status of the daemon
sub print_status {
	my $command = shift;
	
	# Checking weather the user requeste the list of active daemons
	if ($command =~ m/--show-active/) {
		show_active();
		return;
	}
	
	if(check_daemon()){
		unless (get_remote()) {
			print "Daemon is running on $main::daemon_path.\n";
		}
		else {
			print "Daemon is running on $main::connect_to.\n";
		}
	}
	else {
		unless (get_remote()) {
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
	
	# Printing if user cvmfs-test doesn't exist
	unless ($user) {
		print "ERROR: cvmfs-test user doesn't exist.\n";
	}
	
	# Checking if the user own the daemon file
	if (-e "$RealBin/cvmfs-testdwrapper"){
		my $uid = (stat("$RealBin/cvmfs-testdwrapper"))[4];
		$owner = (getpwuid($uid))[0];
	}
	else {
		$owner = 0;
	}
	
	# Printing if owner is not root
	unless ($owner eq 'root') {
		print "ERROR: Wrapper owner isn't root. It's $owner.\n";
	}
	
	# Checking if the file has the setuid bit
	if (-e "$RealBin/cvmfs-testdwrapper") {
		my $mode = (stat("$RealBin/cvmfs-testdwrapper"))[2];
		$suid = $mode & S_ISUID;
	}
	else {
		$suid = 0;
	}
	
	# Printing if suid byte isn't set
	unless ($suid) {
		print "ERROR: Wrapper hasn't suid byte set.\n";
	}
	
	# Checking if the log directory exists and the owner is cvmfs-test
	if ( -e '/var/log/cvmfs-test' ) {
		my $log_uid = (stat('/var/log/cvmfs-test'))[4];
		$log_owner = (getpwuid($log_uid))[0];
	}
	else {
		$log_owner = 0;
	}
	
	# Printing if log folder doesn't belong to cvmfs-test user
	unless ($log_owner eq 'cvmfs-test') {
		print "ERROR: Log folder doesn't belong to cvmfs-test user. It belongs to $log_owner.\n";
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
	if (get_remote()) {
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
			if ($_ =~ m/DAEMON_STOPPED/) { ($socket, $ctxt) = daemon_stopped($socket, $ctxt); $processed = 1 }
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
	return ($socket, $ctxt);
}

# This function is called every time the shell receives a DAEMON_STOPPED message.
sub daemon_stopped {
	my $socket = shift;
	my $ctxt = shift;
	
	# Closing socket and ctxt
	$socket = close_shell_socket($socket);
	$ctxt = term_shell_ctxt($ctxt);
	
	# Checking if a local server is running and trying to reconnect to it.
	if (get_remote() and check_daemon('127.0.0.1:6650')) {
		print 'A local running daemon was found. Reconnecting... ';
		($socket, $ctxt) = connect_shell_socket();
		print "Done.\n";
	}
	
	# Resetting $remote
	set_remote(0) if get_remote();
	
	return ($socket, $ctxt);
}

# This function will start the daemon if it's not already running
sub start_daemon {
	my $daemon_path = shift;
	my $shell_path = shift;
	my $iface = shift;
	if (@_ and scalar(@_) > 0) {
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
	unless (get_remote()) {
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
	else {
		print "You cannot stop and restart a remote server.\n";
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
