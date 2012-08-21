#!/usr/bin/perl

# The next line is here to help me find the directory of the script
# if you have a better method, let me know.
use FindBin qw($RealBin);

# Next line adds the script directory to the lib path
use lib "$RealBin";

use strict;
use warnings;
use Proc::Spawn;
use Functions::Shell qw(check_daemon check_command start_daemon get_daemon_output exit_shell);
use Functions::ShellSocket qw(connect_shell_socket receive_shell_msg send_shell_msg close_shell_socket term_shell_ctxt bind_shell_socket);
use Getopt::Long;

my $command = undef;
my $wait_daemon = undef;
my $setup = undef;
my $help_message = undef;
my $interactive = 1;

# Variables to store daemon ip and port on distributed test
my $daemon_ip = "127.0.0.1";
my $daemon_port = "6650";
my $daemon_path = "$daemon_ip:$daemon_port";


# Next variables is used to control when to skip a loop cicle
my $continue = undef;

# Variables for socket managing
my $socket = undef;
my $ctxt = undef;
my $shell_socket = undef;
my $shell_ctxt = undef;

my $ret = GetOptions ( "c|command=s" => \$command,
					   "wait-daemon" => \$wait_daemon,
					   "setup" => \$setup,
					   "h|help" => \$help_message );

if (defined($help_message)) {
	my $help = <<'END';
Usage: cvmfs-test [--i] [--setup] [--wait-daemon] [--c command]
	
-h|--help	Print this help and exit.
--i		Start the interactive shell. Default.
--setup		Setup the environment.
--wait-daemon	Wait for the daemon to send its ip.
--c command	Executes command and exit.

END
	print $help;
	exit_shell($socket, $ctxt);
}

if (defined($setup)) {
	check_command(undef, undef, undef, 'setup');
	check_command(undef, undef, undef, 'fixperm');
	exit_shell($socket, $ctxt);
}

if (defined($wait_daemon)) {
	# Opening the socket to wait for the daemon to send its ip
	($shell_socket, $shell_ctxt) = bind_shell_socket();
	
	# Wait fo the daemon message
	my $answer = receive_shell_msg($shell_socket);
	
	# Splitting the message and assigning it to variables
	my @ip_port = split /:/, $answer;
	($daemon_ip, $daemon_port) = ($ip_port[0], $ip_port[1]);
	$daemon_path = "$daemon_ip:$daemon_port";
	
	# Closing the socket. The same socket will be used to receive tests output.
	$socket = close_shell_socket($socket);
	$ctxt = term_shell_ctxt($ctxt);
}

if (defined($command)) {
	if (!check_daemon()) {
		($socket, $ctxt) = start_daemon($daemon_path);
	}
	else {
		($socket, $ctxt) = connect_shell_socket($daemon_path);
	}
	my ($continue, $socket, $ctxt) = check_command($socket, $ctxt, $daemon_path, $command);
	unless ($continue) {
		send_shell_msg($socket, $command);
		get_daemon_output($socket, $ctxt);
	}
	exit_shell($socket, $ctxt, 1);
}

if ($interactive) {
	# A simple welcome.
	print '#'x80 . "\n";
	print "Welcome in the interactive shell of the CernVM-FS testing system.\n";
	print "Type 'help' for a list of available commands.\n";
	print '#'x80 . "\n";

	# If the daemon is not running, the shell will ask the use if run it
	if (!check_daemon()) {
		print 'The daemon is not running. Would you like to run it now? [Y/n]';
		my $answer = <STDIN>;
		if($answer eq "\n" or $answer eq "Y\n" or $answer eq "y\n"){
			($socket, $ctxt) = start_daemon($daemon_path);
		}
	}
	else {
		# Starting the socket to communicate with the server
		($socket, $ctxt) = connect_shell_socket($daemon_path);
	}

	# Infinite loop for the shell. It will switch between two shells: the first one
	# is the one used when the shell is connected to the daemon. The second one is used
	# when the daemon is not running and will have less options available.
	# Both shell terminates on exit.
	while(1){
		# This is the first shell, the one used to communicate with the daemon
		while(check_daemon()){
			print '-> ';
			# Reading an input line.
			my $line = STDIN->getline;
			chomp($line);
			
			# Checking again if the daemon is running, maybe something killed it.
			unless (check_daemon()) {
				print "Daemon isn't running anymore. Check logs.\n";
				$socket = close_shell_socket($socket);
				$ctxt = term_shell_ctxt($ctxt);
				next;
			}
			
			# Checking if the command refer to the shell and not to the daemon
			($continue, $socket, $ctxt) = check_command($socket, $ctxt, $daemon_path, $line);
			# If the command was already executed, passing to the next while cicle
			next if $continue;
			
			# Send the command through the socket
			send_shell_msg($socket, $line);
			
			# Get answer from the daemon
			get_daemon_output($socket, $ctxt);
		}
		
		# This is the second shell, use when the daemon is closed
		while(!check_daemon()){
			print '(Daemon not running) -> ';
			# Reading an input line.
			my $line = STDIN->getline;
			chomp($line);
			
			# Launching the command
			($continue, $socket, $ctxt) = check_command($socket, $ctxt, $daemon_path, $line);
		}
	}
}
