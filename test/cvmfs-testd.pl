#!/usr/bin/perl

use strict;
use warnings;
use FindBin qw($RealBin);
use lib "$RealBin";
use IO::Handle;
use Functions::Help qw(help);
use Functions::Launcher qw(launch kill_process jobs killall);
use Functions::Testd qw(stop_daemon);
use Functions::ServerSocket qw(start_socket receive_msg end_msg send_ip);
use Getopt::Long;

my $shell_path = undef;

my $ret = GetOptions ("shell-path=s" => \$shell_path );

if (defined($shell_path)) {
	send_ip($shell_path, 'tcp://', 'eth0');
}

# Opening the socket and dying if fail
my $socket_started = start_socket();
unless ($socket_started) {
	die "Failed to open the socket. Aborting.\n";
}

while(1) {
	my $line = receive_msg();
	
	# If $line is a log about the sender, print it and go again to listen
	if ($line =~ m/Receiving connection from/) {
	    print $line;
	    next;
	}
	
	# Deleting return at the end of the line
	chomp($line);

	print "Processing command: $line...\n";
	
	# Splitting $line in an array depending on blank...
	my @words = split /[[:blank:]]/, $line;
	# ... first word will be the command...
	my $command = shift(@words);
	# ... everything else, if exist, are options.
	my @options = splice(@words, 0);
	
	# Switch on the value of command
	if (defined($command) and $command ne '') {
		for($command){
			
			# Here is the HELP case
			if($_ eq 'help' or $_ eq 'h') { help($line) }
			
			# Here is the STOP case
			elsif($_ eq 'stop') { stop_daemon() }
			
			# Here is the KILL case
			elsif($_ eq 'kill') { kill_process(@options) }
			
			# Here is the KILLALL case
			elsif($_ eq 'killall') { killall() }
			
			# Here is the JOBS case
			elsif ($_ eq 'jobs') { jobs() }
			
			# The default case will try to launch the appropriate plugin
			else { launch($command, @options) }
		}
	}
	
	
	# Sending to the shell a signal to terminate output
	end_msg();
	
	# Logging the end of processing
	print "Done.\n";
	
	STDOUT->flush;
}
	

