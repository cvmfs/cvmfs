package Functions::Active;

# These package will provide functions to check daemon status

use strict;
use warnings;
use threads;
use threads::shared;
use Time::HiRes qw (sleep);
use Functions::Shell qw(get_remote);
use Functions::ShellSocket qw(connect_shell_socket send_shell_msg receive_shell_msg term_shell_ctxt close_shell_socket);

# Next lines are needed to export subroutines to the main package
use base 'Exporter';
use vars qw/ @EXPORT_OK /;
@EXPORT_OK = qw(add_active show_active check_daemon);

# This is, indeed, a very important variable since here we'll store all path to active daemons.
# User will be able to retrieve all active daemons list with 'status show-active'.
my %active;

# This function will be called to add a daemon to the active list
sub add_active {
	my $host_to_add = shift;
	
	unless (exists $active{$host_to_add}) {
		$active{$host_to_add} = 1;
	}
}

# This function will be called to delete a daemon from the active list
sub delete_active {
	my $host_to_delete = shift;
	
	if (exists $active{$host_to_delete}) {
		delete $active{$host_to_delete};
	}
}

sub show_active {
	if (%active) {
		foreach (keys %active) {
			if (check_daemon($_)) {
				print "$_ is active.\n";
			}
			else {
				print "$_ was active, but it's no longer. Removed from list.\n";
				delete $active{$_};
			}
		}
	}
	else {
		print "There are no active daemons.\n";
	}
}

# This function will check whether the daemon is running, locally or remotely.
sub check_daemon {
	# Retrieving daemon path from main package
	my $daemon_path = shift;
	
	# Setting daemon_path default to $main::daemon_path
	unless ($daemon_path) {
		if (Functions::Shell::get_remote()) {
			$daemon_path = $main::connect_to;
		}
		else {
			$daemon_path = $main::daemon_path;
		}
	}
	
	# Checking with ps if the daemon is running locally
	my $running = `ps -ef | grep cvmfs-testdwrapper | grep -v grep | grep -v defunct`;
	
	# If the daemon is local, ps is enough, otherwise the shell will try to ping the daemon
	if ($daemon_path =~ m/127\.0\.0\.1/) {
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

return 1;
