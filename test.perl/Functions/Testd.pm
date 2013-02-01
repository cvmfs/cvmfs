package Functions::Testd;

#########################################
# Here will be stored all the functions needed for the daemon's environement
# mantainance
#########################################

use strict;
use warnings;
use Functions::ServerSocket qw(send_msg close_socket term_ctxt end_msg);
use Scalar::Util qw(looks_like_number);
use Socket;
use Functions::Tools qw(get_interface_address);

# Next lines are needed to export subroutine to the main package
use base 'Exporter';
use vars qw/ @EXPORT_OK /;
@EXPORT_OK = qw(stop_daemon get_interface_address supports_ipv6);

# This function will close the socket, the context and unlink the file.
sub remove_socket {	
	close_socket();
	term_ctxt();
}


# This function will kill all remaining process that were started by the daemon
sub killing_child {
	# Retrieving the list of processes
	my @process = `ps -u cvmfs-test -o pid,args | grep -v defunct | grep -v cvmfs-test | grep -v PID | grep -v grep`;
	
	# Array to store all pids
	my @pids;
	
	# Retrieving pids from the process list
	foreach (@process) {
		my @pid = (split /[[:blank:]]/, $_);

		# I found that in some system the same command has a space before the pid
		# I'm looking which one between the first two fields looks like a number.
		if (looks_like_number($pid[0])){
		    push @pids,$pid[0];
		}
		else {
		    push @pids,$pid[1];
		}
	}
	
	my ($cnt, $success);
	foreach(@pids){
		my $process = `ps -u cvmfs-test -p $_ | grep $_`;
		if ($process) {
			$cnt = kill 0, $_;
			if ($cnt > 0) {
				print "Sending TERM signal to process $_ ... ";
				$success = kill 15, $_;
			}
			if ( defined($success) and $success > 0) {
				print "Process terminated.\n";
			}
			else {
				print "Impossible to terminate the process $_\n";
			}
		}
		else {
			print "No process with PID $_\n";
		}
	}
}

# This functions is called when the daemon gets the 'stop' command. It launches
# some cleaning functions and exit.
sub stop_daemon {	
	# Killing all remaining process
	killing_child();
	
	# Printing to the FIFO the last log. 
	send_msg("Daemon stopped.\n");
	send_msg("DAEMON_STOPPED\n");
	end_msg();
	
	# Removing the socket. Do it only when you're sure you don't have any more output to send.
	remove_socket();
	
	print "Daemon stopped.\n";
	STDOUT->flush();
	exit 0;
}

# This functions checks if the system that is running the daemon supports ipv6
sub supports_ipv6 {
	my $has_ipv6;
	
	eval {
		$has_ipv6 = IO::Socket::IP->new (
			Domain => PF_INET6,
			LocalHost => '::1',
			Listen => 1 ) or die;
	};
	
	if ($@) {
		return 0;
	}
	else {
		return 1;
	}
}

1;
