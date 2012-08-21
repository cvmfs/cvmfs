package Functions::Launcher;

#################################
# Here will be stored all functions to launch the various tests
#################################

use strict;
use warnings;
use File::Find;
use Proc::Spawn;
use Functions::ServerSocket qw(send_msg);
use Scalar::Util qw(looks_like_number);

# The next line is here to help me find the directory of the script
# if you have a better method, let me know.
use FindBin qw($RealBin);

# This code is needed to export the functions in the main package
use base 'Exporter';
use vars qw/ @EXPORT_OK /;
@EXPORT_OK = qw{ launch kill_process jobs killall };

# This function will be launched everytime a command is invoked from the shell. It will
# search recursively from $RealBin for a main.pl file inside a folder named as the requested command.
# If found, it will launch the main.pl file. Else, it will return back to the shell.
sub launch {
	# Retrieving argument: command, output file handler and options
	my $test = shift;
	my @options = @_;
	
	# Declaring variables used for the fork
	my ($pid, $infh, $outfh, $errfh);
	
	# Joining all options in a unique string
	my $options = join(' ', @options);
	
	# Searching for the script to be executed
	my $mainfile;
	my $select = sub {
		if($File::Find::name =~ m/.*\/$test\/main\.pl$/){
			$mainfile = $&;
		}
	};
	finddepth( { wanted => $select }, '.');
	
	# Executing the script, if found
	if(defined ($mainfile)){
		my $error_found = 0;
		($pid, $infh, $outfh, $errfh) = spawn('perl ' . $mainfile . ' ' . $options);
		while (defined(my $err = $errfh->getline)){
			$error_found = 1;
			send_msg($err);
		}
		unless ($error_found) {
			while (defined(my $line = $outfh->getline)){
				send_msg($line);
			}
		}
	}
	else {
		send_msg("Command not found. Type 'help' for a list of available command.\n");
	}
}

# This function will be used to kill the processes.
sub kill_process {
	# Retrieving arguments: a list of PID that must be killed.
	my @pids = @_;
	
	my $success;
	my $cnt;

	# Start killing all process
	foreach(@pids){
		my $process = `ps -u cvmfs-test -p $_ | grep $_`;
		if ($process) {
			$cnt = kill 0, $_;
			if ($cnt > 0) {
				send_msg("Sending TERM signal to process $_ ... ");
				$success = kill 15, $_;
			}
			if ( defined($success) and $success > 0) {
				send_msg("Process terminated.\n");
			}
			else {
				send_msg("Impossible to terminate the process $_.\n");
			}
		}
		else {
			send_msg("No process with PID $_\n");
		}
	}
}

sub jobs {
	# Retrieving all process owned by user cvmfs-test
	my @process = `ps -u cvmfs-test -o pid,args | grep -v defunct | grep -v cvmfs-test`;
	
	foreach (@process) {
		send_msg($_);
	}
}

# This function will retrieve the list of child processes that belong to cvmfs-test user
# and will kill everyone of them. It's a rewrite from Functions::Testd::killing_child that
# sends its output to the shell or plugin.
sub killall {
	# Retrieving the list of processes
	my @process = `ps -u cvmfs-test -o pid,args | grep -v defunct | grep Services | grep -v PID | grep -v grep`;
	
	# Checking that some processes are still running.
	if (scalar(@process) < 1) {
		send_msg("No running process found.\n");
	}
	else {
		# Array to store all pids
		my @pids;
		
		# Retrieving pids from the process list
		foreach (@process) {
			my @pid = (split /[[:blank:]]/, $_);
			
			# I found that on some system, the same commands has a space before the pid.
			# I'm going to look which one looks like a number between the first two field.
			if (looks_like_number($pid[0])) {
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
					send_msg("Sending TERM signal to process $_ ... ");
					$success = kill 15, $_;
				}
				if ( defined($success) and $success > 0) {
					send_msg("Process terminated.\n");
				}
				else {
					send_msg("Impossible to terminate the process $_\n");
				}
			}
			else {
				send_msg("No process with PID $_\n");
			}
		}
	}
}

1;
