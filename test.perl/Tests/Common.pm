package Tests::Common;

##############################
# Common function to set tests environment
##############################

use strict;
use warnings;
use File::Find;
use File::Copy;
use ZeroMQ qw/:all/;
use FindBin qw($RealBin);

use base 'Exporter';
use vars qw/ @EXPORT_OK /;
@EXPORT_OK = qw(get_daemon_output killing_services check_repo setup_environment restart_cvmfs_services
				 check_mount_timeout find_files recursive_mkdir recursive_rm open_test_socket close_test_socket
				 set_stdout_stderr multiple_rm open_shellout_socket restore_dns);

# This function will set STDOUT and STDERR for forked process
sub set_stdout_stderr {
	my $outputfile = shift;
	my $errorfile = shift;
	open (my $errfh, '>', $errorfile) || die "Couldn't open $errorfile: $!\n";
	STDERR->fdopen ( \*$errfh, 'w' ) || die "Couldn't set STDERR to $errorfile: $!\n";
	open (my $outfh, '>', $outputfile) || die "Couldn't open $outputfile: $!\n";
	STDOUT->fdopen( \*$outfh, 'w' ) || die "Couldn't set STDOUT to $outputfile: $!\n";
	# Setting autoflush for STDOUT to read its output in real time
	STDOUT->autoflush;
	STDERR->autoflush;
}

# This functions will wait for output from the daemon
sub get_daemon_output {
	# It needs to know the socket object to use. It must be a ZeroMQ instance.
	my $socket = shift;
	# The array to store services pids will be passed as second argument
	my @pids = @_;
	
	my ($data, $reply) = ('', '');
	# It will stop waiting for output when it receives the string "END\n"
	while ($data ne "END\n") {
		$reply = $socket->recv();
		$data = $reply->data;
		# Daemon will send data about PIDs od service started for this test.
		# This message will be formatted like 'SAVE_PID:PID', where PID is the part
		# that we have to save.
		if ($data =~ m/SAVE_PID/) {
		    my $pid = (split /:/, $data)[-1];
		    push @pids,$pid;
		}
		print $data if $data ne "END\n" and $data !~ m/SAVE_PID/;
	}
	
	# Returning the new pids array.
	return @pids;
}

# This function will kill all services started, so it can start new processes on the same ports
sub killing_services {
	# Retrieving socket handler
	my $socket = shift;
	# Pass the pids array as second argument
	my @pids = @_;
	
	print "Killing services...\n";
	
	# This chomp is necessary since the server would send the message with a carriage
	# return at the end. But we have to erase it if we want the daemon to correctly
	# recognize the command.
	foreach (@pids) {
		chomp($_);
	}
	
	# Joining PIDs in an unique string
	my $pid_list = join (' ', @pids);
	
	# Removing all elements fro @pids. This command will be called more than once during
	# the test. So we have to empty the arrays if don't want that sequent calling will try
	# to kill already killed services.
	undef @pids;
	
	# Sending the command.
	$socket->send("kill $pid_list");
	get_daemon_output($socket);
	print "Done.\n";
	
	# Returning empty pids array
	return @pids;
}

# This function will check if the repository is accessible, it will return 1 on success and 0 on failure.
# Remember that for two of our tests, success is failure and failure is success.
sub check_repo {
	# Retrieving the folder to check
	my $repo = shift;
	
	my ($opened, $readdir, $readfile) = (undef, undef, undef);
	print "Trying to open and listing the directory...\n";
	
	# Opening the directory.
	$opened = opendir (my $dirfh, $repo);
	
	# Returning false if the directory was not open correctly
	unless ($opened){
	    print "Failed to open directory $repo: $!.\n";
	    return 0;
	}
	
	# Reading the list of files.
	my @files = readdir $dirfh;
	
	# Returning false if the directory can't be read correctly.
	unless (@files) {
	    print "Failed to list directories $repo: $!.\n";
	    return 0;
	}
	else {
		$readdir = 1;
	}
	
	# Printing all files in the directory.
	#print "Directory Listing:\n");
	foreach (@files) {
		print $_ . "\n";
	}
	
	# Opening a file.
	$readfile = open(my $filefh, "$repo/$files[2]");
	
	# Returning false if the file can't be correctly read.
	unless ($readfile) {
		print "Failed to open file $files[2]: $!.\n";
		return 0;
	}
	
	print "File $files[2] content:\n";
	while (defined(my $line = $filefh->getline)) {
		print $line;
	}
	closedir($dirfh);
	
	# Returning true if all operation were done correctly.
	if ($readfile and $readdir and $opened) {
		print "Done.\n";
		return 1;
	}
	else {
		print "Done.\n";
		return 0;
	}
}

sub setup_environment {
	# Retrieving directory for repository
	my $tmp_repo = shift;
	# Retrieving repository host
	my $host = shift;

	my $repo_pub = $tmp_repo . 'pub';
	
	print "Creating directory $tmp_repo... ";
	recursive_mkdir($tmp_repo);
	print "Done.\n";

	print "Extracting the repository... ";
	system("tar -xzf Tests/Common/repo/pub.tar.gz -C $tmp_repo");
	print "Done.\n";
	
	print 'Creating RSA key... ';
	system("Tests/Common/creating_rsa.sh");
	print "Done.\n";
	
	print 'Signing files... ';
	my @files_to_sign;
	my $select = sub {
	if ($File::Find::name =~ m/\.cvmfspublished/){
			push @files_to_sign,$File::Find::name;
		}
	};
	find( { wanted => $select }, $repo_pub );
	foreach (@files_to_sign) {
		copy($_,"$_.unsigned");
		system("Tests/Common/cvmfs_sign-linux32.crun -c /tmp/cvmfs_test.crt -k /tmp/cvmfs_test.key -n $host $_");		
	}
	copy('/tmp/whitelist.test.signed', "$repo_pub/catalogs/.cvmfswhitelist");
	print "Done.\n";
	
	print 'Configurin RSA key for cvmfs... ';
	system("Tests/Common/configuring_rsa.sh $host");
	copy('/tmp/whitelist.test.signed', "$repo_pub/catalogs/.cvmfswhitelist");
	print "Done.\n";
}

sub check_mount_timeout {
	# Retrieving folder to mount
	my $repo = shift;
	# Retrieving seconds for timeout
	my $seconds = shift;
	
	my $before = time();
	my $opened = opendir(my $dirfh, $repo);
	my $after = time();
	closedir($dirfh);
	
	my $interval = $after - $before;
	
	print "DNS took $interval seconds to time out.\n";
	
	return $interval;
}

sub restart_cvmfs_services {
	my $options = shift;
	
	if (!defined($options)) {
		$options = '';
	}
	
	print 'Restarting services... ';
	system("sudo Tests/Common/restarting_services.sh $options >> /dev/null 2>&1");
	print "Done.\n";
}

sub find_files {
	# Retrieving root folder
	my $folder = shift;
	
	print 'Retrieving files... ';
	my @file_list;
	my $select_files = sub {
		push @file_list,$File::Find::name if -f $File::Find::name;
	};
	find( { wanted => $select_files }, $folder );
	print "Done.\n";
	
	return @file_list;
}

# This functions accept an absolute path and will recursive
# create the whole path. Is the equivalent of "make_path" in
# newer perl version or 'mkdir -p' in any Linux system.
sub recursive_mkdir {
	my $path = shift;
	unless ($path =~ m/^\/[_0-9a-zA-Z]+$/) {
		my $prevpath = $path;
		$prevpath =~ s/\/(.*)(\/.*)$/\/$1/;
		recursive_mkdir($prevpath);
	}
	if (!-e $path and !-d $path) {
		mkdir($path);
	}
}

# This functions accept an absolute path and will recursive
# remove all files and directories. Is the equivalent of
# 'rm -r' in any Linux system.
sub recursive_rm {
	my $path = shift;
	my $remove = sub {
		if (!-l and -d) {
			rmdir($File::Find::name)
		}
		else {
			unlink($File::Find::name);
		}
	};
	if (-e $path) {
		finddepth ( { wanted => $remove }, $path );
	}
}

# This function accept a filename and a folder, expands the filename
# with '*' and erase any files that match in the folder and its subdirectory.
sub multiple_rm {
	my $path = shift;
	my $filename = shift;
	my $no_perm = sub {
		grep { -w $_ and -r $_ and -x $_ } @_;
	};
	my $remove = sub {
		if ($File::Find::name =~ m/$filename.*/) {
			unlink($File::Find::name)
		}
	};
	if (-e $path) {
		find ( { wanted => $remove, preprocess => $no_perm }, $path );
	}
}

# This function will open and return the socket object used to send messages
# to the daemon. It will aslo set the socket identity.
sub open_test_socket {
	# Retrieving testname
	my $testname = shift;
	
	my $socket_protocol = shift;
	my $socket_path = shift;
	
	unless(defined($socket_protocol) and defined($socket_path)) {
		$socket_protocol = 'tcp://';
		$socket_path = '*:6650';
	}
	
	# Opening the socket to communicate with the server and setting is identity.
	print 'Opening the socket to communicate with the server... ';
	my $ctxt = ZeroMQ::Context->new();
	my $socket = $ctxt->socket(ZMQ_DEALER);
	my $setopt = $socket->setsockopt(ZMQ_IDENTITY, $testname);
	$socket->connect( "${socket_protocol}${socket_path}" );
	print "Done.\n";
	
	return ($socket, $ctxt);
}

# This function will terminate ZeroMQ context and close the socket
sub close_test_socket {
	my $socket = shift;
	my $ctxt = shift;

	$socket->close();
	$ctxt->term();
}

# This function will open the socket to send the output to the shell
sub open_shellout_socket {
	my $socket_protocol = shift;
	my $socket_path = shift;
	
	unless(defined($socket_protocol) and defined($socket_path)) {
		$socket_protocol = 'tcp://';
		$socket_path = '127.0.0.1:6651';
	}
	
	# Opening the socket to communicate with the server and setting is identity.
	print "Opening the socket to communicate with the shell on $socket_path... ";
	my $ctxt = ZeroMQ::Context->new();
	my $socket = $ctxt->socket(ZMQ_PUSH);
	$socket->connect( "${socket_protocol}${socket_path}" );
	print "Done.\n";
	
	return ($socket, $ctxt);
}

# This functions restores resolv.conf backup and erase iptabes rules added for dns test
sub restore_dns {
	my $resolv_temp = shift;
	
	# Restoring resolv.conf
	print 'Restoring resolv.conf backup... ';
	if ($resolv_temp) {
		system("sudo cp $resolv_temp /etc/resolv.conf");
	}
	else {
		system("sudo cp $RealBin/last_resolv.conf /etc/resolv.conf");
	}
	print "Done.\n";
	
	# Restarting iptables, it will load previously saved rules
	print 'Restoring iptables rules... ';
	system('sudo Tests/Common/iptables_rules.sh restore');
	print "Done.\n";
}

1;
