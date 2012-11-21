package Functions::ServerSocket;

################################################
# This package will contain the functions to send and receive message
# on the server side
################################################

use strict;
use warnings;
use ZeroMQ qw/:all/;

# Next lines are needed to export subroutines to the main package
use base 'Exporter';
use vars qw/ @EXPORT_OK /;
@EXPORT_OK = qw(start_socket receive_msg send_msg end_msg close_socket term_ctxt send_ip);

# Modify this variables to change the path to the socket
my $socket_path = '*:6650';
my $socket_protocol = 'tcp://';

# Variables shared among all functions
my $ctxt;
my $socket;
my $sender = undef;

# Starting the daemon socket
sub start_socket {
	$ctxt = ZeroMQ::Raw::zmq_init(5) || die "Couldn't initialise ZeroMQ context.\n";
	$socket = ZeroMQ::Raw::zmq_socket($ctxt, ZMQ_ROUTER) || die "Couldn't create socket.\n";
	ZeroMQ::Raw::zmq_setsockopt($socket, ZMQ_LINGER, 0);
	
	my $rc = ZeroMQ::Raw::zmq_bind( $socket, "${socket_protocol}${socket_path}" );
	
	if ($socket_protocol =~ m/^ipc:\/\/$/) {
		system("chmod 777 $socket_path");
	}
	
	if ($rc != 0) {
		return 0;
	}
	else {
		return 1;
	}
}

# Receiving a message
sub receive_msg {
	my $msg = ZeroMQ::Raw::zmq_recv($socket);
	my $line = ZeroMQ::Raw::zmq_msg_data($msg);
	unless ($line) {
		print "Couldn't retrieve pointer to data: $!\n";
	}

	my @check = split /[[:blank:]]/, $line;
	if (scalar(@check) == 1 and $check[0] eq uc($check[0])){
		$sender = $line;
		return "Receiving connection from $sender.\n";
	}
	else {
		return $line;
	}
}

# Select the recipient socket
sub select_recipient {
	ZeroMQ::Raw::zmq_send($socket, $sender, ZMQ_SNDMORE);
	print "Sending message to $sender:\n";

	# Undefining $sender to avoid multiple use of this function for the same message
	$sender = undef;
}

# Send a message
sub send_msg {
	# Retrieve message
	my $msg = shift;

	# If $sender is not undef, sending the first part of output with socket identity
	if (defined($sender)){
		select_recipient();
	}
	
	ZeroMQ::Raw::zmq_send($socket, $msg, ZMQ_SNDMORE);
	chomp($msg);
	print "MSG: \"$msg\" sent.\n";
}

# End message. Use this functions to tell the shell that no more output will arrive.
sub end_msg {
	# If $sender is not undef, sending the first part of output with socket identity
	if (defined($sender)){
		select_recipient();
	}
	
	ZeroMQ::Raw::zmq_send($socket, "END\n");
	print "MSG: \"END\" sent.\n";
}

# Close the socket
sub close_socket {
	my $socket_closed = ZeroMQ::Raw::zmq_close($socket);
	if ($socket_closed == -1) {
		die "Unable to close the socket: $!.\n";
	}
}

# Term ZeroMQ context
sub term_ctxt {
	my $ctxt_closed = ZeroMQ::Raw::zmq_term($ctxt);
	if ($ctxt_closed == -1) {
		die "Unable to term ZeroMQ context: $!.\n";
	}
}

sub send_ip {
	my $socket_path = shift;
	my $socket_protocol = shift;
	my $interface = shift;

	# Modify this variables to change the default path to the socket
	unless (defined($socket_path)) {
		$socket_path = '127.0.0.1:6651';
	}
	unless (defined($socket_protocol)) {
		$socket_protocol = 'tcp://';
	}
	
	my $ctxt = ZeroMQ::Raw::zmq_init(5) || die "Couldn't initialise ZeroMQ context.\n";
	my $socket = ZeroMQ::Raw::zmq_socket($ctxt, ZMQ_PUSH) || die "Couldn't create socket.\n";
	my $setopt = ZeroMQ::Raw::zmq_setsockopt($socket, ZMQ_IDENTITY, 'DAEMON');

	my $rc = ZeroMQ::Raw::zmq_connect( $socket, "${socket_protocol}${socket_path}" );
	
	my $daemon_ip = Functions::Testd::get_interface_address($interface);
	
	print 'Sending IP to the shell... ';
	ZeroMQ::Raw::zmq_send($socket, "$daemon_ip:6650");
	print "Done.\n";
	
	ZeroMQ::Raw::zmq_close($socket);
	ZeroMQ::Raw::zmq_term($ctxt);
}

1;
