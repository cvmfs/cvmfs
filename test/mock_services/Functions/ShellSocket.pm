package Functions::ShellSocket;

#####################################
# Here will be stored all the functions related to socket on 
# the shell side.
#####################################

use strict;
use warnings;
use ZeroMQ q/:all/;

# Next lines are needed to export subroutines to the main package
use base 'Exporter';
use vars qw/ @EXPORT_OK /;
@EXPORT_OK = qw(connect_shell_socket receive_shell_msg send_shell_msg close_shell_socket term_shell_ctxt bind_shell_socket);

# Starting the shell socket
sub connect_shell_socket {
	my $socket_path = shift;
	my $socket_protocol = shift;
	my $identity = shift;

	# Modify this variables to change the default path to the socket
	unless (defined($socket_path)) {
		$socket_path = '127.0.0.1:6650';
	}
	unless (defined($socket_protocol)) {
		$socket_protocol = 'tcp://';
	}
	
	unless (defined($identity)) {
		$identity = 'SHELL';
	}
	
	my $ctxt = ZeroMQ::Raw::zmq_init(5) || die "Couldn't initialise ZeroMQ context.\n";
	my $socket = ZeroMQ::Raw::zmq_socket($ctxt, ZMQ_DEALER) || die "Couldn't create socket.\n";
	my $setid = ZeroMQ::Raw::zmq_setsockopt($socket, ZMQ_IDENTITY, $identity);
	my $setling = ZeroMQ::Raw::zmq_setsockopt($socket, ZMQ_LINGER, 0);

	my $rc = ZeroMQ::Raw::zmq_connect( $socket, "${socket_protocol}${socket_path}" );
	
	return ($socket, $ctxt);
}

# Receiving a message
sub receive_shell_msg {
	my $socket = shift;
	my $noblock = shift;
	
	my $msg = undef;
	
	unless (defined($noblock)) {
		$msg = ZeroMQ::Raw::zmq_recv($socket);
	}
	else {
		$msg = ZeroMQ::Raw::zmq_recv($socket, ZMQ_NOBLOCK);
	}
	
	my $line = undef;
	
	if ($msg) {
		$line = ZeroMQ::Raw::zmq_msg_data($msg) || die "Couldn't retrieve pointer to data: $!\n";
	}
	
	return $line;
}

# Send a message
sub send_shell_msg {
	my $socket = shift;
	
	# Retrieve message
	my $msg = shift;
	
	ZeroMQ::Raw::zmq_send($socket, $msg);
}

# Close the socket
sub close_shell_socket {
	my $socket = shift;
	if (defined($socket)) {
		ZeroMQ::Raw::zmq_close($socket);
		$socket = undef;
	}
	return $socket;
}

# Term ZeroMQ context
sub term_shell_ctxt {
	my $ctxt = shift;
	if (defined($ctxt)) {
		ZeroMQ::Raw::zmq_term($ctxt);
		$ctxt = undef;
	}
	return $ctxt;
}

# Next function are intended to receive test output through a socket
sub bind_shell_socket {
	my $socket_protocol = shift;
	my $socket_path = shift;
	
	unless (defined($socket_protocol) and defined($socket_path)) {
		$socket_protocol = 'tcp://';
		$socket_path = '*:6651';	
	}
	
	my $ctxt = ZeroMQ::Raw::zmq_init(5) || die "Couldn't initialise ZeroMQ context.\n";
	my $socket = ZeroMQ::Raw::zmq_socket($ctxt, ZMQ_PULL) || die "Couldn't create socket.\n";

	my $rc = ZeroMQ::Raw::zmq_bind( $socket, "${socket_protocol}${socket_path}" );
	
	return ($socket, $ctxt);
}

1;
