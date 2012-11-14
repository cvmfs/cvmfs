use strict;
use warnings;

package HTTP::Server::Simple;
use FileHandle;
use Socket;
use Carp;

use vars qw($VERSION $bad_request_doc);
$VERSION = '0.45_02';

=head1 NAME

HTTP::Server::Simple - Lightweight HTTP server

=head1 SYNOPSIS

 use warnings;
 use strict;
 
 use HTTP::Server::Simple;
 
 my $server = HTTP::Server::Simple->new();
 $server->run();

However, normally you will sub-class the HTTP::Server::Simple::CGI
module (see L<HTTP::Server::Simple::CGI>);

 package Your::Web::Server;
 use base qw(HTTP::Server::Simple::CGI);
 
 sub handle_request {
     my ($self, $cgi) = @_;

     #... do something, print output to default
     # selected filehandle...

 }
 
 1;

=head1 DESCRIPTION

This is a simple standalone HTTP server. By default, it doesn't thread
or fork. It does, however, act as a simple frontend which can be used
to build a standalone web-based application or turn a CGI into one.

It is possible to use L<Net::Server> classes to create forking,
pre-forking, and other types of more complicated servers; see
L</net_server>.

By default, the server traps a few signals:

=over

=item HUP

When you C<kill -HUP> the server, it lets the current request finish being
processed, then uses the C<restart> method to re-exec itself. Please note that
in order to provide restart-on-SIGHUP, HTTP::Server::Simple sets a SIGHUP
handler during initialisation. If your request handling code forks you need to
make sure you reset this or unexpected things will happen if somebody sends a
HUP to all running processes spawned by your app (e.g. by "kill -HUP <script>")

=item PIPE

If the server detects a broken pipe while writing output to the client, 
it ignores the signal. Otherwise, a client closing the connection early 
could kill the server.

=back

=head1 EXAMPLE
 
 #!/usr/bin/perl
 {
 package MyWebServer;
 
 use HTTP::Server::Simple::CGI;
 use base qw(HTTP::Server::Simple::CGI);
 
 my %dispatch = (
     '/hello' => \&resp_hello,
     # ...
 );
 
 sub handle_request {
     my $self = shift;
     my $cgi  = shift;
   
     my $path = $cgi->path_info();
     my $handler = $dispatch{$path};
 
     if (ref($handler) eq "CODE") {
         print "HTTP/1.0 200 OK\r\n";
         $handler->($cgi);
         
     } else {
         print "HTTP/1.0 404 Not found\r\n";
         print $cgi->header,
               $cgi->start_html('Not found'),
               $cgi->h1('Not found'),
               $cgi->end_html;
     }
 }
 
 sub resp_hello {
     my $cgi  = shift;   # CGI.pm object
     return if !ref $cgi;
     
     my $who = $cgi->param('name');
     
     print $cgi->header,
           $cgi->start_html("Hello"),
           $cgi->h1("Hello $who!"),
           $cgi->end_html;
 }
 
 } 
 
 # start the server on port 8080
 my $pid = MyWebServer->new(8080)->background();
 print "Use 'kill $pid' to stop server.\n";

=head1 METHODS

=head2 HTTP::Server::Simple->new($port, $family)

API call to start a new server.  Does not actually start listening
until you call C<-E<gt>run()>.  If omitted, C<$port> defaults to 8080,
and C<$family> defaults to L<Socket::AF_INET>.
The alternative domain is L<Socket::AF_INET6>.

=cut

sub new {
    my ( $proto, $port, $family ) = @_;
    my $class = ref($proto) || $proto;

    if ( $class eq __PACKAGE__ ) {
        require HTTP::Server::Simple::CGI;
        return HTTP::Server::Simple::CGI->new( @_[ 1 .. $#_ ] );
    }

    my $self = {};
    bless( $self, $class );
    $self->port( $port || '8080' );
    $self->family( $family || AF_INET );

    return $self;
}


=head2 lookup_localhost

Looks up the local host's IP address, and returns it.  For most hosts,
this is C<127.0.0.1>, or possibly C<::1>.

=cut

sub lookup_localhost {
    my $self = shift;

    my $local_sockaddr = getsockname( $self->stdio_handle );
    my $local_family = sockaddr_family($local_sockaddr);
    my ( undef, $localiaddr ) =
        ($local_family == AF_INET6) ? sockaddr_in6($local_sockaddr)
                                    : sockaddr_in($local_sockaddr);

    $self->host( gethostbyaddr( $localiaddr, $local_family ) || "localhost");
    $self->{'local_addr'} = Socket::inet_ntop($local_family, $localiaddr)
                            || (($local_family == AF_INET6) ? "::1" : "127.0.0.1");
}


=head2 port [NUMBER]

Takes an optional port number for this server to listen on.

Returns this server's port. (Defaults to 8080)

=cut

sub port {
    my $self = shift;
    $self->{'port'} = shift if (@_);
    return ( $self->{'port'} );

}

=head2 family [NUMBER]

Takes an optional address family for this server to use.  Valid values
are Socket::AF_INET and Socket::AF_INET6.  All other values are silently
changed into Socket::AF_INET for backwards compatibility with previous
versions of the module.

Returns the address family of the present listening socket.  (Defaults to
Socket::AF_INET.)

=cut

sub family {
    my $self = shift;
    if (@_) {
        if ($_[0] == AF_INET || $_[0] == AF_INET6) {
            $self->{'family'} = shift;
        } else {
            $self->{'family'} = AF_INET;
        }
    }
    return ( $self->{'family'} );

}

=head2 host [address]

Takes an optional host address for this server to bind to.

Returns this server's bound address (if any).  Defaults to C<undef>
(bind to all interfaces).

=cut

sub host {
    my $self = shift;
    $self->{'host'} = shift if (@_);
    return ( $self->{'host'} );

}

=head2 background [ARGUMENTS]

Runs the server in the background, and returns the process ID of the
started process.  Any arguments will be passed through to L</run>.

=cut

sub background {
    my $self  = shift;
    my $child = fork;
    croak "Can't fork: $!" unless defined($child);
    return $child if $child;

    srand(); # after a fork, we need to reset the random seed
             # or we'll get the same numbers in both branches
    if ( $^O !~ /MSWin32/ ) {
        require POSIX;
        POSIX::setsid()
            or croak "Can't start a new session: $!";
    }
    $self->run(@_); # should never return
    exit;           # just to be sure
}

=head2 run [ARGUMENTS]

Run the server.  If all goes well, this won't ever return, but it will
start listening for C<HTTP> requests.  Any arguments passed to this
will be passed on to the underlying L<Net::Server> implementation, if
one is used (see L</net_server>).

=cut

my $server_class_id = 0;

use vars '$SERVER_SHOULD_RUN';
$SERVER_SHOULD_RUN = 1;

sub run {
    my $self   = shift;
    my $server = $self->net_server;

    local $SIG{CHLD} = 'IGNORE';    # reap child processes

    # $pkg is generated anew for each invocation to "run"
    # Just so we can use different net_server() implementations
    # in different runs.
    my $pkg = join '::', ref($self), "NetServer" . $server_class_id++;

    no strict 'refs';
    *{"$pkg\::process_request"} = $self->_process_request;

    if ($server) {
        require join( '/', split /::/, $server ) . '.pm';
        *{"$pkg\::ISA"} = [$server];

        # clear the environment before every request
        require HTTP::Server::Simple::CGI;
        *{"$pkg\::post_accept"} = sub {
            HTTP::Server::Simple::CGI::Environment->setup_environment;
            # $self->SUPER::post_accept uses the wrong super package
            $server->can('post_accept')->(@_);
        };
    }
    else {
        $self->setup_listener;
	$self->after_setup_listener();
        *{"$pkg\::run"} = $self->_default_run;
    }

    local $SIG{HUP} = sub { $SERVER_SHOULD_RUN = 0; };

    $pkg->run( port => $self->port, @_ );
}

=head2 net_server

User-overridable method. If you set it to a L<Net::Server> subclass,
that subclass is used for the C<run> method.  Otherwise, a minimal
implementation is used as default.

=cut

sub net_server {undef}

sub _default_run {
    my $self = shift;

    # Default "run" closure method for a stub, minimal Net::Server instance.
    return sub {
        my $pkg = shift;

        $self->print_banner;

        while ($SERVER_SHOULD_RUN) {
            local $SIG{PIPE} = 'IGNORE';    # If we don't ignore SIGPIPE, a
                 # client closing the connection before we
                 # finish sending will cause the server to exit
            while ( accept( my $remote = new FileHandle, HTTPDaemon ) ) {
                $self->stdio_handle($remote);
                $self->lookup_localhost() unless ($self->host);
                $self->accept_hook if $self->can("accept_hook");


                *STDIN  = $self->stdin_handle();
                *STDOUT = $self->stdout_handle();
                select STDOUT;   # required for HTTP::Server::Simple::Recorder
                                 # XXX TODO glasser: why?
                $pkg->process_request;
                close $remote;
            }
        }

        # Got here? Time to restart, due to SIGHUP
        $self->restart;
    };
}

=head2 restart

Restarts the server. Usually called by a HUP signal, not directly.

=cut

sub restart {
    my $self = shift;

    close HTTPDaemon;

    $SIG{CHLD} = 'DEFAULT';
    wait;

    ### if the standalone server was invoked with perl -I .. we will loose
    ### those include dirs upon re-exec. So add them to PERL5LIB, so they
    ### are available again for the exec'ed process --kane
    use Config;
    $ENV{PERL5LIB} .= join $Config{path_sep}, @INC;

    # Server simple
    # do the exec. if $0 is not executable, try running it with $^X.
    exec {$0}( ( ( -x $0 ) ? () : ($^X) ), $0, @ARGV );
}


sub _process_request {
    my $self = shift;

    # Create a callback closure that is invoked for each incoming request;
    # the $self above is bound into the closure.
    sub {

        $self->stdio_handle(*STDIN) unless $self->stdio_handle;

 # Default to unencoded, raw data out.
 # if you're sending utf8 and latin1 data mixed, you may need to override this
        binmode STDIN,  ':raw';
        binmode STDOUT, ':raw';

        # The ternary operator below is to protect against a crash caused by IE
        # Ported from Catalyst::Engine::HTTP (Originally by Jasper Krogh and Peter Edwards)
        # ( http://dev.catalyst.perl.org/changeset/5195, 5221 )
        
        my $remote_sockaddr = getpeername( $self->stdio_handle );
        my $family = sockaddr_family($remote_sockaddr);

        my ( $iport, $iaddr ) = $remote_sockaddr 
                                ? ( ($family == AF_INET6) ? sockaddr_in6($remote_sockaddr)
                                                          : sockaddr_in($remote_sockaddr) )
                                : (undef,undef);

        my $loopback = ($family == AF_INET6) ? "::1" : "127.0.0.1";
        my $peeraddr = $iaddr ? ( Socket::inet_ntop($family, $iaddr) || $loopback ) : $loopback;
        
        my ( $method, $request_uri, $proto ) = $self->parse_request;
        
        unless ($self->valid_http_method($method) ) {
            $self->bad_request;
            return;
        }

        $proto ||= "HTTP/0.9";

        my ( $file, $query_string )
            = ( $request_uri =~ /([^?]*)(?:\?(.*))?/s );    # split at ?

        $self->setup(
            method       => $method,
            protocol     => $proto,
            query_string => ( defined($query_string) ? $query_string : '' ),
            request_uri  => $request_uri,
            path         => $file,
            localname    => $self->host,
            localport    => $self->port,
            peername     => $peeraddr,
            peeraddr     => $peeraddr,
            peerport     => $iport,
        );

        # HTTP/0.9 didn't have any headers (I think)
        if ( $proto =~ m{HTTP/(\d(\.\d)?)$} and $1 >= 1 ) {

            my $headers = $self->parse_headers
                or do { $self->bad_request; return };

            $self->headers($headers);

        }

        $self->post_setup_hook if $self->can("post_setup_hook");

        $self->handler;
    }
}

=head2 stdio_handle [FILEHANDLE]

When called with an argument, sets the socket to the server to that arg.

Returns the socket to the server; you should only use this for actual socket-related
calls like C<getsockname>.  If all you want is to read or write to the socket,
you should use C<stdin_handle> and C<stdout_handle> to get the in and out filehandles
explicitly.

=cut

sub stdio_handle {
    my $self = shift;
    $self->{'_stdio_handle'} = shift if (@_);
    return $self->{'_stdio_handle'};
}

=head2 stdin_handle

Returns a filehandle used for input from the client.  By default,
returns whatever was set with C<stdio_handle>, but a subclass could do
something interesting here.

=cut

sub stdin_handle {
    my $self = shift;
    return $self->stdio_handle;
}

=head2 stdout_handle

Returns a filehandle used for output to the client.  By default, 
returns whatever was set with C<stdio_handle>, but a subclass
could do something interesting here.

=cut

sub stdout_handle {
    my $self = shift;
    return $self->stdio_handle;
}

=head1 IMPORTANT SUB-CLASS METHODS

A selection of these methods should be provided by sub-classes of this
module.

=head2 handler

This method is called after setup, with no parameters.  It should
print a valid, I<full> HTTP response to the default selected
filehandle.

=cut

sub handler {
    my ($self) = @_;
    if ( ref($self) ne __PACKAGE__ ) {
        croak "do not call " . ref($self) . "::SUPER->handler";
    }
    else {
        croak "handler called out of context";
    }
}

=head2 setup(name =E<gt> $value, ...)

This method is called with a name =E<gt> value list of various things
to do with the request.  This list is given below.

The default setup handler simply tries to call methods with the names
of keys of this list.

  ITEM/METHOD   Set to                Example
  -----------  ------------------    ------------------------
  method       Request Method        "GET", "POST", "HEAD"
  protocol     HTTP version          "HTTP/1.1"
  request_uri  Complete Request URI  "/foobar/baz?foo=bar"
  path         Path part of URI      "/foobar/baz"
  query_string Query String          undef, "foo=bar"
  port         Received Port         80, 8080
  peername     Remote name           "200.2.4.5", "foo.com"
  peeraddr     Remote address        "200.2.4.5", "::1"
  peerport     Remote port           42424
  localname    Local interface       "localhost", "myhost.com"

=cut

sub setup {
    my $self = shift;
    while ( my ( $item, $value ) = splice @_, 0, 2 ) {
        $self->$item($value) if $self->can($item);
    }
}

=head2 headers([Header =E<gt> $value, ...])

Receives HTTP headers and does something useful with them.  This is
called by the default C<setup()> method.

You have lots of options when it comes to how you receive headers.

You can, if you really want, define C<parse_headers()> and parse them
raw yourself.

Secondly, you can intercept them very slightly cooked via the
C<setup()> method, above.

Thirdly, you can leave the C<setup()> header as-is (or calling the
superclass C<setup()> for unknown request items).  Then you can define
C<headers()> in your sub-class and receive them all at once.

Finally, you can define handlers to receive individual HTTP headers.
This can be useful for very simple SOAP servers (to name a
crack-fueled standard that defines its own special HTTP headers). 

To do so, you'll want to define the C<header()> method in your subclass.
That method will be handed a (key,value) pair of the header name and the value.


=cut

sub headers {
    my $self    = shift;
    my $headers = shift;

    my $can_header = $self->can("header");
    return unless $can_header;
    while ( my ( $header, $value ) = splice @$headers, 0, 2 ) {
        $self->header( $header => $value );
    }
}

=head2 accept_hook

If defined by a sub-class, this method is called directly after an
accept happens.  An accept_hook to add SSL support might look like this:

    sub accept_hook {
        my $self = shift;
        my $fh   = $self->stdio_handle;

        $self->SUPER::accept_hook(@_);

        my $newfh =
        IO::Socket::SSL->start_SSL( $fh, 
            SSL_server    => 1,
            SSL_use_cert  => 1,
            SSL_cert_file => 'myserver.crt',
            SSL_key_file  => 'myserver.key',
        )
        or warn "problem setting up SSL socket: " . IO::Socket::SSL::errstr();

        $self->stdio_handle($newfh) if $newfh;
    }

=head2 post_setup_hook

If defined by a sub-class, this method is called after all setup has
finished, before the handler method.

=head2  print_banner

This routine prints a banner before the server request-handling loop
starts.

Methods below this point are probably not terribly useful to define
yourself in subclasses.

=cut

sub print_banner {
    my $self = shift;

    print( ref($self) 
            . ": You can connect to your server at "
            . "http://localhost:"
            . $self->port
            . "/\n" );

}

=head2 parse_request

Parse the HTTP request line.  Returns three values, the request
method, request URI and the protocol.

=cut

sub parse_request {
    my $self = shift;
    my $chunk;
    while ( sysread( STDIN, my $buff, 1 ) ) {
        last if $buff eq "\n";
        $chunk .= $buff;
    }
    defined($chunk) or return undef;
    $_ = $chunk;

    m/^(\w+)\s+(\S+)(?:\s+(\S+))?\r?$/;
    my $method   = $1 || '';
    my $uri      = $2 || '';
    my $protocol = $3 || '';

    # strip <scheme>://<host:port> out of HTTP/1.1 requests
    $uri =~ s{^\w+://[^/]+/}{/};

    return ( $method, $uri, $protocol );
}

=head2 parse_headers

Parses incoming HTTP headers from STDIN, and returns an arrayref of
C<(header =E<gt> value)> pairs.  See L</headers> for possibilities on
how to inspect headers.

=cut

sub parse_headers {
    my $self = shift;

    my @headers;

    my $chunk = '';
    while ( sysread( STDIN, my $buff, 1 ) ) {
        if ( $buff eq "\n" ) {
            $chunk =~ s/[\r\l\n\s]+$//;
            if ( $chunk =~ /^([^()<>\@,;:\\"\/\[\]?={} \t]+):\s*(.*)/i ) {
                push @headers, $1 => $2;
            }
            last if ( $chunk =~ /^$/ );
            $chunk = '';
        }
        else { $chunk .= $buff }
    }

    return ( \@headers );
}

=head2 setup_listener

This routine binds the server to a port and interface.

=cut

sub setup_listener {
    my $self = shift;

    my $tcp = getprotobyname('tcp');
    my $sockaddr;
    socket( HTTPDaemon, $self->{'family'}, SOCK_STREAM, $tcp )
        or croak "socket: $!";
    setsockopt( HTTPDaemon, SOL_SOCKET, SO_REUSEADDR, pack( "l", 1 ) )
        or warn "setsockopt: $!";

    if ($self->host) { # Explicit listening address
        my ($err, @res) = Socket::getaddrinfo($self->host, $self->port, { family => $self->{'family'}, socktype => SOCK_STREAM } );
        warn "$err!"
          if ($err);
        # we're binding only to the first returned address in the requested family.
        while ($a = shift(@res)) {
            # Be certain on the address family.
            # TODO Accept AF_UNSPEC, reject SITE-LOCAL
            next unless ($self->{'family'} == $a->{'family'});

            # Use the first plausible address.
            $sockaddr = $a->{'addr'};
            last;
        }
    }
    else { # Use the wildcard address
        $sockaddr = ($self->{'family'} == AF_INET6)
                        ? sockaddr_in6($self->port(), Socket::IN6ADDR_ANY)
                        : sockaddr_in($self->port(), INADDR_ANY);
    }

    bind( HTTPDaemon, $sockaddr)
        or croak "bind to @{[$self->host||'*']}:@{[$self->port]}: $!";
    listen( HTTPDaemon, SOMAXCONN ) or croak "listen: $!";
}


=head2 after_setup_listener

This method is called immediately after setup_listener. It's here just
for you to override.

=cut

sub after_setup_listener {
}

=head2 bad_request

This method should print a valid HTTP response that says that the
request was invalid.

=cut

$bad_request_doc = join "", <DATA>;

sub bad_request {
    my $self = shift;

    print "HTTP/1.0 400 Bad request\r\n";    # probably OK by now
    print "Content-Type: text/html\r\nContent-Length: ",
        length($bad_request_doc), "\r\n\r\n", $bad_request_doc;
}

=head2 valid_http_method($method)

Given a candidate HTTP method in $method, determine if it is valid.
Override if, for example, you'd like to do some WebDAV.  The default
implementation only accepts C<GET>, C<POST>, C<HEAD>, C<PUT>, C<PATCH>
and C<DELETE>.

=cut 

sub valid_http_method {
    my $self   = shift;
    my $method = shift or return 0;
    return $method =~ /^(?:GET|POST|HEAD|PUT|PATCH|DELETE)$/;
}

=head1 AUTHOR

Copyright (c) 2004-2008 Jesse Vincent, <jesse@bestpractical.com>.
All rights reserved.

Marcus Ramberg <drave@thefeed.no> contributed tests, cleanup, etc

Sam Vilain, <samv@cpan.org> contributed the CGI.pm split-out and
header/setup API.

Example section by almut on perlmonks, suggested by Mark Fuller.

=head1 BUGS

There certainly are some. Please report them via rt.cpan.org

=head1 LICENSE

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut

1;

__DATA__
<html>
  <head>
    <title>Bad Request</title>
  </head>
  <body>
    <h1>Bad Request</h1>

    <p>Your browser sent a request which this web server could not
      grok.</p>
  </body>
</html>
