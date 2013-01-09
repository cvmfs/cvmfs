package HTTP::AppServer;
# Simple CRUD server for JSON objects and plain files.
# 2010 by Tom Kirchner
# 
# routes:
#   POST /create = erzeugt neues JSON-Dokument, liefert UUId zurueck
#   GET  /read/<uuid> = liefert JSON-Dokument oder Fehler zurueck
#   POST /update/<uuid> = aendert JSON-Dokument (erzeugt neue Revision)
#   GET  /delete/<uuid> = loescht JSON-Dokument
#   GET  /file/<filename> = gibt Datei innerhalb Document-Root zurueck

#use 5.010000;
use strict;
use warnings;
use Data::Dumper;
use HTTP::AppServer::Base;

our $VERSION = '0.04';

sub new
{
	my ($class, @args) = @_;
	my $self = bless {}, $class;
	return $self->init(@args);
}

sub init
{
	my ($self, %opts) = @_;

	# server options defaults
	my %defaults = (StartBackground => 0, ServerPort => 3000, IPV6 => 0);
	
	# set options or use defaults
	map { $self->{$_} = (exists $opts{$_} ? $opts{$_} : $defaults{$_}) }
		keys %defaults;
	
	if ($self->{'IPV6'}) {
		$self->{'server'} = HTTP::AppServer::Base->new($self->{'ServerPort'}, Socket::AF_INET6);
	}
	else {
		$self->{'server'} = HTTP::AppServer::Base->new($self->{'ServerPort'});
	}

	return $self;
}

sub handle
{
	my ($self, %pairs) = @_;
	map { $self->{'server'}->handle($_, $pairs{$_}) } keys %pairs;
	return 1;
}

sub plugin
{
	my ($self, $name, %options) = @_;
	
	# load module
	eval('use HTTP::AppServer::Plugin::'.$name);
	die "Failed to load plugin '$name': $@\n" if $@;
	
	# call init() method of module to get the handlers of the plugin
	eval('$self->handle( HTTP::AppServer::Plugin::'.$name.'->init($self->{"server"}, %options))');
	die "Failed to install handlers for plugin '$name': $@\n" if $@;
}

sub start
{
	my ($self) = @_;
	
	$self->{'server'}->debug();

	# start the server on port
	if ($self->{'StartBackground'}) {
		my $pid = $self->{'server'}->background();
		print "Server started (http://localhost:$self->{'ServerPort'}) with PID $pid\n";
	} else {
		print "Server started (http://localhost:$self->{'ServerPort'})\n";
		my $pid = $self->{'server'}->run();
	}
}

1;
__END__
# Below is stub documentation for your module. You'd better edit it!

=head1 NAME

HTTP::AppServer - Pure-Perl web application server framework

=head1 SYNOPSIS

  use HTTP::AppServer;
  
  # create server instance at localhost:3000
  my $server = HTTP::AppServer->new( StartBackground => 0, ServerPort => 3000 );
 
  # alias URL
  $server->handle('^\/$', '/index.html');

  # load plugin for simple file retrieving from a document root
  $server->plugin('FileRetriever', DocRoot => '/path/to/docroot');
	
  # start server
  $server->start; 

=head1 DESCRIPTION

HTTP::AppServer was created because a stripped down Perl based
web server was needed that is extendable and really simple to use.

=head2 Creating server

To create a server instance, call the new() class method of
HTTP::AppServer, e.g.:

  my $server = HTTP::AppServer->new( StartBackground => 0, ServerPort => 3000 );

=head3 Constructor options

=head4 StartBackground => 1/0

Defines if the server should be startet in background mode or not.
Default is to NOT start in background.

=head4 ServerPort => I<Port>

Defines the local port the server listens to.
Default is to listen at port 3000.


=head2 Installing URL handlers

The main purpose of having a webserver is to make it able to
bind URLs to server side logic or content. To install such a 
binding in your instance of HTTP::AppServer, you can use
the handle() method.

The URL is given as a Perl regular expression, e.g.

  '^\/hi'

matches all URLs starting with '/hi' and anything coming after that.

You can either install a perl code reference that handles the URL:

  $server->handle('^\/hello$', sub {
    my ($server, $cgi) = @_;
    print "HTTP/1.0 200 Ok\r\n";
    print $cgi->header('text/html');
    print "Hello, visitor!";
  });

Or you can 

  $server->handle('^\/$', '/index.html');

In this example: whenever a URL matches a '/' at the beginning, the URL
is transformed into '/index.html' and HTTP::AppServer tries to find
a handler that handles that URL.

In case of a code reference handler (first example) the parameters
passed to the code reference are first the serve instance and
second the cgi object (instance of CGI). The second comes in handy
when creating HTTP headers and such.

Additional parameters are the groups defined in the regular expression
that matches the URL which brings us to the variable URL parts...

=head3 Variable URL parts

Suppose you want to match a URL that contains a variable ID of
some sort and another part that is variable. You could do it
this way:

  $server->handle('^\/(a|b|c)\/(\d+)', sub {
    my ($server, $cgi, $category, $id) = @_;
    # ...
  });

As you can see, the two groups in the regular expression are passed
as additional parameters to the code reference.

=head3 Multimatching vs. Singlematching

Each called handler (code reference) can tell HTTP::AppServer if
it should continue looking for another matching handler or not.
To make HTTP::AppServer continue searching after the handler, 
the handler has to return 0. If anything else is returned,
HTTP::AppServer tries to find another matching handler.

A handler is executed once at most (even if it matches multiple
times).

=head2 Using plugins

Use the plugin() method to load and configure a plugin, e.g.:

  $server->plugin('FileRetriever', DocRoot => '/path/to/docroot');

The first parameter is the name of the plugin, a class
in the HTTP::AppServer::Plugin:: namespace. After that configuration
options follow, see plugin documentation for details on that.

=head3 Plugin development

See HTTP::AppServer::Plugin

=head2 Handler precedence

When HTTP::AppServer tries to find a handler for an URL it goes
through the list of installed handlers (either by user or a plugin)
and the first that matches, is used.

As described above, each handler can tell if he wants the output
beeing delivered to the client OR continue find another handler.

=head2 Core plugins

These plugins are delivered with HTTP::AppServer itself:

=head3 HTTP::AppServer::Plugin::HTTPAuth

See HTTP::AppServer::Plugin::HTTPAuth for documentation.

=head3 HTTP::AppServer::Plugin::CustomError

See HTTP::AppServer::Plugin::CustomError for documentation.

=head3 HTTP::AppServer::Plugin::Database

See for HTTP::AppServer::Plugin::Database documentation.

=head3 HTTP::AppServer::Plugin::FileRetriever

See for HTTP::AppServer::Plugin::FileRetriever documentation.

=head3 HTTP::AppServer::Plugin::PlainHTML

See for HTTP::AppServer::Plugin::PlainHTML documentation.

=head1 SEE ALSO

HTTP::Server::Simple::CGI

=head1 AUTHOR

Tom Kirchner, E<lt>tom@tkirchner.comE<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2010 by Tom Kirchner

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself, either Perl version 5.10.0 or,
at your option, any later version of Perl 5 you may have available.


=cut
