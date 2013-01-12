package HTTP::AppServer::Plugin;
# Abstract class for all HTTP::AppServer plugins.
# 2010 by Tom Kirchner

#use 5.010000;
use strict;
use warnings;

our $VERSION = '0.01';

# called by the server when the plugin is installed
# to determine which routes are handled by the plugin
sub init
{
	my ($class, 	# the class name
			$server, 	# the server instance (not started yet)
			%options	# config options for the plugin set by the user
			) = @_;
			
	return (
		# <route> => <coderef>
	);
}

1;
__END__
=head1 NAME

HTTP::AppServer::Plugin - Plugin base for HTTP::AppServer plugins.

=head1 SYNOPSIS

  package HTTP::AppServer::Plugin::MyPlugin;

  use 5.010000;
  use strict;
  use warnings;
  use HTTP::AppServer::Plugin;
  use base qw(HTTP::AppServer::Plugin);

  our $VERSION = '0.01';

  # called by the server when the plugin is installed
  # to determine which routes are handled by the plugin
  sub init
  {
    my ($class, $server, %options) = @_;

    # install properties in server
    $server->set('MyPluginVar1', 42);
    $server->set('MyPluginVar2', 21);

    return (
      # handle file (and directory) requests
      '^\/(.*)$' => sub {
        # ...
      },
      # ...
    );
  }
  
  1;

=head1 DESCRIPTION

This class is used as the base for all HTTP::AppServer plugins.

=head2 init()

The init() method is called when the plugin is installed in
an instance of HTTP::AppServer.

It receives as second parameter the server
instance itself (that can be used to extend the server, see below)
followed by arbitrary condiguration options set by the user.

The method returns a plain hash that contains URL mappings
(see HTTP::AppServer for examples of such mappings).

=head2 Extending the server

A plugin usually installs some handlers by returning
them from the init() method, see above.

Another possibility is to define properties and/or methods
in the server itself. These props/methods can then be accessed/used
inside user-defined (actually all) handlers. Applications like
easy database access etc. spring to mind.

To install a propert or method in the server use the set()
method:

  $server->set('prop', 'value');
  $server->set('meth', sub { ... });

The first parameter is the name of the property/method and
the second its value.

After calling set() the property can be accessed this way:

  my $value = $server->prop();

for read access or 

  $server->meth();

for method access.

=head1 SEE ALSO

HTTP::AppServer

=head1 AUTHOR

Tom Kirchner, E<lt>tom@tkirchner.comE<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2010 by Tom Kirchner

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself, either Perl version 5.10.0 or,
at your option, any later version of Perl 5 you may have available.


=cut
