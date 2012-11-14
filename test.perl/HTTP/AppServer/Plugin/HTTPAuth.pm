package HTTP::AppServer::Plugin::HTTPAuth;
# Plugin for HTTP::AppServer uses HTTP authentication to
# authenticate a client. The authentication works based
# on a certain handler regex.
# 2010 by Tom Kirchner

#use 5.010000;
use strict;
use warnings;
use MIME::Base64;
use HTTP::AppServer::Plugin;
use base qw(HTTP::AppServer::Plugin);

our $VERSION = '0.01';

my $Logins = {};
my $URLs = [];

# called by the server when the plugin is installed
# to determine which routes are handled by the plugin
sub init
{
	my ($class, $server, %options) = @_;

	$Logins = $options{'Logins'} if exists $options{'Logins'};
	$URLs   = $options{'URLs'}   if exists $options{'URLs'};
	
	if (exists $options{'LoginsFile'}) {
		my $filename = $options{'LoginsFile'};
		
		open(FH, '<'.$filename) 
			or print STDERR "HTTPAuth: Failed to open loginsfile '$filename': $! $@\n";
		while (my $line = <FH>) {
			chomp $line;
			my ($username, $password) = split /\:/, $line;
			$Logins->{$username} = $password
				if defined $username && length $username && defined $password;
		}
		close FH;
	}

	# hash that contains all active http login accounts
	$server->set('httpauth_logins', $Logins);

	# hash that contains all restricted area URLs
	$server->set('httpauth_urls', $URLs);

	# the plugin installs a match-all URL handler
	return (
		'^(.*)$' => \&_auth,
	);
}

sub _auth
{
	my ($server, $cgi, $url) = @_;
	
	if (scalar grep { ($url =~ /$_/) == 1 } @{$server->httpauth_urls()}) {
		my $auth = $cgi->http('Authorization');
	
		my $authorized = 0;
		if (defined $auth) {
			# try to authenticate user
			my ($prefix, $encoded) = split /\s/, $auth;
			my ($username, $password) = split /\:/, decode_base64($encoded);
			if (exists $server->httpauth_logins()->{$username} && 
				  $server->httpauth_logins()->{$username} eq $password) {		
				  
				my $session_id = time().sprintf('%.0f', rand(100000000));
				$authorized = 1;
			}
		}
		
		unless ($authorized) {
			# tell client to authorizate itself
			print
				"HTTP/1.0 401 Unauthorized\r\n".
				$cgi->header(
					-WWW_Authenticate => 'Basic realm="MySite"',
				);
			return 1;
		}
	}
	return 0;
}

1;
__END__
=head1 NAME

HTTP::AppServer::Plugin::HTTPAuth - Plugin for HTTP::AppServer uses HTTP authentication to authenticate a client. The authentication works based on a certain handler regex.

=head1 SYNOPSIS

  use HTTP::AppServer;
  my $server = HTTP::AppServer->new();
  $server->plugin('HTTPAuth', Logins => {guest => '', mrx => 'pass'}, URLs => ['^\/admin']);

=head1 DESCRIPTION

Plugin for HTTP::AppServer uses HTTP authentication to authenticate a client. 
The authentication works based on a certain handler regex.

=head2 Plugin configuration

=head3 Logins => I<hash>

A hash containing the available accounts that are allowed to
access the restricted URLs, e.g.:

  ..., Logins => {guest => '', mrx => 'pass'}, ...

=head3 URLs => I<array>

This is a list of restricted URLs. When an URL is accessed that matches
any regular expression in this list, a HTTP authorization is preformed.
If the authorization fails an error page is returned. In all other
cases (URL not restricted or authorization was successful) other
handlers are allowed to process the URL.

=head3 LoginsFile => I<filename>

This can be supplied additionally to the Logins option.
The account information is then read from a file that has the
format of normal .htpasswd files, e.g.

  username1:password
  username2:password
  ...

while I<password> is a Base64 encoded password.

=head2 Installed URL handlers

HTTPAuth installs a binding to the URL '^(.*)$', which means
it matches everything. It allows for further processing after
that if the URL is not restricted (is not contained in the URLs
option when loading the plugin).

=head2 Installed server properties

=head3 httpauth_logins

This is a reference to the Logins that are configured when loading the plugin.

=head3 httpauth_urls

This is a reference to the URLs that are configured when loading the plugin.

=head2 Installed server methods

None.

=head1 SEE ALSO

HTTP::AppServer, HTTP::AppServer::Plugin

=head1 AUTHOR

Tom Kirchner, E<lt>tom@tkirchner.comE<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2010 by Tom Kirchner

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself, either Perl version 5.10.0 or,
at your option, any later version of Perl 5 you may have available.


=cut
