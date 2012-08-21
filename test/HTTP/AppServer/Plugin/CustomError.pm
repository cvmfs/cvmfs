package HTTP::AppServer::Plugin::CustomError;
# Plugin for HTTP::AppServer that defines custom error pages
# for all HTTP error codes.
# 2010 by Tom Kirchner

#use 5.010000;
use strict;
use warnings;
use HTTP::AppServer::Plugin;
use base qw(HTTP::AppServer::Plugin);

our $VERSION = '0.01';

# all the mimetypes of files handled by the plugin
my $HTTPErrors = {
	# <code> => [<info>, <html>],
	
	# success
	'200' => ['Ok',''],
	'201' => ['Created',''],
	'202' => ['Accepted',''],
	'203' => ['Partial Information',''],
	'204' => ['No Response',''],

	# error
	'400' => ['Bad Request',''],
	'401' => ['Unauthorized',''],
	'402' => ['Payment required',''],
	'403' => ['Forbidden',''],
	'404' => ['Not found',''],
	'500' => ['Internal Error',''],
	'501' => ['Not implemented',''],
	'502' => ['Service temporarily overloaded',''],
	'503' => ['Gateway timeout',''],

	# redirection
	'301' => ['Moved',''], # + Header: "URL: <url>"
	'302' => ['Found',''], # + ...
	'303' => ['Method',''], # + Header : "Method: <GET/POST> <url>"
	'304' => ['Not modified',''],
};

# called by the server when the plugin is installed
# to determine which routes are handled by the plugin
sub init
{
	my ($class, $server, %options) = @_;

	# install properties in server
	$server->set('errorpage', \&_errorpage);

	# the plugin does not install any URL handlers
	return ();
}

sub _errorpage
{
	my ($server, $code) = @_;
	$code = '500' if !defined $code || !exists $HTTPErrors->{$code};
		
	my ($info, $html) = @{$HTTPErrors->{$code}};
	
	# some nice default html
	$html = 
		'<html><head><title>'.$code.' '.$info.'</title><style type="text/css">'.
			'body { padding: 40pt; }'.
			'body, h1, h2, p { color: #333; font-family: Arial, sans-serif; margin: 0; }'.
			'div { width: 200px; background: #eee; padding: 2em; }'.
		'</style></head><body><div><h1>'.$code.'</h1><h2>'.$info.'</h2></div></body></html>'
			unless length $html;
	
	print "HTTP/1.0 ".$code." ".$info."\r\n";
	print "Content-type: text/html\r\n\r\n";
	print $html;
}

1;
__END__
=head1 NAME

HTTP::AppServer::Plugin::CustomError - Plugin for HTTP::AppServer that defines custom error pages for all HTTP error codes.

=head1 SYNOPSIS

  use HTTP::AppServer;
  my $server = HTTP::AppServer->new();
  $server->plugin('CustomError');

=head1 DESCRIPTION

Plugin for HTTP::AppServer that defines custom error pages for all HTTP error codes.
It can be used to easily create HTML pages for HTTP error codes.

=head2 Plugin configuration

Not possible for now.

=head2 Installed URL handlers

None.

=head2 Installed server properties

None.

=head2 Installed server methods

=head3 errorpage()

This method prints a HTML formatted page to STDOUT that
presents a certain HTTP error/status code to the user.

  $server->errorpage(500);

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
