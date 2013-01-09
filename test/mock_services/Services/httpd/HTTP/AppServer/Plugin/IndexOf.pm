package HTTP::AppServer::Plugin::IndexOf;
# Plugin for HTTP::AppServer that retrieves files from a document root.
# 2010 by Tom Kirchner

#use 5.010000;
use strict;
use warnings;
use IO::File;
use JSON;
use Path::Trim;
use HTTP::AppServer::Plugin;
use base qw(HTTP::AppServer::Plugin);
use File::Find;

our $VERSION = '0.01';

my $PathTrimmer = Path::Trim->new();

# called by the server when the plugin is installed
# to determine which routes are handled by the plugin
sub init
{
	my ($class, $server, %options) = @_;

	$PathTrimmer->set_directory_separator('/');

	return (
		# handle file (and directory) requests
		'^(.*\/)$' => \&_index_of,
	);
}

sub _index_of
{
	my ($server, $cgi, $filename) = @_;

	$filename = $server->docroot().$filename;
	$filename = $PathTrimmer->trim_path($filename);

	my $answer;
	my $mimetype = 'text/html';
	my $docroot_regex = quotemeta $server->docroot();
	if (-d $filename and !-e "$filename/index.html") {
		my @files = `ls $filename`;
		print "HTTP/1.0 200 Ok\r\n";
		print $cgi->header($mimetype);
		print '<html><head><title>Index Of'. $filename .'</title></head><body>';
		print '<h1>Index Of ' . $filename . '</h1>';
		foreach (@files) {
			chomp($_);
			if (-d "$filename"."$_") {
				print '<a href="' . $_ . '/">' . $_ . '</a></br>';
			}
			else {
				print '<a href="' . $_ . '">' . $_ . '</a></br>';
			}
		}
		print '</body></html>';
	}
	elsif (-e "$filename/index.html" and $filename =~ /^$docroot_regex/) {
		my $fh = IO::File->new('< '.$filename.'index.html');
		print "HTTP/1.0 200 Ok\r\n";
		print $cgi->header($mimetype);
		print join '', <$fh>;		
	}
	else {
		$server->errorpage(404);
	}
}

1;
__END__
=head1 NAME

HTTP::AppServer::Plugin::FileRetriever - Plugin for HTTP::AppServer that retrieves files from a document root.

=head1 SYNOPSIS

  use HTTP::AppServer;
  my $server = HTTP::AppServer->new();
  $server->plugin('FileRetriever', DocRoot => '/path/to/docroot');

=head1 DESCRIPTION

Plugin for HTTP::AppServer that retrieves files from a document root.

=head2 Plugin configuration

=head3 DocRoot => I<dir>

Defines the document root directory where the files are retrieved from.

=head2 Installed URL handlers

FileRetriever handles all URLs matching '^\/(.*)$'.
It stops the server from processing any other URL handlers after that
so it is best loaded as the last one.

=head2 Installed server properties

None.

=head2 Installed server methods

None.

=head2 Handled mimetypes

Currently FileRetriever can handle HTML, CSS, JS, PNG, JPG/JPEG
and GIF files. The mimetype is determined by inspecting the
filename extension.

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
